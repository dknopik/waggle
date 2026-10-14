// Copyright 2026 Sigma Prime Pty Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, atomic::AtomicUsize},
    task::Poll,
};

use async_channel::Sender;
use libp2p::{
    PeerId,
    swarm::{ConnectionClosed, ConnectionId, FromSwarm, NetworkBehaviour, NotifyHandler, ToSwarm},
};

use crate::{
    config::Config,
    handler::{self, HandlerIn, Received},
    protocol::proto::ObjectPieces,
    substream::{CloseSignal, DispatchHub},
};

/// Event emitted by the waggle behaviour.
#[derive(Debug)]
pub enum Event {
    /// A piece was received from a remote peer.
    Received {
        /// The topic this piece belongs to.
        topic_id: Vec<u8>,
        /// The peer that sent this piece.
        peer_id: PeerId,
        /// The received pieces.
        received: Received,
    },
    /// A remote peer subscribed to a topic.
    Subscribed {
        /// The remote peer that subscribed.
        peer_id: PeerId,
        /// The topic it subscribed to.
        topic_id: Vec<u8>,
    },
    /// A remote peer unsubscribed from a topic.
    Unsubscribed {
        /// The remote peer that unsubscribed.
        peer_id: PeerId,
        /// The topic it unsubscribed from.
        topic_id: Vec<u8>,
    },
}

/// Network behaviour that handles the waggle protocol.
pub struct Behaviour {
    /// Behaviour configuration: queue sizes, limits, and whitelist.
    config: Config,
    /// Outbound events to the swarm, drained in `poll`.
    pending_events: VecDeque<ToSwarm<Event, HandlerIn>>,
    /// Established connections.
    connections: HashMap<ConnectionId, Connection>,
    /// The topics the local node is currently subscribed to.
    subscribed_topics: HashMap<Vec<u8>, CloseSignal>,
}

/// Per-connection outbound state for one established connection.
///
/// Holds the connection's outbound hub and the topics the remote peer
/// subscribed to on this connection.
#[derive(Debug)]
struct Connection {
    /// The remote peer behind this connection.
    peer_id: PeerId,
    /// The per-connection outbound hub, with its queue budget and senders.
    hub: DispatchHub,
}

impl Connection {
    /// Subscribes the remote connection to topic.
    fn subscribe(&mut self, topic_id: Vec<u8>, sender: Sender<ObjectPieces>) {
        self.hub.senders.insert(topic_id.clone(), sender);
    }

    fn unsubscribe(&mut self, topic_id: &[u8]) {
        self.hub.senders.remove(topic_id);
    }
}

impl Behaviour {
    /// Creates a new waggle [`Behaviour`] instance with the given [`Config`].
    pub fn new(config: Config) -> Self {
        Self {
            config,
            pending_events: VecDeque::new(),
            connections: HashMap::new(),
            subscribed_topics: HashMap::new(),
        }
    }

    /// Subscribes to a topic.
    ///
    /// Returns `false` if the topic was already subscribed.
    pub fn subscribe(&mut self, topic_id: Vec<u8>) -> bool {
        if self.subscribed_topics.contains_key(&topic_id) {
            return false;
        }

        let close_signal = CloseSignal::new();
        // Hand every connection a receiver clone of the topic's close signal.
        for (connection_id, connection) in &self.connections {
            self.pending_events.push_back(ToSwarm::NotifyHandler {
                peer_id: connection.peer_id,
                handler: NotifyHandler::One(*connection_id),
                event: HandlerIn::Subscribe {
                    topic_id: topic_id.clone(),
                    close_signal: close_signal.receiver(),
                },
            });
        }

        self.subscribed_topics.insert(topic_id, close_signal);
        true
    }

    /// Unsubscribes from a topic.
    ///
    /// Returns `true` if the topic was subscribed.
    pub fn unsubscribe(&mut self, topic_id: Vec<u8>) -> bool {
        // Dropping the signal closes its channel; every connection's stream
        // wakes and closes.
        let Some(subscription) = self.subscribed_topics.remove(&topic_id) else {
            return false;
        };
        drop(subscription);

        true
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = handler::Handler;

    type ToSwarm = Event;

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer_id: PeerId,
        _local_addr: &libp2p::Multiaddr,
        _remote_addr: &libp2p::Multiaddr,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        let used = Arc::new(AtomicUsize::new(0));
        let hub = DispatchHub::new(self.config.max_connection_queue_limit(), Arc::clone(&used));
        self.connections
            .insert(connection_id, Connection { peer_id, hub });

        // Subscribe this connection to all the topics we are subscribed to.
        for (topic_id, close_signal) in &self.subscribed_topics {
            self.pending_events.push_back(ToSwarm::NotifyHandler {
                peer_id,
                handler: NotifyHandler::One(connection_id),
                event: HandlerIn::Subscribe {
                    topic_id: topic_id.clone(),
                    close_signal: close_signal.receiver(),
                },
            });
        }

        Ok(handler::Handler::new(used, self.config.clone()))
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer_id: PeerId,
        _addr: &libp2p::Multiaddr,
        _role_override: libp2p::core::Endpoint,
        _port_use: libp2p::core::transport::PortUse,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        let used = Arc::new(AtomicUsize::new(0));
        let hub = DispatchHub::new(self.config.max_connection_queue_limit(), Arc::clone(&used));
        self.connections
            .insert(connection_id, Connection { peer_id, hub });

        // Subscribe this connection to all the topics we are subscribed to.
        for (topic_id, close_signal) in &self.subscribed_topics {
            self.pending_events.push_back(ToSwarm::NotifyHandler {
                peer_id,
                handler: NotifyHandler::One(connection_id),
                event: HandlerIn::Subscribe {
                    topic_id: topic_id.clone(),
                    close_signal: close_signal.receiver(),
                },
            });
        }

        Ok(handler::Handler::new(used, self.config.clone()))
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        if let FromSwarm::ConnectionClosed(ConnectionClosed { connection_id, .. }) = event {
            self.connections.remove(&connection_id);
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: libp2p::PeerId,
        connection_id: libp2p::swarm::ConnectionId,
        event: libp2p::swarm::THandlerOutEvent<Self>,
    ) {
        match event {
            handler::Event::Subscribed { topic_id, sender } => {
                let Some(connection) = self.connections.get_mut(&connection_id) else {
                    tracing::error!("Subscription from an Unknown connection");
                    return;
                };

                connection.subscribe(topic_id.clone(), sender);
                self.pending_events
                    .push_back(ToSwarm::GenerateEvent(Event::Subscribed {
                        peer_id,
                        topic_id,
                    }))
            }
            handler::Event::Received(received) => {
                // Ignore pieces for a topic the local node is not subscribed to.
                if !self.subscribed_topics.contains_key(&received.topic_id) {
                    tracing::debug!(peer_id=%peer_id, "Received piece for an unsubscribed topic");
                    return;
                }

                self.pending_events
                    .push_back(ToSwarm::GenerateEvent(Event::Received {
                        topic_id: received.topic_id.clone(),
                        peer_id,
                        received,
                    }));
            }

            handler::Event::Unsubscribed { topic_id } => {
                let Some(connection) = self.connections.get_mut(&connection_id) else {
                    tracing::error!("Unsubscription from an Unknown connection");
                    return;
                };

                connection.unsubscribe(&topic_id);
                self.pending_events
                    .push_back(ToSwarm::GenerateEvent(Event::Unsubscribed {
                        peer_id,
                        topic_id,
                    }));
            }
        }
    }

    fn poll(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, libp2p::swarm::THandlerInEvent<Self>>> {
        if let Some(event) = self.pending_events.pop_front() {
            return Poll::Ready(event);
        }

        std::task::Poll::Pending
    }
}
