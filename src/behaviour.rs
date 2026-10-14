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
    task::Poll,
};

use libp2p::{
    PeerId,
    swarm::{
        ConnectionClosed, ConnectionId, FromSwarm, NetworkBehaviour, NotifyHandler, ToSwarm,
        behaviour::ConnectionEstablished,
    },
};

use crate::{
    config::Config,
    handler::{self, HandlerIn, ObjectPieces, PieceSender},
    substream::CloseSignal,
};

/// Event emitted by the waggle behaviour.
#[derive(Debug)]
pub enum Event {
    /// A remote peer sent pieces on a topic we subscribed to.
    Received {
        /// The peer that sent the pieces.
        peer_id: PeerId,
        /// The connection the pieces arrived on.
        connection_id: ConnectionId,
        /// The topic the pieces belong to.
        topic_id: Vec<u8>,
        /// The received pieces.
        pieces: ObjectPieces,
    },
    /// A remote peer subscribed to a topic.
    ///
    /// Pieces queued on `sender` are written to the peer's subscription
    /// stream on this connection.
    Subscribed {
        /// The remote peer that subscribed.
        peer_id: PeerId,
        /// The connection the peer subscribed on.
        connection_id: ConnectionId,
        /// The topic it subscribed to.
        topic_id: Vec<u8>,
        /// Queues pieces for the peer on this subscription.
        sender: PieceSender,
    },
    /// A remote peer's subscription to a topic ended.
    ///
    /// The peer unsubscribed, the stream failed, the connection closed, or
    /// every clone of the subscription's `sender` was dropped. The `sender`
    /// no longer accepts pieces.
    Unsubscribed {
        /// The remote peer that unsubscribed.
        peer_id: PeerId,
        /// The connection the peer was subscribed on.
        connection_id: ConnectionId,
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
    /// The remote peer behind each established connection.
    connections: HashMap<ConnectionId, PeerId>,
    /// The topics the local node is currently subscribed to.
    subscribed_topics: HashMap<Vec<u8>, CloseSignal>,
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
        for (connection_id, peer_id) in &self.connections {
            self.pending_events.push_back(ToSwarm::NotifyHandler {
                peer_id: *peer_id,
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

    /// Tracks a newly established connection.
    ///
    /// Subscribes the connection to all the topics we are subscribed to.
    fn on_connection_established(&mut self, connection_id: ConnectionId, peer_id: PeerId) {
        self.connections.insert(connection_id, peer_id);

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
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = handler::Handler;

    type ToSwarm = Event;

    fn handle_established_inbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _peer_id: PeerId,
        _local_addr: &libp2p::Multiaddr,
        _remote_addr: &libp2p::Multiaddr,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        Ok(handler::Handler::new(self.config.clone()))
    }

    fn handle_established_outbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _peer_id: PeerId,
        _addr: &libp2p::Multiaddr,
        _role_override: libp2p::core::Endpoint,
        _port_use: libp2p::core::transport::PortUse,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        Ok(handler::Handler::new(self.config.clone()))
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        match event {
            // Track connections here rather than when creating the handler:
            // another behaviour may still deny the connection, which the
            // swarm then reports as a dial or listen failure instead.
            FromSwarm::ConnectionEstablished(ConnectionEstablished {
                peer_id,
                connection_id,
                ..
            }) => self.on_connection_established(connection_id, peer_id),
            FromSwarm::ConnectionClosed(ConnectionClosed { connection_id, .. }) => {
                self.connections.remove(&connection_id);
            }
            _ => {}
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: libp2p::PeerId,
        connection_id: libp2p::swarm::ConnectionId,
        event: libp2p::swarm::THandlerOutEvent<Self>,
    ) {
        let event = match event {
            handler::Event::Subscribed { topic_id, sender } => Event::Subscribed {
                peer_id,
                connection_id,
                topic_id,
                sender,
            },
            handler::Event::Received { topic_id, pieces } => {
                // Ignore pieces for a topic the local node is not subscribed to.
                if !self.subscribed_topics.contains_key(&topic_id) {
                    tracing::debug!(peer_id=%peer_id, "Received piece for an unsubscribed topic");
                    return;
                }
                Event::Received {
                    peer_id,
                    connection_id,
                    topic_id,
                    pieces,
                }
            }
            handler::Event::Unsubscribed { topic_id } => Event::Unsubscribed {
                peer_id,
                connection_id,
                topic_id,
            },
        };
        self.pending_events.push_back(ToSwarm::GenerateEvent(event));
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

#[cfg(test)]
mod tests {
    use std::task::{Context, Waker};

    use libp2p::Multiaddr;

    use super::*;

    const TOPIC: &[u8] = b"topic";

    fn poll(behaviour: &mut Behaviour) -> Poll<ToSwarm<Event, HandlerIn>> {
        behaviour.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn denied_connection_is_not_tracked() {
        let mut behaviour = Behaviour::new(Config::default());
        // Another behaviour denies the connection after this one created its
        // handler, so the swarm never reports it as established.
        let addr = Multiaddr::empty();
        behaviour
            .handle_established_inbound_connection(
                ConnectionId::new_unchecked(0),
                PeerId::random(),
                &addr,
                &addr,
            )
            .unwrap();

        // No subscription is sent to the denied connection.
        behaviour.subscribe(TOPIC.to_vec());
        assert!(poll(&mut behaviour).is_pending());
    }
}
