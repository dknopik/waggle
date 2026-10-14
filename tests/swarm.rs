//! End-to-end tests through the public API: real swarms connected over the
//! memory transport, exchanging pieces with the application.

use std::time::Duration;

use async_channel::{Receiver, Sender};
use futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId, Swarm, SwarmBuilder,
    core::{Transport, transport::MemoryTransport, upgrade::Version},
    plaintext,
    swarm::SwarmEvent,
    yamux,
};
use waggle::{
    behaviour::{Behaviour, Event},
    config::Config,
    handler::{ObjectPieces, PieceSender, SendError},
};

const TOPIC: &[u8] = b"topic";
const TIMEOUT: Duration = Duration::from_secs(5);

/// Instructions for a node's swarm task.
enum Command {
    Dial(Multiaddr),
}

/// A swarm subscribed to [`TOPIC`] and listening on a memory address, driven
/// by its own task. The test talks to it through channels.
struct Node {
    peer_id: PeerId,
    addr: Multiaddr,
    commands: Sender<Command>,
    events: Receiver<Event>,
}

impl Node {
    fn spawn(config: Config) -> Self {
        let mut swarm: Swarm<Behaviour> = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_other_transport(|key| {
                MemoryTransport::default()
                    .upgrade(Version::V1)
                    .authenticate(plaintext::Config::new(key))
                    .multiplex(yamux::Config::default())
            })
            .unwrap()
            .with_behaviour(|_| Behaviour::new(config))
            .unwrap()
            .with_swarm_config(|config| {
                config.with_idle_connection_timeout(Duration::from_secs(60))
            })
            .build();
        let peer_id = *swarm.local_peer_id();
        let addr: Multiaddr = format!("/memory/{}", rand::random::<u64>())
            .parse()
            .unwrap();
        swarm.listen_on(addr.clone()).unwrap();
        swarm.behaviour_mut().subscribe(TOPIC.to_vec());

        let (commands, command_rx) = async_channel::unbounded();
        let (event_tx, events) = async_channel::unbounded();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    command = command_rx.recv() => match command {
                        Ok(Command::Dial(addr)) => swarm.dial(addr).unwrap(),
                        Err(_) => return,
                    },
                    event = swarm.select_next_some() => {
                        if let SwarmEvent::Behaviour(event) = event {
                            let _ = event_tx.try_send(event);
                        }
                    }
                }
            }
        });

        Self {
            peer_id,
            addr,
            commands,
            events,
        }
    }

    fn command(&self, command: Command) {
        self.commands.try_send(command).unwrap();
    }

    /// Waits for the next behaviour event.
    async fn next(&self) -> Event {
        tokio::time::timeout(TIMEOUT, self.events.recv())
            .await
            .expect("timed out waiting for an event")
            .expect("node stopped")
    }

    /// Asserts that no behaviour event arrives for a while.
    async fn no_event(&self) {
        if let Ok(event) =
            tokio::time::timeout(Duration::from_millis(300), self.events.recv()).await
        {
            panic!("unexpected event {event:?}");
        }
    }

    /// Waits for `peer`'s subscription to [`TOPIC`], and returns the sender to
    /// it.
    async fn subscribed(&self, peer: PeerId) -> PieceSender {
        match self.next().await {
            Event::Subscribed {
                peer_id,
                topic_id,
                sender,
                ..
            } => {
                assert_eq!(peer_id, peer);
                assert_eq!(topic_id, TOPIC);
                sender
            }
            event => panic!("unexpected event {event:?}"),
        }
    }

    /// Waits for the end of `peer`'s subscription to [`TOPIC`].
    async fn unsubscribed(&self, peer: PeerId) {
        match self.next().await {
            Event::Unsubscribed {
                peer_id, topic_id, ..
            } => {
                assert_eq!(peer_id, peer);
                assert_eq!(topic_id, TOPIC);
            }
            event => panic!("unexpected event {event:?}"),
        }
    }

    /// Waits for the next pieces from `peer`.
    async fn recv(&self, peer: PeerId) -> ObjectPieces {
        match self.next().await {
            Event::Received {
                peer_id,
                topic_id,
                pieces,
                ..
            } => {
                assert_eq!(peer_id, peer);
                assert_eq!(topic_id, TOPIC);
                pieces
            }
            event => panic!("unexpected event {event:?}"),
        }
    }
}

/// Connects `a` to `b` and returns each side's sender to the other.
async fn connect(a: &Node, b: &Node) -> (PieceSender, PieceSender) {
    a.command(Command::Dial(b.addr.clone()));
    let a_b = a.subscribed(b.peer_id).await;
    let b_a = b.subscribed(a.peer_id).await;
    (a_b, b_a)
}

fn pieces(i: u8) -> ObjectPieces {
    ObjectPieces {
        object_id: vec![i],
        metadata: Some(vec![i, i]),
        pieces: Some(vec![i; 3]),
    }
}

#[tokio::test]
async fn pieces_flow_both_ways() {
    let a = Node::spawn(Config::default());
    let b = Node::spawn(Config::default());
    let (a_b, b_a) = connect(&a, &b).await;

    a_b.try_send(pieces(1)).unwrap();
    assert_eq!(b.recv(a.peer_id).await, pieces(1));
    b_a.try_send(pieces(2)).unwrap();
    assert_eq!(a.recv(b.peer_id).await, pieces(2));

    a.no_event().await;
    b.no_event().await;
}

#[tokio::test]
async fn connection_queue_limit() {
    let a = Node::spawn(Config::default().set_max_connection_queue_limit(3));
    let b = Node::spawn(Config::default());
    let (a_b, _b_a) = connect(&a, &b).await;

    // Nothing runs between these sends on the current-thread runtime.
    for i in 0..3 {
        a_b.try_send(pieces(i)).unwrap();
    }
    assert!(matches!(a_b.try_send(pieces(3)), Err(SendError::Full)));
    for i in 0..3 {
        assert_eq!(b.recv(a.peer_id).await, pieces(i));
    }

    // The writer released the budget as it drained the queue.
    a_b.try_send(pieces(4)).unwrap();
    assert_eq!(b.recv(a.peer_id).await, pieces(4));
}

#[tokio::test]
async fn dropping_sender_ends_subscription() {
    let a = Node::spawn(Config::default());
    let b = Node::spawn(Config::default());
    let (a_b, _b_a) = connect(&a, &b).await;

    // The pieces queued before the sender was dropped are still sent.
    a_b.try_send(pieces(0)).unwrap();
    drop(a_b);
    a.unsubscribed(b.peer_id).await;
    assert_eq!(b.recv(a.peer_id).await, pieces(0));
}
