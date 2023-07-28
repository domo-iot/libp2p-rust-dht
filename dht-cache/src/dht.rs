//! DHT Abstraction
//!

use crate::domolibp2p::{DomoBehaviour, OutEvent};
use futures::prelude::*;
use libp2p::{gossipsub::IdentTopic as Topic, swarm::SwarmEvent, Swarm};
use libp2p::{mdns, PeerId};
use serde_json::Value;
use time::OffsetDateTime;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

/// Network commands
#[derive(Debug)]
pub enum Command {
    Broadcast(Value),
    Publish(Value),
    Stop,
}

/// Network Events
#[derive(Debug)]
pub enum Event {
    PersistentData(String),
    VolatileData(String),
    Config(String),
    Discovered(Vec<PeerId>),
}

fn handle_command(swarm: &mut Swarm<DomoBehaviour>, cmd: Command) -> bool {
    use Command::*;
    match cmd {
        Broadcast(val) => {
            let topic = Topic::new("domo-volatile-data");
            let m = serde_json::to_string(&val).unwrap();

            if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic, m.as_bytes()) {
                log::info!("Publish error: {e:?}");
            }
            true
        }
        Publish(val) => {
            let topic = Topic::new("domo-persistent-data");
            let m2 = serde_json::to_string(&val).unwrap();

            if let Err(e) = swarm
                .behaviour_mut()
                .gossipsub
                .publish(topic, m2.as_bytes())
            {
                log::info!("Publish error: {e:?}");
            }
            true
        }
        Stop => false,
    }
}

fn handle_swarm_event<E>(
    swarm: &mut Swarm<DomoBehaviour>,
    event: SwarmEvent<OutEvent, E>,
    ev_send: &UnboundedSender<Event>,
) -> Result<(), ()> {
    use Event::*;

    match event {
        SwarmEvent::ExpiredListenAddr { address, .. } => {
            log::info!("Address {address:?} expired");
        }
        SwarmEvent::ConnectionEstablished { .. } => {
            log::info!("Connection established ...");
        }
        SwarmEvent::ConnectionClosed { .. } => {
            log::info!("Connection closed");
        }
        SwarmEvent::ListenerError { .. } => {
            log::info!("Listener Error");
        }
        SwarmEvent::OutgoingConnectionError { .. } => {
            log::info!("Outgoing connection error");
        }
        SwarmEvent::ListenerClosed { .. } => {
            log::info!("Listener Closed");
        }
        SwarmEvent::NewListenAddr { address, .. } => {
            println!("Listening in {address:?}");
        }
        SwarmEvent::Behaviour(crate::domolibp2p::OutEvent::Gossipsub(
            libp2p::gossipsub::Event::Message {
                propagation_source: _peer_id,
                message_id: _id,
                message,
            },
        )) => {
            let data = String::from_utf8(message.data).unwrap();
            match message.topic.as_str() {
                "domo-persistent-data" => {
                    ev_send.send(PersistentData(data)).map_err(|_| ())?;
                }
                "domo-config" => {
                    ev_send.send(Config(data)).map_err(|_| ())?;
                }
                "domo-volatile-data" => {
                    ev_send.send(VolatileData(data)).map_err(|_| ())?;
                }
                _ => {
                    log::info!("Not able to recognize message");
                }
            }
        }
        SwarmEvent::Behaviour(crate::domolibp2p::OutEvent::Mdns(mdns::Event::Expired(list))) => {
            let local = OffsetDateTime::now_utc();

            for (peer, _) in list {
                log::info!("MDNS for peer {peer} expired {local:?}");
            }
        }
        SwarmEvent::Behaviour(crate::domolibp2p::OutEvent::Mdns(mdns::Event::Discovered(list))) => {
            let local = OffsetDateTime::now_utc();
            let peers = list
                .iter()
                .map(|(peer, _)| {
                    swarm.behaviour_mut().gossipsub.add_explicit_peer(peer);
                    log::info!("Discovered peer {peer} {local:?}");
                    peer.to_owned()
                })
                .collect();
            ev_send.send(Discovered(peers)).map_err(|_| ())?;
        }
        _ => {}
    }

    Ok(())
}

/// Spawn a new task polling constantly for new swarm Events
pub fn dht_channel(
    mut swarm: Swarm<DomoBehaviour>,
) -> (
    UnboundedSender<Command>,
    UnboundedReceiver<Event>,
    JoinHandle<()>,
) {
    let (cmd_send, mut cmd_recv) = mpsc::unbounded_channel();
    let (mut ev_send, ev_recv) = mpsc::unbounded_channel();

    let handle = tokio::task::spawn(async move {
        loop {
            tokio::select! {
                cmd = cmd_recv.recv() => {
                    log::debug!("command {cmd:?}");
                    if !cmd.is_some_and(|cmd| handle_command(&mut swarm, cmd)) {
                        log::debug!("Exiting cmd");
                        return
                    }
                }
                ev = swarm.select_next_some() => {
                    if handle_swarm_event(&mut swarm, ev, &mut ev_send).is_err() {
                        log::debug!("Exiting ev");
                        return
                    }
                }
            }
        }
    });

    (cmd_send, ev_recv, handle)
}

#[cfg(test)]
mod test {
    use super::*;
    use libp2p_swarm_test::SwarmExt;
    use serde_json::json;

    #[tokio::test]
    async fn multiple_peers() {
        env_logger::init();
        let mut a = Swarm::new_ephemeral(|identity| DomoBehaviour::new(&identity).unwrap());
        let mut b = Swarm::new_ephemeral(|identity| DomoBehaviour::new(&identity).unwrap());
        let mut c = Swarm::new_ephemeral(|identity| DomoBehaviour::new(&identity).unwrap());

        for a in a.external_addresses() {
            println!("{a:?}");
        }

        a.listen().await;
        b.listen().await;
        c.listen().await;

        a.connect(&mut b).await;
        b.connect(&mut c).await;
        c.connect(&mut a).await;

        let peers: Vec<_> = a.connected_peers().cloned().collect();

        for peer in peers {
            a.behaviour_mut().gossipsub.add_explicit_peer(&peer);
        }

        let peers: Vec<_> = b.connected_peers().cloned().collect();

        for peer in peers {
            b.behaviour_mut().gossipsub.add_explicit_peer(&peer);
        }

        let peers: Vec<_> = c.connected_peers().cloned().collect();

        for peer in peers {
            c.behaviour_mut().gossipsub.add_explicit_peer(&peer);
        }

        let (a_s, mut ar, _) = dht_channel(a);
        let (b_s, br, _) = dht_channel(b);
        let (c_s, cr, _) = dht_channel(c);

        // Wait until peers are discovered
        while let Some(ev) = ar.recv().await {
            match ev {
                Event::VolatileData(data) => log::info!("volatile {data}"),
                Event::PersistentData(data) => log::info!("persistent {data}"),
                Event::Config(cfg) => log::info!("config {cfg}"),
                Event::Discovered(peers) => {
                    log::info!("found peers: {peers:?}");
                    break;
                }
            }
        }

        let msg = json!({"a": "value"});

        a_s.send(Command::Broadcast(msg.clone())).unwrap();

        for r in [br, cr].iter_mut() {
            while let Some(ev) = r.recv().await {
                match ev {
                    Event::VolatileData(data) => {
                        log::info!("volatile {data}");
                        let val: Value = serde_json::from_str(&data).unwrap();
                        assert_eq!(val, msg);
                        break;
                    }
                    Event::PersistentData(data) => log::info!("persistent {data}"),
                    Event::Config(cfg) => log::info!("config {cfg}"),
                    Event::Discovered(peers) => {
                        log::info!("found peers: {peers:?}");
                    }
                }
            }
        }

        drop(b_s);
        drop(c_s);
    }
}
