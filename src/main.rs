mod domolibp2p;
mod domocache;

use async_std::{io, task};
use futures::{prelude::*, select};

// Gossip includes
use libp2p::gossipsub::MessageId;
use libp2p::gossipsub::{
    GossipsubEvent, GossipsubMessage, IdentTopic as Topic, MessageAuthenticity, ValidationMode,
};
use libp2p::{gossipsub, swarm::SwarmEvent, Multiaddr};

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
//

use libp2p::{
    development_transport, identity,
    mdns::{Mdns, MdnsConfig, MdnsEvent},
    swarm::{NetworkBehaviourEventProcess},
    NetworkBehaviour, PeerId, Swarm,
};
use std::error::Error;
use std::time::Duration;


#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    // resto in attesa della creazione dello swarm

    let mut swarm = domolibp2p::start().await.unwrap();

    let mut stdin = io::BufReader::new(io::stdin()).lines().fuse();

    // idle loop
    loop {
        select! {
            line = stdin.select_next_some() => {
                domolibp2p::publish(&mut swarm);
            }
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Listening in {:?}", address);
                },
                _ => {}
            }
        //async_std::task::sleep(Duration::from_secs(10)).await;
        }
    }
}


