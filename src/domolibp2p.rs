use async_std::{io, task};
use futures::{prelude::*, select};

// Gossip includes
use libp2p::gossipsub::MessageId;
use libp2p::gossipsub::{
    Gossipsub, GossipsubEvent, GossipsubMessage, IdentTopic as Topic, MessageAuthenticity, ValidationMode,
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


pub fn publish(swarm: &mut Swarm<DomoBehaviour>){

    let topic = Topic::new("domo-data");

    for i in 0..1000 {

        let message = i.to_string();

        if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic.clone(), message.as_bytes()){
            println!("Publish error: {:?}", e);
        }
        else{
            println!("Publishing message {}", i);
        }
    }
}

pub async fn start() -> Result<Swarm<DomoBehaviour>, Box<dyn Error>>{
    // Create a random key for ourselves.
    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(local_key.public());


    // Create a Gossipsub topic
    let topic = Topic::new("domo-data");

    // Set up a an encrypted DNS-enabled TCP Transport over the Mplex protocol.
    let transport = development_transport(local_key.clone()).await?;

    // Create a swarm to manage peers and events.
    let mut swarm = {

        let mdns = task::block_on(Mdns::new(MdnsConfig::default()))?;

        // To content-address message, we can take the hash of message and use it as an ID.
        let message_id_fn = |message: &GossipsubMessage| {
            let mut s = DefaultHasher::new();
            message.data.hash(&mut s);
            MessageId::from(s.finish().to_string())
        };

        // Set a custom gossipsub
        let gossipsub_config = gossipsub::GossipsubConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
            .validation_mode(ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message signing)
            .message_id_fn(message_id_fn) // content-address messages. No two messages of the
            // same content will be propagated.
            .build()
            .expect("Valid config");

        // build a gossipsub network behaviour
        let mut gossipsub: gossipsub::Gossipsub =
            gossipsub::Gossipsub::new(MessageAuthenticity::Signed(local_key), gossipsub_config)
                .expect("Correct configuration");

        // subscribes to our topic
        gossipsub.subscribe(&topic).unwrap();

        let behaviour = DomoBehaviour { mdns, gossipsub};
        Swarm::new(transport, behaviour, local_peer_id)
    };

    // Listen on all interfaces and whatever port the OS assigns.
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    Ok(swarm)
}



// We create a custom network behaviour that combines mDNS and gossipsub.
#[derive(NetworkBehaviour)]
#[behaviour(out_event = "OutEvent")]

pub struct DomoBehaviour {
    pub mdns: Mdns,
    pub gossipsub: Gossipsub
}

#[derive(Debug)]
pub enum OutEvent {
    Gossipsub(GossipsubEvent),
    Mdns(MdnsEvent),
}

impl From<MdnsEvent> for OutEvent {
    fn from(v: MdnsEvent) -> Self {
        Self::Mdns(v)
    }
}

impl From<GossipsubEvent> for OutEvent {
    fn from(v: GossipsubEvent) -> Self {
        Self::Gossipsub(v)
    }
}



