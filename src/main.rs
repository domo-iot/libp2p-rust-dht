mod domolibp2p;
mod domocache;

use std::collections::HashMap;
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
use serde_json::{Value, json};
use crate::domocache::DomoCacheOperations;


#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    // resto in attesa della creazione dello swarm

    let mut swarm = domolibp2p::start().await.unwrap();

    let house_uuid = String::from("CasaProva");
    let mut domo_cache = domocache::DomoCache{
        house_uuid: house_uuid.clone(),
        is_persistent_cache: true,
        storage: Box::new(
            domocache::SqliteStorage {
                house_uuid: house_uuid.clone(),
                sqlite_file: String::from("./uno.sqlite"),
                sqlite_connection: None
            }),
        cache: HashMap::new()
    };

    domo_cache.init();

    let mut stdin = io::BufReader::new(io::stdin()).lines().fuse();

    // idle loop
    loop {
        select! {
            line = stdin.select_next_some() => {
                let line = line.expect("Stdin error");
                let mut args = line.split(" ");

                match args.next(){
                    Some("PUB") => {
                        let topic_name = match args.next() {
                            Some(topic_name) => Some(topic_name),
                            None => None
                        };

                        let topic_uuid = match args.next() {
                            Some(topic_uuid) => Some(topic_uuid),
                            None => None
                        };

                        let value = match args.next() {
                            Some(value) => Some(value),
                            None => None
                        };

                        // se uno degli argomenti è vuoto
                        if topic_name == None || topic_uuid == None || value == None{
                            println!("topic_name, topic_uuid, value are mandatory arguments");
                        } else{
                            let topic_name= topic_name.unwrap();
                            let topic_uuid= topic_uuid.unwrap();
                            let value = value.unwrap();

                            println!("{} {} {}", topic_name, topic_uuid, value);
                            let val = json!({ "field": value});

                            domo_cache.write_value(topic_name, topic_uuid, val.clone());
                            domolibp2p::pub_element(&mut swarm, topic_name, topic_uuid, val);
                        }
                    },
                    _ => {
                        println!("expected PUB <topic_name> <topic_uuid> <json>");
                    }
                }

                //domolibp2p::publish(&mut swarm);

            }
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Listening in {:?}", address);
                },
                SwarmEvent::Behaviour(
                    domolibp2p::OutEvent::Gossipsub(
                    GossipsubEvent::Message{
                    propagation_source: peer_id,
                    message_id: id,
                    message
                        })) => {
                    println!(
                        "Got message: {} with id: {} from peer: {:?}, topic {}",
                        String::from_utf8_lossy(&message.data),
                        id,
                        peer_id,
                        &message.topic);

                    type JsonMap = HashMap<String, serde_json::Value>;
                    let val : Value = serde_json::from_str(&String::from_utf8_lossy(&message.data))?;
                    let map: JsonMap = serde_json::from_str(&String::from_utf8_lossy(&message.data))?;

                    let mut topic_name = String::from("");
                    let mut topic_uuid = String::from("");

                    for (key, value) in map.iter() {

                        match key.as_str() {
                            "topic_name" =>  {topic_name = String::from(value.as_str().unwrap());}
                            "topic_uuid" => {topic_uuid = String::from(value.as_str().unwrap());}
                            _ => ()
                        }

                    }


                    //let topic_name = val.get("topic_name").unwrap();
                    //let topic_uuid = val.get("topic_uuid").unwrap();

                    domo_cache.write_value(&topic_name, &topic_uuid, val.clone());
                },
                SwarmEvent::Behaviour(domolibp2p::OutEvent::Mdns(
                    MdnsEvent::Discovered(list)
                )) => {
                    for (peer, _) in list {
                        swarm
                            .behaviour_mut()
                            .gossipsub
                            .add_explicit_peer(&peer);
                        println!("Discovered peer {}", peer);
                    }
                }
                _ => {}
            }
        //async_std::task::sleep(Duration::from_secs(10)).await;
        }
    }
}


