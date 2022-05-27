mod domocache;
mod domolibp2p;

use async_std::{io, task};
use futures::{prelude::*, select};
use serde_json::{json, Value};
use std::error::Error;
use std::time::Duration;
use std::{env, time};

use chrono::prelude::*;
use domocache::DomoCacheOperations;

#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Usage: ./domo-libp2p <sqlite_file_path>");

    let local = Utc::now();

    println!("Program started at {:?}", local);

    let args: Vec<String> = env::args().collect();

    let sqlite_file = &args[1];

    env_logger::init();

    let house_uuid = "CasaProva";

    let storage = domocache::SqliteStorage::new(house_uuid, sqlite_file, true);

    let mut domo_cache = domocache::DomoCache::new(house_uuid, true, storage).await;

    let mut stdin = io::BufReader::new(io::stdin()).lines().fuse();

    // idle loop
    loop {
        select! {
            ret = domo_cache.wait_for_messages().fuse() => {
                println!("Application got message ... {:?}", ret);
            },
            line = stdin.select_next_some() => {
                let line = line.expect("Stdin error");
                let mut args = line.split(" ");

                match args.next(){
                    Some("PRINT") => {
                        domo_cache.print()
                    }
                    Some("PEERS") => {
                        println!("Peers:");
                        for (peer, _) in domo_cache
                            .swarm
                            .behaviour_mut()
                            .gossipsub.all_peers() {

                            println!("{:?}", peer.to_string());
                        }

                    }
                    Some("PUB") => {
                        let topic_name = args.next();

                        let topic_uuid = args.next();

                        let value = args.next();

                        // se uno degli argomenti è vuoto
                        if topic_name == None || topic_uuid == None || value == None{
                            println!("topic_name, topic_uuid, value are mandatory arguments");
                        } else{
                            let topic_name= topic_name.unwrap();
                            let topic_uuid= topic_uuid.unwrap();
                            let value = value.unwrap();

                            println!("{} {} {}", topic_name, topic_uuid, value);

                            let val = json!({ "payload": value});

                            domo_cache.write_value(topic_name, topic_uuid, val);
                        }
                    },
                    _ => {
                        println!("expected PUB <topic_name> <topic_uuid> <value>");
                    }
                }

            }


        }
    }
}
