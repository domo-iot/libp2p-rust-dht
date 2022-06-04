mod domocache;
mod domolibp2p;
mod utils;
mod domopersistentstorage;

use serde_json::{json};
use std::error::Error;
use std::{env};

use chrono::prelude::*;
use domocache::DomoCacheOperations;

use domopersistentstorage::SqliteStorage;

use tokio::io::{self, AsyncBufReadExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        println!("Usage: ./domo-libp2p <sqlite_file_path> <persistent_cache>");
        return Ok(());
    }

    let local = Utc::now();

    log::info!("Program started at {:?}", local);

    let sqlite_file = &args[1];

    let is_persistent_cache: bool = String::from(&args[2]).parse().unwrap();

    env_logger::init();

    let storage = SqliteStorage::new(sqlite_file, is_persistent_cache);
    let mut domo_cache = domocache::DomoCache::new(is_persistent_cache, storage).await;


    let mut stdin = io::BufReader::new(io::stdin()).lines();

    //let mut stdin = io::BufReader::new(io::stdin()).lines().fuse();


    loop {
        tokio::select! {
            m = domo_cache.cache_event_loop() => {
                match m {
                    Ok(domocache::DomoEvent::None) => { },
                    Ok(domocache::DomoEvent::PersistentData(m)) => {
                        println!("Persistent message received {} {}", m.topic_name, m.topic_uuid);
                    },
                    Ok(domocache::DomoEvent::VolatileData(m)) => {
                        println!("Volatile message {}", m.to_string());
                    }
                    _ => {}
                }
            },
            line = stdin.next_line() => {
                let line = line?.expect("Stdin error");
                let mut args = line.split(" ");

                match args.next(){
                    Some("HASH") => {
                        domo_cache.print_cache_hash();
                    }
                    Some("PRINT") => {
                        domo_cache.print()
                    }
                    Some("PEERS") => {
                        println!("Peers:");
                        domo_cache.print_peers_cache()
                    }
                    Some("DEL") => {
                        let topic_name = args.next();
                        let topic_uuid = args.next();

                        if topic_name == None || topic_uuid == None {
                            println!("topic_name, topic_uuid are mandatory arguments");
                        } else {
                            let topic_name= topic_name.unwrap();
                            let topic_uuid= topic_uuid.unwrap();

                            domo_cache.delete_value(topic_name, topic_uuid);
                        }


                    }
                    Some("PUB") => {
                        let value = args.next();

                        if value == None {
                            println!("value is mandatory");
                        }

                        let val = json!({ "payload": value});

                        domo_cache.pub_value(val);

                    }
                    Some("PUT") => {
                        let topic_name = args.next();

                        let topic_uuid = args.next();

                        let value = args.next();

                        if topic_name == None || topic_uuid == None || value == None{
                            println!("topic_name, topic_uuid, values are mandatory arguments");
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
                        println!("Commands:");
                        println!("HASH");
                        println!("PRINT");
                        println!("PEERS");
                        println!("PUB <value>");
                        println!("PUT <topic_name> <topic_uuid> <value>");
                        println!("DEL <topic_name> <topic_uuid>");
                    }
                }

            }


        }
    }
}
