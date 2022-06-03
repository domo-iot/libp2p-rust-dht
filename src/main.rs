mod domocache;
mod domolibp2p;

use async_std::{io};
use futures::{prelude::*, select};
use serde_json::{json};
use std::error::Error;
use std::{env};

use chrono::prelude::*;
use domocache::DomoCacheOperations;


#[async_std::main]
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

    let house_uuid = "CasaProva";

    let storage = domocache::SqliteStorage::new(house_uuid, sqlite_file, is_persistent_cache);

    let mut domo_cache = domocache::DomoCache::new(house_uuid, is_persistent_cache, storage).await;

    let mut stdin = io::BufReader::new(io::stdin()).lines().fuse();

    // idle loop
    loop {
        select! {
            _ = domo_cache.cache_event_loop().fuse() => {
                log::info!("Application got message ...");
            },
            line = stdin.select_next_some() => {
                let line = line.expect("Stdin error");
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
                        let topic_name = args.next();

                        let topic_uuid = args.next();

                        let value = args.next();

                        // se uno degli argomenti è vuoto
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
                        println!("PUB <topic_name> <topic_uuid> <value>");
                    }
                }

            }


        }
    }
}
