use serde_json::json;
use time::OffsetDateTime;

use std::error::Error;

use tokio::io::{self, AsyncBufReadExt};

use clap::Parser;
use serde::{Deserialize, Serialize};
use sifis_config::{Broker, ConfigParser};
use sifis_dht::domocache::DomoEvent;
use sifis_dht_broker::domobroker::DomoBroker;

#[derive(Parser, Debug, Serialize, Deserialize)]
struct Opt {
    #[clap(flatten)]
    broker: Broker,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let opt = ConfigParser::<Opt>::new()
        .with_config_path("/etc/domo/broker.toml")
        .parse();

    let local = OffsetDateTime::now_utc();

    log::info!("Program started at {:?}", local);

    env_logger::init();

    let mut stdin = io::BufReader::new(io::stdin()).lines();
    let debug_console = std::env::var("DHT_DEBUG_CONSOLE").is_ok();

    let mut domo_broker = DomoBroker::new(opt.broker).await?;

    if debug_console {
        loop {
            tokio::select! {
                m = domo_broker.event_loop() => report_event(&m),

                line = stdin.next_line() => {
                    handle_user_input(line, &mut domo_broker).await;
                },
            }
        }
    } else {
        loop {
            tokio::select! {
                m = domo_broker.event_loop() => report_event(&m),
            }
        }
    }
}

fn report_event(m: &DomoEvent) {
    println!("Domo Event received");
    match m {
        DomoEvent::None => {
            println!("None {:?}", m);
        }
        DomoEvent::VolatileData(_v) => {
            println!("Volatile");
        }
        DomoEvent::PersistentData(_v) => {
            println!("Persistent");
        }
        DomoEvent::NewPeers(peers) => {
            println!("New peers {:#?}", peers);
        }
    }
}

async fn handle_user_input(line: io::Result<Option<String>>, domo_broker: &mut DomoBroker) {
    let line = match line {
        Err(_) | Ok(None) => return,
        Ok(Some(s)) => s,
    };

    let mut args = line.split(' ');

    match args.next() {
        Some("HASH") => {
            domo_broker.domo_cache.print_cache_hash();
        }
        Some("PRINT") => domo_broker.domo_cache.print(),
        Some("PEERS") => {
            println!("Own peer ID: {}", domo_broker.domo_cache.local_peer_id);
            println!("Peers:");
            domo_broker.domo_cache.print_peers_cache()
        }
        Some("DEL") => {
            if let (Some(topic_name), Some(topic_uuid)) = (args.next(), args.next()) {
                domo_broker
                    .domo_cache
                    .delete_value(topic_name, topic_uuid)
                    .await;
            } else {
                println!("topic_name, topic_uuid are mandatory arguments");
            }
        }
        Some("PUB") => {
            let value = args.next();

            if value.is_none() {
                println!("value is mandatory");
            }

            let val = json!({ "payload": value });

            domo_broker.domo_cache.pub_value(val).await;
        }
        Some("PUT") => {
            let arguments = (args.next(), args.next(), args.next());

            if let (Some(topic_name), Some(topic_uuid), Some(value)) = arguments {
                println!("{topic_name} {topic_uuid} {value}");

                let val = json!({ "payload": value });

                domo_broker
                    .domo_cache
                    .write_value(topic_name, topic_uuid, val)
                    .await;
            } else {
                println!("topic_name, topic_uuid, values are mandatory arguments");
            }
        }
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
