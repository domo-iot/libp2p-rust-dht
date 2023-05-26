use serde_json::json;
use time::OffsetDateTime;

use std::error::Error;

use tokio::io::{self, AsyncBufReadExt};

use clap::Parser;
use sifis_dht::domocache::DomoEvent;
use sifis_dht_broker::domobroker::{DomoBroker, DomoBrokerConf};

#[derive(Parser, Debug)]
struct Opt {
    /// Path to a sqlite file
    #[clap(parse(try_from_str))]
    db_url: String,

    /// Path to a sqlite file
    #[clap(parse(try_from_str))]
    db_table: String,

    /// Use a persistent cache
    #[clap(parse(try_from_str))]
    is_persistent_cache: bool,

    /// Path to a private key file
    #[clap(parse(try_from_str))]
    private_key_file: String,

    /// 32 bytes long shared key in hex format
    #[clap(parse(try_from_str))]
    shared_key: String,

    /// use only loopback iface for libp2p
    #[clap(parse(try_from_str))]
    loopback_only: bool,

    /// HTTP port
    #[clap(parse(try_from_str))]
    http_port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let opt = Opt::parse();

    let local = OffsetDateTime::now_utc();

    log::info!("Program started at {:?}", local);

    let Opt {
        db_url,
        db_table,
        private_key_file,
        is_persistent_cache,
        shared_key,
        http_port,
        loopback_only,
    } = opt;

    env_logger::init();

    let mut stdin = io::BufReader::new(io::stdin()).lines();
    let debug_console = std::env::var("DHT_DEBUG_CONSOLE").is_ok();

    let domo_broker_conf = DomoBrokerConf {
        db_url,
        db_table,
        private_key_file: Some(private_key_file),
        is_persistent_cache,
        shared_key,
        http_port,
        loopback_only,
    };

    let mut domo_broker = DomoBroker::new(domo_broker_conf).await?;

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
        DomoEvent::None => {}
        DomoEvent::VolatileData(_v) => {
            println!("Volatile");
        }
        DomoEvent::PersistentData(_v) => {
            println!("Persistent");
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
