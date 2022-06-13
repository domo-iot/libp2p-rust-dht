mod domocache;
mod domolibp2p;
mod domopersistentstorage;
mod restmessage;
mod utils;
mod webapimanager;
mod websocketmessage;

use serde_json::json;

use std::error::Error;

use chrono::prelude::*;

use domopersistentstorage::SqliteStorage;

use tokio::io::{self, AsyncBufReadExt};

use std::path::PathBuf;

use tokio::sync::mpsc::Sender;

use tokio::sync::{broadcast, mpsc, oneshot};

use axum::extract::ws::Message;

use axum::extract::ws::WebSocketUpgrade;

use axum::extract::Path;

use clap::Parser;

use crate::websocketmessage::{
    AsyncWebSocketDomoMessage, SyncWebSocketDomoMessage, SyncWebSocketDomoRequest,
};

#[derive(Parser, Debug)]
struct Opt {
    /// Path to a sqlite file
    #[clap(parse(from_os_str))]
    sqlite_file: PathBuf,

    /// Use a persistent cache
    #[clap(parse(try_from_str))]
    is_persistent_cache: bool,

    /// 32 bytes long shared key in hex format
    #[clap(parse(try_from_str))]
    shared_key: String,

    /// HTTP port
    #[clap(parse(try_from_str))]
    http_port: u16,

    /// use only loopback iface for libp2p
    #[clap(parse(try_from_str))]
    loopback_only: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let opt = Opt::parse();

    let local = Utc::now();

    log::info!("Program started at {:?}", local);

    let Opt {
        sqlite_file,
        is_persistent_cache,
        shared_key,
        http_port,
        loopback_only,
    } = opt;

    env_logger::init();

    let storage = SqliteStorage::new(sqlite_file, is_persistent_cache);

    let mut domo_cache =
        domocache::DomoCache::new(is_persistent_cache, storage, shared_key, loopback_only).await;

    let mut stdin = io::BufReader::new(io::stdin()).lines();

    let mut webmanager = webapimanager::WebApiManager::new(http_port);

    loop {
        tokio::select! {
            webs_message = webmanager.sync_rx_websocket.recv() => {

                let message = webs_message.unwrap();


                match message.request {
                    SyncWebSocketDomoRequest::RequestGetAll => {

                        println!("WebSocket RequestGetAll");


                        let resp = SyncWebSocketDomoRequest::Response {
                            value: domo_cache.get_all()
                        };

                        let r = SyncWebSocketDomoMessage {
                            ws_client_id: message.ws_client_id,
                            req_id: message.req_id,
                            request: resp
                        };

                        let _ret = webmanager.sync_tx_websocket.send(r);
                    }

                    SyncWebSocketDomoRequest::RequestGetTopicName { topic_name } => {

                        println!("WebSocket RequestGetTopicName");

                        let ret = domo_cache.get_topic_name(&topic_name);

                        let value = match ret {
                            Ok(m) => m,
                            Err(_e) => json!({})
                        };

                        let resp = SyncWebSocketDomoRequest::Response {
                            value
                        };

                        let r = SyncWebSocketDomoMessage {
                            ws_client_id: message.ws_client_id,
                            req_id: message.req_id,
                            request: resp
                        };

                        let _ret = webmanager.sync_tx_websocket.send(r);
                    }

                    SyncWebSocketDomoRequest::RequestGetTopicUUID { topic_name, topic_uuid } => {

                        println!("WebSocket RequestGetTopicUUID");

                        let ret = domo_cache.get_topic_uuid(&topic_name, &topic_uuid);
                        let value = match ret {
                            Ok(m) => m,
                            Err(_e) => json!({})
                        };

                        let resp = SyncWebSocketDomoRequest::Response {
                            value
                        };

                        let r = SyncWebSocketDomoMessage {
                            ws_client_id: message.ws_client_id,
                            req_id: message.req_id,
                            request: resp
                        };

                        let _ret = webmanager.sync_tx_websocket.send(r);
                    }

                    SyncWebSocketDomoRequest::RequestDeleteTopicUUID { topic_name, topic_uuid } => {

                        let _ret = domo_cache.delete_value(&topic_name, &topic_uuid).await;
                        println!("WebSocket RequestDeleteTopicUUID");

                    }

                    SyncWebSocketDomoRequest::RequestPostTopicUUID { topic_name, topic_uuid, value } => {

                        println!("WebSocket RequestPostTopicUUID");

                        let _ret = domo_cache.write_value(&topic_name, &topic_uuid, value.clone()).await;

                    }

                    SyncWebSocketDomoRequest::RequestPubMessage {  value } => {

                        println!("WebSocket RequestPubMessage");
                        let _ret = domo_cache.pub_value(value.clone()).await;
                    }

                    _ => {}
                }

            }
            Some(rest_message) = webmanager.rx_rest.recv() => {
                println!("Rest request");

                match rest_message {
                    restmessage::RestMessage::GetAll {responder} => {
                        let resp = domo_cache.get_all();
                        responder.send(Ok(resp))
                    }
                    restmessage::RestMessage::GetTopicName {topic_name, responder} => {
                        let resp = domo_cache.get_topic_name(&topic_name);
                        responder.send(resp)
                    }
                    restmessage::RestMessage::GetTopicUUID {topic_name, topic_uuid, responder} => {
                        let resp = domo_cache.get_topic_uuid(&topic_name, &topic_uuid);
                        responder.send(resp)
                    }
                    restmessage::RestMessage::PostTopicUUID {topic_name, topic_uuid, value, responder} => {
                        domo_cache.write_value(&topic_name, &topic_uuid, value.clone()).await;
                        responder.send(Ok(value))
                    }
                    restmessage::RestMessage::DeleteTopicUUID {topic_name, topic_uuid, responder} => {
                        domo_cache.delete_value(&topic_name, &topic_uuid).await;
                        responder.send(Ok(json!({})))
                    }
                    restmessage::RestMessage::PubMessage {value, responder} => {
                        domo_cache.pub_value(value.clone()).await;
                        responder.send(Ok(value))
                    }
                }.expect("Cannot send to the channel");
            }

            m = domo_cache.cache_event_loop() => {
                match m {
                    Ok(domocache::DomoEvent::None) => { },
                    Ok(domocache::DomoEvent::PersistentData(m)) => {
                        println!("Persistent message received {} {}", m.topic_name, m.topic_uuid);
                        let _ret = webmanager.async_tx_websocket.send(
                            AsyncWebSocketDomoMessage::Persistent {
                             topic_name: m.topic_name,
                             topic_uuid: m.topic_uuid,
                             value: m.value,
                             deleted: m.deleted
                        });

                    },
                    Ok(domocache::DomoEvent::VolatileData(m)) => {
                        println!("Volatile message {}", m);

                        let _ret = webmanager.async_tx_websocket.send(
                            AsyncWebSocketDomoMessage::Volatile {
                                value: m
                        });

                    }
                    _ => {}
                }
            },
            line = stdin.next_line() => {
                let line = line?.expect("Stdin error");
                let mut args = line.split(' ');

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

                            domo_cache.delete_value(topic_name, topic_uuid).await;
                        }


                    }
                    Some("PUB") => {
                        let value = args.next();

                        if value == None {
                            println!("value is mandatory");
                        }

                        let val = json!({ "payload": value});

                        domo_cache.pub_value(val).await;

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

                            domo_cache.write_value(topic_name, topic_uuid, val).await;
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
