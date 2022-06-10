mod domocache;
mod domolibp2p;
mod domopersistentstorage;
mod restmessage;
mod utils;
mod websocketmessage;

use serde_json::json;

use std::error::Error;

use chrono::prelude::*;

use domopersistentstorage::SqliteStorage;

use tokio::io::{self, AsyncBufReadExt};

use axum::{
    extract::Extension,
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};

use std::net::SocketAddr;
use std::path::PathBuf;

use tokio::sync::mpsc::Sender;
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::websocketmessage::{
    AsyncWebSocketDomoMessage, SyncWebSocketDomoMessage, SyncWebSocketDomoRequest,
};
use axum::extract::ws::Message;
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::Path;

use clap::Parser;

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

    let addr = SocketAddr::from(([127, 0, 0, 1], http_port));

    let (tx_rest, mut rx_rest) = mpsc::channel(32);

    let tx_get_all = tx_rest.clone();

    let tx_get_topicname = tx_rest.clone();

    let tx_get_topicname_topicuuid = tx_rest.clone();

    let tx_post_topicname_topicuuid = tx_rest.clone();

    let tx_delete_topicname_topicuuid = tx_rest.clone();

    let tx_pub_message = tx_rest.clone();

    let (async_tx_websocket, mut _async_rx_websocket) =
        broadcast::channel::<AsyncWebSocketDomoMessage>(16);

    let async_tx_websocket_copy = async_tx_websocket.clone();

    let (sync_tx_websocket, mut sync_rx_websocket) =
        broadcast::channel::<SyncWebSocketDomoMessage>(16);

    let sync_tx_websocket_copy = sync_tx_websocket.clone();

    let app = Router::new()
        // `GET /` goes to `root`
        .route(
            "/get_all",
            get(get_all_handler).layer(Extension(tx_get_all)),
        )
        .route(
            "/topic_name/:topic_name",
            get(get_topicname_handler).layer(Extension(tx_get_topicname)),
        )
        .route(
            "/topic_name/:topic_name/topic_uuid/:topic_uuid",
            get(get_topicname_topicuuid_handler).layer(Extension(tx_get_topicname_topicuuid)),
        )
        .route(
            "/topic_name/:topic_name/topic_uuid/:topic_uuid",
            post(post_topicname_topicuuid_handler).layer(Extension(tx_post_topicname_topicuuid)),
        )
        .route(
            "/topic_name/:topic_name/topic_uuid/:topic_uuid",
            delete(delete_topicname_topicuuid_handler)
                .layer(Extension(tx_delete_topicname_topicuuid)),
        )
        .route("/pub", post(pub_message).layer(Extension(tx_pub_message)))
        .route(
            "/ws",
            get(handle_websocket_req)
                .layer(Extension(async_tx_websocket_copy))
                .layer(Extension(sync_tx_websocket_copy)),
        );

    tokio::spawn(async move {
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
    });

    loop {
        tokio::select! {
            webs_message = sync_rx_websocket.recv() => {

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

                        let _ret = sync_tx_websocket.send(r);
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

                        let _ret = sync_tx_websocket.send(r);
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

                        let _ret = sync_tx_websocket.send(r);
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
            Some(rest_message) = rx_rest.recv() => {
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
                        let _ret = async_tx_websocket.send(
                            AsyncWebSocketDomoMessage::Persistent {
                             topic_name: m.topic_name,
                             topic_uuid: m.topic_uuid,
                             value: m.value,
                             deleted: m.deleted
                        });

                    },
                    Ok(domocache::DomoEvent::VolatileData(m)) => {
                        println!("Volatile message {}", m);

                        let _ret = async_tx_websocket.send(
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

async fn delete_topicname_topicuuid_handler(
    Path((topic_name, topic_uuid)): Path<(String, String)>,
    Extension(tx_rest): Extension<Sender<restmessage::RestMessage>>,
) -> impl IntoResponse {
    let (tx_resp, rx_resp) = oneshot::channel();

    let m = restmessage::RestMessage::DeleteTopicUUID {
        topic_name,
        topic_uuid,
        responder: tx_resp,
    };

    tx_rest.send(m).await.unwrap();

    let resp = rx_resp.await.unwrap();

    match resp {
        Ok(resp) => (StatusCode::OK, Json(resp)),
        Err(_e) => (StatusCode::NOT_FOUND, Json(json!({}))),
    }
}

async fn pub_message(
    Json(value): Json<serde_json::Value>,
    Extension(tx_rest): Extension<Sender<restmessage::RestMessage>>,
) -> impl IntoResponse {
    let (tx_resp, rx_resp) = oneshot::channel();

    let m = restmessage::RestMessage::PubMessage {
        value,
        responder: tx_resp,
    };

    tx_rest.send(m).await.unwrap();

    let resp = rx_resp.await.unwrap();

    match resp {
        Ok(resp) => (StatusCode::OK, Json(resp)),
        Err(_e) => (StatusCode::NOT_FOUND, Json(json!({}))),
    }
}

async fn post_topicname_topicuuid_handler(
    Json(value): Json<serde_json::Value>,
    Path((topic_name, topic_uuid)): Path<(String, String)>,
    Extension(tx_rest): Extension<Sender<restmessage::RestMessage>>,
) -> impl IntoResponse {
    let (tx_resp, rx_resp) = oneshot::channel();

    let m = restmessage::RestMessage::PostTopicUUID {
        topic_name,
        topic_uuid,
        value,
        responder: tx_resp,
    };

    tx_rest.send(m).await.unwrap();

    let resp = rx_resp.await.unwrap();

    match resp {
        Ok(resp) => (StatusCode::OK, Json(resp)),
        Err(_e) => (StatusCode::NOT_FOUND, Json(json!({}))),
    }
}

async fn get_topicname_topicuuid_handler(
    Path((topic_name, topic_uuid)): Path<(String, String)>,
    Extension(tx_rest): Extension<Sender<restmessage::RestMessage>>,
) -> impl IntoResponse {
    let (tx_resp, rx_resp) = oneshot::channel();

    let m = restmessage::RestMessage::GetTopicUUID {
        topic_name,
        topic_uuid,
        responder: tx_resp,
    };

    tx_rest.send(m).await.unwrap();

    let resp = rx_resp.await.unwrap();

    match resp {
        Ok(resp) => (StatusCode::OK, Json(resp)),
        Err(_e) => (StatusCode::NOT_FOUND, Json(json!({}))),
    }
}

async fn get_topicname_handler(
    Path(topic_name): Path<String>,
    Extension(tx_rest): Extension<Sender<restmessage::RestMessage>>,
) -> impl IntoResponse {
    let (tx_resp, rx_resp) = oneshot::channel();

    let m = restmessage::RestMessage::GetTopicName {
        topic_name,
        responder: tx_resp,
    };
    tx_rest.send(m).await.unwrap();

    let resp = rx_resp.await.unwrap();

    match resp {
        Ok(resp) => (StatusCode::OK, Json(resp)),
        Err(_e) => (StatusCode::NOT_FOUND, Json(json!({}))),
    }
}

async fn get_all_handler(
    Extension(tx_rest): Extension<Sender<restmessage::RestMessage>>,
) -> impl IntoResponse {
    let (tx_resp, rx_resp) = oneshot::channel();

    let m = restmessage::RestMessage::GetAll { responder: tx_resp };
    tx_rest.send(m).await.unwrap();

    let resp = rx_resp.await.unwrap();

    (StatusCode::OK, Json(resp.unwrap()))
}

async fn handle_websocket_req(
    ws: WebSocketUpgrade,
    Extension(async_tx_ws): Extension<broadcast::Sender<AsyncWebSocketDomoMessage>>,
    Extension(sync_tx_ws): Extension<broadcast::Sender<SyncWebSocketDomoMessage>>,
) -> impl IntoResponse {
    // channel for receiving async messages
    let mut async_rx_ws = async_tx_ws.subscribe();

    // channel for receiving sync messages
    let mut sync_rx_ws = sync_tx_ws.subscribe();

    ws.on_upgrade(|mut socket| async move {
        let my_id = utils::get_epoch_ms().to_string();

        loop {
            tokio::select! {
                        sync_rx = sync_rx_ws.recv() => {

                            let msg: SyncWebSocketDomoMessage = sync_rx.unwrap();

                            let req = msg.request.clone();

                            if let SyncWebSocketDomoRequest::Response { value: _ } = msg.request {
                                 if msg.ws_client_id == my_id {
                                     let _ret = socket.send(
                                     Message::Text(serde_json::to_string(&req).unwrap()))
                                     .await;
                                 }
                            }

                        }
                        Some(msg) = socket.recv() => {

                            match msg.unwrap() {
                                Message::Text(message) => {
                                    // parso il messaggio
                                    println!("Received command {}", message);

                                    let req : SyncWebSocketDomoRequest = serde_json::from_str(&message).unwrap();

                                    let msg = SyncWebSocketDomoMessage {
                                        ws_client_id: my_id.clone(),
                                        req_id: my_id.clone(),
                                        request: req
                                    };

                                    let _ret = sync_tx_ws.send(msg);

                                }
                                Message::Close(_) => {
                                    return;
                                }
                                _ => {}
                            }
                        }
                        async_rx = async_rx_ws.recv() => {
                             let msg = async_rx.unwrap();
                             let string_msg = serde_json::to_string(&msg).unwrap();
                             let _ret = socket.send(Message::Text(string_msg)).await;
                        }

            }
        }
    })
}
