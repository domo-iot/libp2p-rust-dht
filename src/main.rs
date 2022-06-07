mod domocache;
mod domolibp2p;
mod utils;
mod domopersistentstorage;
mod restmessage;

use serde_json::{json};
use std::error::Error;
use std::{env};

use chrono::prelude::*;
use domocache::DomoCacheOperations;

use domopersistentstorage::SqliteStorage;

use tokio::io::{self, AsyncBufReadExt};

use axum::{routing::{get, post}, http::StatusCode, response::IntoResponse, Json, Router, extract::Extension};

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use crate::domocache::DomoCache;

use tokio::sync::{mpsc, oneshot};
use tokio::sync::mpsc::Sender;

use axum::{
    extract::ws::{WebSocketUpgrade, WebSocket},
};
use axum::extract::Path;
use axum::extract::ws::Message;
use tokio::time::sleep;


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

    let mut domo_cache =
        domocache::DomoCache::new(is_persistent_cache, storage).await;

    let mut stdin = io::BufReader::new(io::stdin()).lines();

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));


    let (tx_rest, mut rx_rest) = mpsc::channel(32);

    let tx_get_all = tx_rest.clone();

    let tx_get_topicname = tx_rest.clone();

    let tx_get_topicname_topicuuid = tx_rest.clone();

    let tx_post_topicname_topicuuid = tx_rest.clone();

    let app = Router::new()
        // `GET /` goes to `root`
        .route("/get_all", get(get_all_handler)
            .layer(Extension(tx_get_all)))
        .route("/topic_name/:topic_name", get(get_topicname_handler)
            .layer(Extension(tx_get_topicname)))
        .route("/topic_name/:topic_name/topic_uuid/:topic_uuid", get(get_topicname_topicuuid_handler)
            .layer(Extension(tx_get_topicname_topicuuid)))
        .route("/topic_name/:topic_name/topic_uuid/:topic_uuid", post(post_topicname_topicuuid_handler)
            .layer(Extension(tx_post_topicname_topicuuid)))


        .route("/ws", get(handle_websocket_req));;

    tokio::spawn(async move {
                     axum::Server::bind(&addr).serve(app.into_make_service()).await
                 });


    loop {

        tokio::select! {
            Some(rest_message) = rx_rest.recv() => {
                println!("Rest request");

                match rest_message {
                    restmessage::RestMessage::GetAll {responder} => {
                        let resp = domo_cache.get_all();
                        responder.send(Ok(resp));
                    }
                    restmessage::RestMessage::GetTopicName {topic_name, responder} => {
                        let resp = domo_cache.get_topic_name(&topic_name);
                        responder.send(resp);
                    }
                    restmessage::RestMessage::GetTopicUUID {topic_name, topic_uuid, responder} => {
                        let resp = domo_cache.get_topic_uuid(&topic_name, &topic_uuid);
                        responder.send(resp);
                    }
                    restmessage::RestMessage::PostTopicUUID {topic_name, topic_uuid, value, responder} => {
                        domo_cache.write_value(&topic_name, &topic_uuid, value.clone());
                        responder.send(Ok(value));
                    }
                }
            }

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

async fn post_topicname_topicuuid_handler(
    Json(value): Json<serde_json::Value>,
    Path((topic_name, topic_uuid)): Path<(String, String)>,
    Extension(tx_rest): Extension<Sender<restmessage::RestMessage>>
) -> impl IntoResponse {

    let (tx_resp, rx_resp) = oneshot::channel();

    let m = restmessage::RestMessage::PostTopicUUID{
        topic_name: topic_name,
        topic_uuid: topic_uuid,
        value: value,
        responder: tx_resp };

    tx_rest.send(m).await.unwrap();

    let resp = rx_resp.await.unwrap();

    match resp {
        Ok(resp) => return (StatusCode::OK, Json(resp)),
        Err(e) => return (StatusCode::NOT_FOUND, Json(json!({})))
    }



}

async fn get_topicname_topicuuid_handler(
    Path((topic_name, topic_uuid)): Path<(String, String)>,
    Extension(tx_rest): Extension<Sender<restmessage::RestMessage>>
) -> impl IntoResponse {

    let (tx_resp, rx_resp) = oneshot::channel();

    let m = restmessage::RestMessage::GetTopicUUID{
        topic_name: topic_name,
        topic_uuid: topic_uuid,
        responder: tx_resp };

    tx_rest.send(m).await.unwrap();

    let resp = rx_resp.await.unwrap();

    match resp {
        Ok(resp) => return (StatusCode::OK, Json(resp)),
        Err(e) => return (StatusCode::NOT_FOUND, Json(json!({})))
    }


}

async fn get_topicname_handler(
    Path(topic_name): Path<String>,
    Extension(tx_rest): Extension<Sender<restmessage::RestMessage>>
) -> impl IntoResponse {

    let (tx_resp, rx_resp) = oneshot::channel();

    let m = restmessage::RestMessage::GetTopicName{ topic_name: topic_name, responder: tx_resp };
    tx_rest.send(m).await.unwrap();

    let resp = rx_resp.await.unwrap();

    match resp {
        Ok(resp) => return (StatusCode::OK, Json(resp)),
        Err(e) => return (StatusCode::NOT_FOUND, Json(json!({})))
    }


}



async fn get_all_handler(
    Extension(tx_rest): Extension<Sender<restmessage::RestMessage>>,
) -> impl IntoResponse {

    let (tx_resp, rx_resp) = oneshot::channel();

    let m = restmessage::RestMessage::GetAll{ responder: tx_resp };
    tx_rest.send(m).await.unwrap();

    let resp = rx_resp.await.unwrap();

    (StatusCode::OK, Json(resp.unwrap()))

}


async fn handle_websocket(mut socket: WebSocket) {
    while let Some(msg) = socket.recv().await {

        let msg = msg.unwrap();

        let msg2 = msg.clone();
        match msg2 {
            Message::Text(message) => {
                println!("Received {}", message);
            }
            Message::Binary(_) => {}
            Message::Ping(_) => {}
            Message::Pong(_) => {}
            Message::Close(_) => {}
        }

        socket.send(msg).await.unwrap();
    }
}


async fn handle_websocket_req(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(handle_websocket)
}
