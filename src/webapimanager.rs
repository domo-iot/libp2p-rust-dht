use crate::utils;

use axum::{
    extract::Extension,
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};

use axum::extract::ws::Message;
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::Path;

use crate::websocketmessage::{
    AsyncWebSocketDomoMessage, SyncWebSocketDomoMessage, SyncWebSocketDomoRequest,
};

use tokio::sync::{broadcast, mpsc, oneshot};

use crate::restmessage;

use std::net::SocketAddr;

use tokio::sync::mpsc::Sender;

use serde_json::json;

pub struct WebApiManager {
    // rest api listening port
    pub http_port: u16,

    // channel where synchronous web socket requests are sent
    pub sync_rx_websocket: broadcast::Receiver<SyncWebSocketDomoMessage>,

    // channel where synchronous web socket responses are sent
    pub sync_tx_websocket: broadcast::Sender<SyncWebSocketDomoMessage>,

    // channel where asynchronous web socket messages are sent
    pub async_tx_websocket: broadcast::Sender<AsyncWebSocketDomoMessage>,

    // channel used to receive rest requests
    pub rx_rest: mpsc::Receiver<restmessage::RestMessage>,
}

impl WebApiManager {
    pub fn new(http_port: u16) -> Self {
        let addr = SocketAddr::from(([127, 0, 0, 1], http_port));

        let (tx_rest, rx_rest) = mpsc::channel(32);

        let tx_get_all = tx_rest.clone();

        let tx_get_topicname = tx_rest.clone();

        let tx_get_topicname_topicuuid = tx_rest.clone();

        let tx_post_topicname_topicuuid = tx_rest.clone();

        let tx_delete_topicname_topicuuid = tx_rest.clone();

        let tx_pub_message = tx_rest.clone();

        let (async_tx_websocket, mut _async_rx_websocket) =
            broadcast::channel::<AsyncWebSocketDomoMessage>(16);

        let async_tx_websocket_copy = async_tx_websocket.clone();

        let (sync_tx_websocket, sync_rx_websocket) =
            broadcast::channel::<SyncWebSocketDomoMessage>(16);

        let sync_tx_websocket_copy = sync_tx_websocket.clone();

        let app = Router::new()
            // `GET /` goes to `root`
            .route(
                "/get_all",
                get(WebApiManager::get_all_handler).layer(Extension(tx_get_all)),
            )
            .route(
                "/topic_name/:topic_name",
                get(WebApiManager::get_topicname_handler).layer(Extension(tx_get_topicname)),
            )
            .route(
                "/topic_name/:topic_name/topic_uuid/:topic_uuid",
                get(WebApiManager::get_topicname_topicuuid_handler)
                    .layer(Extension(tx_get_topicname_topicuuid)),
            )
            .route(
                "/topic_name/:topic_name/topic_uuid/:topic_uuid",
                post(WebApiManager::post_topicname_topicuuid_handler)
                    .layer(Extension(tx_post_topicname_topicuuid)),
            )
            .route(
                "/topic_name/:topic_name/topic_uuid/:topic_uuid",
                delete(WebApiManager::delete_topicname_topicuuid_handler)
                    .layer(Extension(tx_delete_topicname_topicuuid)),
            )
            .route(
                "/pub",
                post(WebApiManager::pub_message).layer(Extension(tx_pub_message)),
            )
            .route(
                "/ws",
                get(WebApiManager::handle_websocket_req)
                    .layer(Extension(async_tx_websocket_copy))
                    .layer(Extension(sync_tx_websocket_copy)),
            );

        tokio::spawn(async move {
            axum::Server::bind(&addr)
                .serve(app.into_make_service())
                .await
        });

        let webmanager = WebApiManager {
            http_port: http_port,
            sync_rx_websocket: sync_rx_websocket,
            sync_tx_websocket: sync_tx_websocket,
            rx_rest: rx_rest,
            async_tx_websocket: async_tx_websocket,
        };

        webmanager
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
}
