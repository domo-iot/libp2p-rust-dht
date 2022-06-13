use crate::{
    restmessage, AsyncWebSocketDomoMessage, DomoCache, DomoEvent, SqliteStorage,
    SyncWebSocketDomoMessage, SyncWebSocketDomoRequest, WebApiManager,
};

use std::error::Error;

use serde_json::json;

pub struct DomoBroker {
    pub domo_cache: DomoCache<SqliteStorage>,
    pub web_manager: WebApiManager,
}

pub struct DomoBrokerConf {
    pub sqlite_file: String,
    pub is_persistent_cache: bool,
    pub shared_key: String,
    pub http_port: u16,
    pub loopback_only: bool,
}

impl DomoBroker {
    pub async fn new(conf: DomoBrokerConf) -> Result<Self, String> {
        if conf.sqlite_file == "" {
            return Err(String::from("sqlite_file path needed"));
        }

        let storage = SqliteStorage::new(conf.sqlite_file, conf.is_persistent_cache);

        let domo_cache = DomoCache::new(
            conf.is_persistent_cache,
            storage,
            conf.shared_key,
            conf.loopback_only,
        )
        .await;

        let web_manager = WebApiManager::new(conf.http_port);

        Ok(DomoBroker {
            domo_cache,
            web_manager,
        })
    }

    pub async fn event_loop(&mut self) -> DomoEvent {
        loop {
            tokio::select! {
                webs_message = self.web_manager.sync_rx_websocket.recv() => {
                    let message = webs_message.unwrap();
                    self.handle_websocket_sync_request(message).await;
                },

                Some(rest_message) = self.web_manager.rx_rest.recv() => {
                    self.handle_rest_request(rest_message).await;
                },

                m = self.domo_cache.cache_event_loop() => {
                    let ret = self.handle_cache_event_loop(m);
                    return ret;
                }
            }
        }
    }

    async fn handle_websocket_sync_request(&mut self, message: SyncWebSocketDomoMessage) {
        match message.request {
            SyncWebSocketDomoRequest::RequestGetAll => {
                println!("WebSocket RequestGetAll");

                let resp = SyncWebSocketDomoRequest::Response {
                    value: self.domo_cache.get_all(),
                };

                let r = SyncWebSocketDomoMessage {
                    ws_client_id: message.ws_client_id,
                    req_id: message.req_id,
                    request: resp,
                };

                let _ret = self.web_manager.sync_tx_websocket.send(r);
            }

            SyncWebSocketDomoRequest::RequestGetTopicName { topic_name } => {
                println!("WebSocket RequestGetTopicName");

                let ret = self.domo_cache.get_topic_name(&topic_name);

                let value = match ret {
                    Ok(m) => m,
                    Err(_e) => json!({}),
                };

                let resp = SyncWebSocketDomoRequest::Response { value };

                let r = SyncWebSocketDomoMessage {
                    ws_client_id: message.ws_client_id,
                    req_id: message.req_id,
                    request: resp,
                };

                let _ret = self.web_manager.sync_tx_websocket.send(r);
            }

            SyncWebSocketDomoRequest::RequestGetTopicUUID {
                topic_name,
                topic_uuid,
            } => {
                println!("WebSocket RequestGetTopicUUID");

                let ret = self.domo_cache.get_topic_uuid(&topic_name, &topic_uuid);
                let value = match ret {
                    Ok(m) => m,
                    Err(_e) => json!({}),
                };

                let resp = SyncWebSocketDomoRequest::Response { value };

                let r = SyncWebSocketDomoMessage {
                    ws_client_id: message.ws_client_id,
                    req_id: message.req_id,
                    request: resp,
                };

                let _ret = self.web_manager.sync_tx_websocket.send(r);
            }

            SyncWebSocketDomoRequest::RequestDeleteTopicUUID {
                topic_name,
                topic_uuid,
            } => {
                let _ret = self.domo_cache.delete_value(&topic_name, &topic_uuid).await;
                println!("WebSocket RequestDeleteTopicUUID");
            }

            SyncWebSocketDomoRequest::RequestPostTopicUUID {
                topic_name,
                topic_uuid,
                value,
            } => {
                println!("WebSocket RequestPostTopicUUID");

                let _ret = self
                    .domo_cache
                    .write_value(&topic_name, &topic_uuid, value.clone())
                    .await;
            }

            SyncWebSocketDomoRequest::RequestPubMessage { value } => {
                println!("WebSocket RequestPubMessage");
                let _ret = self.domo_cache.pub_value(value.clone()).await;
            }

            _ => {}
        }
    }

    async fn handle_rest_request(&mut self, rest_message: restmessage::RestMessage) {
        match rest_message {
            restmessage::RestMessage::GetAll { responder } => {
                let resp = self.domo_cache.get_all();
                match responder.send(Ok(resp)) {
                    Ok(_m) => log::debug!("Rest response ok"),
                    Err(_e) => log::debug!("Error while sending Rest Reponse"),
                }
            }
            restmessage::RestMessage::GetTopicName {
                topic_name,
                responder,
            } => {
                let resp = self.domo_cache.get_topic_name(&topic_name);
                if let Ok(resp) = resp {
                    match responder.send(Ok(resp)) {
                        Ok(_m) => log::debug!("Rest response ok"),
                        Err(_e) => log::debug!("Error while sending Rest Reponse"),
                    }
                }
            }
            restmessage::RestMessage::GetTopicUUID {
                topic_name,
                topic_uuid,
                responder,
            } => {
                let resp = self.domo_cache.get_topic_uuid(&topic_name, &topic_uuid);

                if let Ok(resp) = resp {
                    match responder.send(Ok(resp)) {
                        Ok(_m) => log::debug!("Rest response ok"),
                        Err(_e) => log::debug!("Error while sending Rest Reponse"),
                    }
                }
            }
            restmessage::RestMessage::PostTopicUUID {
                topic_name,
                topic_uuid,
                value,
                responder,
            } => {
                self.domo_cache
                    .write_value(&topic_name, &topic_uuid, value.clone())
                    .await;
                match responder.send(Ok(value)) {
                    Ok(_m) => log::debug!("Rest response ok"),
                    Err(_e) => log::debug!("Error while sending Rest Reponse"),
                }
            }
            restmessage::RestMessage::DeleteTopicUUID {
                topic_name,
                topic_uuid,
                responder,
            } => {
                self.domo_cache.delete_value(&topic_name, &topic_uuid).await;
                match responder.send(Ok(json!({}))) {
                    Ok(_m) => log::debug!("Rest response ok"),
                    Err(_e) => log::debug!("Error while sending Rest Reponse"),
                }
            }
            restmessage::RestMessage::PubMessage { value, responder } => {
                self.domo_cache.pub_value(value.clone()).await;
                match responder.send(Ok(value)) {
                    Ok(_m) => log::debug!("Rest response ok"),
                    Err(_e) => log::debug!("Error while sending Rest Reponse"),
                }
            }
        }
    }

    fn handle_cache_event_loop(&mut self, m: Result<DomoEvent, Box<dyn Error>>) -> DomoEvent {
        match m {
            Ok(DomoEvent::None) => DomoEvent::None,
            Ok(DomoEvent::PersistentData(m)) => {
                println!(
                    "Persistent message received {} {}",
                    m.topic_name, m.topic_uuid
                );

                let m2 = m.clone();
                let _ret = self.web_manager.async_tx_websocket.send(
                    AsyncWebSocketDomoMessage::Persistent {
                        topic_name: m.topic_name,
                        topic_uuid: m.topic_uuid,
                        value: m.value,
                        deleted: m.deleted,
                    },
                );
                DomoEvent::PersistentData(m2)
            }
            Ok(DomoEvent::VolatileData(m)) => {
                println!("Volatile message {}", m);

                let m2 = m.clone();
                let _ret = self
                    .web_manager
                    .async_tx_websocket
                    .send(AsyncWebSocketDomoMessage::Volatile { value: m });

                DomoEvent::VolatileData(m2)
            }
            _ => DomoEvent::None,
        }
    }
}

mod tests {
    use tokio::sync::mpsc;

    #[cfg(test)]
    #[tokio::test]
    async fn domo_broker() {
        let _remove = std::fs::remove_file("/tmp/test_domo_broker.sqlite");

        let domo_broker_conf = super::DomoBrokerConf {
            sqlite_file: String::from("/tmp/test_domo_broker.sqlite"),
            is_persistent_cache: true,
            shared_key: String::from(
                "d061545647652562b4648f52e8373b3a417fc0df56c332154460da1801b341e9",
            ),
            http_port: 3000,
            loopback_only: false,
        };

        let mut domo_broker = super::DomoBroker::new(domo_broker_conf).await.unwrap();

        let (tx_rest, mut rx_rest) = mpsc::channel(1);

        let hnd = tokio::spawn(async move {
            let http_call = reqwest::get("http://localhost:3000/get_all").await.unwrap();

            let text: serde_json::Value =
                serde_json::from_str(&http_call.text().await.unwrap()).unwrap();

            assert_eq!(text, serde_json::json!({}));

            let _ret = tx_rest.send("Done").await;
        });

        tokio::select! {
            _m = domo_broker.event_loop() => {},
            _waiting = rx_rest.recv() => {
                println!("Test finished");
            }
        }

        let _ret = hnd.await;
    }
}
