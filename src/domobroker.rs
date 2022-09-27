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
        if conf.sqlite_file.is_empty() {
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

                let value = ret.unwrap_or_else(|_| json!([]));

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
                let value = ret.unwrap_or_else(|_| json!({}));

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
                self.domo_cache.delete_value(&topic_name, &topic_uuid).await;
                println!("WebSocket RequestDeleteTopicUUID");
            }

            SyncWebSocketDomoRequest::RequestPostTopicUUID {
                topic_name,
                topic_uuid,
                value,
            } => {
                println!("WebSocket RequestPostTopicUUID");

                self.domo_cache
                    .write_value(&topic_name, &topic_uuid, value.clone())
                    .await;
            }

            SyncWebSocketDomoRequest::RequestPubMessage { value } => {
                println!("WebSocket RequestPubMessage");
                self.domo_cache.pub_value(value.clone()).await;
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
                } else {
                    let resp = json!([]);
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
                } else {
                    let resp = json!({});
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
                println!("Volatile message {m}");

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

#[cfg(test)]
mod tests {
    use crate::DomoBroker;

    async fn setup_broker(sqlite_file: &str, http_port: u16) -> DomoBroker {
        let _remove = std::fs::remove_file(sqlite_file);

        let domo_broker_conf = super::DomoBrokerConf {
            sqlite_file: sqlite_file.to_owned(),
            is_persistent_cache: true,
            shared_key: String::from(
                "d061545647652562b4648f52e8373b3a417fc0df56c332154460da1801b341e9",
            ),
            http_port,
            loopback_only: false,
        };

        let domo_broker = super::DomoBroker::new(domo_broker_conf).await.unwrap();

        domo_broker
    }

    #[tokio::test]
    async fn domo_broker_empty_cache() {
        use tokio::sync::mpsc;

        let mut domo_broker = setup_broker("/tmp/test_domo_broker_empty_cache.sqlite", 3000).await;

        let (tx_rest, mut rx_rest) = mpsc::channel(1);

        let hnd = tokio::spawn(async move {
            let http_call = reqwest::get("http://localhost:3000/get_all").await.unwrap();

            let result: serde_json::Value =
                serde_json::from_str(&http_call.text().await.unwrap()).unwrap();

            let _ret = tx_rest.send(result).await;
        });

        let mut stopped = false;
        while !stopped {
            tokio::select! {
                _m = domo_broker.event_loop() => {},
                result = rx_rest.recv() => {
                    let result = result.unwrap();
                    stopped = true;
                    assert_eq!(result, serde_json::json!([]));
                }
            }
        }

        let _ret = hnd.await;
    }

    #[tokio::test]
    async fn domo_broker_rest_get_all() {
        use tokio::sync::mpsc;

        let mut domo_broker = setup_broker("/tmp/test_domo_broker_get_all.sqlite", 3001).await;

        domo_broker
            .domo_cache
            .write_value("Domo::Light", "uno", serde_json::json!({"connected": true}))
            .await;

        domo_broker
            .domo_cache
            .write_value("Domo::Light", "due", serde_json::json!({"connected": true}))
            .await;

        let (tx_rest, mut rx_rest) = mpsc::channel(1);

        let hnd = tokio::spawn(async move {
            let http_call = reqwest::get("http://localhost:3001/get_all").await.unwrap();

            let result: serde_json::Value =
                serde_json::from_str(&http_call.text().await.unwrap()).unwrap();
            let _ret = tx_rest.send(result).await;
        });

        let mut stopped = false;

        while !stopped {
            tokio::select! {
                _m = domo_broker.event_loop() => {

                },
                result = rx_rest.recv() => {

                    let result = result.unwrap();

                    assert_eq!(
                        result,
                        serde_json::json!([
                            {
                                "topic_name": "Domo::Light",
                                "topic_uuid": "due",
                                "value": {
                                    "connected": true
                                }
                            },
                            {
                                "topic_name": "Domo::Light",
                                "topic_uuid": "uno",
                                "value": {
                                    "connected": true
                                }
                            }
                        ])
                    );
                    stopped = true;
                }
            }
        }

        let _ret = hnd.await;
    }

    #[tokio::test]
    async fn domo_broker_rest_get_topicname() {
        use tokio::sync::mpsc;

        let mut domo_broker =
            setup_broker("/tmp/test_domo_broker_get_topicname.sqlite", 3002).await;

        domo_broker
            .domo_cache
            .write_value("Domo::Light", "uno", serde_json::json!({"connected": true}))
            .await;

        domo_broker
            .domo_cache
            .write_value(
                "Domo::Socket",
                "due",
                serde_json::json!({"connected": true}),
            )
            .await;

        let (tx_rest, mut rx_rest) = mpsc::channel(1);

        let hnd = tokio::spawn(async move {
            let http_call = reqwest::get("http://localhost:3002/topic_name/Domo::Light")
                .await
                .unwrap();

            let result: serde_json::Value =
                serde_json::from_str(&http_call.text().await.unwrap()).unwrap();
            let _ret = tx_rest.send(result).await;
        });

        let mut stopped = false;

        while !stopped {
            tokio::select! {
                _m = domo_broker.event_loop() => {

                },
                result = rx_rest.recv() => {

                    let result = result.unwrap();

                    assert_eq!(
                        result,
                        serde_json::json!([
                            {
                                "topic_name": "Domo::Light",
                                "topic_uuid": "uno",
                                "value": {
                                    "connected": true
                                }
                            }
                        ])
                    );
                    stopped = true;
                }
            }
        }

        let _ret = hnd.await;
    }

    #[tokio::test]
    async fn domo_broker_rest_get_topicuuid() {
        use tokio::sync::mpsc;

        let mut domo_broker =
            setup_broker("/tmp/test_domo_broker_get_topicuuid.sqlite", 3003).await;

        domo_broker
            .domo_cache
            .write_value("Domo::Light", "uno", serde_json::json!({"connected": true}))
            .await;

        domo_broker
            .domo_cache
            .write_value(
                "Domo::Socket",
                "due",
                serde_json::json!({"connected": true}),
            )
            .await;

        let (tx_rest, mut rx_rest) = mpsc::channel(1);

        let hnd = tokio::spawn(async move {
            let http_call =
                reqwest::get("http://localhost:3003/topic_name/Domo::Light/topic_uuid/uno")
                    .await
                    .unwrap();

            let result: serde_json::Value =
                serde_json::from_str(&http_call.text().await.unwrap()).unwrap();
            let _ret = tx_rest.send(result).await;
        });

        let mut stopped = false;

        while !stopped {
            tokio::select! {
                _m = domo_broker.event_loop() => {

                },
                result = rx_rest.recv() => {

                    let result = result.unwrap();

                    assert_eq!(
                        result,
                        serde_json::json!(
                            {
                                "topic_name": "Domo::Light",
                                "topic_uuid": "uno",
                                "value": {
                                    "connected": true
                                }
                            }
                        )
                    );
                    stopped = true;
                }
            }
        }

        let _ret = hnd.await;
    }

    #[tokio::test]
    async fn domo_broker_rest_get_topicname_not_present() {
        use tokio::sync::mpsc;

        let mut domo_broker = setup_broker(
            "/tmp/test_domo_broker_get_topicname_not_present.sqlite",
            3004,
        )
        .await;

        domo_broker
            .domo_cache
            .write_value("Domo::Light", "uno", serde_json::json!({"connected": true}))
            .await;

        let (tx_rest, mut rx_rest) = mpsc::channel(1);

        let hnd = tokio::spawn(async move {
            let http_call = reqwest::get("http://localhost:3004/topic_name/Domo::Not")
                .await
                .unwrap();

            let result: serde_json::Value =
                serde_json::from_str(&http_call.text().await.unwrap()).unwrap();
            let _ret = tx_rest.send(result).await;
        });

        let mut stopped = false;

        while !stopped {
            tokio::select! {
                _m = domo_broker.event_loop() => {

                },
                result = rx_rest.recv() => {

                    let result = result.unwrap();

                    assert_eq!(
                        result,
                        serde_json::json!([])
                    );
                    stopped = true;
                }
            }
        }

        let _ret = hnd.await;
    }

    #[tokio::test]
    async fn domo_broker_rest_get_topicuuid_not_present() {
        use tokio::sync::mpsc;

        let mut domo_broker = setup_broker(
            "/tmp/test_domo_broker_get_topicuuid_not_present.sqlite",
            3005,
        )
        .await;

        domo_broker
            .domo_cache
            .write_value("Domo::Light", "uno", serde_json::json!({"connected": true}))
            .await;

        let (tx_rest, mut rx_rest) = mpsc::channel(1);

        let hnd = tokio::spawn(async move {
            let http_call =
                reqwest::get("http://localhost:3005/topic_name/Domo::Light/topic_uuid/due")
                    .await
                    .unwrap();

            let result: serde_json::Value =
                serde_json::from_str(&http_call.text().await.unwrap()).unwrap();
            let _ret = tx_rest.send(result).await;
        });

        let mut stopped = false;

        while !stopped {
            tokio::select! {
                _m = domo_broker.event_loop() => {

                },
                result = rx_rest.recv() => {

                    let result = result.unwrap();

                    assert_eq!(
                        result,
                        serde_json::json!({})
                    );
                    stopped = true;
                }
            }
        }

        let _ret = hnd.await;
    }

    #[tokio::test]
    async fn domo_broker_rest_post_test() {
        use std::collections::HashMap;
        use tokio::sync::mpsc;

        let mut domo_broker = setup_broker("/tmp/test_domo_broker_post_test.sqlite", 3006).await;

        let (tx_rest, mut rx_rest) = mpsc::channel(1);

        let hnd = tokio::spawn(async move {
            let mut body = HashMap::new();
            body.insert("connected", true);

            let client = reqwest::Client::new();

            let _http_call = client
                .post("http://localhost:3006/topic_name/Domo::Light/topic_uuid/uno")
                .json(&body)
                .send()
                .await
                .unwrap();

            let _ret = tx_rest.send("done").await;
        });

        let mut stopped = false;

        while !stopped {
            tokio::select! {
                _m = domo_broker.event_loop() => {

                },
                _result = rx_rest.recv() => {

                    let ret = domo_broker.domo_cache.get_topic_uuid("Domo::Light", "uno");

                    match ret {
                        Ok(m) =>  {
                           assert_eq!(m, serde_json::json!(
                                {
                                    "topic_name": "Domo::Light",
                                    "topic_uuid": "uno",
                                    "value": {
                                        "connected": true
                                    }
                                }
                            ));
                        },
                        Err(_e) => assert_eq!(true, false)
                    }

                    stopped = true;
                }
            }
        }

        let _ret = hnd.await;
    }

    #[tokio::test]
    async fn domo_broker_rest_delete_test() {
        use tokio::sync::mpsc;

        let mut domo_broker = setup_broker("/tmp/test_domo_broker_delete_test.sqlite", 3007).await;

        domo_broker
            .domo_cache
            .write_value("Domo::Light", "uno", serde_json::json!({"connected": true}))
            .await;

        let (tx_rest, mut rx_rest) = mpsc::channel(1);

        let hnd = tokio::spawn(async move {
            let client = reqwest::Client::new();

            let _http_call = client
                .delete("http://localhost:3007/topic_name/Domo::Light/topic_uuid/uno")
                .send()
                .await
                .unwrap();

            let _ret = tx_rest.send("done").await;
        });

        let mut stopped = false;

        while !stopped {
            tokio::select! {
                _m = domo_broker.event_loop() => {

                },
                _result = rx_rest.recv() => {

                    let ret = domo_broker.domo_cache.get_topic_uuid("Domo::Light", "uno");

                    match ret {
                        Ok(m) =>  {
                           assert_eq!(m, serde_json::json!(
                                {}
                            ));
                        },
                        Err(_e) => assert_eq!(true, false)
                    }

                    stopped = true;
                }
            }
        }

        let _ret = hnd.await;
    }

    #[tokio::test]
    async fn domo_broker_rest_pub_test() {
        use crate::domocache::DomoEvent;
        use std::collections::HashMap;
        use tokio::sync::mpsc;

        let mut domo_broker = setup_broker("/tmp/test_domo_broker_pub_test.sqlite", 3008).await;

        let (tx_rest, mut rx_rest) = mpsc::channel(1);

        let hnd = tokio::spawn(async move {
            let mut body = HashMap::new();
            body.insert("message", "hello");

            let client = reqwest::Client::new();

            let _http_call = client
                .post("http://localhost:3008/pub")
                .json(&body)
                .send()
                .await
                .unwrap();

            let _ret = tx_rest.send("done").await;
        });

        let mut stopped = false;

        while !stopped {
            tokio::select! {
                m = domo_broker.event_loop() => {

                    match m {
                        DomoEvent::VolatileData( value ) => {
                            assert_eq!(value, serde_json::json!({"message": "hello"}));
                        },
                        _ => {}
                    }
                },
                _result = rx_rest.recv() => {
                    stopped = true;
                }
            }
        }

        let _ret = hnd.await;
    }

    #[tokio::test]
    async fn domo_broker_test_websocket_empty() {
        use futures_util::{SinkExt, StreamExt};

        use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

        use tokio::sync::mpsc;

        let (tx_rest, mut rx_rest) = mpsc::channel(1);

        let mut domo_broker =
            setup_broker("/tmp/test_domo_broker_test_websocket_empty.sqlite", 3009).await;

        tokio::spawn(async move {
            let url = url::Url::parse("ws://localhost:3009/ws").unwrap();

            let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

            let (mut write, mut read) = ws_stream.split();

            let _ret = write
                .send(Message::Text("\"RequestGetAll\"".to_owned()))
                .await;

            let msg = read.next().await.unwrap().unwrap();

            match msg {
                Message::Text(text) => {
                    let expected = serde_json::json!(
                        {
                            "Response": {
                                "value": []
                            }
                        }
                    );

                    let rcv_value: serde_json::Value = serde_json::from_str(&text).unwrap();

                    if rcv_value == expected {
                        let _ret = tx_rest.send("OK").await;
                    }
                }
                _ => {}
            }
        });

        let mut results_received = false;
        while !results_received {
            tokio::select! {
                result = rx_rest.recv() => {
                    results_received = true;

                    let result = result.unwrap();
                    assert_eq!(result, "OK");
                },
                _m = domo_broker.event_loop() => {

                }
            }
        }
    }

    #[tokio::test]
    async fn domo_broker_test_websocket_getall() {
        use futures_util::{SinkExt, StreamExt};

        use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

        use tokio::sync::mpsc;

        let (tx_rest, mut rx_rest) = mpsc::channel(1);

        let mut domo_broker =
            setup_broker("/tmp/test_domo_broker_test_websocket_getall.sqlite", 3010).await;

        domo_broker
            .domo_cache
            .write_value("Domo::Light", "uno", serde_json::json!({"connected": true}))
            .await;

        domo_broker
            .domo_cache
            .write_value("Domo::Light", "due", serde_json::json!({"connected": true}))
            .await;

        tokio::spawn(async move {
            let url = url::Url::parse("ws://localhost:3010/ws").unwrap();

            let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

            let (mut write, mut read) = ws_stream.split();

            let _ret = write
                .send(Message::Text("\"RequestGetAll\"".to_owned()))
                .await;

            let msg = read.next().await.unwrap().unwrap();

            match msg {
                Message::Text(text) => {
                    let expected = serde_json::json!(
                        {
                            "Response": {
                                "value": [

                                {
                                    "topic_name": "Domo::Light",
                                    "topic_uuid": "due",
                                    "value": {
                                        "connected": true
                                    }
                                },
                                {
                                    "topic_name": "Domo::Light",
                                    "topic_uuid": "uno",
                                    "value": {
                                        "connected": true
                                    }
                                }

                                ]
                            }
                        }
                    );

                    let rcv_value: serde_json::Value = serde_json::from_str(&text).unwrap();

                    if rcv_value == expected {
                        let _ret = tx_rest.send("OK").await;
                    }
                }
                _ => {}
            }
        });

        let mut results_received = false;
        while !results_received {
            tokio::select! {
                result = rx_rest.recv() => {
                    results_received = true;

                    let result = result.unwrap();
                    assert_eq!(result, "OK");
                },
                _m = domo_broker.event_loop() => {

                }
            }
        }
    }

    #[tokio::test]
    async fn domo_broker_test_websocket_get_topicname() {
        use futures_util::{SinkExt, StreamExt};

        use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

        use crate::SyncWebSocketDomoRequest;

        use tokio::sync::mpsc;

        let (tx_rest, mut rx_rest) = mpsc::channel(1);

        let mut domo_broker = setup_broker(
            "/tmp/test_domo_broker_test_websocket_get_topicname.sqlite",
            3011,
        )
        .await;

        domo_broker
            .domo_cache
            .write_value("Domo::Light", "uno", serde_json::json!({"connected": true}))
            .await;

        domo_broker
            .domo_cache
            .write_value(
                "Domo::Socket",
                "due",
                serde_json::json!({"connected": true}),
            )
            .await;

        tokio::spawn(async move {
            let url = url::Url::parse("ws://localhost:3011/ws").unwrap();

            let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

            let (mut write, mut read) = ws_stream.split();

            let req = serde_json::to_string(&SyncWebSocketDomoRequest::RequestGetTopicName {
                topic_name: "Domo::Light".to_string(),
            })
            .unwrap();

            let _ret = write.send(Message::Text(req)).await;

            let msg = read.next().await.unwrap().unwrap();

            match msg {
                Message::Text(text) => {
                    let expected = serde_json::json!(
                        {
                            "Response": {
                                "value": [
                                {
                                    "topic_name": "Domo::Light",
                                    "topic_uuid": "uno",
                                    "value": {
                                        "connected": true
                                    }
                                }

                                ]
                            }
                        }
                    );

                    let rcv_value: serde_json::Value = serde_json::from_str(&text).unwrap();

                    if rcv_value == expected {
                        let _ret = tx_rest.send("OK").await;
                    }
                }
                _ => {}
            }
        });

        let mut results_received = false;
        while !results_received {
            tokio::select! {
                result = rx_rest.recv() => {
                    results_received = true;

                    let result = result.unwrap();
                    assert_eq!(result, "OK");
                },
                _m = domo_broker.event_loop() => {

                }
            }
        }
    }

    #[tokio::test]
    async fn domo_broker_test_websocket_get_topicuuid() {
        use futures_util::{SinkExt, StreamExt};

        use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

        use crate::SyncWebSocketDomoRequest;

        use tokio::sync::mpsc;

        let (tx_rest, mut rx_rest) = mpsc::channel(1);

        let mut domo_broker = setup_broker(
            "/tmp/test_domo_broker_test_websocket_get_topicuuid.sqlite",
            3012,
        )
        .await;

        domo_broker
            .domo_cache
            .write_value("Domo::Light", "uno", serde_json::json!({"connected": true}))
            .await;

        domo_broker
            .domo_cache
            .write_value(
                "Domo::Socket",
                "due",
                serde_json::json!({"connected": true}),
            )
            .await;

        tokio::spawn(async move {
            let url = url::Url::parse("ws://localhost:3012/ws").unwrap();

            let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

            let (mut write, mut read) = ws_stream.split();

            let req = serde_json::to_string(&SyncWebSocketDomoRequest::RequestGetTopicUUID {
                topic_name: "Domo::Light".to_string(),
                topic_uuid: "uno".to_string(),
            })
            .unwrap();

            let _ret = write.send(Message::Text(req)).await;

            let msg = read.next().await.unwrap().unwrap();

            match msg {
                Message::Text(text) => {
                    let expected = serde_json::json!(
                        {
                            "Response": {
                                "value":
                                {
                                    "topic_name": "Domo::Light",
                                    "topic_uuid": "uno",
                                    "value": {
                                        "connected": true
                                    }
                                }

                            }
                        }
                    );

                    let rcv_value: serde_json::Value = serde_json::from_str(&text).unwrap();

                    if rcv_value == expected {
                        let _ret = tx_rest.send("OK").await;
                    }
                }
                _ => {}
            }
        });

        let mut results_received = false;
        while !results_received {
            tokio::select! {
                result = rx_rest.recv() => {
                    results_received = true;

                    let result = result.unwrap();
                    assert_eq!(result, "OK");
                },
                _m = domo_broker.event_loop() => {

                }
            }
        }
    }

    #[tokio::test]
    async fn domo_broker_test_websocket_post() {
        use futures_util::{SinkExt, StreamExt};

        use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

        use crate::{AsyncWebSocketDomoMessage, SyncWebSocketDomoRequest};

        use tokio::sync::mpsc;

        let (tx_rest, mut rx_rest) = mpsc::channel(1);

        let mut domo_broker =
            setup_broker("/tmp/test_domo_broker_test_websocket_post.sqlite", 3013).await;

        tokio::spawn(async move {
            let url = url::Url::parse("ws://localhost:3013/ws").unwrap();

            let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

            let (mut write, mut read) = ws_stream.split();

            let req = serde_json::to_string(&SyncWebSocketDomoRequest::RequestPostTopicUUID {
                topic_name: "Domo::Light".to_string(),
                topic_uuid: "uno".to_string(),
                value: serde_json::json!({"connected": true}),
            })
            .unwrap();

            let _ret = write.send(Message::Text(req)).await;

            let msg = read.next().await.unwrap().unwrap();

            match msg {
                Message::Text(text) => {
                    let expected = serde_json::to_string(&AsyncWebSocketDomoMessage::Persistent {
                        topic_name: "Domo::Light".to_owned(),
                        topic_uuid: "uno".to_owned(),
                        value: serde_json::json!({"connected": true}),
                        deleted: false,
                    })
                    .unwrap();

                    if text == expected {
                        let _ret = tx_rest.send("OK").await;
                    }
                }
                _ => {}
            }
        });

        let mut results_received = false;
        while !results_received {
            tokio::select! {
                result = rx_rest.recv() => {
                    results_received = true;

                    let result = result.unwrap();
                    assert_eq!(result, "OK");
                },
                _m = domo_broker.event_loop() => {

                }
            }
        }
    }

    #[tokio::test]
    async fn domo_broker_test_websocket_delete() {
        use futures_util::{SinkExt, StreamExt};

        use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

        use crate::{AsyncWebSocketDomoMessage, SyncWebSocketDomoRequest};

        use tokio::sync::mpsc;

        let (tx_rest, mut rx_rest) = mpsc::channel(1);

        let mut domo_broker =
            setup_broker("/tmp/test_domo_broker_test_websocket_delete.sqlite", 3014).await;

        domo_broker
            .domo_cache
            .write_value("Domo::Light", "uno", serde_json::json!({"connected": true}))
            .await;

        tokio::spawn(async move {
            let url = url::Url::parse("ws://localhost:3014/ws").unwrap();

            let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

            let (mut write, mut read) = ws_stream.split();

            let req = serde_json::to_string(&SyncWebSocketDomoRequest::RequestDeleteTopicUUID {
                topic_name: "Domo::Light".to_string(),
                topic_uuid: "uno".to_string(),
            })
            .unwrap();

            let _ret = write.send(Message::Text(req)).await;

            let msg = read.next().await.unwrap().unwrap();

            match msg {
                Message::Text(text) => {
                    let expected = serde_json::to_string(&AsyncWebSocketDomoMessage::Persistent {
                        topic_name: "Domo::Light".to_owned(),
                        topic_uuid: "uno".to_owned(),
                        value: serde_json::Value::Null,
                        deleted: true,
                    })
                    .unwrap();

                    if text == expected {
                        let _ret = tx_rest.send("OK").await;
                    }
                }
                _ => {}
            }
        });

        let mut results_received = false;
        while !results_received {
            tokio::select! {
                result = rx_rest.recv() => {
                    results_received = true;

                    let result = result.unwrap();
                    assert_eq!(result, "OK");
                },
                _m = domo_broker.event_loop() => {

                }
            }
        }
    }

    #[tokio::test]
    async fn domo_broker_test_websocket_pub() {
        use futures_util::{SinkExt, StreamExt};

        use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

        use crate::{AsyncWebSocketDomoMessage, SyncWebSocketDomoRequest};

        use tokio::sync::mpsc;

        let (tx_rest, mut rx_rest) = mpsc::channel(1);

        let mut domo_broker =
            setup_broker("/tmp/test_domo_broker_test_websocket_pub.sqlite", 3015).await;

        tokio::spawn(async move {
            let url = url::Url::parse("ws://localhost:3015/ws").unwrap();

            let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

            let (mut write, mut read) = ws_stream.split();

            let req = serde_json::to_string(&SyncWebSocketDomoRequest::RequestPubMessage {
                value: serde_json::json!({"message": "hello"}),
            })
            .unwrap();

            let _ret = write.send(Message::Text(req)).await;

            let msg = read.next().await.unwrap().unwrap();

            match msg {
                Message::Text(text) => {
                    let expected = serde_json::to_string(&AsyncWebSocketDomoMessage::Volatile {
                        value: serde_json::json!({"message": "hello"}),
                    })
                    .unwrap();

                    if text == expected {
                        let _ret = tx_rest.send("OK").await;
                    }
                }
                _ => {}
            }
        });

        let mut results_received = false;
        while !results_received {
            tokio::select! {
                result = rx_rest.recv() => {
                    results_received = true;

                    let result = result.unwrap();
                    assert_eq!(result, "OK");
                },
                _m = domo_broker.event_loop() => {

                }
            }
        }
    }
}
