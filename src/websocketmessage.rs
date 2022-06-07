use serde::Serialize;
use std::result;

#[derive(Debug, Clone, Serialize)]
pub enum AsyncWebSocketDomoMessage {
    Volatile {
        value: serde_json::Value,
    },
    Persistent {
        value: serde_json::Value,
        topic_name: String,
        topic_uuid: String,
    },
}

#[derive(Debug, Clone, Serialize)]
pub enum SyncWebSocketDomoMessage {
    RequestGetAll {
        ws_client_id: String,
        req_id: String,
    },
    RequestGetTopicName {
        ws_client_id: String,
        topic_name: String,
        req_id: String,
    },
    RequestGetTopicUUID {
        ws_client_id: String,
        topic_name: String,
        topic_uuid: String,
        req_id: String,
    },
    RequestDeleteTopicUUID {
        ws_client_id: String,
        topic_name: String,
        topic_uuid: String,
        req_id: String,
    },
    RequestPubMessage {
        ws_client_id: String,
        value: serde_json::Value,
        req_id: String,
    },
    RequestPostTopicUUID {
        topic_name: String,
        topic_uuid: String,
        value: serde_json::Value,
        ws_client_id: String,
        req_id: String,
    },
    Response {
        value: serde_json::Value,
        ws_client_id: String,
        req_id: String,
    },
}
