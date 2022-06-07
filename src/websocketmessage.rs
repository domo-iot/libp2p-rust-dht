use std::result;

#[derive(Debug, Clone)]
pub enum WebSocketDomoMessage {
    Volatile {
        value: serde_json::Value,
    },
    Persistent {
        value: serde_json::Value,
        topic_name: String,
        topic_uuid: String,
    },
}
