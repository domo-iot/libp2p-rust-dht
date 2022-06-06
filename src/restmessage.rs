use std::result;
use tokio::sync::oneshot;

type RestResponder = oneshot::Sender<serde_json::Value>;

#[derive(Debug)]
pub enum RestMessage {
    GetAll {responder: RestResponder},
    GetTopicName{topic_name: String, responder: RestResponder},
    GetTopicUUID{topic_name: String, topic_uuid: String, responder: RestResponder}
}

