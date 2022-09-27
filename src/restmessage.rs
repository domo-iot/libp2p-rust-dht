use tokio::sync::oneshot;

type RestResponder = oneshot::Sender<Result<serde_json::Value, String>>;

#[derive(Debug)]
pub enum RestMessage {
    GetAll {
        responder: RestResponder,
    },
    GetTopicName {
        topic_name: String,
        responder: RestResponder,
    },
    GetTopicUUID {
        topic_name: String,
        topic_uuid: String,
        responder: RestResponder,
    },
    DeleteTopicUUID {
        topic_name: String,
        topic_uuid: String,
        responder: RestResponder,
    },
    PostTopicUUID {
        topic_name: String,
        topic_uuid: String,
        value: serde_json::Value,
        responder: RestResponder,
    },
    PubMessage {
        value: serde_json::Value,
        responder: RestResponder,
    },
}
