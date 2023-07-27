//! Cached access to the DHT
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::{Display, Formatter};

/// Events returned by [DomoCache::cache_event_loop]
#[derive(Debug)]
pub enum DomoEvent {
    None,
    VolatileData(serde_json::Value),
    PersistentData(DomoCacheElement),
}

/// Full Cache Element
#[derive(Default, Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct DomoCacheElement {
    /// Free-form topic name
    pub topic_name: String,
    /// Unique identifier of the element
    pub topic_uuid: String,
    /// JSON-serializable Value
    pub value: Value,
    /// If true the element could be expunged from the local cache
    pub deleted: bool,
    /// Time of the first pubblication
    pub publication_timestamp: u128,
    /// First peer publishing it
    pub publisher_peer_id: String,
    /// If non-zero the element is republished as part of a cache sync
    pub republication_timestamp: u128,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub(crate) struct DomoCacheStateMessage {
    pub peer_id: String,
    pub cache_hash: u64,
    pub publication_timestamp: u128,
}

impl Display for DomoCacheElement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(topic_name: {}, topic_uuid:{}, \
            value: {}, deleted: {}, publication_timestamp: {}, \
            peer_id: {})",
            self.topic_name,
            self.topic_uuid,
            self.value,
            self.deleted,
            self.publication_timestamp,
            self.publisher_peer_id
        )
    }
}
