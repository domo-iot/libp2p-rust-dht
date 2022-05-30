use async_std::task;
use chrono::prelude::*;
use futures::prelude::*;
use futures::{prelude::*, select};
use itertools::Itertools;
use libp2p::gossipsub::IdentTopic as Topic;
use libp2p::swarm::SwarmEvent;
use rusqlite::{params, Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn get_epoch_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

pub trait DomoPersistentStorage {
    fn store(&mut self, element: &DomoCacheElement);
    fn get_all_elements(&mut self) -> Vec<DomoCacheElement>;
}

pub struct SqliteStorage {
    pub house_uuid: String,
    pub sqlite_file: String,
    pub sqlite_connection: Connection,
}

impl SqliteStorage {
    pub fn new(house_uuid: &str, sqlite_file: &str, write_access: bool) -> Self {
        let conn = if write_access == false {
            match Connection::open_with_flags(sqlite_file, OpenFlags::SQLITE_OPEN_READ_ONLY) {
                Ok(conn) => conn,
                _ => {
                    panic!("Error while opening the sqlite DB");
                }
            }
        } else {
            let conn = match Connection::open(sqlite_file) {
                Ok(conn) => conn,
                _ => {
                    panic!("Error while opening the sqlite DB");
                }
            };

            let _ = conn
                .execute(
                    "CREATE TABLE IF NOT EXISTS domo_data (
                  topic_name             TEXT,
                  topic_uuid             TEXT,
                  value                  TEXT,
                  deleted                INTEGER,
                  publication_timestamp   TEXT,
                  publisher_peer_id       TEXT,
                  PRIMARY KEY (topic_name, topic_uuid)
                  )",
                    [],
                )
                .unwrap();

            conn
        };

        SqliteStorage {
            house_uuid: house_uuid.to_owned(),
            sqlite_file: sqlite_file.to_owned(),
            sqlite_connection: conn,
        }
    }
}

impl DomoPersistentStorage for SqliteStorage {
    fn store(&mut self, element: &DomoCacheElement) {
        let _ = match self.sqlite_connection.execute(
            "INSERT OR REPLACE INTO domo_data\
             (topic_name, topic_uuid, value, deleted, publication_timestamp, publisher_peer_id)\
              VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                element.topic_name,
                element.topic_uuid,
                element.value.to_string(),
                element.deleted,
                element.publication_timestamp.to_string(),
                element.publisher_peer_id
            ],
        ) {
            Ok(ret) => ret,
            _ => {
                panic!("Error while executing write operation on sqlite")
            }
        };
    }

    fn get_all_elements(&mut self) -> Vec<DomoCacheElement> {
        // read all not deleted elements
        let mut stmt = self
            .sqlite_connection
            .prepare("SELECT * FROM domo_data WHERE deleted=0")
            .unwrap();

        let values_iter = stmt
            .query_map([], |row| {
                let jvalue: String = row.get(2)?;
                let jvalue = serde_json::from_str(&jvalue);

                let pub_timestamp_string: String = row.get(4)?;

                Ok(DomoCacheElement {
                    topic_name: row.get(0)?,
                    topic_uuid: row.get(1)?,
                    value: jvalue.unwrap(),
                    deleted: row.get(3)?,
                    publication_timestamp: pub_timestamp_string.parse().unwrap(),
                    publisher_peer_id: row.get(5)?,
                })
            })
            .unwrap();

        let mut ret = vec![];
        for val in values_iter {
            let v = val.unwrap();
            ret.push(v);
        }

        ret
    }
}

pub trait DomoCacheOperations {
    // method to write a DomoCacheElement
    fn insert_cache_element(
        &mut self,
        cache_element: DomoCacheElement,
        persist: bool,
        publish: bool,
    );

    // method to read a DomoCacheElement
    fn read_cache_element(
        &mut self,
        topic_name: &str,
        topic_uuid: &str,
    ) -> Option<DomoCacheElement>;

    // method to write a value in cache, with no timestamp check
    // the publication_timestamp is generated by the method itself
    fn write_value(&mut self, topic_name: &str, topic_uuid: &str, value: Value);

    // method to write a value checking timestamp
    // returns the value in cache if it is more recent that the one used while calling the method
    fn write_with_timestamp_check(
        &mut self,
        topic_name: &str,
        topic_uuid: &str,
        elem: DomoCacheElement,
    ) -> Option<DomoCacheElement>;
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct DomoCacheElement {
    pub topic_name: String,
    pub topic_uuid: String,
    pub value: Value,
    pub deleted: bool,
    pub publication_timestamp: u128,
    pub publisher_peer_id: String,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct DomoCacheStateMessage {
    pub peer_id: String,
    pub cache_hash: u64,
    pub publication_timestamp: u128,
}

impl Display for DomoCacheElement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "({}, {}, {}, {}, {}, {})",
            self.topic_name,
            self.topic_uuid,
            self.value.to_string(),
            self.deleted,
            self.publication_timestamp,
            self.publisher_peer_id
        )
    }
}

pub struct DomoCache<T: DomoPersistentStorage> {
    pub house_uuid: String,
    pub is_persistent_cache: bool,
    pub storage: T,
    pub cache: BTreeMap<String, BTreeMap<String, DomoCacheElement>>,
    pub peers_caches_state: BTreeMap<String, DomoCacheStateMessage>,
    pub swarm: libp2p::Swarm<crate::domolibp2p::DomoBehaviour>,
    pub local_peer_id: String,
}

impl<T: DomoPersistentStorage> Hash for DomoCache<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for (topic_name, map_topic_name) in self.cache.iter() {
            topic_name.hash(state);

            for (topic_uuid, value) in map_topic_name.iter() {
                topic_uuid.hash(state);
                value.to_string().hash(state);
            }
        }
    }
}

impl<T: DomoPersistentStorage> DomoCache<T> {
    fn handle_message_data(
        &mut self,
        message: &str,
    ) -> std::result::Result<DomoCacheElement, Box<dyn Error>> {
        let m: DomoCacheElement = serde_json::from_str(message)?;

        let topic_name = m.topic_name.clone();
        let topic_uuid = m.topic_uuid.clone();

        match self.write_with_timestamp_check(&topic_name, &topic_uuid, m.clone()) {
            None => {
                println!("New message received");
                return Ok(m);
            }
            _ => {
                println!("Old message received");
                return Ok(m);
            }
        }
    }

    fn is_syncrhonized(&self, peer_map: &BTreeMap<String, DomoCacheStateMessage>) -> bool {
        let local_hash = self.get_cache_hash();
        let fil: Vec<u64> = self
            .peers_caches_state
            .iter()
            .filter(|(peer_id, data)| (data.cache_hash != local_hash))
            .map(|(peer_id, data)| data.cache_hash)
            .collect();

        // se ci sono hashes diversi dal mio non è consistente
        if fil.len() > 0 {
            return false;
        }
        true
    }

    fn handle_config_data(&mut self, message: &str) {
        let m: DomoCacheStateMessage = serde_json::from_str(message).unwrap();
        self.peers_caches_state.insert(m.peer_id.clone(), m);

        if !self.is_syncrhonized(&self.peers_caches_state) {
            println!("Caches are not synchronized");
        }
    }

    fn send_cache_state(&mut self) {
        let m = DomoCacheStateMessage {
            peer_id: self.local_peer_id.to_string(),
            cache_hash: self.get_cache_hash(),
            publication_timestamp: get_epoch_ms(),
        };

        let topic = Topic::new("domo-config");

        let m = serde_json::to_string(&m).unwrap();

        if let Err(e) = self
            .swarm
            .behaviour_mut()
            .gossipsub
            .publish(topic.clone(), m.as_bytes())
        {
            println!("Publish error: {:?}", e);
        } else {
            println!("Publishing message");
        }
    }

    pub fn print_peers_cache(&self) {
        for (peer_id, peer_data) in self.peers_caches_state.iter() {
            println!(
                "Peer {} {} {}",
                peer_id, peer_data.cache_hash, peer_data.publication_timestamp
            );
        }
    }

    pub async fn wait_for_messages(
        &mut self,
    ) -> std::result::Result<DomoCacheElement, Box<dyn Error>> {
        loop {
            select!(
                timer = task::sleep(Duration::from_secs(10)).fuse() => {
                            println!("Periodic cache state exchange");
                            self.send_cache_state();
                },
                event = self.swarm.select_next_some() => {
                match event {
                    SwarmEvent::ExpiredListenAddr { address, .. } => {
                        println!("Address {:?} expired", address);
                    }
                    SwarmEvent::ConnectionClosed { .. } => {
                        println!("Connection closed");
                    }
                    SwarmEvent::ListenerError { .. } => {
                        println!("Listener Error");
                    }
                    SwarmEvent::OutgoingConnectionError { .. } => {
                        println!("Outgoing connection error");
                    }
                    SwarmEvent::ListenerClosed { .. } => {
                        println!("Listener Closed");
                    }
                    SwarmEvent::NewListenAddr { address, .. } => {
                        println!("Listening in {:?}", address);
                    }
                    SwarmEvent::Behaviour(crate::domolibp2p::OutEvent::Gossipsub(
                        libp2p::gossipsub::GossipsubEvent::Message {
                            propagation_source: peer_id,
                            message_id: id,
                            message,
                        },
                    )) => {
                        println!(
                            "Got message: {} with id: {} from peer: {:?}, topic {}",
                            String::from_utf8_lossy(&message.data),
                            id,
                            peer_id,
                            &message.topic
                        );

                        if message.topic.to_string() == "domo-data" {
                            return self.handle_message_data(&String::from_utf8_lossy(&message.data));
                        } else if message.topic.to_string() == "domo-config" {
                            self.handle_config_data(&String::from_utf8_lossy(&message.data));
                        } else {
                                println!("Not able to recognize message");
                            }
                    }
                    SwarmEvent::Behaviour(crate::domolibp2p::OutEvent::Mdns(
                        libp2p::mdns::MdnsEvent::Expired(list),
                    )) => {
                        let local = Utc::now();

                        for (peer, _) in list {
                            println!("MDNS for peer {} expired {:?}", peer, local);
                        }
                    }
                    SwarmEvent::Behaviour(crate::domolibp2p::OutEvent::Mdns(
                        libp2p::mdns::MdnsEvent::Discovered(list),
                    )) => {
                        let local = Utc::now();
                        for (peer, _) in list {
                            self.swarm
                                .behaviour_mut()
                                .gossipsub
                                .add_explicit_peer(&peer);
                            println!("Discovered peer {} {:?}", peer, local);
                        }
                    }
                    _ => {}
                    }
                }
            );
        }
    }

    pub async fn new(house_uuid: &str, is_persistent_cache: bool, storage: T) -> Self {
        let swarm = crate::domolibp2p::start().await.unwrap();

        let peer_id = swarm.local_peer_id().to_string();

        let mut c = DomoCache {
            house_uuid: house_uuid.to_owned(),
            is_persistent_cache: is_persistent_cache,
            storage: storage,
            cache: BTreeMap::new(),
            peers_caches_state: BTreeMap::new(),
            swarm: swarm,
            local_peer_id: peer_id,
        };

        // popolo la mia cache con il contenuto dello sqlite
        let ret = c.storage.get_all_elements();

        for elem in ret {
            // non ripubblicp
            c.insert_cache_element(elem, false, false);
        }
        c
    }

    pub fn gossip_pub(&mut self, m: DomoCacheElement) {
        let topic = Topic::new("domo-data");

        let m = serde_json::to_string(&m).unwrap();

        if let Err(e) = self
            .swarm
            .behaviour_mut()
            .gossipsub
            .publish(topic.clone(), m.as_bytes())
        {
            println!("Publish error: {:?}", e);
        } else {
            println!("Publishing message");
        }
    }

    pub fn print(&self) {
        for (topic_name, topic_name_map) in self.cache.iter() {
            println!(" TopicName {} ", topic_name);

            for (topic_uuid, value) in topic_name_map.iter() {
                println!("{}", value);
            }
        }
    }

    pub fn print_cache_hash(&self) {
        println!("Hash {}", self.get_cache_hash())
    }
    pub fn get_cache_hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.hash(&mut s);
        s.finish()
    }
}

impl<T: DomoPersistentStorage> DomoCacheOperations for DomoCache<T> {
    fn insert_cache_element(
        &mut self,
        cache_element: DomoCacheElement,
        persist: bool,
        publish: bool,
    ) {
        // topic_name already present
        if self.cache.contains_key(&cache_element.topic_name) {
            self.cache
                .get_mut(&cache_element.topic_name)
                .unwrap()
                .insert(cache_element.topic_uuid.clone(), cache_element.clone());
        } else {
            // first time that we add an element of topic_name type
            self.cache
                .insert(cache_element.topic_name.clone(), BTreeMap::new());
            self.cache
                .get_mut(&cache_element.topic_name)
                .unwrap()
                .insert(cache_element.topic_uuid.clone(), cache_element.clone());
        }

        if persist {
            self.storage.store(&cache_element);
        }

        if publish {
            self.gossip_pub(cache_element);
        }
    }

    fn read_cache_element(
        &mut self,
        topic_name: &str,
        topic_uuid: &str,
    ) -> Option<DomoCacheElement> {
        let value = self.cache.get(topic_name);

        match value {
            Some(topic_map) => match topic_map.get(topic_uuid) {
                Some(element) => Some((*element).clone()),
                None => None,
            },
            None => None,
        }
    }

    // metodo chiamato dall'applicazione, metto in cache e pubblico
    fn write_value(&mut self, topic_name: &str, topic_uuid: &str, value: Value) {
        let timest = get_epoch_ms();
        let elem = DomoCacheElement {
            topic_name: String::from(topic_name),
            topic_uuid: String::from(topic_uuid),
            publication_timestamp: timest,
            value: value.clone(),
            deleted: false,
            publisher_peer_id: self.local_peer_id.clone(),
        };

        self.insert_cache_element(elem.clone(), self.is_persistent_cache, true);
    }

    fn write_with_timestamp_check(
        &mut self,
        topic_name: &str,
        topic_uuid: &str,
        elem: DomoCacheElement,
    ) -> Option<DomoCacheElement> {
        match self.read_cache_element(topic_name, topic_uuid) {
            Some(value) => {
                if value.publication_timestamp > elem.publication_timestamp {
                    Some(value)
                } else {
                    // inserisco in cache ma non ripubblico
                    self.insert_cache_element(elem, self.is_persistent_cache, false);
                    None
                }
            }
            None => {
                // inserisco in cache ma non ripubblico
                self.insert_cache_element(elem, self.is_persistent_cache, false);
                None
            }
        }
    }
}

mod tests {
    use super::DomoCacheOperations;

    #[cfg(test)]
    #[async_std::test]
    async fn test_write_and_read_key() {
        let house_uuid = "CasaProva";
        let storage = super::SqliteStorage::new(house_uuid, "./prova.sqlite", true);
        let mut domo_cache = super::DomoCache::new(house_uuid, true, storage).await;

        domo_cache.print();

        domo_cache.write_value(
            "Domo::Light",
            "luce-1",
            serde_json::json!({ "connected": true}),
        );

        let val = domo_cache
            .read_cache_element("Domo::Light", "luce-1")
            .unwrap()
            .value;
        assert_eq!(serde_json::json!({ "connected": true}), val)
    }

    #[async_std::test]
    async fn test_write_twice_same_key() {
        let house_uuid = "CasaProva";
        let storage = super::SqliteStorage::new(house_uuid, "./prova.sqlite", true);

        let mut domo_cache = super::DomoCache::new(house_uuid, true, storage).await;

        domo_cache.write_value(
            "Domo::Light",
            "luce-1",
            serde_json::json!({ "connected": true}),
        );

        let val = domo_cache
            .read_cache_element("Domo::Light", "luce-1")
            .unwrap()
            .value;

        assert_eq!(serde_json::json!({ "connected": true}), val);

        domo_cache.write_value(
            "Domo::Light",
            "luce-1",
            serde_json::json!({ "connected": false}),
        );

        let val = domo_cache
            .read_cache_element("Domo::Light", "luce-1")
            .unwrap()
            .value;

        assert_eq!(serde_json::json!({ "connected": false}), val)
    }

    #[async_std::test]
    async fn test_write_old_timestamp() {
        let house_uuid = "CasaProva";
        let storage = super::SqliteStorage::new(house_uuid, "./prova.sqlite", true);

        let mut domo_cache = super::DomoCache::new(house_uuid, true, storage).await;

        domo_cache.write_value(
            "Domo::Light",
            "luce-timestamp",
            serde_json::json!({ "connected": true}),
        );

        let old_val = domo_cache
            .read_cache_element("Domo::Light", "luce-timestamp")
            .unwrap();

        let el = super::DomoCacheElement {
            topic_name: String::from("Domo::Light"),
            topic_uuid: String::from("luce-timestamp"),
            value: Default::default(),
            deleted: false,
            publication_timestamp: 0,
            publisher_peer_id: domo_cache.local_peer_id.clone(),
        };

        // mi aspetto il vecchio valore perchè non deve fare la scrittura

        let ret = domo_cache
            .write_with_timestamp_check("Domo::Light", "luce-timestamp", el)
            .unwrap();

        // mi aspetto di ricevere il vecchio valore
        assert_eq!(ret, old_val);

        domo_cache.write_value(
            "Domo::Light",
            "luce-timestamp",
            serde_json::json!({ "connected": false}),
        );

        let val = domo_cache
            .read_cache_element("Domo::Light", "luce-timestamp")
            .unwrap();

        assert_ne!(ret, val);
    }
}
