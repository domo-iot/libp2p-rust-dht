//! Cached access to the DHT
pub use crate::data::*;
use crate::domolibp2p::{generate_rsa_key, parse_hex_key};
use crate::domopersistentstorage::{DomoPersistentStorage, SqlxStorage};
use crate::utils;
use crate::Error;
use futures::prelude::*;
use libp2p::gossipsub::IdentTopic as Topic;
use libp2p::identity::Keypair;
use libp2p::mdns;
use libp2p::swarm::SwarmEvent;
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::io::ErrorKind;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

// period at which we send messages containing our cache hash
const SEND_CACHE_HASH_PERIOD: u8 = 120;

/// Cached access to the DHT
///
/// It keeps an in-memory (or persistent) cache of the whole DHT.
pub struct DomoCache {
    storage: SqlxStorage,
    pub(crate) cache: BTreeMap<String, BTreeMap<String, DomoCacheElement>>,
    peers_caches_state: BTreeMap<String, DomoCacheStateMessage>,
    pub(crate) publish_cache_counter: u8,
    pub(crate) last_cache_repub_timestamp: u128,
    pub(crate) swarm: libp2p::Swarm<crate::domolibp2p::DomoBehaviour>,
    pub is_persistent_cache: bool,
    pub local_peer_id: String,
    client_tx_channel: Sender<DomoEvent>,
    client_rx_channel: Receiver<DomoEvent>,
    send_cache_state_timer: tokio::time::Instant,
}

enum Event {
    Client(DomoEvent),
    RefreshTime,
    PersistentData(String),
    VolatileData(String),
    Config(String),
    Continue,
}

impl Hash for DomoCache {
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

impl DomoCache {
    /// Apply a jsonpath expression over the elements of a topic.
    pub fn filter_with_topic_name(
        &self,
        topic_name: &str,
        jsonpath_expr: &str,
    ) -> Result<serde_json::Value, String> {
        let mut ret = serde_json::json!([]);
        match self.cache.get(topic_name) {
            None => Ok(serde_json::json!([])),
            Some(topic_map) => {
                for (_topic_uuid, topic_value) in topic_map.iter() {
                    let val = serde_json::json!(
                        {
                            "value": [topic_value.value]
                        }
                    );

                    let result = jsonpath_lib::select(&val, jsonpath_expr);

                    match result {
                        Ok(res) => {
                            for r in res {
                                ret.as_array_mut().unwrap().push(r.clone());
                            }
                        }
                        Err(e) => return Err(e.to_string()),
                    };
                }

                Ok(ret)
            }
        }
    }

    fn handle_volatile_data(&self, message: &str) -> std::result::Result<DomoEvent, Error> {
        let m: serde_json::Value = serde_json::from_str(message)?;
        Ok(DomoEvent::VolatileData(m))
    }

    async fn handle_persistent_message_data(
        &mut self,
        message: &str,
    ) -> std::result::Result<DomoEvent, Error> {
        let mut m: DomoCacheElement = serde_json::from_str(message)?;

        // rimetto a 0 il republication timestamp altrimenti cambia hash
        m.republication_timestamp = 0;

        let topic_name = m.topic_name.clone();
        let topic_uuid = m.topic_uuid.clone();

        let ret = self
            .write_with_timestamp_check(&topic_name, &topic_uuid, m.clone())
            .await;

        match ret {
            None => {
                log::info!("New message received");
                // since a new message arrived, we invalidate peers cache states
                self.peers_caches_state.clear();
                Ok(DomoEvent::PersistentData(m))
            }
            _ => {
                log::info!("Old message received");
                Ok(DomoEvent::None)
            }
        }
    }

    /// Returns a tuple (is_synchronized, is_hash_leader)
    fn is_synchronized(
        &self,
        local_hash: u64,
        peers_caches_state: &BTreeMap<String, DomoCacheStateMessage>,
    ) -> (bool, bool) {
        // If there are hashes different from the current node,
        // the state is not consistent. Then we need to check
        // whether we are the leaders for the current hash.
        if peers_caches_state
            .iter()
            .filter(|(_, data)| {
                (data.cache_hash != local_hash)
                    && (data.publication_timestamp
                        > (utils::get_epoch_ms() - (1000 * 2 * u128::from(SEND_CACHE_HASH_PERIOD))))
            })
            .count()
            > 0
        {
            if peers_caches_state
                .iter()
                .filter(|(peer_id, data)| {
                    (data.cache_hash == local_hash)
                        && (*self.local_peer_id < *peer_id.as_str())
                        && (data.publication_timestamp
                            > (utils::get_epoch_ms()
                                - (1000 * 2 * u128::from(SEND_CACHE_HASH_PERIOD))))
                })
                .count()
                > 0
            {
                // Our node is not the leader of the hash
                return (false, false);
            } else {
                // Our node is the leader of the hash
                return (false, true);
            }
        }

        // We are synchronized
        (true, true)
    }

    /// Get a value identified by its uuid within a topic.
    pub fn get_topic_uuid(&self, topic_name: &str, topic_uuid: &str) -> Result<Value, String> {
        let ret = self.read_cache_element(topic_name, topic_uuid);
        match ret {
            None => Ok(serde_json::json!({})),
            Some(cache_element) => Ok(serde_json::json!({
                        "topic_name": topic_name.to_owned(),
                        "topic_uuid": topic_uuid.to_owned(),
                        "value": cache_element.value
            })),
        }
    }

    /// Get all the values within a topic.
    pub fn get_topic_name(&self, topic_name: &str) -> Result<Value, String> {
        let s = r#"[]"#;
        let mut ret: Value = serde_json::from_str(s).unwrap();

        match self.cache.get(topic_name) {
            None => Ok(serde_json::json!([])),
            Some(topic_name_map) => {
                for (topic_uuid, cache_element) in topic_name_map.iter() {
                    if !cache_element.deleted {
                        let val = serde_json::json!({
                            "topic_name": topic_name.to_owned(),
                            "topic_uuid": topic_uuid.to_owned(),
                            "value": cache_element.value.clone()
                        });
                        ret.as_array_mut().unwrap().push(val);
                    }
                }
                Ok(ret)
            }
        }
    }

    /// Return the whole cache as a JSON Value
    pub fn get_all(&self) -> Value {
        let s = r#"[]"#;
        let mut ret: Value = serde_json::from_str(s).unwrap();

        for (topic_name, topic_name_map) in self.cache.iter() {
            for (topic_uuid, cache_element) in topic_name_map.iter() {
                if !cache_element.deleted {
                    let val = serde_json::json!({
                    "topic_name": topic_name.clone(),
                    "topic_uuid": topic_uuid.clone(),
                    "value": cache_element.value.clone()
                            }
                    );
                    ret.as_array_mut().unwrap().push(val);
                }
            }
        }
        ret
    }

    async fn publish_cache(&mut self) {
        let mut cache_elements = vec![];

        for (_, topic_name_map) in self.cache.iter() {
            for (_, cache_element) in topic_name_map.iter() {
                cache_elements.push(cache_element.clone());
            }
        }

        for elem in cache_elements {
            self.gossip_pub(elem, true).await;
        }

        self.last_cache_repub_timestamp = utils::get_epoch_ms();
    }

    async fn handle_config_data(&mut self, message: &str) {
        log::info!("Received cache message, check caches ...");
        let m: DomoCacheStateMessage = serde_json::from_str(message).unwrap();
        self.peers_caches_state.insert(m.peer_id.clone(), m);
        self.check_caches_desynchronization().await;
    }

    async fn check_caches_desynchronization(&mut self) {
        let local_hash = self.get_cache_hash();
        let (sync, leader) = self.is_synchronized(local_hash, &self.peers_caches_state);
        if !sync {
            log::info!("Caches are not synchronized");
            if leader {
                log::info!("Publishing my cache since I am the leader for the hash");
                if self.last_cache_repub_timestamp
                    < (utils::get_epoch_ms() - 1000 * u128::from(SEND_CACHE_HASH_PERIOD))
                {
                    self.publish_cache().await;
                } else {
                    log::info!("Skipping cache repub since it occurred not so much time ago");
                }
            } else {
                log::info!("I am not the leader for the hash");
            }
        } else {
            log::info!("Caches are synchronized");
        }
    }

    async fn send_cache_state(&mut self) {
        let m = DomoCacheStateMessage {
            peer_id: self.local_peer_id.to_string(),
            cache_hash: self.get_cache_hash(),
            publication_timestamp: crate::utils::get_epoch_ms(),
        };

        let topic = Topic::new("domo-config");

        let m = serde_json::to_string(&m).unwrap();

        if let Err(e) = self
            .swarm
            .behaviour_mut()
            .gossipsub
            .publish(topic.clone(), m.as_bytes())
        {
            log::info!("Publish error: {e:?}");
        } else {
            log::info!("Published cache hash");
        }

        self.publish_cache_counter -= 1;
        if self.publish_cache_counter == 0 {
            self.publish_cache_counter = 4;
            self.check_caches_desynchronization().await;
        }
    }

    /// Print the status of the cache across the known peers.
    pub fn print_peers_cache(&self) {
        for (peer_id, peer_data) in self.peers_caches_state.iter() {
            println!(
                "Peer {}, HASH: {}, TIMESTAMP: {}",
                peer_id, peer_data.cache_hash, peer_data.publication_timestamp
            );
        }
    }

    /// Get the currently seen peers
    ///
    /// And their known hash and timestamp
    pub fn get_peers_stats(&self) -> impl Iterator<Item = (&str, u64, u128)> {
        self.peers_caches_state
            .values()
            .map(|v| (v.peer_id.as_str(), v.cache_hash, v.publication_timestamp))
    }

    async fn inner_select(&mut self) -> Event {
        use Event::*;
        tokio::select!(
            // a client of this router published something
            m = self.client_rx_channel.recv() => {
                let dm = m.unwrap();
                return Client(dm);
            },

            _ = tokio::time::sleep_until(self.send_cache_state_timer) => {
                return RefreshTime;
            },

            event = self.swarm.select_next_some() => {
                match event {
                    SwarmEvent::ExpiredListenAddr { address, .. } => {
                        log::info!("Address {address:?} expired");
                    }
                    SwarmEvent::ConnectionEstablished {..} => {
                            log::info!("Connection established ...");
                    }
                    SwarmEvent::ConnectionClosed { .. } => {
                        log::info!("Connection closed");
                    }
                    SwarmEvent::ListenerError { .. } => {
                        log::info!("Listener Error");
                    }
                    SwarmEvent::OutgoingConnectionError { .. } => {
                        log::info!("Outgoing connection error");
                    }
                    SwarmEvent::ListenerClosed { .. } => {
                        log::info!("Listener Closed");
                    }
                    SwarmEvent::NewListenAddr { address, .. } => {
                        println!("Listening in {address:?}");
                    }
                    SwarmEvent::Behaviour(crate::domolibp2p::OutEvent::Gossipsub(
                        libp2p::gossipsub::Event::Message {
                            propagation_source: _peer_id,
                            message_id: _id,
                            message,
                        },
                    )) => {
                        let data = String::from_utf8(message.data).unwrap();
                        match message.topic.as_str() {
                            "domo-persistent-data" => {
                                return PersistentData(data);
                            }
                            "domo-config" => {
                                return Config(data);
                            }
                            "domo-volatile-data" => {
                                return VolatileData(data);
                            }
                            _ => {
                                log::info!("Not able to recognize message");
                            }
                        }
                    }
                    SwarmEvent::Behaviour(crate::domolibp2p::OutEvent::Mdns(
                        mdns::Event::Expired(list),
                    )) => {
                        let local = OffsetDateTime::now_utc();

                        for (peer, _) in list {
                            log::info!("MDNS for peer {peer} expired {local:?}");
                        }
                    }
                    SwarmEvent::Behaviour(crate::domolibp2p::OutEvent::Mdns(
                        mdns::Event::Discovered(list),
                    )) => {
                        let local = OffsetDateTime::now_utc();
                        for (peer, _) in list {
                            self.swarm
                                .behaviour_mut()
                                .gossipsub
                                .add_explicit_peer(&peer);
                            log::info!("Discovered peer {peer} {local:?}");
                        }

                    }
                    _ => {}
                }
            }
        );
        Continue
    }

    /// Cache event loop
    ///
    /// To be called as often as needed to keep the cache in-sync and receive new data.
    pub async fn cache_event_loop(&mut self) -> std::result::Result<DomoEvent, Error> {
        use Event::*;
        loop {
            match self.inner_select().await {
                Client(ev) => {
                    return Ok(ev);
                }
                RefreshTime => {
                    self.send_cache_state_timer = tokio::time::Instant::now()
                        + Duration::from_secs(u64::from(SEND_CACHE_HASH_PERIOD));
                    self.send_cache_state().await;
                }
                PersistentData(data) => {
                    return self.handle_persistent_message_data(&data).await;
                }
                VolatileData(data) => {
                    return self.handle_volatile_data(&data);
                }
                Config(data) => {
                    self.handle_config_data(&data).await;
                }
                Continue => {}
            }
        }
    }

    /// Instantiate a new cache
    ///
    /// See [sifis_config::Cache] for the available parameters.
    pub async fn new(conf: crate::Config) -> Result<Self, Error> {
        if conf.url.is_empty() {
            panic!("db_url needed");
        }

        let is_persistent_cache = conf.persistent;
        let loopback_only = conf.loopback;
        let shared_key = parse_hex_key(&conf.shared_key)?;
        let private_key_file = conf.private_key.clone();

        let storage = SqlxStorage::new(&conf).await;

        // Create a random local key.
        let mut pkcs8_der = if let Some(pk_path) = private_key_file {
            match std::fs::read(&pk_path) {
                Ok(pem) => {
                    let der = pem_rfc7468::decode_vec(&pem)?;
                    der.1
                }
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    // Generate a new key and put it into the file at the given path
                    let (pem, der) = generate_rsa_key();
                    std::fs::write(pk_path, pem)?;
                    der
                }
                Err(e) => Err(e)?,
            }
        } else {
            generate_rsa_key().1
        };

        let local_key_pair = Keypair::rsa_from_pkcs8(&mut pkcs8_der)?;

        let swarm = crate::domolibp2p::start(shared_key, local_key_pair, loopback_only)
            .await
            .unwrap();

        let peer_id = swarm.local_peer_id().to_string();

        let (client_tx_channel, client_rx_channel) = mpsc::channel::<DomoEvent>(32);

        let send_cache_state_timer: tokio::time::Instant =
            tokio::time::Instant::now() + Duration::from_secs(u64::from(SEND_CACHE_HASH_PERIOD));

        let mut c = DomoCache {
            is_persistent_cache,
            swarm,
            local_peer_id: peer_id,
            publish_cache_counter: 4,
            last_cache_repub_timestamp: 0,
            storage,
            cache: BTreeMap::new(),
            peers_caches_state: BTreeMap::new(),
            client_tx_channel,
            client_rx_channel,
            send_cache_state_timer,
        };

        // Populate the cache with the sqlite contents
        let ret = c.storage.get_all_elements().await;

        for elem in ret {
            // non ripubblico
            c.insert_cache_element(elem, false, false).await;
        }
        Ok(c)
    }

    /// Publish a volatile value on the DHT
    ///
    /// All the peers reachable will receive it.
    /// Peers joining later would not receive it.
    pub async fn pub_value(&mut self, value: Value) {
        let topic = Topic::new("domo-volatile-data");

        let m = serde_json::to_string(&value).unwrap();

        if let Err(e) = self
            .swarm
            .behaviour_mut()
            .gossipsub
            .publish(topic.clone(), m.as_bytes())
        {
            log::info!("Publish error: {:?}", e);
        }

        // signal a volatile pub by part of clients
        let ev = DomoEvent::VolatileData(value);
        self.client_tx_channel.send(ev).await.unwrap();
    }

    async fn gossip_pub(&mut self, mut m: DomoCacheElement, republished: bool) {
        let topic = Topic::new("domo-persistent-data");

        if republished {
            m.republication_timestamp = utils::get_epoch_ms();
        } else {
            m.republication_timestamp = 0;
        }

        let m2 = serde_json::to_string(&m).unwrap();

        if let Err(e) = self
            .swarm
            .behaviour_mut()
            .gossipsub
            .publish(topic.clone(), m2.as_bytes())
        {
            log::info!("Publish error: {e:?}");
        }
        if !republished {
            // signal a volatile pub by part of clients
            let ev = DomoEvent::PersistentData(m);
            self.client_tx_channel.send(ev).await.unwrap();
        }
    }

    /// Print the current DHT state
    pub fn print(&self) {
        for (topic_name, topic_name_map) in self.cache.iter() {
            let mut first = true;

            for (_, value) in topic_name_map.iter() {
                if !value.deleted {
                    if first {
                        println!("TopicName {topic_name}");
                        first = false;
                    }
                    println!("{value}");
                }
            }
        }
    }

    /// Print the DHT current hash
    pub fn print_cache_hash(&self) {
        println!("Hash {}", self.get_cache_hash())
    }

    /// Compute the hash of the current DHT state
    pub fn get_cache_hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.hash(&mut s);
        s.finish()
    }

    /// Mark a persistent value as deleted
    ///
    /// It will not be propagated and it is expunged from the initial DHT state fed to new peers
    /// joining.
    pub async fn delete_value(&mut self, topic_name: &str, topic_uuid: &str) {
        let mut value_to_set = serde_json::Value::Null;

        if let Some(old_value) = self.read_cache_element(topic_name, topic_uuid) {
            value_to_set = old_value.value;
        }

        let timest = utils::get_epoch_ms();
        let elem = DomoCacheElement {
            topic_name: String::from(topic_name),
            topic_uuid: String::from(topic_uuid),
            publication_timestamp: timest,
            value: value_to_set,
            deleted: true,
            publisher_peer_id: self.local_peer_id.clone(),
            republication_timestamp: 0,
        };

        self.insert_cache_element(elem.clone(), self.is_persistent_cache, true)
            .await;
    }

    /// Write/Update a persistent value
    ///
    /// The value will be part of the initial DHT state a peer joining will receive.
    pub async fn write_value(&mut self, topic_name: &str, topic_uuid: &str, value: Value) {
        let timest = utils::get_epoch_ms();
        let elem = DomoCacheElement {
            topic_name: String::from(topic_name),
            topic_uuid: String::from(topic_uuid),
            publication_timestamp: timest,
            value: value.clone(),
            deleted: false,
            publisher_peer_id: self.local_peer_id.clone(),
            republication_timestamp: 0,
        };

        self.insert_cache_element(elem.clone(), self.is_persistent_cache, true)
            .await;
    }

    async fn insert_cache_element(
        &mut self,
        cache_element: DomoCacheElement,
        persist: bool,
        publish: bool,
    ) {
        {
            // If topic_name is already present, insert into it,
            // otherwise create a new map.
            // We could be using the entry api here together with or_default,
            // but it would require copying the key for the lookup, even if a
            // reference would have been enough. We try to optimize more for
            // the reading use case, instead of the writing use case, so we
            // rather try to avoid the clone rather than the two map lookups.
            // Once raw_entry APIs are available on stable Rust, we can switch
            // to those.
            if let Some(key) = self.cache.get_mut(&cache_element.topic_name) {
                key.insert(cache_element.topic_uuid.clone(), cache_element.clone());
            } else {
                // first time that we add an element of topic_name type
                let mut map = BTreeMap::new();
                map.insert(cache_element.topic_uuid.clone(), cache_element.clone());
                self.cache.insert(cache_element.topic_name.clone(), map);
            }

            if persist {
                self.storage.store(&cache_element).await;
            }
        }

        if publish {
            self.gossip_pub(cache_element, false).await;
        }
    }

    async fn write_with_timestamp_check(
        &mut self,
        topic_name: &str,
        topic_uuid: &str,
        elem: DomoCacheElement,
    ) -> Option<DomoCacheElement> {
        let ret = self.cache.get(topic_name);

        match ret {
            None => {
                self.insert_cache_element(elem, self.is_persistent_cache, false)
                    .await;
                None
            }
            Some(topic_map) => match topic_map.get(topic_uuid) {
                None => {
                    self.insert_cache_element(elem, self.is_persistent_cache, false)
                        .await;
                    None
                }
                Some(value) => {
                    if elem.publication_timestamp > value.publication_timestamp {
                        self.insert_cache_element(elem, self.is_persistent_cache, false)
                            .await;
                        None
                    } else {
                        Some((*value).clone())
                    }
                }
            },
        }
    }

    fn read_cache_element(&self, topic_name: &str, topic_uuid: &str) -> Option<DomoCacheElement> {
        let value = self.cache.get(topic_name);

        match value {
            Some(topic_map) => match topic_map.get(topic_uuid) {
                Some(element) => {
                    if !element.deleted {
                        Some((*element).clone())
                    } else {
                        None
                    }
                }
                None => None,
            },
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    async fn make_cache() -> super::DomoCache {
        let conf = sifis_config::Cache {
            url: "sqlite::memory:".to_string(),
            table: "domo_data".to_string(),
            persistent: true,
            private_key: Some("/tmp/test_key.pem".into()),
            shared_key: "d061545647652562b4648f52e8373b3a417fc0df56c332154460da1801b341e9"
                .to_string(),
            loopback: false,
        };

        if let Ok(cache) = super::DomoCache::new(conf).await {
            return cache;
        }

        panic!("cannot create cache");
    }

    #[tokio::test]
    async fn test_delete() {
        let mut domo_cache = make_cache().await;

        domo_cache
            .write_value(
                "Domo::Light",
                "luce-delete",
                serde_json::json!({ "connected": true}),
            )
            .await;

        let _v = domo_cache
            .read_cache_element("Domo::Light", "luce-delete")
            .unwrap();

        domo_cache.delete_value("Domo::Light", "luce-delete").await;

        let v = domo_cache.read_cache_element("Domo::Light", "luce-delete");

        assert_eq!(v, None)
    }

    #[tokio::test]
    async fn test_write_and_read_key() {
        let mut domo_cache = make_cache().await;

        domo_cache
            .write_value(
                "Domo::Light",
                "luce-1",
                serde_json::json!({ "connected": true}),
            )
            .await;

        let val = domo_cache
            .read_cache_element("Domo::Light", "luce-1")
            .unwrap()
            .value;
        assert_eq!(serde_json::json!({ "connected": true}), val)
    }

    #[tokio::test]
    async fn test_write_twice_same_key() {
        let mut domo_cache = make_cache().await;

        domo_cache
            .write_value(
                "Domo::Light",
                "luce-1",
                serde_json::json!({ "connected": true}),
            )
            .await;

        let val = domo_cache
            .read_cache_element("Domo::Light", "luce-1")
            .unwrap()
            .value;

        assert_eq!(serde_json::json!({ "connected": true}), val);

        domo_cache
            .write_value(
                "Domo::Light",
                "luce-1",
                serde_json::json!({ "connected": false}),
            )
            .await;

        let val = domo_cache
            .read_cache_element("Domo::Light", "luce-1")
            .unwrap()
            .value;

        assert_eq!(serde_json::json!({ "connected": false}), val)
    }

    #[tokio::test]
    async fn test_write_old_timestamp() {
        let mut domo_cache = make_cache().await;

        domo_cache
            .write_value(
                "Domo::Light",
                "luce-timestamp",
                serde_json::json!({ "connected": true}),
            )
            .await;

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
            republication_timestamp: 0,
        };

        let ret = domo_cache
            .write_with_timestamp_check("Domo::Light", "luce-timestamp", el)
            .await
            .unwrap();

        assert_eq!(ret, old_val);

        domo_cache
            .write_value(
                "Domo::Light",
                "luce-timestamp",
                serde_json::json!({ "connected": false}),
            )
            .await;

        let val = domo_cache
            .read_cache_element("Domo::Light", "luce-timestamp")
            .unwrap();

        assert_ne!(ret, val);
    }

    #[tokio::test]
    async fn test_filter_topic_name() {
        let mut domo_cache = make_cache().await;

        domo_cache
            .write_value(
                "Domo::Light",
                "one",
                serde_json::json!(
                    {
                         "description": "first_light",
                         "connected": false
                    }
                ),
            )
            .await;

        domo_cache
            .write_value(
                "Domo::Light",
                "two",
                serde_json::json!(
                    {
                         "description": "second_light",
                         "connected": true
                    }
                ),
            )
            .await;

        domo_cache
            .write_value(
                "Domo::Light",
                "three",
                serde_json::json!(
                    {
                         "description": "third_light",
                         "floor_number": 3
                    }
                ),
            )
            .await;

        domo_cache
            .write_value(
                "Domo::Light",
                "four",
                serde_json::json!(
                    {
                         "description": "light_4",
                         "categories": [1, 2]
                    }
                ),
            )
            .await;

        let mut filter_exp = "$.value[?(@.floor_number && @.floor_number > 2)].description";

        let values = domo_cache
            .filter_with_topic_name("Domo::Light", filter_exp)
            .unwrap();

        let _str_value = values.to_string();

        assert_eq!(values, serde_json::json!(["third_light"]));

        filter_exp =
            "$.value[?(@.floor_number && @.floor_number > 2 && @.description ==\"third_light\")].description";

        let values = domo_cache
            .filter_with_topic_name("Domo::Light", filter_exp)
            .unwrap();

        let _str_value = values.to_string();

        assert_eq!(values, serde_json::json!(["third_light"]));
    }
}
