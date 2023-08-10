//! Local in-memory cache

pub use crate::data::*;
use crate::domopersistentstorage::{DomoPersistentStorage, SqlxStorage};
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

#[derive(Default)]
pub(crate) struct InnerCache {
    pub mem: BTreeMap<String, BTreeMap<String, DomoCacheElement>>,
    pub store: Option<SqlxStorage>,
}

/// SAFETY: the SqlxStorage access is only over write()
unsafe impl std::marker::Sync for InnerCache {}

impl InnerCache {
    pub fn put(&mut self, elem: &DomoCacheElement) {
        let topic_name = elem.topic_name.clone();
        let topic_uuid = &elem.topic_uuid;

        self.mem
            .entry(topic_name)
            .and_modify(|topic| {
                topic.insert(topic_uuid.to_owned(), elem.to_owned());
            })
            .or_insert_with(|| [(topic_uuid.to_owned(), elem.to_owned())].into());
    }
}

/// Local cache
#[derive(Default, Clone)]
pub struct LocalCache(Arc<RwLock<InnerCache>>);

impl LocalCache {
    pub async fn with_config(db_config: &sifis_config::Cache) -> Self {
        let mut inner = InnerCache::default();
        let mut store = SqlxStorage::new(db_config).await;

        for a in store.get_all_elements().await {
            inner.put(&a);
        }

        if db_config.persistent {
            inner.store = Some(store);
        }

        LocalCache(Arc::new(RwLock::new(inner)))
    }

    pub fn new() -> Self {
        Default::default()
    }

    /// Feeds a slice of this type into the given [`Hasher`].
    pub async fn hash<H: Hasher>(&self, state: &mut H) {
        let cache = &self.0.read().await.mem;
        for (topic_name, map_topic_name) in cache.iter() {
            topic_name.hash(state);

            for (topic_uuid, value) in map_topic_name.iter() {
                topic_uuid.hash(state);
                value.to_string().hash(state);
            }
        }
    }

    /// Put the element in the cache
    ///
    /// If it is already present overwrite it
    pub async fn put(&self, elem: &DomoCacheElement) {
        let mut cache = self.0.write().await;

        if let Some(storage) = cache.store.as_mut() {
            storage.store(&elem).await;
        }

        cache.put(elem);
    }

    /// Try to insert the element in the cache
    ///
    /// Return Err(()) if the element to insert is older than the one in the cache
    pub async fn try_put(&self, elem: &DomoCacheElement) -> Result<(), ()> {
        let mut cache = self.0.write().await;
        let topic_name = elem.topic_name.clone();
        let topic_uuid = &elem.topic_uuid;

        let topic = cache.mem.entry(topic_name).or_default();

        let e = if topic
            .get(topic_uuid)
            .is_some_and(|cur| elem.publication_timestamp <= cur.publication_timestamp)
        {
            Err(())
        } else {
            topic.insert(topic_uuid.to_owned(), elem.to_owned());
            Ok(())
        };

        if e.is_ok() {
            if let Some(s) = cache.store.as_mut() {
                s.store(&elem).await;
            }
        }

        e
    }

    /// Retrieve an element by its uuid and topic
    pub async fn get(&self, topic_name: &str, topic_uuid: &str) -> Option<DomoCacheElement> {
        let cache = self.0.read().await;

        cache
            .mem
            .get(topic_name)
            .and_then(|topic| topic.get(topic_uuid))
            .cloned()
    }

    /// Instantiate a query over the local cache
    pub fn query(&self, topic: &str) -> Query {
        Query::new(topic, self.clone())
    }

    /// Compute the current hash value
    pub async fn get_hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.hash(&mut s).await;
        s.finish()
    }

    pub(crate) async fn read_owned(&self) -> OwnedRwLockReadGuard<InnerCache> {
        self.0.clone().read_owned().await
    }
}

/// Query the local DHT cache
#[derive(Clone)]
pub struct Query {
    cache: LocalCache,
    topic: String,
    uuid: Option<String>,
}

impl Query {
    /// Create a new query over a local cache
    pub fn new(topic: &str, cache: LocalCache) -> Self {
        Self {
            topic: topic.to_owned(),
            cache,
            uuid: None,
        }
    }
    /// Look up for a specific uuid
    pub fn with_uuid(mut self, uuid: &str) -> Self {
        self.uuid = Some(uuid.to_owned());
        self
    }

    /// Execute the query and return a Value if found
    pub async fn get(&self) -> Vec<Value> {
        let cache = self.cache.0.read().await;

        if let Some(topics) = cache.mem.get(&self.topic) {
            if let Some(ref uuid) = self.uuid {
                topics
                    .get(uuid)
                    .into_iter()
                    .map(|elem| elem.value.clone())
                    .collect()
            } else {
                topics.values().map(|elem| elem.value.clone()).collect()
            }
        } else {
            Vec::new()
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::data::DomoCacheElement;
    use serde_json::*;

    fn make_test_element(topic_name: &str, topic_uuid: &str, value: &Value) -> DomoCacheElement {
        DomoCacheElement {
            topic_name: topic_name.to_owned(),
            topic_uuid: topic_uuid.to_owned(),
            value: value.to_owned(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn hash() {
        let cache = LocalCache::new();

        let hash = cache.get_hash().await;
        println!("{hash}");

        let elem = make_test_element("Domo::Light", "luce-1", &json!({ "connected": true}));
        cache.put(&elem).await;

        let hash2 = cache.get_hash().await;
        println!("{hash2}");

        assert_ne!(hash, hash2);

        let elem = make_test_element("Domo::Light", "luce-1", &json!({ "connected": false}));
        cache.put(&elem).await;

        let hash3 = cache.get_hash().await;
        println!("{hash3}");

        assert_ne!(hash2, hash3);

        let elem = make_test_element("Domo::Light", "luce-1", &json!({ "connected": true}));
        cache.put(&elem).await;

        let hash4 = cache.get_hash().await;
        println!("{hash4}");

        assert_eq!(hash2, hash4);
    }

    #[tokio::test]
    async fn put() {
        let cache = LocalCache::new();

        let elem = make_test_element("Domo::Light", "luce-1", &json!({ "connected": true}));

        cache.put(&elem).await;

        let out = cache.get("Domo::Light", "luce-1").await.expect("element");

        assert_eq!(out, elem);

        let elem2 = make_test_element("Domo::Light", "luce-1", &json!({ "connected": false}));

        cache.put(&elem2).await;

        let out = cache.get("Domo::Light", "luce-1").await.expect("element");

        assert_eq!(out, elem2);
    }

    #[tokio::test]
    async fn try_put() {
        let cache = LocalCache::new();

        let mut elem = make_test_element("Domo::Light", "luce-1", &json!({ "connected": true}));

        cache.try_put(&elem).await.unwrap();

        let out = cache.get("Domo::Light", "luce-1").await.expect("element");

        assert_eq!(out, elem);

        elem.publication_timestamp = 1;

        cache.try_put(&elem).await.expect("Update entry");

        let out: DomoCacheElement = cache.get("Domo::Light", "luce-1").await.expect("element");

        assert_eq!(out, elem);

        elem.publication_timestamp = 0;

        cache
            .try_put(&elem)
            .await
            .expect_err("The update should fail");

        let out: DomoCacheElement = cache.get("Domo::Light", "luce-1").await.expect("element");

        assert_eq!(out.publication_timestamp, 1);
    }

    #[tokio::test]
    async fn query() {
        let cache = LocalCache::new();

        for item in 0..10 {
            let elem = make_test_element(
                "Domo::Light",
                &format!("luce-{item}"),
                &json!({ "connected": true, "count": item}),
            );

            cache.put(&elem).await;
        }

        let q = cache.query("Domo::Light");

        assert_eq!(q.get().await.len(), 10);

        assert_eq!(q.clone().with_uuid("not-existent").get().await.len(), 0);

        assert_eq!(
            q.clone().with_uuid("luce-1").get().await[0]
                .get("count")
                .unwrap(),
            1
        );
    }
}
