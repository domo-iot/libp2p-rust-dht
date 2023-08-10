//! Cached access to the DHT

mod local;

use std::sync::Arc;
use std::{collections::BTreeMap, time::Duration};

use futures_util::{Stream, StreamExt};
use libp2p::Swarm;
use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::RwLock;
use tokio::time;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::{
    cache::local::DomoCacheStateMessage,
    data::DomoEvent,
    dht::{dht_channel, Command, Event},
    domolibp2p::DomoBehaviour,
    utils, Error,
};

use self::local::{DomoCacheElement, LocalCache, Query};

/// Cached DHT
///
/// It keeps a local cache of the dht state and allow to query the persistent topics
pub struct Cache {
    peer_id: String,
    local: LocalCache,
    cmd: UnboundedSender<Command>,
}

impl Cache {
    /// Send a volatile message
    ///
    /// Volatile messages are unstructured and do not persist in the DHT.
    pub fn send(&self, value: &Value) -> Result<(), Error> {
        self.cmd
            .send(Command::Broadcast(value.to_owned()))
            .map_err(|_| Error::Channel)?;

        Ok(())
    }

    /// Persist a value within the DHT
    ///
    /// It is identified by the topic and uuid value
    pub async fn put(&self, topic: &str, uuid: &str, value: &Value) -> Result<(), Error> {
        let elem = DomoCacheElement {
            topic_name: topic.to_string(),
            topic_uuid: uuid.to_string(),
            value: value.to_owned(),
            publication_timestamp: utils::get_epoch_ms(),
            publisher_peer_id: self.peer_id.clone(),
            ..Default::default()
        };

        self.local.put(&elem).await;

        self.cmd
            .send(Command::Publish(serde_json::to_value(&elem)?))
            .map_err(|_| Error::Channel)?;

        Ok(())
    }

    /// Delete a value within the DHT
    ///
    /// It inserts the deletion entry and the entry value will be marked as deleted and removed
    /// from the stored cache.
    pub async fn del(&self, topic: &str, uuid: &str) -> Result<(), Error> {
        let elem = DomoCacheElement {
            topic_name: topic.to_string(),
            topic_uuid: uuid.to_string(),
            publication_timestamp: utils::get_epoch_ms(),
            publisher_peer_id: self.peer_id.clone(),
            deleted: true,
            ..Default::default()
        };

        self.local.put(&elem).await;

        self.cmd
            .send(Command::Publish(serde_json::to_value(&elem)?))
            .map_err(|_| Error::Channel)?;

        Ok(())
    }

    /// Query the local cache
    pub fn query(&self, topic: &str) -> Query {
        self.local.query(topic)
    }
}

#[derive(Default, Debug, Clone)]
pub(crate) struct PeersState {
    list: BTreeMap<String, DomoCacheStateMessage>,
    last_repub_timestamp: u128,
    repub_interval: u128,
}

#[derive(Debug)]
enum CacheState {
    Synced,
    Desynced { is_leader: bool },
}

impl PeersState {
    fn with_interval(repub_interval: u128) -> Self {
        Self {
            repub_interval,
            ..Default::default()
        }
    }

    fn insert(&mut self, state: DomoCacheStateMessage) {
        self.list.insert(state.peer_id.to_string(), state);
    }

    async fn is_synchronized(&self, peer_id: &str, hash: u64) -> CacheState {
        let cur_ts = utils::get_epoch_ms() - self.repub_interval;
        let desync = self
            .list
            .values()
            .any(|data| data.cache_hash != hash && data.publication_timestamp > cur_ts);

        if desync {
            CacheState::Desynced {
                is_leader: self
                    .list
                    .values()
                    .find(|data| {
                        data.cache_hash == hash
                            && data.peer_id.as_str() < peer_id
                            && data.publication_timestamp > cur_ts
                    })
                    .is_none(),
            }
        } else {
            CacheState::Synced
        }
    }
}

/// Join the dht and keep a local cache up to date
///
/// the resend interval is expressed in milliseconds
pub fn cache_channel(
    local: LocalCache,
    swarm: Swarm<DomoBehaviour>,
    resend_interval: u64,
) -> (Cache, impl Stream<Item = DomoEvent>) {
    let local_peer_id = swarm.local_peer_id().to_string();

    let (cmd, r, _j) = dht_channel(swarm);

    let cache = Cache {
        local: local.clone(),
        cmd: cmd.clone(),
        peer_id: local_peer_id.clone(),
    };

    let stream = UnboundedReceiverStream::new(r);

    let peers_state = Arc::new(RwLock::new(PeersState::with_interval(
        resend_interval as u128,
    )));

    let local_read = local.clone();
    let cmd_update = cmd.clone();
    let peer_id = local_peer_id.clone();

    tokio::task::spawn(async move {
        let mut interval = time::interval(Duration::from_millis(resend_interval.max(100)));
        while !cmd_update.is_closed() {
            interval.tick().await;
            let hash = local_read.get_hash().await;
            let m = DomoCacheStateMessage {
                peer_id: peer_id.clone(),
                cache_hash: hash,
                publication_timestamp: utils::get_epoch_ms(),
            };

            if cmd_update
                .send(Command::Config(serde_json::to_value(&m).unwrap()))
                .is_err()
            {
                break;
            }
        }
    });

    // TODO: refactor once async closures are stable
    let events = stream.filter_map(move |ev| {
        let local_write = local.clone();
        let peers_state = peers_state.clone();
        let peer_id = local_peer_id.clone();
        let cmd = cmd.clone();
        async move {
            match ev {
                Event::Config(cfg) => {
                    let m: DomoCacheStateMessage = serde_json::from_str(&cfg).unwrap();

                    let hash = local_write.get_hash().await;

                    // SAFETY: only user
                    let mut peers_state = peers_state.write().await;

                    // update the peers_caches_state
                    peers_state.insert(m);

                    let sync_info = peers_state.is_synchronized(&peer_id, hash).await;

                    log::debug!("local {peer_id:?} {sync_info:?}  -> {peers_state:#?}");

                    if let CacheState::Desynced { is_leader } = sync_info {
                        if is_leader
                            && utils::get_epoch_ms() - peers_state.last_repub_timestamp
                                >= peers_state.repub_interval
                        {
                            local_write
                                .read_owned()
                                .await
                                .values()
                                .flat_map(|topic| topic.values())
                                .for_each(|elem| {
                                    let mut elem = elem.to_owned();
                                    log::debug!("resending {}", elem.topic_uuid);
                                    elem.republication_timestamp = utils::get_epoch_ms();
                                    cmd.send(Command::Publish(
                                        serde_json::to_value(&elem).unwrap(),
                                    ))
                                    .unwrap();
                                });
                            peers_state.last_repub_timestamp = utils::get_epoch_ms();
                        }
                    }

                    // check for desync
                    // republish the local cache if needed
                    None
                }
                Event::Discovered(who) => Some(DomoEvent::NewPeers(
                    who.into_iter().map(|w| w.to_string()).collect(),
                )),
                Event::VolatileData(data) => {
                    // TODO we swallow errors quietly here
                    serde_json::from_str(&data)
                        .ok()
                        .map(DomoEvent::VolatileData)
                }
                Event::PersistentData(data) => {
                    if let Ok(mut elem) = serde_json::from_str::<DomoCacheElement>(&data) {
                        if elem.republication_timestamp != 0 {
                            log::debug!("Retransmission");
                        }
                        // TODO: do something with this value instead
                        elem.republication_timestamp = 0;
                        local_write
                            .try_put(&elem)
                            .await
                            .ok()
                            .map(|_| DomoEvent::PersistentData(elem))
                    } else {
                        None
                    }
                }
            }
        }
    });

    (cache, events)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::dht::test::*;
    use std::{collections::HashSet, pin::pin};

    #[tokio::test(flavor = "multi_thread")]
    async fn syncronization() {
        let [mut a, mut b, mut c] = make_peers().await;
        let mut d = make_peer().await;

        connect_peer(&mut a, &mut d).await;
        connect_peer(&mut b, &mut d).await;
        connect_peer(&mut c, &mut d).await;

        let a_local_cache = LocalCache::new();
        let b_local_cache = LocalCache::new();
        let c_local_cache = LocalCache::new();
        let d_local_cache = LocalCache::new();

        let mut expected: HashSet<_> = (0..10)
            .into_iter()
            .map(|uuid| format!("uuid-{uuid}"))
            .collect();

        tokio::task::spawn(async move {
            let (a_c, a_ev) = cache_channel(a_local_cache, a, 1000);
            let (_b_c, b_ev) = cache_channel(b_local_cache, b, 1000);
            let (_c_c, c_ev) = cache_channel(c_local_cache, c, 1000);

            let mut a_ev = pin!(a_ev);
            let mut b_ev = pin!(b_ev);
            let mut c_ev = pin!(c_ev);
            for uuid in 0..10 {
                let _ = a_c
                    .put(
                        "Topic",
                        &format!("uuid-{uuid}"),
                        &serde_json::json!({"key": uuid}),
                    )
                    .await;
            }

            loop {
                let (node, ev) = tokio::select! {
                    v = a_ev.next() => ("a", v.unwrap()),
                    v = b_ev.next() => ("b", v.unwrap()),
                    v = c_ev.next() => ("c", v.unwrap()),
                };

                match ev {
                    DomoEvent::PersistentData(data) => {
                        log::debug!("{node}: Got data {data:?}");
                    }
                    _ => {
                        log::debug!("{node}: Other {ev:?}");
                    }
                }
            }
        });

        log::info!("Adding D");

        let (_d_c, d_ev) = cache_channel(d_local_cache, d, 1000);

        let mut d_ev = pin!(d_ev);
        while !expected.is_empty() {
            let ev = d_ev.next().await.unwrap();
            match ev {
                DomoEvent::PersistentData(data) => {
                    assert!(expected.remove(&data.topic_uuid));
                    log::warn!("d: Got data {data:?}");
                }
                _ => {
                    log::warn!("d: Other {ev:?}");
                }
            }
        }
    }
}
