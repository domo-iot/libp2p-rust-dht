use crate::domocache::DomoCacheElement;
use sqlx::{
    any::AnyRow, sqlite::SqliteConnectOptions, AnyConnection, ConnectOptions, Connection, Executor,
    Row, SqliteConnection,
};
use std::path::Path;

pub const SQLITE_MEMORY_STORAGE: &str = "<memory>";

#[async_trait::async_trait]
pub trait DomoPersistentStorage {
    async fn store(&mut self, element: &DomoCacheElement);
    async fn get_all_elements(&mut self) -> Vec<DomoCacheElement>;
}

pub struct SqlxStorage {
    pub(crate) connection: AnyConnection,
}

impl SqlxStorage {
    async fn with_connection(mut conn: AnyConnection, write_access: bool) -> Self {
        if write_access {
            _ = conn
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
                )
                .await
                .unwrap();
        }

        Self { connection: conn }
    }

    pub async fn new_in_memory() -> Self {
        let conn = SqliteConnection::connect("sqlite::memory:").await.unwrap();

        Self::with_connection(conn.into(), true).await
    }

    pub async fn new<P: AsRef<Path>>(sqlite_file: P, write_access: bool) -> Self {
        if let Some(s) = sqlite_file.as_ref().to_str() {
            if s == SQLITE_MEMORY_STORAGE {
                return Self::new_in_memory().await;
            }
        }
        let conn = SqliteConnectOptions::new()
            .filename(sqlite_file)
            .read_only(!write_access)
            .create_if_missing(write_access)
            .connect()
            .await
            .expect("Cannot access the sqlite file");

        Self::with_connection(conn.into(), write_access).await
    }
}

#[async_trait::async_trait]
impl DomoPersistentStorage for SqlxStorage {
    async fn store(&mut self, element: &DomoCacheElement) {
        sqlx::query(
            "INSERT OR REPLACE INTO domo_data\
             (topic_name, topic_uuid, value, deleted, publication_timestamp, publisher_peer_id)\
              VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .bind(&element.topic_name)
        .bind(&element.topic_uuid)
        .bind(&element.value.to_string())
        .bind(&element.deleted)
        .bind(&element.publication_timestamp.to_string())
        .bind(&element.publisher_peer_id)
        .execute(&mut self.connection)
        .await
        .expect("database error");
    }

    async fn get_all_elements(&mut self) -> Vec<DomoCacheElement> {
        sqlx::query("SELECT * FROM domo_data")
            .try_map(|row: AnyRow| {
                let jvalue = row.get(2);
                let jvalue = serde_json::from_str(jvalue);

                let pub_timestamp_string: &str = row.get(4);

                Ok(DomoCacheElement {
                    topic_name: row.get(0),
                    topic_uuid: row.get(1),
                    value: jvalue.unwrap(),
                    deleted: row.get(3),
                    publication_timestamp: pub_timestamp_string.parse().unwrap(),
                    publisher_peer_id: row.get(5),
                    republication_timestamp: 0,
                })
            })
            .fetch_all(&mut self.connection)
            .await
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    #[should_panic]
    async fn open_read_from_memory() {
        let _s = super::SqlxStorage::new(super::SQLITE_MEMORY_STORAGE, false).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn open_read_non_existent_file() {
        let _s = super::SqlxStorage::new("aaskdjkasdka.sqlite", false).await;
    }

    #[tokio::test]
    async fn test_initial_get_all_elements() {
        use super::DomoPersistentStorage;

        let mut s = super::SqlxStorage::new_in_memory().await;
        let v = s.get_all_elements().await;
        assert_eq!(v.len(), 0);
    }

    #[tokio::test]
    async fn test_store() {
        use super::DomoPersistentStorage;
        let mut s = super::SqlxStorage::new_in_memory().await;

        let m = crate::domocache::DomoCacheElement {
            topic_name: "a".to_string(),
            topic_uuid: "a".to_string(),
            value: Default::default(),
            deleted: false,
            publication_timestamp: 0,
            publisher_peer_id: "a".to_string(),
            republication_timestamp: 0,
        };

        s.store(&m).await;

        let v = s.get_all_elements().await;

        assert_eq!(v.len(), 1);
        assert_eq!(v[0], m);
    }
}
