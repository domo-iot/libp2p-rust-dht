use crate::domocache::DomoCacheElement;
use sea_query::{
    Alias, ColumnDef, Iden, Index, PostgresQueryBuilder, Query, SqliteQueryBuilder, Table,
};
use sea_query_binder::SqlxBinder;
use sqlx::{
    any::{AnyConnectOptions, AnyKind, AnyRow},
    postgres::PgConnectOptions,
    sqlite::SqliteConnectOptions,
    AnyConnection, ConnectOptions, Connection, Executor, Row, SqliteConnection,
};

use std::str::FromStr;

#[async_trait::async_trait]
pub trait DomoPersistentStorage {
    async fn store(&mut self, element: &DomoCacheElement);
    async fn get_all_elements(&mut self) -> Vec<DomoCacheElement>;
}

pub struct SqlxStorage {
    pub(crate) connection: AnyConnection,
    pub(crate) db_table: Alias,
}

#[derive(Iden)]
enum Id {
    TopicName,
    TopicUuid,
    Value,
    Deleted,
    PublicationTimestamp,
    PublisherPeerId,
}

impl SqlxStorage {
    async fn with_connection(mut conn: AnyConnection, db_table: &str, write_access: bool) -> Self {
        let db_table = Alias::new(db_table);
        let mut table = Table::create();
        let sql = table
            .table(db_table.clone())
            .if_not_exists()
            .col(ColumnDef::new(Id::TopicName).text().not_null())
            .col(ColumnDef::new(Id::TopicUuid).text().not_null())
            .col(ColumnDef::new(Id::Value).text())
            .col(ColumnDef::new(Id::Deleted).integer())
            .col(ColumnDef::new(Id::PublicationTimestamp).text().not_null())
            .col(ColumnDef::new(Id::PublisherPeerId).text().not_null())
            .primary_key(Index::create().col(Id::TopicName).col(Id::TopicUuid));

        let sql = match conn.kind() {
            AnyKind::Sqlite => sql.build(SqliteQueryBuilder),
            AnyKind::Postgres => sql.build(PostgresQueryBuilder),
        };

        /*
                let create_table_command = "CREATE TABLE IF NOT EXISTS ".to_owned() + db_table
                    + " (
                    topic_name             TEXT,
                    topic_uuid             TEXT,
                    value                  TEXT,
                    deleted                INTEGER,
                    publication_timestamp   TEXT,
                    publisher_peer_id       TEXT,
                    PRIMARY KEY (topic_name, topic_uuid)
                )";
        */
        if write_access {
            _ = conn.execute(sql.as_str()).await.unwrap();
        }

        Self {
            connection: conn,
            db_table,
        }
    }

    pub async fn new_in_memory(db_table: &str) -> Self {
        let conn = SqliteConnection::connect("sqlite::memory:").await.unwrap();

        Self::with_connection(conn.into(), db_table, true).await
    }

    // TODO: reconsider write_access
    pub async fn new<U: AsRef<str>>(url: U, db_table: &str, write_access: bool) -> Self {
        let opts = AnyConnectOptions::from_str(url.as_ref()).expect("Cannot parse the uri");

        let opts: AnyConnectOptions = match opts.kind() {
            AnyKind::Sqlite => SqliteConnectOptions::try_from(opts)
                .unwrap()
                .read_only(!write_access)
                .create_if_missing(write_access)
                .into(),
            AnyKind::Postgres => PgConnectOptions::try_from(opts)
                .unwrap()
                .options([(
                    "default_transaction_read_only",
                    if write_access { "off" } else { "on" },
                )])
                .into(),
        };

        let conn = opts
            .connect()
            .await
            .expect("Cannot perform connection to the DB");

        Self::with_connection(conn, db_table, write_access).await
    }
}

#[async_trait::async_trait]
impl DomoPersistentStorage for SqlxStorage {
    async fn store(&mut self, element: &DomoCacheElement) {
        let mut insert = Query::insert();
        let sql = insert
            .replace()
            .into_table(self.db_table.clone())
            .columns([
                Id::TopicName,
                Id::TopicUuid,
                Id::Value,
                Id::Deleted,
                Id::PublicationTimestamp,
                Id::PublisherPeerId,
            ])
            .values_panic([
                element.topic_name.to_string().into(),
                element.topic_uuid.to_string().into(),
                element.value.to_string().into(),
                i32::from(element.deleted).into(),
                element.publication_timestamp.to_string().into(),
                element.publisher_peer_id.to_string().into(),
            ]);
        let (sql, values) = match self.connection.kind() {
            AnyKind::Sqlite => sql.build_sqlx(SqliteQueryBuilder),
            AnyKind::Postgres => sql.build_sqlx(PostgresQueryBuilder),
        };
        /*
                let command: String = if self.connection.kind() == AnyKind::Sqlite {
                    "INSERT OR REPLACE INTO ".to_owned() + &self.db_table + " (topic_name, topic_uuid, value, deleted, publication_timestamp, publisher_peer_id)\
                      VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
                } else if self.connection.kind() == AnyKind::Postgres {
                    "INSERT INTO ".to_owned()
                        + &self.db_table
                        + "
                     (topic_name, topic_uuid, value, deleted, publication_timestamp, publisher_peer_id)\
                      VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT(topic_name, topic_uuid) DO UPDATE SET \
                      value = $3, deleted = $4, publication_timestamp = $5, publisher_peer_id = $6"
                } else {
                    String::from("")
                };
        */
        sqlx::query_with(&sql, values)
            .execute(&mut self.connection)
            .await
            .expect("database error");
    }

    async fn get_all_elements(&mut self) -> Vec<DomoCacheElement> {
        let command = format!("SELECT * FROM {}", self.db_table.to_string());
        sqlx::query(&command)
            .try_map(|row: AnyRow| {
                let jvalue = row.get(2);
                let jvalue = serde_json::from_str(jvalue);

                let pub_timestamp_string: &str = row.get(4);

                let del: i32 = row.get(3);
                let deleted = del == 1;

                Ok(DomoCacheElement {
                    topic_name: row.get(0),
                    topic_uuid: row.get(1),
                    value: jvalue.unwrap(),
                    deleted,
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
    async fn open_read_from_memory() {
        let _s = super::SqlxStorage::new("sqlite::memory:", "domo_data", false).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn open_read_non_existent_file() {
        let _s = super::SqlxStorage::new("sqlite://aaskdjkasdka.sqlite", "domo_data", false).await;
    }

    #[tokio::test]
    async fn test_pgsql_db_connection() {
        let _s = super::SqlxStorage::new(
            "postgres://postgres:mysecretpassword@localhost/postgres",
            "domo_test_pgsql_connection",
            true,
        )
        .await;
    }

    #[tokio::test]
    async fn test_initial_get_all_elements() {
        use super::DomoPersistentStorage;

        let mut s = super::SqlxStorage::new_in_memory("domo_data").await;
        let v = s.get_all_elements().await;
        assert_eq!(v.len(), 0);

        let mut s = super::SqlxStorage::new(
            "postgres://postgres:mysecretpassword@localhost/postgres",
            "test_initial_get_all_elements",
            true,
        )
        .await;
        let v = s.get_all_elements().await;
        assert_eq!(v.len(), 0);
    }

    #[tokio::test]
    async fn test_store() {
        use super::DomoPersistentStorage;
        let mut s = super::SqlxStorage::new_in_memory("domo_data").await;

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

        let mut s = super::SqlxStorage::new(
            "postgres://postgres:mysecretpassword@localhost/postgres",
            "test_store",
            true,
        )
        .await;

        s.store(&m).await;

        let v = s.get_all_elements().await;

        assert_eq!(v.len(), 1);
        assert_eq!(v[0], m);
    }
}
