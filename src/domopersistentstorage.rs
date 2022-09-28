use rusqlite::{params, Connection, OpenFlags};

use crate::domocache::DomoCacheElement;
use std::path::{Path, PathBuf};

pub trait DomoPersistentStorage {
    fn store(&mut self, element: &DomoCacheElement);
    fn get_all_elements(&mut self) -> Vec<DomoCacheElement>;
}

pub struct SqliteStorage {
    pub sqlite_file: PathBuf,
    pub sqlite_connection: Connection,
}

impl SqliteStorage {
    pub fn new<P: AsRef<Path>>(sqlite_file: P, write_access: bool) -> Self {
        let conn = if !write_access {
            match Connection::open_with_flags(&sqlite_file, OpenFlags::SQLITE_OPEN_READ_ONLY) {
                Ok(conn) => conn,
                Err(e) => panic!("Error while opening the sqlite DB: {e:?}"),
            }
        } else {
            let conn = match Connection::open(&sqlite_file) {
                Ok(conn) => conn,
                Err(e) => panic!("Error while opening the sqlite DB: {e:?}"),
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
            sqlite_file: sqlite_file.as_ref().to_path_buf(),
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
            Err(e) => panic!("Error while executing write operation on sqlite: {e:?}"),
        };
    }

    fn get_all_elements(&mut self) -> Vec<DomoCacheElement> {
        // read all not deleted elements
        let mut stmt = self
            .sqlite_connection
            .prepare("SELECT * FROM domo_data")
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
                    republication_timestamp: 0,
                })
            })
            .unwrap();

        values_iter.collect::<Result<Vec<_>, _>>().unwrap()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[should_panic]
    fn open_read_non_existent_file() {
        let _s = super::SqliteStorage::new("/tmp/aaskdjkasdka.sqlite", false);
    }

    #[test]
    fn open_write_non_existent_file() {
        let s = super::SqliteStorage::new("/tmp/nkasjkldjad.sqlite", true);
        assert_eq!(
            s.sqlite_file,
            std::path::Path::new("/tmp/nkasjkldjad.sqlite")
        );
    }

    #[test]
    fn test_initial_get_all_elements() {
        use super::DomoPersistentStorage;

        let mut s =
            crate::domopersistentstorage::SqliteStorage::new("/tmp/nkasjkldjad.sqlite", true);
        let v = s.get_all_elements();
        assert_eq!(v.len(), 0);
    }

    #[test]
    fn test_store() {
        use super::DomoPersistentStorage;
        let mut s =
            crate::domopersistentstorage::SqliteStorage::new("/tmp/nkasjkldjsdasd.sqlite", true);

        let m = crate::domocache::DomoCacheElement {
            topic_name: "a".to_string(),
            topic_uuid: "a".to_string(),
            value: Default::default(),
            deleted: false,
            publication_timestamp: 0,
            publisher_peer_id: "a".to_string(),
            republication_timestamp: 0,
        };

        s.store(&m);

        let v = s.get_all_elements();

        assert_eq!(v.len(), 1);
        assert_eq!(v[0], m);
    }
}
