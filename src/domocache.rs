use serde_json::{Value, json};
use std::collections::HashMap;
use libp2p::wasm_ext::ffi::ConnectionEvent;
use rusqlite::{params, Connection, Result};

pub trait DomoPersistentStorage{
    fn init(&mut self);
    fn store(&mut self, key: &str, key_value: &Value);
    fn populate_map(&mut self, map: &mut HashMap<String, Value>);
}

struct SqliteStorage {
    house_uuid: String,
    sqlite_file: String,
    sqlite_connection: Option<Connection>
}

impl DomoPersistentStorage for SqliteStorage{

    fn init(&mut self){
        self.sqlite_file = String::from("/home/domenico/");
        self.sqlite_file.push_str(&self.house_uuid);
        self.sqlite_file.push_str(&String::from(".sqlite"));


        let conn = Connection::open(&self.sqlite_file).unwrap();

        conn.execute(
            "CREATE TABLE IF NOT EXISTS domo_data (
                  key             TEXT PRIMARY KEY,
                  key_value       BLOB,
                  deleted         INTEGER
                  )",
            [],
        ).unwrap();

        self.sqlite_connection = Some(conn);

    }

    fn store(&mut self, key: &str, key_value: &Value) {
        println!("Store {} {}", key, key_value.to_string());

        self.sqlite_connection.as_ref().unwrap().execute(
            "INSERT INTO domo_data (key, key_value, deleted) VALUES (?1, ?2, ?3)",
            params![key, key_value.to_string(), false],
        ).unwrap();

    }

    fn populate_map(&mut self, map: &mut HashMap<String, Value>){

        let mut stmt = self.sqlite_connection.as_ref().unwrap().prepare(
            "SELECT key, key_value FROM domo_data WHERE deleted=0").unwrap();

        struct Elem {
            key: String,
            value: String
        }

        let values_iter = stmt.query_map([], |row| {
            Ok(
                Elem{
                    key: row.get(0)?,
                    value: row.get(1)?
                })
        }).unwrap();

        for val in values_iter {
            let v = val.unwrap();
            map.insert(v.key, serde_json::from_str(&v.value).unwrap());
        }


    }

}

pub trait DomoCacheOperations{
    // method to write a certain topic
    fn write(&mut self, key: &str, key_value: &Value);

    // method to read a certain topic
    fn read(&mut self, key: &str) -> Option<Value>;
}

pub struct DomoCache {
    house_uuid: String,
    is_persistent_cache: bool,
    storage: Box<dyn DomoPersistentStorage>,
    cache: HashMap<String, Value>
}


impl DomoCache{

    fn init(&mut self){
        // apro la connessione al DB e se non presente creo la tabella domo_data
        if self.is_persistent_cache {
            self.storage.init();
        }

        // popolo la mia cache con il contenuto dello sqlite
        self.storage.populate_map(&mut self.cache);
    }

    fn print(&self){

        for (key, value) in self.cache.iter() {
            println!(" {} - {}", key, value.to_string());
        }

    }
}
impl DomoCacheOperations for DomoCache {

    fn write(&mut self, key: &str, key_value: &Value) {
        self.cache.insert(String::from(key), key_value.clone());
        // if persistent cache
        if self.is_persistent_cache {
            self.storage.store(key, key_value);
        }
    }

    fn read(&mut self, key: &str) -> Option<Value> {
        let value = self.cache.get(key);

        match value {
            Some(val) => Some(val.clone()),
            None => None
        }
    }


}

#[cfg(test)]

#[test]
// fn test_write_and_read_key(){
//     let house_uuid = String::from("CasaProva");
//     let mut domoCache = DomoCache{
//         house_uuid: house_uuid.clone(),
//         is_persistent_cache: true,
//         storage: Box::new(
//             SqliteStorage {
//                 house_uuid: house_uuid.clone(),
//                 sqlite_file: String::from("./prova.sqlite"),
//                 sqlite_connection: None
//             }),
//         cache: HashMap::new()
//     };
//
//
//     domoCache.init();
//
//     domoCache.write("luce-1", &json!({ "connected": false}));
//
//     let val = domoCache.read("luce-1").unwrap();
//     assert_eq!(json!({ "connected": false}), val)
//
// }

fn test_populate_from_sqlite(){
    let house_uuid = String::from("CasaProva");
    let mut domoCache = DomoCache{
        house_uuid: house_uuid.clone(),
        is_persistent_cache: true,
        storage: Box::new(
            SqliteStorage {
                house_uuid: house_uuid.clone(),
                sqlite_file: String::from("./prova.sqlite"),
                sqlite_connection: None
            }),
        cache: HashMap::new()
    };


    domoCache.init();

    let val = domoCache.read("luce-1").unwrap();
    assert_eq!(json!({ "connected": false}), val)

}