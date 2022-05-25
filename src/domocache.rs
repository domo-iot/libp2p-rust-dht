use serde_json::{Value, json};
use std::collections::HashMap;
use libp2p::wasm_ext::ffi::ConnectionEvent;
use rusqlite::{params, Connection, Result, OpenFlags};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn get_epoch_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}


pub trait DomoPersistentStorage{
    fn init(&mut self, write_access: bool);
    fn store(&mut self, element: &DomoCacheElement, deleted: bool);
    fn get_all_elements(&mut self) -> Vec<DomoCacheElement>;
}

struct SqliteStorage {
    house_uuid: String,
    sqlite_file: String,
    sqlite_connection: Option<Connection>
}

impl DomoPersistentStorage for SqliteStorage{

    fn init(&mut self, write_access: bool){


        let conn = if write_access == false {
            match Connection::open_with_flags(&self.sqlite_file,
                                        OpenFlags::SQLITE_OPEN_READ_ONLY) {
                Ok(conn) => conn,
                _ => {
                    panic!("Error while opening the sqlite DB");
                }
            }
        }
        else {
            let conn =
                match Connection::open(&self.sqlite_file) {
                    Ok(conn) => conn,
                    _ => {
                        panic!("Error while opening the sqlite DB");
                    }
                };

            let res = match conn.execute(
                "CREATE TABLE IF NOT EXISTS domo_data (
                  topic_name             TEXT,
                  topic_uuid             TEXT,
                  value                  TEXT,
                  deleted                INTEGER,
                  publication_timestamp   TEXT,
                  PRIMARY KEY (topic_name, topic_uuid)
                  )",
                [],
            ) {
                Ok(ret) => ret,
                _ => {
                    panic!("Error while executing operation on sqlite DB");
                }
            };

            conn
        };

        self.sqlite_connection = Some(conn);

    }

    fn store(&mut self, element: &DomoCacheElement, deleted: bool){

        let ret = match self.sqlite_connection.as_ref().unwrap().execute(
            "INSERT OR REPLACE INTO domo_data\
             (topic_name, topic_uuid, value, deleted, publication_timestamp)\
              VALUES (?1, ?2, ?3, ?4, ?5)",
            params![element.topic_name, element.topic_uuid, element.value.to_string(),
                deleted, element.publication_timestamp.to_string()],
        ) {

            Ok(ret) => ret,
            _ => {
                panic!("Error while executing write operation on sqlite")
            }
        };


    }

    fn get_all_elements(&mut self) -> Vec<DomoCacheElement>{

        // read all not deleted elements
        let mut stmt = self.sqlite_connection.as_ref().unwrap().prepare(
            "SELECT * FROM domo_data WHERE deleted=0").unwrap();


        let values_iter = stmt.query_map([], |row| {

            let jvalue: String = row.get(2)?;
            let jvalue = serde_json::from_str(&jvalue);

            let pub_timestamp_string: String = row.get(4)?;

            Ok(
                DomoCacheElement{
                    topic_name: row.get(0)?,
                    topic_uuid: row.get(1)?,
                    value: jvalue.unwrap(),
                    publication_timestamp: pub_timestamp_string.parse().unwrap()
                })
        }).unwrap();

        let mut ret = vec![];
        for val in values_iter {
            let v = val.unwrap();
            ret.push(v);
        }

        ret

    }

}

pub trait DomoCacheOperations{
    // method to write a DomoCacheElement
    fn insert_cache_element(&mut self, cache_element: DomoCacheElement, persist: bool);

    // method to read a DomoCacheElement
    fn read_cache_element(&mut self, topic_name: &str, topic_uuid: &str) -> Option<DomoCacheElement>;

    // method to write a value in cache
    fn write(&mut self, topic_name: &str, topic_uuid: &str, value: Value);

    // method to write a value checking timestamp
    // returns the value in cache if it is more recent that the one used while calling the method
    fn write_with_timestamp_check(&mut self, topic_name: &str, topic_uuid: &str, elem: DomoCacheElement)
                                  -> Option<DomoCacheElement>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct DomoCacheElement {
    topic_name: String,
    topic_uuid: String,
    value: Value,
    publication_timestamp: u128
}

pub struct DomoCache {
    house_uuid: String,
    is_persistent_cache: bool,
    storage: Box<dyn DomoPersistentStorage>,
    cache: HashMap<String, HashMap<String, DomoCacheElement>>
}


impl DomoCache{

    fn init(&mut self){
        // open DB connection, WRITE access if is_persistent_cache, READ ONLY otherwise
        self.storage.init(self.is_persistent_cache);

        // popolo la mia cache con il contenuto dello sqlite
        let ret = self.storage.get_all_elements();

        for elem in ret {
            self.insert_cache_element(elem, false);
        }
    }

    fn print(&self){

        for (topic_name, topic_name_map) in self.cache.iter() {
            println!(" TopicName {} ", topic_name);

            for (topic_uuid, value ) in topic_name_map.iter(){
                println!("uuid: {} value: {}", topic_uuid, value.value.to_string());
            }

        }

    }
}

impl DomoCacheOperations for DomoCache {

    fn insert_cache_element(&mut self, cache_element: DomoCacheElement, persist: bool){
        // topic_name already present
        if self.cache.contains_key(&cache_element.topic_name) {
            self.cache.get_mut(&cache_element.topic_name).unwrap().
                insert(cache_element.topic_uuid.clone(), cache_element.clone());
        }
        else {
            // first time that we add an element of topic_name type
            self.cache.insert(cache_element.topic_name.clone(), HashMap::new());
            self.cache.get_mut(&cache_element.topic_name).unwrap().
                insert(cache_element.topic_uuid.clone(), cache_element.clone());
        }

        if persist {
            self.storage.store(&cache_element, false);
        }

    }

    fn read_cache_element(&mut self, topic_name: &str, topic_uuid: &str) -> Option<DomoCacheElement>{
        let value = self.cache.get(topic_name);

        match value {
            Some(topic_map) => {
                match topic_map.get(topic_uuid) {
                    Some(element) => Some((*element).clone()),
                    None => None
                }
            },
            None => None
        }
    }

    fn write(&mut self, topic_name: &str, topic_uuid: &str, value: Value){

        let elem = DomoCacheElement{
            topic_name: String::from(topic_name),
            topic_uuid: String::from(topic_uuid),
            publication_timestamp: get_epoch_ms(),
            value: value
        };

        self.insert_cache_element(elem, true);

        // TBD: pubblicazione

    }

    fn write_with_timestamp_check(&mut self, topic_name: &str, topic_uuid: &str, elem: DomoCacheElement)
                                  -> Option<DomoCacheElement> {

        match self.read_cache_element(topic_name, topic_uuid) {
            Some(value) => {
                if value.publication_timestamp > elem.publication_timestamp {
                    Some(value)
                } else {
                    self.insert_cache_element(elem, true);
                    None
                }
            },
            None => {
                self.insert_cache_element(elem, true);
                None
            }
        }

    }





}

#[cfg(test)]

#[test]
fn test_write_and_read_key(){
    let house_uuid = String::from("CasaProva");
    let mut domo_cache = DomoCache{
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


    domo_cache.init();

    domo_cache.print();

    domo_cache.write("Domo::Light", "luce-1", json!({ "connected": true}));

    let val =
        domo_cache.read_cache_element("Domo::Light", "luce-1").unwrap().value;
    assert_eq!(json!({ "connected": true}), val)

}

#[test]
fn test_populate_from_sqlite(){
    let house_uuid = String::from("CasaProva");
    let mut domo_cache = DomoCache{
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


    domo_cache.init();

    let val = domo_cache.
        read_cache_element("Domo::Light", "luce-1").unwrap().value;

    assert_eq!(json!({ "connected": false}), val)

}


#[test]
fn test_write_twice_same_key(){
    let house_uuid = String::from("CasaProva");
    let mut domo_cache = DomoCache{
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


    domo_cache.init();

    domo_cache.write("Domo::Light", "luce-1", json!({ "connected": true}));

    let val = domo_cache.
        read_cache_element("Domo::Light", "luce-1").unwrap().value;

    assert_eq!(json!({ "connected": true}), val);


    domo_cache.write("Domo::Light", "luce-1", json!({ "connected": false}));

    let val = domo_cache.
        read_cache_element("Domo::Light", "luce-1").unwrap().value;

    assert_eq!(json!({ "connected": false}), val)

}



#[test]
fn test_write_old_timestamp(){
    let house_uuid = String::from("CasaProva");
    let mut domo_cache = DomoCache{
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

    domo_cache.init();

    domo_cache.write("Domo::Light", "luce-timestamp", json!({ "connected": true}));

    let old_val =
        domo_cache.read_cache_element("Domo::Light", "luce-timestamp").unwrap();

    let el = DomoCacheElement{
        topic_name: String::from("Domo::Light"),
        topic_uuid: String::from("luce-timestamp"),
        value: Default::default(),
        publication_timestamp: 0
    };

    // mi aspetto il vecchio valore perchè non deve fare la scrittura

    let ret = match domo_cache.write_with_timestamp_check("Domo::Light",
                                                           "luce-timestamp",
                                                           el) {
        Some(val) => Some(val),
        None => None
    }.unwrap();

    // mi aspetto di ricevere il vecchio valore
    assert_eq!(ret, old_val);

    domo_cache.write("Domo::Light", "luce-timestamp",
                     json!({ "connected": false}));

    let val = domo_cache.
        read_cache_element("Domo::Light", "luce-timestamp").unwrap();

    assert_ne!(ret, val);


}