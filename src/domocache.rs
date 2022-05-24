use serde_json::{Result, Value, json};
use std::collections::HashMap;

pub trait DomoPersistentStorage{
    fn init(&mut self);
    fn store(&mut self, key: &str, key_value: &Value);
}

struct SqliteStorage {
    file: String
}

impl DomoPersistentStorage for SqliteStorage{
    fn init(&mut self){
        println!("Init SqliteStorage");
    }

    fn store(&mut self, key: &str, key_value: &Value) {
        println!("Store {} {}", key, key_value.to_string());
    }

}

pub trait DomoCacheOperations{
    // method to write a certain topic
    fn write(&mut self, key: &str, key_value: &Value);

    // method to read a certain topic
    fn read(&mut self, key: &str) -> Option<Value>;
}

pub struct DomoCache {
    isPersistentCache: bool,
    storage: Box<dyn DomoPersistentStorage>,
    cache: HashMap<String, Value>
}

impl DomoCacheOperations for DomoCache {

    fn write(&mut self, key: &str, key_value: &Value) {
        self.cache.insert(String::from(key), key_value.clone());
        // if persistent cache
        if self.isPersistentCache {

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

fn test_write_and_read_key(){
    let mut domoCache = DomoCache{
        isPersistentCache: true,
        storage: Box::new(SqliteStorage {file: String::from("./prova.sqlite")}),
        cache: HashMap::new()
    };

    domoCache.write("luce-1", &json!({ "connected": false}));

    let val = domoCache.read("luce-1").unwrap();


    assert_eq!(json!({ "connected": false}), val)

}