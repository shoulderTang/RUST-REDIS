use crate::db::{Db, Value, Entry};
use crate::resp::Resp;

pub fn set(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'SET'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    let val = match &items[2] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::BulkString(None),
    };
    db.insert(key, Entry::new(Value::String(val), None));
    Resp::SimpleString(bytes::Bytes::from_static(b"OK"))
}

pub fn get(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'GET'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    if let Some(entry) = db.get(&key) {
        if entry.is_expired() {
            drop(entry);
            db.remove(&key);
            Resp::BulkString(None)
        } else {
            match &entry.value {
                Value::String(b) => Resp::BulkString(Some(b.clone())),
                _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        }
    } else {
        Resp::BulkString(None)
    }
}
