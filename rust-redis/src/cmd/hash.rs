use crate::db::{Db, Value, Entry};
use crate::resp::Resp;
use std::collections::HashMap;

pub fn hset(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 4 {
        return Resp::Error("ERR wrong number of arguments for 'HSET'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    let field = match &items[2] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid field".to_string()),
    };
    let val = match &items[3] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid value".to_string()),
    };

    let mut entry = db.entry(key).or_insert_with(|| Entry::new(Value::Hash(HashMap::new()), None));
    if entry.is_expired() {
        entry.value = Value::Hash(HashMap::new());
        entry.expires_at = None;
    }

    if let Value::Hash(map) = &mut entry.value {
        let is_new = map.insert(field, val).is_none();
        Resp::Integer(if is_new { 1 } else { 0 })
    } else {
        Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
    }
}

pub fn hget(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'HGET'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    let field = match &items[2] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid field".to_string()),
    };

    if let Some(entry) = db.get(&key) {
        if entry.is_expired() {
            drop(entry);
            db.remove(&key);
            return Resp::BulkString(None);
        }
        match &entry.value {
            Value::Hash(map) => {
                match map.get(&field) {
                    Some(v) => Resp::BulkString(Some(v.clone())),
                    None => Resp::BulkString(None),
                }
            }
            _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        Resp::BulkString(None)
    }
}

pub fn hgetall(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'HGETALL'".to_string());
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
            return Resp::Array(Some(vec![]));
        }
        match &entry.value {
            Value::Hash(map) => {
                let mut result = Vec::with_capacity(map.len() * 2);
                for (k, v) in map {
                    result.push(Resp::BulkString(Some(k.clone())));
                    result.push(Resp::BulkString(Some(v.clone())));
                }
                Resp::Array(Some(result))
            }
            _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        Resp::Array(Some(vec![]))
    }
}

pub fn hmset(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 4 || items.len() % 2 != 0 {
        return Resp::Error("ERR wrong number of arguments for 'HMSET'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let mut entry = db.entry(key).or_insert_with(|| Entry::new(Value::Hash(HashMap::new()), None));
    if entry.is_expired() {
        entry.value = Value::Hash(HashMap::new());
        entry.expires_at = None;
    }

    if let Value::Hash(map) = &mut entry.value {
        for chunk in items[2..].chunks(2) {
            let field = match &chunk[0] {
                Resp::BulkString(Some(b)) => b.clone(),
                Resp::SimpleString(s) => s.clone(),
                _ => return Resp::Error("ERR invalid field".to_string()),
            };
            let val = match &chunk[1] {
                Resp::BulkString(Some(b)) => b.clone(),
                Resp::SimpleString(s) => s.clone(),
                _ => return Resp::Error("ERR invalid value".to_string()),
            };
            map.insert(field, val);
        }
        Resp::SimpleString(bytes::Bytes::from_static(b"OK"))
    } else {
        Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
    }
}

pub fn hmget(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'HMGET'".to_string());
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
            let len = items.len() - 2;
            let mut result = Vec::with_capacity(len);
            for _ in 0..len {
                result.push(Resp::BulkString(None));
            }
            return Resp::Array(Some(result));
        }
        match &entry.value {
            Value::Hash(map) => {
                let mut result = Vec::with_capacity(items.len() - 2);
                for i in 2..items.len() {
                    let field = match &items[i] {
                        Resp::BulkString(Some(b)) => b.clone(),
                        Resp::SimpleString(s) => s.clone(),
                        _ => return Resp::Error("ERR invalid field".to_string()),
                    };
                    match map.get(&field) {
                        Some(v) => result.push(Resp::BulkString(Some(v.clone()))),
                        None => result.push(Resp::BulkString(None)),
                    }
                }
                Resp::Array(Some(result))
            }
            _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        let len = items.len() - 2;
        let mut result = Vec::with_capacity(len);
        for _ in 0..len {
            result.push(Resp::BulkString(None));
        }
        Resp::Array(Some(result))
    }
}

pub fn hdel(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'HDEL'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    if let Some(mut entry) = db.get_mut(&key) {
        if entry.is_expired() {
            drop(entry);
            db.remove(&key);
            return Resp::Integer(0);
        }
        match &mut entry.value {
            Value::Hash(map) => {
                let mut count = 0;
                for i in 2..items.len() {
                    let field = match &items[i] {
                        Resp::BulkString(Some(b)) => b.clone(),
                        Resp::SimpleString(s) => s.clone(),
                        _ => return Resp::Error("ERR invalid field".to_string()),
                    };
                    if map.remove(&field).is_some() {
                        count += 1;
                    }
                }
                Resp::Integer(count)
            }
            _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        Resp::Integer(0)
    }
}

pub fn hlen(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'HLEN'".to_string());
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
            return Resp::Integer(0);
        }
        match &entry.value {
            Value::Hash(map) => Resp::Integer(map.len() as i64),
            _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        Resp::Integer(0)
    }
}
