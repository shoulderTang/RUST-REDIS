use crate::db::{Db, Value, Entry};
use crate::resp::Resp;
use std::collections::HashSet;

pub fn sadd(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'SADD'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let mut entry = db.entry(key).or_insert_with(|| Entry::new(Value::Set(HashSet::new()), None));
    if entry.is_expired() {
        entry.value = Value::Set(HashSet::new());
        entry.expires_at = None;
    }

    if let Value::Set(set) = &mut entry.value {
        let mut count = 0;
        for i in 2..items.len() {
            let member = match &items[i] {
                Resp::BulkString(Some(b)) => b.clone(),
                Resp::SimpleString(s) => s.clone(),
                _ => return Resp::Error("ERR invalid member".to_string()),
            };
            if set.insert(member) {
                count += 1;
            }
        }
        Resp::Integer(count)
    } else {
        Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
    }
}

pub fn srem(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'SREM'".to_string());
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
            Value::Set(set) => {
                let mut count = 0;
                for i in 2..items.len() {
                    let member = match &items[i] {
                        Resp::BulkString(Some(b)) => b.clone(),
                        Resp::SimpleString(s) => s.clone(),
                        _ => return Resp::Error("ERR invalid member".to_string()),
                    };
                    if set.remove(&member) {
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

pub fn sismember(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'SISMEMBER'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    let member = match &items[2] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid member".to_string()),
    };

    if let Some(entry) = db.get(&key) {
        if entry.is_expired() {
            drop(entry);
            db.remove(&key);
            return Resp::Integer(0);
        }
        match &entry.value {
            Value::Set(set) => {
                Resp::Integer(if set.contains(&member) { 1 } else { 0 })
            }
            _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        Resp::Integer(0)
    }
}

pub fn smembers(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'SMEMBERS'".to_string());
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
            Value::Set(set) => {
                let mut result = Vec::with_capacity(set.len());
                for member in set {
                    result.push(Resp::BulkString(Some(member.clone())));
                }
                Resp::Array(Some(result))
            }
            _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        Resp::Array(Some(vec![]))
    }
}

pub fn scard(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'SCARD'".to_string());
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
            Value::Set(set) => Resp::Integer(set.len() as i64),
            _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        Resp::Integer(0)
    }
}
