use crate::db::{Db, Entry, Value};
use crate::resp::Resp;
use std::collections::VecDeque;

pub fn lpush(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'LPUSH'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let mut entry = db
        .entry(key)
        .or_insert_with(|| Entry::new(Value::List(VecDeque::new()), None));
    if entry.is_expired() {
        entry.value = Value::List(VecDeque::new());
        entry.expires_at = None;
    }

    if let Value::List(list) = &mut entry.value {
        for i in 2..items.len() {
            let val = match &items[i] {
                Resp::BulkString(Some(b)) => b.clone(),
                Resp::SimpleString(s) => s.clone(),
                _ => return Resp::Error("ERR invalid value".to_string()),
            };
            list.push_front(val);
        }
        Resp::Integer(list.len() as i64)
    } else {
        Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
    }
}

pub fn rpush(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'RPUSH'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let mut entry = db
        .entry(key)
        .or_insert_with(|| Entry::new(Value::List(VecDeque::new()), None));
    if entry.is_expired() {
        entry.value = Value::List(VecDeque::new());
        entry.expires_at = None;
    }

    if let Value::List(list) = &mut entry.value {
        for i in 2..items.len() {
            let val = match &items[i] {
                Resp::BulkString(Some(b)) => b.clone(),
                Resp::SimpleString(s) => s.clone(),
                _ => return Resp::Error("ERR invalid value".to_string()),
            };
            list.push_back(val);
        }
        Resp::Integer(list.len() as i64)
    } else {
        Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
    }
}

pub fn lpop(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'LPOP'".to_string());
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
            return Resp::BulkString(None);
        }
        match &mut entry.value {
            Value::List(list) => match list.pop_front() {
                Some(v) => Resp::BulkString(Some(v)),
                None => Resp::BulkString(None),
            },
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
        }
    } else {
        Resp::BulkString(None)
    }
}

pub fn rpop(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'RPOP'".to_string());
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
            return Resp::BulkString(None);
        }
        match &mut entry.value {
            Value::List(list) => match list.pop_back() {
                Some(v) => Resp::BulkString(Some(v)),
                None => Resp::BulkString(None),
            },
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
        }
    } else {
        Resp::BulkString(None)
    }
}

pub fn llen(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'LLEN'".to_string());
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
            Value::List(list) => Resp::Integer(list.len() as i64),
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
        }
    } else {
        Resp::Integer(0)
    }
}

pub fn lrange(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 4 {
        return Resp::Error("ERR wrong number of arguments for 'LRANGE'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    let start_str = match std::str::from_utf8(match &items[2] {
        Resp::BulkString(Some(b)) => b,
        Resp::SimpleString(s) => s,
        _ => return Resp::Error("ERR invalid start".to_string()),
    }) {
        Ok(s) => s,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    let start: i64 = match start_str.parse() {
        Ok(s) => s,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    let stop_str = match std::str::from_utf8(match &items[3] {
        Resp::BulkString(Some(b)) => b,
        Resp::SimpleString(s) => s,
        _ => return Resp::Error("ERR invalid stop".to_string()),
    }) {
        Ok(s) => s,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    let stop: i64 = match stop_str.parse() {
        Ok(s) => s,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    if let Some(entry) = db.get(&key) {
        if entry.is_expired() {
            drop(entry);
            db.remove(&key);
            return Resp::Array(Some(vec![]));
        }
        match &entry.value {
            Value::List(list) => {
                let len = list.len() as i64;
                let mut start_idx = start;
                let mut stop_idx = stop;

                if start_idx < 0 {
                    start_idx += len;
                }
                if stop_idx < 0 {
                    stop_idx += len;
                }
                if start_idx < 0 {
                    start_idx = 0;
                }
                if stop_idx >= len {
                    stop_idx = len - 1;
                }

                if start_idx > stop_idx || start_idx >= len {
                    return Resp::Array(Some(vec![]));
                }

                let mut result = Vec::new();
                for i in start_idx..=stop_idx {
                    if let Some(val) = list.get(i as usize) {
                        result.push(Resp::BulkString(Some(val.clone())));
                    }
                }
                Resp::Array(Some(result))
            }
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
        }
    } else {
        Resp::Array(Some(vec![]))
    }
}
