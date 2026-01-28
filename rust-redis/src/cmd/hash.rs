use crate::db::{Db, Entry, Value};
use crate::resp::Resp;
use bytes::Bytes;
use std::collections::HashMap;
use crate::cmd::key::match_pattern;

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

    let mut entry = db
        .entry(key)
        .or_insert_with(|| Entry::new(Value::Hash(HashMap::new()), None));
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

pub fn hscan(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'HSCAN'".to_string());
    }

    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let cursor_bytes = match &items[2] {
        Resp::BulkString(Some(b)) => b,
        Resp::SimpleString(s) => s,
        _ => return Resp::Error("ERR invalid cursor".to_string()),
    };
    let cursor_str = match std::str::from_utf8(cursor_bytes) {
        Ok(s) => s,
        Err(_) => return Resp::Error("ERR invalid cursor".to_string()),
    };
    let cursor: usize = match cursor_str.parse() {
        Ok(i) => i,
        Err(_) => return Resp::Error("ERR invalid cursor".to_string()),
    };

    let mut match_pattern_str: Option<&[u8]> = None;
    let mut count = 10;

    let mut idx = 3;
    while idx < items.len() {
        let arg = match &items[idx] {
            Resp::BulkString(Some(b)) => b,
            Resp::SimpleString(s) => s,
            _ => return Resp::Error("ERR syntax error".to_string()),
        };
        let arg_str = match std::str::from_utf8(arg) {
            Ok(s) => s.to_uppercase(),
            Err(_) => return Resp::Error("ERR syntax error".to_string()),
        };

        match arg_str.as_str() {
            "MATCH" => {
                if idx + 1 >= items.len() {
                    return Resp::Error("ERR syntax error".to_string());
                }
                match_pattern_str = match &items[idx+1] {
                    Resp::BulkString(Some(b)) => Some(b.as_ref()),
                    Resp::SimpleString(s) => Some(s.as_ref()),
                    _ => return Resp::Error("ERR syntax error".to_string()),
                };
                idx += 2;
            },
            "COUNT" => {
                if idx + 1 >= items.len() {
                    return Resp::Error("ERR syntax error".to_string());
                }
                let count_bytes = match &items[idx+1] {
                    Resp::BulkString(Some(b)) => b,
                    Resp::SimpleString(s) => s,
                    _ => return Resp::Error("ERR syntax error".to_string()),
                };
                let count_str = match std::str::from_utf8(count_bytes) {
                    Ok(s) => s,
                    Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
                };
                count = match count_str.parse() {
                    Ok(i) => i,
                    Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
                };
                idx += 2;
            },
            _ => return Resp::Error("ERR syntax error".to_string()),
        }
    }

    if let Some(entry) = db.get(&key) {
        if entry.is_expired() {
             return Resp::Array(Some(vec![
                Resp::BulkString(Some(Bytes::from("0"))),
                Resp::Array(Some(vec![])),
            ]));
        }
        
        if let Value::Hash(map) = &entry.value {
            let mut all_fields: Vec<bytes::Bytes> = map.keys().cloned().collect();
            all_fields.sort();

            let total_len = all_fields.len();
            if cursor >= total_len {
                 return Resp::Array(Some(vec![
                    Resp::BulkString(Some(Bytes::from("0"))),
                    Resp::Array(Some(vec![])),
                ]));
            }

            let end = std::cmp::min(cursor + count, total_len);
            let next_cursor = if end == total_len { 0 } else { end };

            let mut result_entries = Vec::new();
            for field in &all_fields[cursor..end] {
                if let Some(pattern) = match_pattern_str {
                    if !match_pattern(pattern, field) {
                        continue;
                    }
                }
                
                if let Some(val) = map.get(field) {
                    result_entries.push(Resp::BulkString(Some(field.clone())));
                    result_entries.push(Resp::BulkString(Some(val.clone())));
                }
            }

             Resp::Array(Some(vec![
                Resp::BulkString(Some(Bytes::from(next_cursor.to_string()))),
                Resp::Array(Some(result_entries)),
            ]))
        } else {
             Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        }
    } else {
        Resp::Array(Some(vec![
            Resp::BulkString(Some(Bytes::from("0"))),
            Resp::Array(Some(vec![])),
        ]))
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
            Value::Hash(map) => match map.get(&field) {
                Some(v) => Resp::BulkString(Some(v.clone())),
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
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
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

    let mut entry = db
        .entry(key)
        .or_insert_with(|| Entry::new(Value::Hash(HashMap::new()), None));
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
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
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
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
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
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
        }
    } else {
        Resp::Integer(0)
    }
}
