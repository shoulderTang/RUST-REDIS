use crate::db::{Db, Entry, Value};
use crate::resp::Resp;
use bytes::Bytes;
use std::collections::HashMap;
use crate::cmd::key::match_pattern;
use rand::seq::IteratorRandom;
use rand::seq::IndexedRandom;

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

pub fn hsetnx(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 4 {
        return Resp::Error("ERR wrong number of arguments for 'HSETNX'".to_string());
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
        if map.contains_key(&field) {
            Resp::Integer(0)
        } else {
            map.insert(field, val);
            Resp::Integer(1)
        }
    } else {
        Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
    }
}

pub fn hincrby(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 4 {
        return Resp::Error("ERR wrong number of arguments for 'HINCRBY'".to_string());
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
    let increment = match &items[3] {
        Resp::BulkString(Some(b)) => {
             match std::str::from_utf8(b) {
                Ok(s) => match s.parse::<i64>() {
                    Ok(i) => i,
                    Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
                },
                Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
             }
        },
        Resp::SimpleString(s) => {
            match std::str::from_utf8(s) {
                Ok(s_str) => match s_str.parse::<i64>() {
                    Ok(i) => i,
                    Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
                },
                Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
            }
        },
        _ => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    let mut entry = db
        .entry(key)
        .or_insert_with(|| Entry::new(Value::Hash(HashMap::new()), None));
    
    if entry.is_expired() {
        entry.value = Value::Hash(HashMap::new());
        entry.expires_at = None;
    }

    if let Value::Hash(map) = &mut entry.value {
        let new_val = if let Some(old_val) = map.get(&field) {
            match std::str::from_utf8(old_val) {
                Ok(s) => match s.parse::<i64>() {
                    Ok(old_i) => {
                        match old_i.checked_add(increment) {
                            Some(sum) => sum,
                            None => return Resp::Error("ERR increment or decrement would overflow".to_string()),
                        }
                    },
                    Err(_) => return Resp::Error("ERR hash value is not an integer".to_string()),
                },
                Err(_) => return Resp::Error("ERR hash value is not an integer".to_string()),
            }
        } else {
            increment
        };

        let val_str = new_val.to_string();
        map.insert(field, Bytes::from(val_str));
        Resp::Integer(new_val)
    } else {
        Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
    }
}

pub fn hincrbyfloat(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 4 {
        return Resp::Error("ERR wrong number of arguments for 'HINCRBYFLOAT'".to_string());
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
    let increment = match &items[3] {
        Resp::BulkString(Some(b)) => {
             match std::str::from_utf8(b) {
                Ok(s) => match s.parse::<f64>() {
                    Ok(f) => {
                        if f.is_nan() || f.is_infinite() {
                            return Resp::Error("ERR value is not a valid float".to_string());
                        }
                        f
                    },
                    Err(_) => return Resp::Error("ERR value is not a valid float".to_string()),
                },
                Err(_) => return Resp::Error("ERR value is not a valid float".to_string()),
             }
        },
        Resp::SimpleString(s) => {
            match std::str::from_utf8(s) {
                Ok(s_str) => match s_str.parse::<f64>() {
                    Ok(f) => {
                         if f.is_nan() || f.is_infinite() {
                            return Resp::Error("ERR value is not a valid float".to_string());
                        }
                        f
                    },
                    Err(_) => return Resp::Error("ERR value is not a valid float".to_string()),
                },
                Err(_) => return Resp::Error("ERR value is not a valid float".to_string()),
            }
        },
        _ => return Resp::Error("ERR value is not a valid float".to_string()),
    };

    let mut entry = db
        .entry(key)
        .or_insert_with(|| Entry::new(Value::Hash(HashMap::new()), None));
    
    if entry.is_expired() {
        entry.value = Value::Hash(HashMap::new());
        entry.expires_at = None;
    }

    if let Value::Hash(map) = &mut entry.value {
        let new_val = if let Some(old_val) = map.get(&field) {
            match std::str::from_utf8(old_val) {
                Ok(s) => match s.parse::<f64>() {
                    Ok(old_f) => {
                         if old_f.is_nan() || old_f.is_infinite() {
                            return Resp::Error("ERR value is not a valid float".to_string());
                        }
                        old_f + increment
                    },
                    Err(_) => return Resp::Error("ERR hash value is not a float".to_string()),
                },
                Err(_) => return Resp::Error("ERR hash value is not a float".to_string()),
            }
        } else {
            increment
        };
        
        if new_val.is_nan() || new_val.is_infinite() {
             return Resp::Error("ERR increment would produce NaN or Infinity".to_string());
        }

        let val_str = new_val.to_string();
        let val_bytes = Bytes::from(val_str);
        map.insert(field, val_bytes.clone());
        Resp::BulkString(Some(val_bytes))
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
            return Resp::Array(Some(Vec::new()));
        }
        match &entry.value {
            Value::Hash(map) => {
                let mut res = Vec::new();
                for (k, v) in map {
                    res.push(Resp::BulkString(Some(k.clone())));
                    res.push(Resp::BulkString(Some(v.clone())));
                }
                Resp::Array(Some(res))
            }
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
        }
    } else {
        Resp::Array(Some(Vec::new()))
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
        for i in (2..items.len()).step_by(2) {
            let field = match &items[i] {
                Resp::BulkString(Some(b)) => b.clone(),
                Resp::SimpleString(s) => s.clone(),
                _ => return Resp::Error("ERR invalid field".to_string()),
            };
            let val = match &items[i + 1] {
                Resp::BulkString(Some(b)) => b.clone(),
                Resp::SimpleString(s) => s.clone(),
                _ => return Resp::Error("ERR invalid value".to_string()),
            };
            map.insert(field, val);
        }
        Resp::SimpleString(Bytes::from_static(b"OK"))
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
            let mut res = Vec::new();
            for _ in 2..items.len() {
                res.push(Resp::BulkString(None));
            }
            return Resp::Array(Some(res));
        }
        match &entry.value {
            Value::Hash(map) => {
                let mut res = Vec::new();
                for i in 2..items.len() {
                    let field = match &items[i] {
                        Resp::BulkString(Some(b)) => b.clone(),
                        Resp::SimpleString(s) => s.clone(),
                        _ => return Resp::Error("ERR invalid field".to_string()),
                    };
                    match map.get(&field) {
                        Some(v) => res.push(Resp::BulkString(Some(v.clone()))),
                        None => res.push(Resp::BulkString(None)),
                    }
                }
                Resp::Array(Some(res))
            }
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
        }
    } else {
        let mut res = Vec::new();
        for _ in 2..items.len() {
            res.push(Resp::BulkString(None));
        }
        Resp::Array(Some(res))
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
    
    // Parse cursor
    let cursor = match &items[2] {
        Resp::BulkString(Some(b)) => match std::str::from_utf8(b) {
            Ok(s) => match s.parse::<u64>() {
                Ok(i) => i,
                Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
            },
            Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
        },
        Resp::SimpleString(s) => match std::str::from_utf8(s) {
            Ok(s_str) => match s_str.parse::<u64>() {
                Ok(i) => i,
                Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
            },
            Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
        },
        _ => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    // Parse options
    let mut match_pattern_str: Option<String> = None;
    let mut count: usize = 10;
    
    let mut i = 3;
    while i < items.len() {
        let arg = match &items[i] {
            Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
            Resp::SimpleString(s) => String::from_utf8_lossy(s).to_string(),
            _ => return Resp::Error("ERR syntax error".to_string()),
        };
        
        match arg.to_uppercase().as_str() {
            "MATCH" => {
                if i + 1 >= items.len() {
                    return Resp::Error("ERR syntax error".to_string());
                }
                match_pattern_str = match &items[i + 1] {
                    Resp::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                    Resp::SimpleString(s) => Some(String::from_utf8_lossy(s).to_string()),
                    _ => return Resp::Error("ERR syntax error".to_string()),
                };
                i += 2;
            }
            "COUNT" => {
                if i + 1 >= items.len() {
                    return Resp::Error("ERR syntax error".to_string());
                }
                count = match &items[i + 1] {
                    Resp::BulkString(Some(b)) => match std::str::from_utf8(b) {
                        Ok(s) => match s.parse::<usize>() {
                            Ok(c) => c,
                            Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
                        },
                        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
                    },
                    Resp::SimpleString(s) => match std::str::from_utf8(s) {
                        Ok(s_str) => match s_str.parse::<usize>() {
                            Ok(c) => c,
                            Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
                        },
                        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
                    },
                    _ => return Resp::Error("ERR value is not an integer or out of range".to_string()),
                };
                i += 2;
            }
            _ => return Resp::Error("ERR syntax error".to_string()),
        }
    }

    if let Some(entry) = db.get(&key) {
        if entry.is_expired() {
            drop(entry);
            db.remove(&key);
            return Resp::Array(Some(vec![
                Resp::BulkString(Some(Bytes::from("0"))),
                Resp::Array(Some(Vec::new())),
            ]));
        }
        
        match &entry.value {
            Value::Hash(map) => {
                let keys: Vec<&Bytes> = map.keys().collect();
                
                let mut res = Vec::new();
                let mut next_cursor = 0;
                
                if !keys.is_empty() {
                    let start = if cursor as usize >= keys.len() {
                        0 
                    } else {
                        cursor as usize
                    };
                    
                    let mut current = start;
                    let mut added = 0;
                    
                    while added < count && current < keys.len() {
                        let key_bytes = keys[current];
                        
                        let include = if let Some(pattern) = &match_pattern_str {
                             match_pattern(pattern.as_bytes(), key_bytes)
                        } else {
                            true
                        };
                        
                        if include {
                            if let Some(val) = map.get(key_bytes) {
                                res.push(Resp::BulkString(Some(key_bytes.clone())));
                                res.push(Resp::BulkString(Some(val.clone())));
                                added += 1;
                            }
                        }
                        current += 1;
                    }
                    
                    if current < keys.len() {
                        next_cursor = current as u64;
                    }
                }
                
                Resp::Array(Some(vec![
                    Resp::BulkString(Some(Bytes::from(next_cursor.to_string()))),
                    Resp::Array(Some(res)),
                ]))
            }
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
        }
    } else {
        Resp::Array(Some(vec![
            Resp::BulkString(Some(Bytes::from("0"))),
            Resp::Array(Some(Vec::new())),
        ]))
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
                if map.is_empty() {
                    drop(entry);
                    db.remove(&key);
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

pub fn hstrlen(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'HSTRLEN'".to_string());
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

    if let Some(mut entry) = db.get_mut(&key) {
        if entry.is_expired() {
            drop(entry);
            db.remove(&key);
            return Resp::Integer(0);
        }
        match &mut entry.value {
            Value::Hash(map) => match map.get(&field) {
                Some(v) => Resp::Integer(v.len() as i64),
                None => Resp::Integer(0),
            },
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
        }
    } else {
        Resp::Integer(0)
    }
}

pub fn hkeys(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'HKEYS'".to_string());
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
            return Resp::Array(Some(Vec::new()));
        }
        match &entry.value {
            Value::Hash(map) => {
                let mut keys = Vec::new();
                for (k, _) in map {
                    keys.push(Resp::BulkString(Some(k.clone())));
                }
                Resp::Array(Some(keys))
            }
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
        }
    } else {
        Resp::Array(Some(Vec::new()))
    }
}

pub fn hvals(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'HVALS'".to_string());
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
            return Resp::Array(Some(Vec::new()));
        }
        match &entry.value {
            Value::Hash(map) => {
                let mut vals = Vec::new();
                for (_, v) in map {
                    vals.push(Resp::BulkString(Some(v.clone())));
                }
                Resp::Array(Some(vals))
            }
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
        }
    } else {
        Resp::Array(Some(Vec::new()))
    }
}

pub fn hrandfield(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'HRANDFIELD'".to_string());
    }

    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let mut count: Option<i64> = None;
    let mut with_values = false;

    if items.len() > 2 {
        match &items[2] {
            Resp::BulkString(Some(b)) => {
                 match String::from_utf8_lossy(b).parse::<i64>() {
                     Ok(n) => count = Some(n),
                     Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
                 }
            }
            Resp::SimpleString(s) => {
                 match String::from_utf8_lossy(s).parse::<i64>() {
                     Ok(n) => count = Some(n),
                     Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
                 }
            }
            Resp::Integer(i) => count = Some(*i),
             _ => return Resp::Error("ERR value is not an integer or out of range".to_string()),
        }
    }

    if items.len() > 3 {
        match &items[3] {
            Resp::BulkString(Some(b)) => {
                if String::from_utf8_lossy(b).to_uppercase() == "WITHVALUES" {
                    with_values = true;
                } else {
                     return Resp::Error("ERR syntax error".to_string());
                }
            }
            Resp::SimpleString(s) => {
                 if String::from_utf8_lossy(s).to_uppercase() == "WITHVALUES" {
                    with_values = true;
                } else {
                     return Resp::Error("ERR syntax error".to_string());
                }
            }
             _ => return Resp::Error("ERR syntax error".to_string()),
        }
    }

    let entry_guard = db.get(&key);
    
    if let Some(entry) = entry_guard {
        if entry.is_expired() {
             if count.is_some() {
                 return Resp::Array(Some(Vec::new()));
             } else {
                 return Resp::BulkString(None);
             }
        }
        
        if let Value::Hash(map) = &entry.value {
             if map.is_empty() {
                 if count.is_some() {
                     return Resp::Array(Some(Vec::new()));
                 } else {
                     return Resp::BulkString(None);
                 }
             }

             let mut rng = rand::rng();

             match count {
                 None => {
                     // Return single random field
                     if let Some((k, _)) = map.iter().choose(&mut rng) {
                         return Resp::BulkString(Some(k.clone()));
                     } else {
                         return Resp::BulkString(None);
                     }
                 }
                 Some(c) => {
                     let mut result = Vec::new();
                     if c >= 0 {
                         let count_val = c as usize;
                         let selected: Vec<_> = map.iter().choose_multiple(&mut rng, count_val);
                         for (k, v) in selected {
                             result.push(Resp::BulkString(Some(k.clone())));
                             if with_values {
                                 result.push(Resp::BulkString(Some(v.clone())));
                             }
                         }
                     } else {
                         let count_val = (-c) as usize;
                         let keys: Vec<_> = map.keys().collect();
                         for _ in 0..count_val {
                             if let Some(k) = keys.choose(&mut rng) {
                                 result.push(Resp::BulkString(Some((*k).clone())));
                                 if with_values {
                                     if let Some(v) = map.get(*k) {
                                         result.push(Resp::BulkString(Some(v.clone())));
                                     }
                                 }
                             }
                         }
                     }
                     return Resp::Array(Some(result));
                 }
             }

        } else {
            return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
        }
    } else {
        if count.is_some() {
             return Resp::Array(Some(Vec::new()));
         } else {
             return Resp::BulkString(None);
         }
    }
}
