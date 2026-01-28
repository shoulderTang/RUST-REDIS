use crate::db::{Db, Entry, Value};
use crate::resp::Resp;
use std::collections::HashSet;
use bytes::Bytes;
use crate::cmd::key::match_pattern;

pub fn sadd(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'SADD'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let mut entry = db
        .entry(key)
        .or_insert_with(|| Entry::new(Value::Set(HashSet::new()), None));
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
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
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
            Value::Set(set) => Resp::Integer(if set.contains(&member) { 1 } else { 0 }),
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
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
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
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
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
        }
    } else {
        Resp::Integer(0)
    }
}

pub fn sscan(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'SSCAN'".to_string());
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
        
        if let Value::Set(set) = &entry.value {
            let mut all_members: Vec<bytes::Bytes> = set.iter().cloned().collect();
            all_members.sort();

            let total_len = all_members.len();
            if cursor >= total_len {
                 return Resp::Array(Some(vec![
                    Resp::BulkString(Some(Bytes::from("0"))),
                    Resp::Array(Some(vec![])),
                ]));
            }

            let end = std::cmp::min(cursor + count, total_len);
            let next_cursor = if end == total_len { 0 } else { end };

            let mut result_entries = Vec::new();
            for member in &all_members[cursor..end] {
                if let Some(pattern) = match_pattern_str {
                    if !match_pattern(pattern, member) {
                        continue;
                    }
                }
                result_entries.push(Resp::BulkString(Some(member.clone())));
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
