use crate::db::{Db, Entry, Value};
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::Arc;

pub fn del(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'DEL'".to_string());
    }

    let mut deleted = 0;
    for item in &items[1..] {
        let key = match item {
            Resp::BulkString(Some(b)) => b,
            Resp::SimpleString(s) => s,
            _ => continue,
        };

        if let Some(entry) = db.get(key) {
            if entry.is_expired() {
                drop(entry);
                db.remove(key);
            } else {
                drop(entry);
                if db.remove(key).is_some() {
                    deleted += 1;
                }
            }
        }
    }
    Resp::Integer(deleted)
}

pub fn expire(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'EXPIRE'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    let seconds_bytes = match &items[2] {
        Resp::BulkString(Some(b)) => b,
        Resp::SimpleString(s) => s,
        _ => return Resp::Error("ERR invalid seconds".to_string()),
    };
    let seconds_str = match std::str::from_utf8(seconds_bytes) {
        Ok(s) => s,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    let seconds: u64 = match seconds_str.parse() {
        Ok(s) => s,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    if let Some(mut entry) = db.get_mut(&key) {
        if entry.is_expired() {
            drop(entry);
            db.remove(&key);
            Resp::Integer(0)
        } else {
            let new_entry = Entry::new(entry.value.clone(), Some(seconds * 1000));
            entry.expires_at = new_entry.expires_at;
            Resp::Integer(1)
        }
    } else {
        Resp::Integer(0)
    }
}

pub fn pexpire(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'PEXPIRE'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    let ms_bytes = match &items[2] {
        Resp::BulkString(Some(b)) => b,
        Resp::SimpleString(s) => s,
        _ => return Resp::Error("ERR invalid milliseconds".to_string()),
    };
    let ms_str = match std::str::from_utf8(ms_bytes) {
        Ok(s) => s,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    let ms: u64 = match ms_str.parse() {
        Ok(s) => s,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    if let Some(mut entry) = db.get_mut(&key) {
        if entry.is_expired() {
            drop(entry);
            db.remove(&key);
            Resp::Integer(0)
        } else {
            let new_entry = Entry::new(entry.value.clone(), Some(ms));
            entry.expires_at = new_entry.expires_at;
            Resp::Integer(1)
        }
    } else {
        Resp::Integer(0)
    }
}

pub fn expireat(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'EXPIREAT'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    let timestamp_bytes = match &items[2] {
        Resp::BulkString(Some(b)) => b,
        Resp::SimpleString(s) => s,
        _ => return Resp::Error("ERR invalid timestamp".to_string()),
    };
    let timestamp_str = match std::str::from_utf8(timestamp_bytes) {
        Ok(s) => s,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    let timestamp: u64 = match timestamp_str.parse() {
        Ok(s) => s,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    if let Some(mut entry) = db.get_mut(&key) {
        if entry.is_expired() {
            drop(entry);
            db.remove(&key);
            Resp::Integer(0)
        } else {
            entry.expires_at = Some(timestamp * 1000);
            Resp::Integer(1)
        }
    } else {
        Resp::Integer(0)
    }
}

pub fn pexpireat(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'PEXPIREAT'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    let timestamp_bytes = match &items[2] {
        Resp::BulkString(Some(b)) => b,
        Resp::SimpleString(s) => s,
        _ => return Resp::Error("ERR invalid timestamp".to_string()),
    };
    let timestamp_str = match std::str::from_utf8(timestamp_bytes) {
        Ok(s) => s,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    let timestamp: u64 = match timestamp_str.parse() {
        Ok(s) => s,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    if let Some(mut entry) = db.get_mut(&key) {
        if entry.is_expired() {
            drop(entry);
            db.remove(&key);
            Resp::Integer(0)
        } else {
            entry.expires_at = Some(timestamp);
            Resp::Integer(1)
        }
    } else {
        Resp::Integer(0)
    }
}

pub fn ttl(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'TTL'".to_string());
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
            Resp::Integer(-2)
        } else {
            match entry.expires_at {
                Some(at) => {
                    let start = std::time::SystemTime::now();
                    let since_the_epoch = start
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("Time went backwards");
                    let now = since_the_epoch.as_millis() as u64;
                    if now >= at {
                        drop(entry);
                        db.remove(&key);
                        Resp::Integer(-2)
                    } else {
                        let ttl_ms = at - now;
                        Resp::Integer((ttl_ms / 1000) as i64)
                    }
                }
                None => Resp::Integer(-1),
            }
        }
    } else {
        Resp::Integer(-2)
    }
}

pub fn pttl(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'PTTL'".to_string());
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
            Resp::Integer(-2)
        } else {
            match entry.expires_at {
                Some(at) => {
                    let start = std::time::SystemTime::now();
                    let since_the_epoch = start
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("Time went backwards");
                    let now = since_the_epoch.as_millis() as u64;
                    if now >= at {
                        drop(entry);
                        db.remove(&key);
                        Resp::Integer(-2)
                    } else {
                        let ttl_ms = at - now;
                        Resp::Integer(ttl_ms as i64)
                    }
                }
                None => Resp::Integer(-1),
            }
        }
    } else {
        Resp::Integer(-2)
    }
}

pub fn exists(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'EXISTS'".to_string());
    }

    let mut count = 0;
    for item in &items[1..] {
        let key = match item {
            Resp::BulkString(Some(b)) => b,
            Resp::SimpleString(s) => s,
            _ => continue,
        };
        
        if let Some(entry) = db.get(key) {
            if !entry.is_expired() {
                count += 1;
            } else {
                drop(entry);
                db.remove(key);
            }
        }
    }
    Resp::Integer(count)
}

pub fn touch(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'TOUCH'".to_string());
    }

    let mut count = 0;
    for item in &items[1..] {
        let key = match item {
            Resp::BulkString(Some(b)) => b,
            Resp::SimpleString(s) => s,
            _ => continue,
        };
        
        if let Some(entry) = db.get(key) {
            if !entry.is_expired() {
                count += 1;
                // TODO: Update LRU/LFU access time when implemented in Entry
            } else {
                drop(entry);
                db.remove(key);
            }
        }
    }
    Resp::Integer(count)
}

pub fn type_(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'TYPE'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b,
        Resp::SimpleString(s) => s,
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    
    if let Some(entry) = db.get(key) {
        if entry.is_expired() {
            drop(entry);
            db.remove(key);
            Resp::SimpleString(Bytes::from("none"))
        } else {
            let type_str = match &entry.value {
                Value::String(_) => "string",
                Value::List(_) => "list",
                Value::Set(_) => "set",
                Value::ZSet(_) => "zset",
                Value::Hash(_) => "hash",
                Value::Stream(_) => "stream",
                Value::HyperLogLog(_) => "string",
            };
            Resp::SimpleString(Bytes::from(type_str))
        }
    } else {
        Resp::SimpleString(Bytes::from("none"))
    }
}

pub fn flushdb(items: &[Resp], db: &Db) -> Resp {
    if items.len() > 2 {
         // Redis 6.2 supports FLUSHDB [ASYNC|SYNC]
    }
    
    db.clear();
    Resp::SimpleString(Bytes::from("OK"))
}

use std::sync::RwLock;

pub fn flushall(items: &[Resp], databases: &Arc<Vec<RwLock<Db>>>) -> Resp {
    if items.len() > 2 {
        // Just warning or handling if needed. For now simple clear.
    }
    
    for db_lock in databases.iter() {
        db_lock.read().unwrap().clear();
    }
    Resp::SimpleString(Bytes::from("OK"))
}

pub fn dbsize(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 1 {
         return Resp::Error("ERR wrong number of arguments for 'DBSIZE'".to_string());
    }
    Resp::Integer(db.len() as i64)
}

pub fn keys(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'KEYS'".to_string());
    }
    
    let pattern = match &items[1] {
        Resp::BulkString(Some(b)) => b,
        Resp::SimpleString(s) => s,
        _ => return Resp::Error("ERR invalid pattern".to_string()),
    };
    
    let mut matched_keys = Vec::new();
    for r in db.iter() {
        let key = r.key();
        if match_pattern(pattern, key) {
             // Check expiration
             if !r.value().is_expired() {
                 matched_keys.push(Resp::BulkString(Some(key.clone())));
             }
        }
    }
    
    Resp::Array(Some(matched_keys))
}

pub fn match_pattern(pattern: &[u8], key: &[u8]) -> bool {
    let pattern_str = match std::str::from_utf8(pattern) {
        Ok(s) => s,
        Err(_) => return false,
    };
    let key_str = String::from_utf8_lossy(key);
    
    match glob::Pattern::new(pattern_str) {
        Ok(p) => p.matches(&key_str),
        Err(_) => false,
    }
}

pub fn rename(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'RENAME'".to_string());
    }
    let old_key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    let new_key = match &items[2] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    if old_key == new_key {
        if let Some(entry) = db.get(&old_key) {
            if entry.is_expired() {
                drop(entry);
                db.remove(&old_key);
                return Resp::Error("ERR no such key".to_string());
            }
        } else {
            return Resp::Error("ERR no such key".to_string());
        }
        return Resp::SimpleString(Bytes::from("OK"));
    }

    if let Some((_, entry)) = db.remove(&old_key) {
        if entry.is_expired() {
             return Resp::Error("ERR no such key".to_string());
        }
        db.insert(new_key, entry);
        Resp::SimpleString(Bytes::from("OK"))
    } else {
        Resp::Error("ERR no such key".to_string())
    }
}

pub fn renamenx(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'RENAMENX'".to_string());
    }
    let old_key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    let new_key = match &items[2] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    if old_key == new_key {
        if let Some(entry) = db.get(&old_key) {
            if entry.is_expired() {
                drop(entry);
                db.remove(&old_key);
                return Resp::Error("ERR no such key".to_string());
            }
        } else {
            return Resp::Error("ERR no such key".to_string());
        }
        return Resp::Integer(0);
    }
    
    if let Some(entry) = db.get(&new_key) {
         if !entry.is_expired() {
             return Resp::Integer(0);
         }
         drop(entry);
         db.remove(&new_key);
    }

    if let Some((_, entry)) = db.remove(&old_key) {
         if entry.is_expired() {
             return Resp::Error("ERR no such key".to_string());
         }
         db.insert(new_key, entry);
         Resp::Integer(1)
    } else {
        Resp::Error("ERR no such key".to_string())
    }
}

pub fn persist(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'PERSIST'".to_string());
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
        
        if entry.expires_at.is_some() {
            entry.expires_at = None;
            return Resp::Integer(1);
        }
    }
    Resp::Integer(0)
}

use crate::cmd::{as_bytes, ConnectionContext, ServerContext};

pub fn move_(items: &[Resp], conn_ctx: &mut ConnectionContext, server_ctx: &ServerContext) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'move' command".to_string());
    }

    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let db_idx_bytes = match &items[2] {
        Resp::BulkString(Some(b)) => b,
        Resp::SimpleString(s) => s,
        _ => return Resp::Error("ERR invalid database index".to_string()),
    };

    let db_idx_str = match std::str::from_utf8(db_idx_bytes) {
        Ok(s) => s,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    let dst_idx: usize = match db_idx_str.parse() {
        Ok(idx) => idx,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    if dst_idx == conn_ctx.db_index {
        return Resp::Error("ERR source and destination objects are the same".to_string());
    }

    if dst_idx >= server_ctx.databases.len() {
        return Resp::Error("ERR DB index is out of range".to_string());
    }

    let src_db = server_ctx.databases[conn_ctx.db_index].read().unwrap().clone();
    let dst_db = server_ctx.databases[dst_idx].read().unwrap().clone();

    if let Some(entry) = src_db.get(&key) {
        if entry.is_expired() {
            drop(entry);
            src_db.remove(&key);
            return Resp::Integer(0);
        }

        // Check if key exists in destination
        if let Some(dst_entry) = dst_db.get(&key) {
            if !dst_entry.is_expired() {
                return Resp::Integer(0);
            }
            drop(dst_entry);
            dst_db.remove(&key);
        }

        // Move it
        drop(entry);
        if let Some((_, entry)) = src_db.remove(&key) {
            dst_db.insert(key, entry);
            return Resp::Integer(1);
        }
    }

    Resp::Integer(0)
}

pub fn swapdb(items: &[Resp], server_ctx: &ServerContext) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'swapdb' command".to_string());
    }

    let idx1: usize = match as_bytes(&items[1]) {
        Some(b) => match std::str::from_utf8(&b) {
            Ok(s) => match s.parse() {
                Ok(idx) => idx,
                Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
            },
            Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
        },
        None => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    let idx2: usize = match as_bytes(&items[2]) {
        Some(b) => match std::str::from_utf8(&b) {
            Ok(s) => match s.parse() {
                Ok(idx) => idx,
                Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
            },
            Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
        },
        None => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    if idx1 >= server_ctx.databases.len() || idx2 >= server_ctx.databases.len() {
        return Resp::Error("ERR DB index is out of range".to_string());
    }

    if idx1 == idx2 {
        return Resp::SimpleString(Bytes::from("OK"));
    }

    // Swap the databases in the map
    let mut db1 = server_ctx.databases[idx1].write().unwrap();
    let mut db2 = server_ctx.databases[idx2].write().unwrap();

    std::mem::swap(&mut *db1, &mut *db2);

    Resp::SimpleString(Bytes::from("OK"))
}

pub fn scan(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'SCAN'".to_string());
    }

    let cursor_bytes = match &items[1] {
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
    let mut type_filter: Option<&str> = None;

    let mut idx = 2;
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
            "TYPE" => {
                if idx + 1 >= items.len() {
                    return Resp::Error("ERR syntax error".to_string());
                }
                let type_bytes = match &items[idx+1] {
                    Resp::BulkString(Some(b)) => b,
                    Resp::SimpleString(s) => s,
                    _ => return Resp::Error("ERR syntax error".to_string()),
                };
                type_filter = match std::str::from_utf8(type_bytes) {
                    Ok(s) => Some(s),
                    Err(_) => return Resp::Error("ERR syntax error".to_string()),
                };
                idx += 2;
            },
            _ => return Resp::Error("ERR syntax error".to_string()),
        }
    }

    let mut all_keys: Vec<bytes::Bytes> = db.iter().map(|r| r.key().clone()).collect();
    all_keys.sort();

    let total_len = all_keys.len();
    if cursor >= total_len {
        return Resp::Array(Some(vec![
            Resp::BulkString(Some(Bytes::from("0"))),
            Resp::Array(Some(vec![])),
        ]));
    }

    let end = std::cmp::min(cursor + count, total_len);
    let next_cursor = if end == total_len { 0 } else { end };

    let mut result_keys = Vec::new();
    for key in &all_keys[cursor..end] {
        if let Some(pattern) = match_pattern_str {
            if !match_pattern(pattern, key) {
                continue;
            }
        }

        if let Some(entry) = db.get(key) {
            if entry.is_expired() {
                continue;
            }
            if let Some(type_str) = type_filter {
                let actual_type = match &entry.value {
                    Value::String(_) => "string",
                    Value::List(_) => "list",
                    Value::Set(_) => "set",
                    Value::ZSet(_) => "zset",
                    Value::Hash(_) => "hash",
                    Value::Stream(_) => "stream",
                    Value::HyperLogLog(_) => "string",
                };
                if !actual_type.eq_ignore_ascii_case(type_str) {
                    continue;
                }
            }
            result_keys.push(Resp::BulkString(Some(key.clone())));
        }
    }

    Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from(next_cursor.to_string()))),
        Resp::Array(Some(result_keys)),
    ]))
}
