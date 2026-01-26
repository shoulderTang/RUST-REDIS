use crate::db::{Db, Entry, Value};
use crate::resp::{Resp, as_bytes};
use bytes::Bytes;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn set(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
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

    let mut nx = false;
    let mut xx = false;
    let mut expire_at = None;
    let mut keepttl = false;
    let mut get = false;
    let mut expire_flag = false;

    let mut i = 3;
    while i < items.len() {
        if let Some(arg) = as_bytes(&items[i]) {
            if arg.eq_ignore_ascii_case(b"NX") {
                nx = true;
            } else if arg.eq_ignore_ascii_case(b"XX") {
                xx = true;
            } else if arg.eq_ignore_ascii_case(b"GET") {
                get = true;
            } else if arg.eq_ignore_ascii_case(b"KEEPTTL") {
                if expire_flag {
                    return Resp::Error("ERR syntax error".to_string());
                }
                keepttl = true;
                expire_flag = true;
            } else if arg.eq_ignore_ascii_case(b"EX") {
                if expire_flag {
                    return Resp::Error("ERR syntax error".to_string());
                }
                if i + 1 >= items.len() {
                    return Resp::Error("ERR syntax error".to_string());
                }
                if let Some(s) = as_bytes(&items[i + 1]) {
                    if let Ok(s) = std::str::from_utf8(s) {
                        if let Ok(secs) = s.parse::<u64>() {
                            expire_at = Some(
                                SystemTime::now()
                                    .checked_add(Duration::from_secs(secs))
                                    .unwrap(),
                            );
                            expire_flag = true;
                            i += 1;
                        } else {
                            return Resp::Error(
                                "ERR value is not an integer or out of range".to_string(),
                            );
                        }
                    } else {
                        return Resp::Error("ERR syntax error".to_string());
                    }
                }
            } else if arg.eq_ignore_ascii_case(b"PX") {
                if expire_flag {
                    return Resp::Error("ERR syntax error".to_string());
                }
                if i + 1 >= items.len() {
                    return Resp::Error("ERR syntax error".to_string());
                }
                if let Some(s) = as_bytes(&items[i + 1]) {
                    if let Ok(s) = std::str::from_utf8(s) {
                        if let Ok(millis) = s.parse::<u64>() {
                            expire_at = Some(
                                SystemTime::now()
                                    .checked_add(Duration::from_millis(millis))
                                    .unwrap(),
                            );
                            expire_flag = true;
                            i += 1;
                        } else {
                            return Resp::Error(
                                "ERR value is not an integer or out of range".to_string(),
                            );
                        }
                    } else {
                        return Resp::Error("ERR syntax error".to_string());
                    }
                }
            } else if arg.eq_ignore_ascii_case(b"EXAT") {
                if expire_flag {
                    return Resp::Error("ERR syntax error".to_string());
                }
                if i + 1 >= items.len() {
                    return Resp::Error("ERR syntax error".to_string());
                }
                if let Some(s) = as_bytes(&items[i + 1]) {
                    if let Ok(s) = std::str::from_utf8(s) {
                        if let Ok(secs) = s.parse::<u64>() {
                            expire_at = Some(UNIX_EPOCH + Duration::from_secs(secs));
                            expire_flag = true;
                            i += 1;
                        } else {
                            return Resp::Error(
                                "ERR value is not an integer or out of range".to_string(),
                            );
                        }
                    } else {
                        return Resp::Error("ERR syntax error".to_string());
                    }
                }
            } else if arg.eq_ignore_ascii_case(b"PXAT") {
                if expire_flag {
                    return Resp::Error("ERR syntax error".to_string());
                }
                if i + 1 >= items.len() {
                    return Resp::Error("ERR syntax error".to_string());
                }
                if let Some(s) = as_bytes(&items[i + 1]) {
                    if let Ok(s) = std::str::from_utf8(s) {
                        if let Ok(millis) = s.parse::<u64>() {
                            expire_at = Some(UNIX_EPOCH + Duration::from_millis(millis));
                            expire_flag = true;
                            i += 1;
                        } else {
                            return Resp::Error(
                                "ERR value is not an integer or out of range".to_string(),
                            );
                        }
                    } else {
                        return Resp::Error("ERR syntax error".to_string());
                    }
                }
            }
        }
        i += 1;
    }

    if nx && xx {
        return Resp::Error("ERR syntax error".to_string());
    }

    // Check existence
    let exists = db.contains_key(&key) && !db.get(&key).unwrap().is_expired();

    // Check NX/XX
    if nx && exists {
        return Resp::BulkString(None);
    }
    if xx && !exists {
        return Resp::BulkString(None);
    }

    // Handle GET
    let mut old_val = None;
    if get {
        if let Some(entry) = db.get(&key) {
            if !entry.is_expired() {
                match &entry.value {
                    Value::String(s) => old_val = Some(s.clone()),
                    _ => {
                        return Resp::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        );
                    }
                }
            }
        }
    }

    // Determine Expiry
    let expire_at_millis = if keepttl {
        if let Some(entry) = db.get(&key) {
            if !entry.is_expired() {
                entry.expires_at
            } else {
                None
            }
        } else {
            None
        }
    } else if let Some(t) = expire_at {
        Some(t.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64)
    } else {
        None
    };

    db.insert(
        key,
        Entry {
            value: Value::String(val),
            expires_at: expire_at_millis,
        },
    );

    if get {
        if let Some(v) = old_val {
            Resp::BulkString(Some(v))
        } else {
            Resp::BulkString(None)
        }
    } else {
        Resp::SimpleString(bytes::Bytes::from_static(b"OK"))
    }
}

pub fn incr(items: &[Resp], db: &Db) -> Resp {
    incr_decr_helper(items, db, 1)
}

pub fn decr(items: &[Resp], db: &Db) -> Resp {
    incr_decr_helper(items, db, -1)
}

pub fn incrby(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'INCRBY'".to_string());
    }
    let by = match as_bytes(&items[2]) {
        Some(b) => {
            if let Ok(s) = std::str::from_utf8(b) {
                if let Ok(n) = s.parse::<i64>() {
                    n
                } else {
                    return Resp::Error("ERR value is not an integer or out of range".to_string());
                }
            } else {
                return Resp::Error("ERR value is not an integer or out of range".to_string());
            }
        }
        None => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    incr_decr_helper(items, db, by)
}

pub fn decrby(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'DECRBY'".to_string());
    }
    let by = match as_bytes(&items[2]) {
        Some(b) => {
            if let Ok(s) = std::str::from_utf8(b) {
                if let Ok(n) = s.parse::<i64>() {
                    n
                } else {
                    return Resp::Error("ERR value is not an integer or out of range".to_string());
                }
            } else {
                return Resp::Error("ERR value is not an integer or out of range".to_string());
            }
        }
        None => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    incr_decr_helper(items, db, -by)
}

fn incr_decr_helper(items: &[Resp], db: &Db, by: i64) -> Resp {
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    // We need to lock or atomic update. DashMap doesn't support atomic update of value based on old value easily without a lock if we want to return new value.
    // But since we are using DashMap, we can use entry API if available or just get and insert.
    // Since this is a simple implementation, get and insert is fine for now, but not atomic.
    // However, DashMap `entry` API allows modification.

    // NOTE: DashMap's entry API might deadlock if we hold it too long? No, it locks the shard.

    let mut val: i64 = 0;

    // We need to handle expiration too.
    if let Some(entry) = db.get_mut(&key) {
        if entry.is_expired() {
            val = 0;
            // It's expired, so we treat as new.
            // We will overwrite it below.
        } else {
            match &entry.value {
                Value::String(s) => {
                    if let Ok(s_str) = std::str::from_utf8(s) {
                        if let Ok(n) = s_str.parse::<i64>() {
                            val = n;
                        } else {
                            return Resp::Error(
                                "ERR value is not an integer or out of range".to_string(),
                            );
                        }
                    } else {
                        return Resp::Error(
                            "ERR value is not an integer or out of range".to_string(),
                        );
                    }
                }
                _ => {
                    return Resp::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    );
                }
            }
        }
    }

    // Check overflow
    if let Some(new_val) = val.checked_add(by) {
        let new_val_str = new_val.to_string();

        // Correct logic with TTL preservation:
        if let Some(mut entry) = db.get_mut(&key) {
            if !entry.is_expired() {
                entry.value = Value::String(Bytes::from(new_val_str));
                return Resp::Integer(new_val);
            }
        }

        // If we are here, either it didn't exist or it was expired.
        db.insert(
            key,
            Entry::new(Value::String(Bytes::from(new_val_str)), None),
        );
        Resp::Integer(new_val)
    } else {
        Resp::Error("ERR increment or decrement would overflow".to_string())
    }
}

pub fn append(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'APPEND'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    let val_to_append = match &items[2] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::BulkString(None),
    };

    if let Some(mut entry) = db.get_mut(&key) {
        if !entry.is_expired() {
            match &mut entry.value {
                Value::String(s) => {
                    let mut new_s = Vec::with_capacity(s.len() + val_to_append.len());
                    new_s.extend_from_slice(s);
                    new_s.extend_from_slice(&val_to_append);
                    *s = Bytes::from(new_s);
                    return Resp::Integer(s.len() as i64);
                }
                _ => {
                    return Resp::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    );
                }
            }
        }
    }

    // Key doesn't exist or expired
    db.insert(key, Entry::new(Value::String(val_to_append.clone()), None));
    Resp::Integer(val_to_append.len() as i64)
}

pub fn strlen(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'STRLEN'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    if let Some(entry) = db.get(&key) {
        if entry.is_expired() {
            Resp::Integer(0)
        } else {
            match &entry.value {
                Value::String(s) => Resp::Integer(s.len() as i64),
                _ => Resp::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
            }
        }
    } else {
        Resp::Integer(0)
    }
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
                _ => Resp::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
            }
        }
    } else {
        Resp::BulkString(None)
    }
}

pub fn mset(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 || items.len() % 2 == 0 {
        return Resp::Error("ERR wrong number of arguments for 'MSET'".to_string());
    }

    // Validate keys and values first to ensure atomicity-like behavior (though we are single threaded mostly)
    // Redis MSET is atomic, so we should either do all or none.
    // In our case, we can just iterate and insert since we are in a lock or single thread?
    // The Db is thread-safe (DashMap), but process_frame runs in a task.
    // However, since we don't have transactions yet, we just loop and set.

    for i in (1..items.len()).step_by(2) {
        let key = match &items[i] {
            Resp::BulkString(Some(b)) => b.clone(),
            Resp::SimpleString(s) => s.clone(),
            _ => return Resp::Error("ERR invalid key".to_string()),
        };
        let val = match &items[i + 1] {
            Resp::BulkString(Some(b)) => b.clone(),
            Resp::SimpleString(s) => s.clone(),
            _ => return Resp::BulkString(None),
        };
        db.insert(key, Entry::new(Value::String(val), None));
    }

    Resp::SimpleString(bytes::Bytes::from_static(b"OK"))
}

pub fn mget(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'MGET'".to_string());
    }

    let mut values = Vec::with_capacity(items.len() - 1);

    for i in 1..items.len() {
        let key = match &items[i] {
            Resp::BulkString(Some(b)) => b.clone(),
            Resp::SimpleString(s) => s.clone(),
            _ => return Resp::Error("ERR invalid key".to_string()),
        };

        if let Some(entry) = db.get(&key) {
            if entry.is_expired() {
                drop(entry);
                db.remove(&key);
                values.push(Resp::BulkString(None));
            } else {
                match &entry.value {
                    Value::String(b) => values.push(Resp::BulkString(Some(b.clone()))),
                    _ => values.push(Resp::BulkString(None)), // Redis MGET treats wrong type as nil
                }
            }
        } else {
            values.push(Resp::BulkString(None));
        }
    }

    Resp::Array(Some(values))
}
