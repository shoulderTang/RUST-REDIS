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
    let mut expire_at: Option<u64> = None;
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
                        if let Ok(v) = s.parse::<u64>() {
                            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                            expire_at = Some(now + v * 1000);
                            expire_flag = true;
                        } else {
                            return Resp::Error("ERR value is not an integer or out of range".to_string());
                        }
                    } else {
                        return Resp::Error("ERR value is not an integer or out of range".to_string());
                    }
                }
                i += 1;
            } else if arg.eq_ignore_ascii_case(b"PX") {
                if expire_flag {
                    return Resp::Error("ERR syntax error".to_string());
                }
                if i + 1 >= items.len() {
                    return Resp::Error("ERR syntax error".to_string());
                }
                if let Some(s) = as_bytes(&items[i + 1]) {
                    if let Ok(s) = std::str::from_utf8(s) {
                        if let Ok(v) = s.parse::<u64>() {
                            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                            expire_at = Some(now + v);
                            expire_flag = true;
                        } else {
                            return Resp::Error("ERR value is not an integer or out of range".to_string());
                        }
                    } else {
                        return Resp::Error("ERR value is not an integer or out of range".to_string());
                    }
                }
                i += 1;
            } else if arg.eq_ignore_ascii_case(b"EXAT") {
                 if expire_flag {
                    return Resp::Error("ERR syntax error".to_string());
                }
                if i + 1 >= items.len() {
                    return Resp::Error("ERR syntax error".to_string());
                }
                if let Some(s) = as_bytes(&items[i + 1]) {
                    if let Ok(s) = std::str::from_utf8(s) {
                        if let Ok(v) = s.parse::<u64>() {
                            expire_at = Some(v * 1000);
                            expire_flag = true;
                        } else {
                            return Resp::Error("ERR value is not an integer or out of range".to_string());
                        }
                    } else {
                        return Resp::Error("ERR value is not an integer or out of range".to_string());
                    }
                }
                i += 1;
            } else if arg.eq_ignore_ascii_case(b"PXAT") {
                if expire_flag {
                    return Resp::Error("ERR syntax error".to_string());
                }
                if i + 1 >= items.len() {
                    return Resp::Error("ERR syntax error".to_string());
                }
                if let Some(s) = as_bytes(&items[i + 1]) {
                    if let Ok(s) = std::str::from_utf8(s) {
                        if let Ok(v) = s.parse::<u64>() {
                            expire_at = Some(v);
                            expire_flag = true;
                        } else {
                            return Resp::Error("ERR value is not an integer or out of range".to_string());
                        }
                    } else {
                        return Resp::Error("ERR value is not an integer or out of range".to_string());
                    }
                }
                i += 1;
            } else if arg.eq_ignore_ascii_case(b"PERSIST") {
                if expire_flag {
                    return Resp::Error("ERR syntax error".to_string());
                }
                expire_flag = true;
                expire_at = None;
            } else {
                return Resp::Error("ERR syntax error".to_string());
            }
        }
        i += 1;
    }

    let mut old_val = None;

    if get {
        if let Some(entry) = db.get(&key) {
             if entry.is_expired() {
                 // expired, return nil
             } else {
                match &entry.value {
                    Value::String(s) => old_val = Some(s.clone()),
                    _ => return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
             }
        }
    }

    if nx {
        if let Some(entry) = db.get(&key) {
            if !entry.is_expired() {
                if get {
                     return Resp::BulkString(old_val);
                }
                return Resp::BulkString(None);
            }
        }
    }

    if xx {
        if let Some(entry) = db.get(&key) {
            if entry.is_expired() {
                if get {
                     return Resp::BulkString(old_val);
                }
                return Resp::BulkString(None);
            }
        } else {
            if get {
                 return Resp::BulkString(old_val);
            }
            return Resp::BulkString(None);
        }
    }
    
    // If KEEPTTL, get existing ttl
    if keepttl {
         if let Some(entry) = db.get(&key) {
             if !entry.is_expired() {
                 expire_at = entry.expires_at;
             }
         }
    }

    db.insert(key, Entry::new_with_expire(Value::String(val), expire_at));
    
    if get {
        Resp::BulkString(old_val)
    } else {
        Resp::SimpleString(Bytes::from_static(b"OK"))
    }
}

fn incr_decr_helper(items: &[Resp], db: &Db, inc: i64) -> Resp {
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    
    let mut val: i64 = 0;
    let mut expire_at = None;
    
    if let Some(entry) = db.get_mut(&key) {
        if !entry.is_expired() {
             match &entry.value {
                 Value::String(b) => {
                     if let Ok(s) = std::str::from_utf8(b) {
                         if let Ok(v) = s.parse::<i64>() {
                             val = v;
                             expire_at = entry.expires_at;
                         } else {
                             return Resp::Error("ERR value is not an integer or out of range".to_string());
                         }
                     } else {
                          return Resp::Error("ERR value is not an integer or out of range".to_string());
                     }
                 },
                 _ => return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
             }
        }
    }
    
    let (new_val, overflow) = val.overflowing_add(inc);
    if overflow {
        return Resp::Error("ERR increment or decrement would overflow".to_string());
    }
    
    db.insert(key, Entry::new_with_expire(Value::String(Bytes::from(new_val.to_string())), expire_at));
    Resp::Integer(new_val)
}

pub fn incr(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'INCR'".to_string());
    }
    incr_decr_helper(items, db, 1)
}

pub fn decr(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'DECR'".to_string());
    }
    incr_decr_helper(items, db, -1)
}

pub fn incrby(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'INCRBY'".to_string());
    }
    let inc = match &items[2] {
        Resp::BulkString(Some(b)) => {
            if let Ok(s) = std::str::from_utf8(b) {
                if let Ok(v) = s.parse::<i64>() {
                    v
                } else {
                     return Resp::Error("ERR value is not an integer or out of range".to_string());
                }
            } else {
                 return Resp::Error("ERR value is not an integer or out of range".to_string());
            }
        },
        Resp::SimpleString(s) => {
             if let Ok(s) = std::str::from_utf8(s) {
                if let Ok(v) = s.parse::<i64>() {
                    v
                } else {
                     return Resp::Error("ERR value is not an integer or out of range".to_string());
                }
            } else {
                 return Resp::Error("ERR value is not an integer or out of range".to_string());
            }
        },
        _ => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    incr_decr_helper(items, db, inc)
}

pub fn decrby(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'DECRBY'".to_string());
    }
    let inc = match &items[2] {
        Resp::BulkString(Some(b)) => {
            if let Ok(s) = std::str::from_utf8(b) {
                if let Ok(v) = s.parse::<i64>() {
                    v
                } else {
                     return Resp::Error("ERR value is not an integer or out of range".to_string());
                }
            } else {
                 return Resp::Error("ERR value is not an integer or out of range".to_string());
            }
        },
        Resp::SimpleString(s) => {
             if let Ok(s) = std::str::from_utf8(s) {
                if let Ok(v) = s.parse::<i64>() {
                    v
                } else {
                     return Resp::Error("ERR value is not an integer or out of range".to_string());
                }
            } else {
                 return Resp::Error("ERR value is not an integer or out of range".to_string());
            }
        },
        _ => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    
    if inc == i64::MIN {
        return Resp::Error("ERR decrement would overflow".to_string());
    }
    
    incr_decr_helper(items, db, -inc)
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
    let val = match &items[2] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid value".to_string()),
    };
    
    if let Some(mut entry) = db.get_mut(&key) {
        if entry.is_expired() {
            drop(entry);
            let len = val.len();
            db.insert(key, Entry::new(Value::String(val), None));
            return Resp::Integer(len as i64);
        }
        
        match &mut entry.value {
            Value::String(s) => {
                let mut vec = s.to_vec();
                vec.extend_from_slice(&val);
                let len = vec.len();
                *s = Bytes::from(vec);
                Resp::Integer(len as i64)
            },
            _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        let len = val.len();
        db.insert(key, Entry::new(Value::String(val), None));
        Resp::Integer(len as i64)
    }
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
            return Resp::Integer(0);
        }
        match &entry.value {
            Value::String(s) => Resp::Integer(s.len() as i64),
            _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        Resp::Integer(0)
    }
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
                     Value::String(s) => values.push(Resp::BulkString(Some(s.clone()))),
                     _ => values.push(Resp::BulkString(None)),
                 }
             }
        } else {
            values.push(Resp::BulkString(None));
        }
    }
    
    Resp::Array(Some(values))
}

pub fn setnx(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'SETNX'".to_string());
    }
    // Convert to SET key val NX
    let mut new_items = Vec::with_capacity(4);
    new_items.push(Resp::BulkString(Some(Bytes::from_static(b"SET"))));
    new_items.push(items[1].clone());
    new_items.push(items[2].clone());
    new_items.push(Resp::BulkString(Some(Bytes::from_static(b"NX"))));
    
    let res = set(&new_items, db);
    match res {
        Resp::SimpleString(s) if s == Bytes::from_static(b"OK") => Resp::Integer(1),
        Resp::BulkString(None) => Resp::Integer(0),
        _ => res,
    }
}

pub fn setex(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 4 {
        return Resp::Error("ERR wrong number of arguments for 'SETEX'".to_string());
    }
    // SET key val EX seconds
    // args: SETEX key seconds val -> SET key val EX seconds
    let mut new_items = Vec::with_capacity(5);
    new_items.push(Resp::BulkString(Some(Bytes::from_static(b"SET"))));
    new_items.push(items[1].clone()); // key
    new_items.push(items[3].clone()); // val
    new_items.push(Resp::BulkString(Some(Bytes::from_static(b"EX"))));
    new_items.push(items[2].clone()); // seconds
    
    set(&new_items, db)
}

pub fn psetex(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 4 {
        return Resp::Error("ERR wrong number of arguments for 'PSETEX'".to_string());
    }
    // SET key val PX milliseconds
    // args: PSETEX key milliseconds val -> SET key val PX milliseconds
    let mut new_items = Vec::with_capacity(5);
    new_items.push(Resp::BulkString(Some(Bytes::from_static(b"SET"))));
    new_items.push(items[1].clone()); // key
    new_items.push(items[3].clone()); // val
    new_items.push(Resp::BulkString(Some(Bytes::from_static(b"PX"))));
    new_items.push(items[2].clone()); // milliseconds
    
    set(&new_items, db)
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
             return Resp::BulkString(None);
        }
        match &entry.value {
            Value::String(s) => Resp::BulkString(Some(s.clone())),
            _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        Resp::BulkString(None)
    }
}

pub fn getset(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'GETSET'".to_string());
    }
    // Convert to SET key val GET
    let mut new_items = Vec::with_capacity(4);
    new_items.push(Resp::BulkString(Some(Bytes::from_static(b"SET"))));
    new_items.push(items[1].clone());
    new_items.push(items[2].clone());
    new_items.push(Resp::BulkString(Some(Bytes::from_static(b"GET"))));
    
    set(&new_items, db)
}

pub fn getdel(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'GETDEL'".to_string());
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
             return Resp::BulkString(None);
        }
        match &entry.value {
             Value::String(s) => {
                 let res = Resp::BulkString(Some(s.clone()));
                 drop(entry);
                 db.remove(&key);
                 res
             }
             _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        Resp::BulkString(None)
    }
}

pub fn getex(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'GETEX'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    
    // Parse options
    let mut expire_at: Option<u64> = None;
    let mut persist = false;
    let mut expire_set = false;
    
    let mut i = 2;
    while i < items.len() {
        if let Some(arg) = as_bytes(&items[i]) {
            if arg.eq_ignore_ascii_case(b"EX") {
                if expire_set { return Resp::Error("ERR syntax error".to_string()); }
                if i + 1 >= items.len() { return Resp::Error("ERR syntax error".to_string()); }
                if let Some(s) = as_bytes(&items[i + 1]) {
                    if let Ok(s) = std::str::from_utf8(s) {
                        if let Ok(v) = s.parse::<u64>() {
                            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                            expire_at = Some(now + v * 1000);
                            expire_set = true;
                        } else { return Resp::Error("ERR value is not an integer or out of range".to_string()); }
                    } else { return Resp::Error("ERR value is not an integer or out of range".to_string()); }
                }
                i += 1;
            } else if arg.eq_ignore_ascii_case(b"PX") {
                if expire_set { return Resp::Error("ERR syntax error".to_string()); }
                if i + 1 >= items.len() { return Resp::Error("ERR syntax error".to_string()); }
                if let Some(s) = as_bytes(&items[i + 1]) {
                    if let Ok(s) = std::str::from_utf8(s) {
                        if let Ok(v) = s.parse::<u64>() {
                            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                            expire_at = Some(now + v);
                            expire_set = true;
                        } else { return Resp::Error("ERR value is not an integer or out of range".to_string()); }
                    } else { return Resp::Error("ERR value is not an integer or out of range".to_string()); }
                }
                i += 1;
            } else if arg.eq_ignore_ascii_case(b"EXAT") {
                if expire_set { return Resp::Error("ERR syntax error".to_string()); }
                if i + 1 >= items.len() { return Resp::Error("ERR syntax error".to_string()); }
                if let Some(s) = as_bytes(&items[i + 1]) {
                    if let Ok(s) = std::str::from_utf8(s) {
                        if let Ok(v) = s.parse::<u64>() {
                            expire_at = Some(v * 1000);
                            expire_set = true;
                        } else { return Resp::Error("ERR value is not an integer or out of range".to_string()); }
                    } else { return Resp::Error("ERR value is not an integer or out of range".to_string()); }
                }
                i += 1;
            } else if arg.eq_ignore_ascii_case(b"PXAT") {
                if expire_set { return Resp::Error("ERR syntax error".to_string()); }
                if i + 1 >= items.len() { return Resp::Error("ERR syntax error".to_string()); }
                if let Some(s) = as_bytes(&items[i + 1]) {
                    if let Ok(s) = std::str::from_utf8(s) {
                        if let Ok(v) = s.parse::<u64>() {
                            expire_at = Some(v);
                            expire_set = true;
                        } else { return Resp::Error("ERR value is not an integer or out of range".to_string()); }
                    } else { return Resp::Error("ERR value is not an integer or out of range".to_string()); }
                }
                i += 1;
            } else if arg.eq_ignore_ascii_case(b"PERSIST") {
                if expire_set { return Resp::Error("ERR syntax error".to_string()); }
                persist = true;
                expire_set = true;
            } else {
                return Resp::Error("ERR syntax error".to_string());
            }
        }
        i += 1;
    }

    if let Some(mut entry) = db.get_mut(&key) {
        if entry.is_expired() {
             drop(entry);
             db.remove(&key);
             return Resp::BulkString(None);
        }
        match &entry.value {
             Value::String(s) => {
                 let val = s.clone();
                 if expire_set {
                     if persist {
                         entry.expires_at = None;
                     } else {
                         entry.expires_at = expire_at;
                     }
                 }
                 Resp::BulkString(Some(val))
             }
             _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        Resp::BulkString(None)
    }
}

pub fn mset(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 || items.len() % 2 == 0 {
        return Resp::Error("ERR wrong number of arguments for 'MSET'".to_string());
    }
    
    for i in (1..items.len()).step_by(2) {
        let key = match &items[i] {
            Resp::BulkString(Some(b)) => b.clone(),
            Resp::SimpleString(s) => s.clone(),
            _ => return Resp::Error("ERR invalid key".to_string()),
        };
        let val = match &items[i+1] {
            Resp::BulkString(Some(b)) => b.clone(),
            Resp::SimpleString(s) => s.clone(),
            _ => return Resp::Error("ERR invalid value".to_string()),
        };
        db.insert(key, Entry::new(Value::String(val), None));
    }
    
    Resp::SimpleString(Bytes::from_static(b"OK"))
}

pub fn msetnx(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 || items.len() % 2 == 0 {
        return Resp::Error("ERR wrong number of arguments for 'MSETNX'".to_string());
    }
    
    // Check if any key exists
    for i in (1..items.len()).step_by(2) {
        let key = match &items[i] {
            Resp::BulkString(Some(b)) => b.clone(),
            Resp::SimpleString(s) => s.clone(),
            _ => return Resp::Error("ERR invalid key".to_string()),
        };
        
        if let Some(entry) = db.get(&key) {
            if !entry.is_expired() {
                return Resp::Integer(0);
            }
        }
    }
    
    // Set all
    for i in (1..items.len()).step_by(2) {
        let key = match &items[i] {
            Resp::BulkString(Some(b)) => b.clone(),
            Resp::SimpleString(s) => s.clone(),
            _ => return Resp::Error("ERR invalid key".to_string()),
        };
        let val = match &items[i+1] {
            Resp::BulkString(Some(b)) => b.clone(),
            Resp::SimpleString(s) => s.clone(),
            _ => return Resp::Error("ERR invalid value".to_string()),
        };
        db.insert(key, Entry::new(Value::String(val), None));
    }
    
    Resp::Integer(1)
}

pub fn incrbyfloat(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'INCRBYFLOAT'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    
    let increment = match &items[2] {
        Resp::BulkString(Some(b)) => {
            if let Ok(s) = std::str::from_utf8(b) {
                if let Ok(v) = s.parse::<f64>() {
                    v
                } else {
                    return Resp::Error("ERR value is not a valid float".to_string());
                }
            } else {
                return Resp::Error("ERR value is not a valid float".to_string());
            }
        },
        Resp::SimpleString(s) => {
             if let Ok(s) = std::str::from_utf8(s) {
                if let Ok(v) = s.parse::<f64>() {
                    v
                } else {
                     return Resp::Error("ERR value is not a valid float".to_string());
                }
            } else {
                return Resp::Error("ERR value is not a valid float".to_string());
            }
        },
        _ => return Resp::Error("ERR value is not a valid float".to_string()),
    };

    let mut new_val = increment;
    let mut expire_at = None;

    if let Some(entry) = db.get_mut(&key) {
        if !entry.is_expired() {
             match &entry.value {
                 Value::String(b) => {
                     if let Ok(s) = std::str::from_utf8(b) {
                         if let Ok(v) = s.parse::<f64>() {
                             new_val = v + increment;
                             expire_at = entry.expires_at;
                         } else {
                             return Resp::Error("ERR value is not a valid float".to_string());
                         }
                     } else {
                          return Resp::Error("ERR value is not a valid float".to_string());
                     }
                 },
                 _ => return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
             }
        }
    }
    
    if !new_val.is_finite() {
        return Resp::Error("ERR increment would produce NaN or Infinity".to_string());
    }
    
    let new_val_str = new_val.to_string();
    db.insert(key, Entry::new_with_expire(Value::String(Bytes::from(new_val_str.clone())), expire_at));
    Resp::BulkString(Some(Bytes::from(new_val_str)))
}

pub fn getrange(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 4 {
        return Resp::Error("ERR wrong number of arguments for 'GETRANGE'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    
    let start = match &items[2] {
        Resp::BulkString(Some(b)) => {
            if let Ok(s) = std::str::from_utf8(b) {
                if let Ok(v) = s.parse::<i64>() {
                    v
                } else {
                    return Resp::Error("ERR value is not an integer or out of range".to_string());
                }
            } else {
                return Resp::Error("ERR value is not an integer or out of range".to_string());
            }
        },
        Resp::SimpleString(s) => {
             if let Ok(s) = std::str::from_utf8(s) {
                if let Ok(v) = s.parse::<i64>() {
                    v
                } else {
                    return Resp::Error("ERR value is not an integer or out of range".to_string());
                }
            } else {
                return Resp::Error("ERR value is not an integer or out of range".to_string());
            }
        },
        _ => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    
    let end = match &items[3] {
        Resp::BulkString(Some(b)) => {
            if let Ok(s) = std::str::from_utf8(b) {
                if let Ok(v) = s.parse::<i64>() {
                    v
                } else {
                    return Resp::Error("ERR value is not an integer or out of range".to_string());
                }
            } else {
                return Resp::Error("ERR value is not an integer or out of range".to_string());
            }
        },
        Resp::SimpleString(s) => {
             if let Ok(s) = std::str::from_utf8(s) {
                if let Ok(v) = s.parse::<i64>() {
                    v
                } else {
                    return Resp::Error("ERR value is not an integer or out of range".to_string());
                }
            } else {
                return Resp::Error("ERR value is not an integer or out of range".to_string());
            }
        },
        _ => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    
    if let Some(entry) = db.get(&key) {
        if entry.is_expired() {
            drop(entry);
            db.remove(&key);
            return Resp::BulkString(Some(Bytes::new()));
        }
        match &entry.value {
            Value::String(s) => {
                let len = s.len() as i64;
                if len == 0 {
                    return Resp::BulkString(Some(Bytes::new()));
                }
                
                let mut start_idx = start;
                let mut end_idx = end;
                
                if start_idx < 0 {
                    start_idx = len + start_idx;
                }
                if end_idx < 0 {
                    end_idx = len + end_idx;
                }
                
                if start_idx < 0 { start_idx = 0; }
                if end_idx < 0 { end_idx = 0; }
                if end_idx >= len { end_idx = len - 1; }
                
                if start_idx > end_idx || start_idx >= len {
                     return Resp::BulkString(Some(Bytes::new()));
                }
                
                let sub = s.slice(start_idx as usize..=(end_idx as usize));
                Resp::BulkString(Some(sub))
            },
            _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        Resp::BulkString(Some(Bytes::new()))
    }
}

pub fn setrange(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 4 {
        return Resp::Error("ERR wrong number of arguments for 'SETRANGE'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    
    let offset = match &items[2] {
        Resp::BulkString(Some(b)) => {
            if let Ok(s) = std::str::from_utf8(b) {
                if let Ok(v) = s.parse::<u64>() { // Redis offset is positive integer (>=0)
                    v
                } else {
                     return Resp::Error("ERR value is not an integer or out of range".to_string());
                }
            } else {
                 return Resp::Error("ERR value is not an integer or out of range".to_string());
            }
        },
        Resp::SimpleString(s) => {
             if let Ok(s) = std::str::from_utf8(s) {
                if let Ok(v) = s.parse::<u64>() {
                    v
                } else {
                     return Resp::Error("ERR value is not an integer or out of range".to_string());
                }
            } else {
                 return Resp::Error("ERR value is not an integer or out of range".to_string());
            }
        },
        _ => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    
    let value = match &items[3] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::BulkString(None),
    };
    
    // Check for max size (proto-max-bulk-len is 512MB by default, but let's just check overflow)
    // 536870911 is 512*1024*1024 - 1. Redis allows 512MB.
    if offset + (value.len() as u64) > 536870912 {
         return Resp::Error("ERR string exceeds maximum allowed size (512MB)".to_string());
    }

    let offset = offset as usize;
    
    if let Some(mut entry) = db.get_mut(&key) {
        if entry.is_expired() {
            drop(entry);
            // Treat as new
            let mut new_vec = Vec::with_capacity(offset + value.len());
            // Pad with zeros
            if offset > 0 {
                new_vec.resize(offset, 0);
            }
            new_vec.extend_from_slice(&value);
            let len = new_vec.len();
            db.insert(key, Entry::new(Value::String(Bytes::from(new_vec)), None));
            return Resp::Integer(len as i64);
        }
        
        match &mut entry.value {
             Value::String(s) => {
                 let mut vec = s.to_vec();
                 // Pad if needed
                 if offset > vec.len() {
                     vec.resize(offset, 0);
                 }
                 
                 // Overwrite or Append
                 let required_len = offset + value.len();
                 if vec.len() < required_len {
                     vec.resize(required_len, 0);
                 }
                 
                 // Copy bytes
                 // This handles overwrite
                 vec[offset..offset+value.len()].copy_from_slice(&value);
                 
                 let len = vec.len();
                 *s = Bytes::from(vec);
                 Resp::Integer(len as i64)
             },
             _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        // Create new
        let mut new_vec = Vec::with_capacity(offset + value.len());
        // Pad with zeros
        if offset > 0 {
            new_vec.resize(offset, 0);
        }
        new_vec.extend_from_slice(&value);
        let len = new_vec.len();
        db.insert(key, Entry::new(Value::String(Bytes::from(new_vec)), None));
        Resp::Integer(len as i64)
    }
}

pub fn stralgo(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'STRALGO'".to_string());
    }
    
    // Arg 1 must be LCS
    match as_bytes(&items[1]) {
        Some(b) if b.eq_ignore_ascii_case(b"LCS") => {},
        _ => return Resp::Error("ERR syntax error".to_string()),
    }

    let mut get_len = false;
    let mut get_idx = false;
    let mut with_match_len = false;
    let mut min_match_len = 0;
    
    let mut str_a: Option<Bytes> = None;
    let mut str_b: Option<Bytes> = None;
    
    let mut i = 2;
    while i < items.len() {
        if let Some(arg) = as_bytes(&items[i]) {
             if arg.eq_ignore_ascii_case(b"KEYS") {
                 if i + 2 >= items.len() {
                      return Resp::Error("ERR syntax error".to_string());
                 }
                 if str_a.is_some() {
                      return Resp::Error("ERR Either use STRINGS or KEYS".to_string());
                 }
                 // Fetch keys
                 let key1 = match &items[i+1] {
                     Resp::BulkString(Some(b)) => b.clone(),
                     Resp::SimpleString(s) => s.clone(),
                     _ => return Resp::Error("ERR invalid key".to_string()),
                 };
                 let key2 = match &items[i+2] {
                     Resp::BulkString(Some(b)) => b.clone(),
                     Resp::SimpleString(s) => s.clone(),
                     _ => return Resp::Error("ERR invalid key".to_string()),
                 };
                 
                 let v1 = db.get(&key1).map(|e| e.value.clone());
                 let v2 = db.get(&key2).map(|e| e.value.clone());
                 
                 str_a = match v1 {
                     Some(Value::String(b)) => Some(b),
                     None => Some(Bytes::new()),
                     _ => return Resp::Error("ERR value is not a string".to_string()),
                 };
                 
                 str_b = match v2 {
                     Some(Value::String(b)) => Some(b),
                     None => Some(Bytes::new()),
                     _ => return Resp::Error("ERR value is not a string".to_string()),
                 };
                 
                 i += 3;
             } else if arg.eq_ignore_ascii_case(b"STRINGS") {
                 if i + 2 >= items.len() {
                      return Resp::Error("ERR syntax error".to_string());
                 }
                 if str_a.is_some() {
                      return Resp::Error("ERR Either use STRINGS or KEYS".to_string());
                 }
                 str_a = match &items[i+1] {
                     Resp::BulkString(Some(b)) => Some(b.clone()),
                     Resp::SimpleString(s) => Some(s.clone()),
                     _ => return Resp::Error("ERR invalid string".to_string()),
                 };
                 str_b = match &items[i+2] {
                     Resp::BulkString(Some(b)) => Some(b.clone()),
                     Resp::SimpleString(s) => Some(s.clone()),
                     _ => return Resp::Error("ERR invalid string".to_string()),
                 };
                 i += 3;
             } else if arg.eq_ignore_ascii_case(b"LEN") {
                 get_len = true;
                 i += 1;
             } else if arg.eq_ignore_ascii_case(b"IDX") {
                 get_idx = true;
                 i += 1;
             } else if arg.eq_ignore_ascii_case(b"WITHMATCHLEN") {
                 with_match_len = true;
                 i += 1;
             } else if arg.eq_ignore_ascii_case(b"MINMATCHLEN") || arg.eq_ignore_ascii_case(b"MINLEN") {
                 if i + 1 >= items.len() {
                      return Resp::Error("ERR syntax error".to_string());
                 }
                 // Parse len
                 if let Some(s) = as_bytes(&items[i+1]) {
                     if let Ok(str_val) = std::str::from_utf8(s) {
                         if let Ok(v) = str_val.parse::<usize>() {
                             min_match_len = v;
                         } else {
                             return Resp::Error("ERR minmatchlen is not an integer".to_string());
                         }
                     } else {
                          return Resp::Error("ERR minmatchlen is not an integer".to_string());
                     }
                 } else {
                      return Resp::Error("ERR minmatchlen is not an integer".to_string());
                 }
                 i += 2;
             } else {
                 return Resp::Error("ERR syntax error".to_string());
             }
        } else {
             return Resp::Error("ERR syntax error".to_string());
        }
    }
    
    let a = str_a.unwrap_or_else(Bytes::new);
    let b = str_b.unwrap_or_else(Bytes::new);
    
    // Calculate LCS
    let m = a.len();
    let n = b.len();
    
    let mut dp = vec![0u32; (m + 1) * (n + 1)];
    // Access: dp[i * (n + 1) + j]
    
    for i in 1..=m {
        for j in 1..=n {
            if a[i-1] == b[j-1] {
                dp[i * (n + 1) + j] = dp[(i - 1) * (n + 1) + (j - 1)] + 1;
            } else {
                let v1 = dp[(i - 1) * (n + 1) + j];
                let v2 = dp[i * (n + 1) + (j - 1)];
                dp[i * (n + 1) + j] = std::cmp::max(v1, v2);
            }
        }
    }
    
    let lcs_len = dp[m * (n + 1) + n];
    
    if get_idx {
        // Backtrack for matches
        let mut matches = Vec::new();
        let mut i = m;
        let mut j = n;
        
        let mut current_match_len = 0;
        let mut match_end_a = 0;
        let mut match_end_b = 0;
        
        while i > 0 && j > 0 {
            if a[i-1] == b[j-1] {
                if current_match_len == 0 {
                    match_end_a = i - 1;
                    match_end_b = j - 1;
                }
                current_match_len += 1;
                i -= 1;
                j -= 1;
            } else {
                // End of current match block (if any)
                if current_match_len > 0 {
                     // Since we go backwards, start is i, end is match_end
                     // Range is [i, match_end_a] inclusive.
                     // Length is current_match_len.
                     if current_match_len >= min_match_len {
                         matches.push((i, match_end_a, j, match_end_b, current_match_len));
                     }
                     current_match_len = 0;
                }
                
                let v1 = dp[(i - 1) * (n + 1) + j];
                let v2 = dp[i * (n + 1) + (j - 1)];
                if v1 > v2 {
                    i -= 1;
                } else {
                    j -= 1;
                }
            }
        }
        if current_match_len > 0 {
             if current_match_len >= min_match_len {
                 matches.push((i, match_end_a, j, match_end_b, current_match_len));
             }
        }
        
        // Matches are collected in reverse order (end to start).
        matches.reverse();
        
        // Construct response
        let mut match_arr = Vec::new();
        for (sa, ea, sb, eb, len) in matches {
            let mut item = Vec::new();
            item.push(Resp::Array(Some(vec![
                Resp::Integer(sa as i64),
                Resp::Integer(ea as i64)
            ])));
            item.push(Resp::Array(Some(vec![
                Resp::Integer(sb as i64),
                Resp::Integer(eb as i64)
            ])));
            if with_match_len {
                item.push(Resp::Integer(len as i64));
            }
            match_arr.push(Resp::Array(Some(item)));
        }
        
        let mut map = Vec::new();
        map.push(Resp::BulkString(Some(Bytes::from("matches"))));
        map.push(Resp::Array(Some(match_arr)));
        map.push(Resp::BulkString(Some(Bytes::from("len"))));
        map.push(Resp::Integer(lcs_len as i64));
        
        return Resp::Array(Some(map)); 
    } else if get_len {
        return Resp::Integer(lcs_len as i64);
    } else {
        // Return LCS string
        let mut res = Vec::with_capacity(lcs_len as usize);
        let mut i = m;
        let mut j = n;
        while i > 0 && j > 0 {
            if a[i-1] == b[j-1] {
                res.push(a[i-1]);
                i -= 1;
                j -= 1;
            } else {
                let v1 = dp[(i - 1) * (n + 1) + j];
                let v2 = dp[i * (n + 1) + (j - 1)];
                if v1 > v2 {
                    i -= 1;
                } else {
                    j -= 1;
                }
            }
        }
        res.reverse();
        return Resp::BulkString(Some(Bytes::from(res)));
    }
}

