use crate::db::{Db, Value, Entry};
use crate::resp::Resp;
use crate::hll::HyperLogLog;
use bytes::Bytes;

pub fn pfadd(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'pfadd' command".to_string());
    }

    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        _ => return Resp::Error("ERR key must be a string".to_string()),
    };

    let mut updated = false;

    // Use a scope to drop the lock on entry before re-acquiring it if needed
    {
        // Try to get the entry first
        let entry = db.get(&key);
        if entry.is_none() {
            // Key doesn't exist, create it
            drop(entry);
            let hll = HyperLogLog::new();
            db.insert(key.clone(), Entry::new(Value::HyperLogLog(hll), None));
        }
    }
    
    // Now get mutable access
    let mut entry = db.get_mut(&key).unwrap();

    // Check if we need to promote String to HyperLogLog
    let is_string_hll = if let Value::String(s) = &entry.value {
        s.len() == 16384
    } else {
        false
    };

    if is_string_hll {
        let s = if let Value::String(s) = &entry.value { s.clone() } else { unreachable!() };
        entry.value = Value::HyperLogLog(HyperLogLog { registers: s.to_vec() });
    }

    if let Value::HyperLogLog(hll) = &mut entry.value {
        for i in 2..items.len() {
            let element = match &items[i] {
                Resp::BulkString(Some(b)) => b.as_ref(),
                _ => return Resp::Error("ERR element must be a string".to_string()),
            };
            if hll.add(element) {
                updated = true;
            }
        }
    } else {
        return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
    }

    Resp::Integer(if updated { 1 } else { 0 })
}

pub fn pfcount(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'pfcount' command".to_string());
    }

    if items.len() == 2 {
        let key = match &items[1] {
            Resp::BulkString(Some(b)) => b.clone(),
            _ => return Resp::Error("ERR key must be a string".to_string()),
        };

        if let Some(entry) = db.get(&key) {
            match &entry.value {
                Value::HyperLogLog(hll) => Resp::Integer(hll.count() as i64),
                Value::String(s) if s.len() == 16384 => {
                     let hll = HyperLogLog { registers: s.to_vec() };
                     Resp::Integer(hll.count() as i64)
                }
                _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Resp::Integer(0)
        }
    } else {
        // Merge multiple keys
        let mut temp_hll = HyperLogLog::new();
        for i in 1..items.len() {
            let key = match &items[i] {
                Resp::BulkString(Some(b)) => b.clone(),
                _ => return Resp::Error("ERR key must be a string".to_string()),
            };

            if let Some(entry) = db.get(&key) {
                match &entry.value {
                    Value::HyperLogLog(hll) => temp_hll.merge(hll),
                    Value::String(s) if s.len() == 16384 => {
                        let hll = HyperLogLog { registers: s.to_vec() };
                        temp_hll.merge(&hll);
                    }
                    _ => return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            }
        }
        Resp::Integer(temp_hll.count() as i64)
    }
}

pub fn pfmerge(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'pfmerge' command".to_string());
    }

    let dest_key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        _ => return Resp::Error("ERR key must be a string".to_string()),
    };

    let mut temp_hll = HyperLogLog::new();
    
    // Merge all keys including destination (starting from index 1)
    for i in 1..items.len() {
        let key = match &items[i] {
            Resp::BulkString(Some(b)) => b.clone(),
            _ => return Resp::Error("ERR key must be a string".to_string()),
        };

        if let Some(entry) = db.get(&key) {
            match &entry.value {
                Value::HyperLogLog(hll) => temp_hll.merge(hll),
                Value::String(s) if s.len() == 16384 => {
                     let hll = HyperLogLog { registers: s.to_vec() };
                     temp_hll.merge(&hll);
                }
                _ => return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        }
    }

    // Save result to destination
    db.insert(dest_key, Entry::new(Value::HyperLogLog(temp_hll), None));

    Resp::SimpleString(Bytes::from("OK"))
}
