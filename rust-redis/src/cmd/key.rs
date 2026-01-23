use crate::db::{Db, Entry};
use crate::resp::Resp;

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
                },
                None => Resp::Integer(-1)
            }
        }
    } else {
        Resp::Integer(-2)
    }
}

pub fn dbsize(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 1 {
        return Resp::Error("ERR wrong number of arguments for 'DBSIZE'".to_string());
    }
    Resp::Integer(db.len() as i64)
}
