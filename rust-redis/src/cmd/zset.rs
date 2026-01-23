use crate::db::{Db, Value, Entry, SortedSet, TotalOrderF64};
use crate::resp::Resp;

pub fn zadd(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 4 || items.len() % 2 != 0 {
        return Resp::Error("ERR wrong number of arguments for 'ZADD'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let mut entry = db.entry(key).or_insert_with(|| Entry::new(Value::ZSet(SortedSet::new()), None));
    if entry.is_expired() {
        entry.value = Value::ZSet(SortedSet::new());
        entry.expires_at = None;
    }

    if let Value::ZSet(zset) = &mut entry.value {
        let mut count = 0;
        for chunk in items[2..].chunks(2) {
            let score_bytes = match &chunk[0] {
                Resp::BulkString(Some(b)) => b,
                Resp::SimpleString(s) => s,
                _ => return Resp::Error("ERR invalid score".to_string()),
            };
            let score_str = match std::str::from_utf8(score_bytes) {
                Ok(s) => s,
                Err(_) => return Resp::Error("ERR value is not a valid float".to_string()),
            };
            let score: f64 = match score_str.parse() {
                Ok(s) => s,
                Err(_) => return Resp::Error("ERR value is not a valid float".to_string()),
            };
            
            let member = match &chunk[1] {
                Resp::BulkString(Some(b)) => b.clone(),
                Resp::SimpleString(s) => s.clone(),
                _ => return Resp::Error("ERR invalid member".to_string()),
            };

            if let Some(old_score) = zset.members.get(&member) {
                if *old_score != score {
                    zset.scores.remove(&(TotalOrderF64(*old_score), member.clone()));
                    zset.members.insert(member.clone(), score);
                    zset.scores.insert((TotalOrderF64(score), member));
                }
            } else {
                zset.members.insert(member.clone(), score);
                zset.scores.insert((TotalOrderF64(score), member));
                count += 1;
            }
        }
        Resp::Integer(count)
    } else {
        Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
    }
}

pub fn zrem(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'ZREM'".to_string());
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
            Value::ZSet(zset) => {
                let mut count = 0;
                for i in 2..items.len() {
                    let member = match &items[i] {
                        Resp::BulkString(Some(b)) => b.clone(),
                        Resp::SimpleString(s) => s.clone(),
                        _ => return Resp::Error("ERR invalid member".to_string()),
                    };
                    if let Some(score) = zset.members.remove(&member) {
                        zset.scores.remove(&(TotalOrderF64(score), member));
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

pub fn zscore(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'ZSCORE'".to_string());
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
            return Resp::BulkString(None);
        }
        match &entry.value {
            Value::ZSet(zset) => {
                if let Some(score) = zset.members.get(&member) {
                    Resp::BulkString(Some(bytes::Bytes::from(score.to_string())))
                } else {
                    Resp::BulkString(None)
                }
            }
            _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        Resp::BulkString(None)
    }
}

pub fn zcard(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'ZCARD'".to_string());
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
            Value::ZSet(zset) => Resp::Integer(zset.members.len() as i64),
            _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        Resp::Integer(0)
    }
}

pub fn zrank(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'ZRANK'".to_string());
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
            return Resp::BulkString(None);
        }
        match &entry.value {
            Value::ZSet(zset) => {
                if let Some(score) = zset.members.get(&member) {
                    // Iterate to find rank
                    let target = (TotalOrderF64(*score), member);
                    if let Some(rank) = zset.scores.iter().position(|x| *x == target) {
                        Resp::Integer(rank as i64)
                    } else {
                        // Should not happen if data structures are consistent
                        Resp::BulkString(None)
                    }
                } else {
                    Resp::BulkString(None)
                }
            }
            _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        Resp::BulkString(None)
    }
}

pub fn zrange(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 4 || items.len() > 5 {
        return Resp::Error("ERR wrong number of arguments for 'ZRANGE'".to_string());
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

    let withscores = if items.len() == 5 {
        match &items[4] {
            Resp::BulkString(Some(b)) => {
                if b.eq_ignore_ascii_case(b"WITHSCORES") {
                    true
                } else {
                    return Resp::Error("ERR syntax error".to_string());
                }
            },
            Resp::SimpleString(s) => {
                if s.eq_ignore_ascii_case(b"WITHSCORES") {
                    true
                } else {
                    return Resp::Error("ERR syntax error".to_string());
                }
            },
            _ => return Resp::Error("ERR syntax error".to_string()),
        }
    } else {
        false
    };

    if let Some(entry) = db.get(&key) {
        if entry.is_expired() {
            drop(entry);
            db.remove(&key);
            return Resp::Array(Some(vec![]));
        }
        match &entry.value {
            Value::ZSet(zset) => {
                let len = zset.scores.len() as i64;
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
                for (score, member) in zset.scores.iter().skip(start_idx as usize).take((stop_idx - start_idx + 1) as usize) {
                    result.push(Resp::BulkString(Some(member.clone())));
                    if withscores {
                         result.push(Resp::BulkString(Some(bytes::Bytes::from(score.0.to_string()))));
                    }
                }
                Resp::Array(Some(result))
            }
            _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        Resp::Array(Some(vec![]))
    }
}
