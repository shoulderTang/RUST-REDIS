use crate::db::{Db, Entry, Value};
use crate::resp::Resp;
use std::collections::HashSet;
use bytes::Bytes;
use crate::cmd::key::match_pattern;
use rand::seq::{IteratorRandom, IndexedRandom};

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

fn compute_sintersection(keys: &[Bytes], db: &Db) -> Result<HashSet<Bytes>, Resp> {
    // Pass 1: Check existence, type, and find cardinalities
    let mut key_sizes: Vec<(usize, usize)> = Vec::with_capacity(keys.len());

    for (i, key) in keys.iter().enumerate() {
        if let Some(entry) = db.get(key) {
            if entry.is_expired() {
                // Treated as empty set
                key_sizes.push((i, 0));
            } else {
                match &entry.value {
                    Value::Set(set) => {
                        key_sizes.push((i, set.len()));
                    }
                    _ => {
                        return Err(Resp::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ));
                    }
                }
            }
        } else {
            // Missing key means empty set
            key_sizes.push((i, 0));
        }
    }

    // Optimization: if any set is empty, intersection is empty
    if key_sizes.iter().any(|&(_, size)| size == 0) {
        return Ok(HashSet::new());
    }

    // Sort by size to start with smallest set
    key_sizes.sort_by(|a, b| a.1.cmp(&b.1));

    // Get the smallest set's members
    let smallest_idx = key_sizes[0].0;
    let smallest_key = &keys[smallest_idx];
    
    let mut result_members: HashSet<Bytes>;

    // Scope for read lock
    {
        if let Some(entry) = db.get(smallest_key) {
             if entry.is_expired() {
                return Ok(HashSet::new());
            }
            match &entry.value {
                Value::Set(set) => {
                    result_members = set.clone();
                }
                _ => return Err(Resp::Error("WRONGTYPE".to_string())),
            }
        } else {
             return Ok(HashSet::new());
        }
    }

    // Intersect with others
    for (idx, _) in key_sizes.iter().skip(1) {
        let key = &keys[*idx];
        if let Some(entry) = db.get(key) {
             if entry.is_expired() {
                return Ok(HashSet::new());
            }
            match &entry.value {
                Value::Set(set) => {
                    result_members.retain(|m| set.contains(m));
                    if result_members.is_empty() {
                        return Ok(HashSet::new());
                    }
                }
                _ => return Err(Resp::Error("WRONGTYPE".to_string())),
            }
        } else {
             return Ok(HashSet::new());
        }
    }
    
    Ok(result_members)
}

pub fn sinter(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'SINTER'".to_string());
    }

    let mut keys = Vec::new();
    for item in &items[1..] {
        match item {
            Resp::BulkString(Some(b)) => keys.push(b.clone()),
            Resp::SimpleString(s) => keys.push(s.clone()),
            _ => return Resp::Error("ERR invalid key".to_string()),
        }
    }

    match compute_sintersection(&keys, db) {
        Ok(members) => {
            let mut resp_array = Vec::new();
            for m in members {
                resp_array.push(Resp::BulkString(Some(m)));
            }
            Resp::Array(Some(resp_array))
        }
        Err(e) => e,
    }
}

pub fn sinterstore(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'SINTERSTORE'".to_string());
    }

    let destination = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid destination key".to_string()),
    };

    let mut keys = Vec::new();
    for item in &items[2..] {
        match item {
            Resp::BulkString(Some(b)) => keys.push(b.clone()),
            Resp::SimpleString(s) => keys.push(s.clone()),
            _ => return Resp::Error("ERR invalid key".to_string()),
        }
    }

    match compute_sintersection(&keys, db) {
        Ok(members) => {
            let count = members.len() as i64;
            db.insert(destination, Entry::new(Value::Set(members), None));
            Resp::Integer(count)
        }
        Err(e) => e,
    }
}

fn compute_sunion(keys: &[Bytes], db: &Db) -> Result<HashSet<Bytes>, Resp> {
    let mut result_members: HashSet<Bytes> = HashSet::new();

    for key in keys {
        if let Some(entry) = db.get(key) {
             if entry.is_expired() {
                continue;
            }
            match &entry.value {
                Value::Set(set) => {
                    for member in set {
                        result_members.insert(member.clone());
                    }
                }
                _ => return Err(Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())),
            }
        }
    }
    
    Ok(result_members)
}

pub fn sunion(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'SUNION'".to_string());
    }

    let mut keys = Vec::new();
    for item in &items[1..] {
        match item {
            Resp::BulkString(Some(b)) => keys.push(b.clone()),
            Resp::SimpleString(s) => keys.push(s.clone()),
            _ => return Resp::Error("ERR invalid key".to_string()),
        }
    }

    match compute_sunion(&keys, db) {
        Ok(members) => {
            let mut resp_array = Vec::new();
            for m in members {
                resp_array.push(Resp::BulkString(Some(m)));
            }
            Resp::Array(Some(resp_array))
        }
        Err(e) => e,
    }
}

pub fn sunionstore(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'SUNIONSTORE'".to_string());
    }

    let destination = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid destination key".to_string()),
    };

    let mut keys = Vec::new();
    for item in &items[2..] {
        match item {
            Resp::BulkString(Some(b)) => keys.push(b.clone()),
            Resp::SimpleString(s) => keys.push(s.clone()),
            _ => return Resp::Error("ERR invalid key".to_string()),
        }
    }

    match compute_sunion(&keys, db) {
        Ok(members) => {
            let count = members.len() as i64;
            db.insert(destination, Entry::new(Value::Set(members), None));
            Resp::Integer(count)
        }
        Err(e) => e,
    }
}

fn compute_sdiff(keys: &[Bytes], db: &Db) -> Result<HashSet<Bytes>, Resp> {
    let first_key = &keys[0];
    let mut result_members: HashSet<Bytes>;

    // 1. Get members of the first set
    if let Some(entry) = db.get(first_key) {
        if entry.is_expired() {
            return Ok(HashSet::new());
        }
        match &entry.value {
            Value::Set(set) => {
                result_members = set.clone();
            }
            _ => return Err(Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())),
        }
    } else {
        // If first key is missing, diff is empty
        return Ok(HashSet::new());
    }

    // 2. Remove members present in subsequent sets
    for key in &keys[1..] {
        if let Some(entry) = db.get(key) {
             if entry.is_expired() {
                continue;
            }
            match &entry.value {
                Value::Set(set) => {
                    for member in set {
                        result_members.remove(member);
                    }
                }
                _ => return Err(Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())),
            }
        }
    }
    
    Ok(result_members)
}

pub fn sdiff(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'SDIFF'".to_string());
    }

    let mut keys = Vec::new();
    for item in &items[1..] {
        match item {
            Resp::BulkString(Some(b)) => keys.push(b.clone()),
            Resp::SimpleString(s) => keys.push(s.clone()),
            _ => return Resp::Error("ERR invalid key".to_string()),
        }
    }

    match compute_sdiff(&keys, db) {
        Ok(members) => {
            let mut resp_array = Vec::new();
            for m in members {
                resp_array.push(Resp::BulkString(Some(m)));
            }
            Resp::Array(Some(resp_array))
        }
        Err(e) => e,
    }
}

pub fn sdiffstore(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'SDIFFSTORE'".to_string());
    }

    let destination = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid destination key".to_string()),
    };

    let mut keys = Vec::new();
    for item in &items[2..] {
        match item {
            Resp::BulkString(Some(b)) => keys.push(b.clone()),
            Resp::SimpleString(s) => keys.push(s.clone()),
            _ => return Resp::Error("ERR invalid key".to_string()),
        }
    }

    match compute_sdiff(&keys, db) {
        Ok(members) => {
            let count = members.len() as i64;
            db.insert(destination, Entry::new(Value::Set(members), None));
            Resp::Integer(count)
        }
        Err(e) => e,
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
    
    let cursor = match &items[2] {
        Resp::BulkString(Some(b)) => match String::from_utf8_lossy(b).parse::<usize>() {
            Ok(n) => n,
            Err(_) => return Resp::Error("ERR invalid cursor".to_string()),
        },
        Resp::SimpleString(s) => match String::from_utf8_lossy(s).parse::<usize>() {
            Ok(n) => n,
            Err(_) => return Resp::Error("ERR invalid cursor".to_string()),
        },
        _ => return Resp::Error("ERR invalid cursor".to_string()),
    };

    let mut count = 10;
    let mut match_pattern_str: Option<String> = None;

    let mut i = 3;
    while i < items.len() {
        let arg = match &items[i] {
            Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
            Resp::SimpleString(s) => String::from_utf8_lossy(s).to_string(),
            _ => return Resp::Error("ERR syntax error".to_string()),
        };

        if arg.to_uppercase() == "MATCH" {
            if i + 1 >= items.len() {
                return Resp::Error("ERR syntax error".to_string());
            }
            match_pattern_str = match &items[i+1] {
                Resp::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                Resp::SimpleString(s) => Some(String::from_utf8_lossy(s).to_string()),
                _ => return Resp::Error("ERR syntax error".to_string()),
            };
            i += 2;
        } else if arg.to_uppercase() == "COUNT" {
            if i + 1 >= items.len() {
                return Resp::Error("ERR syntax error".to_string());
            }
            count = match &items[i+1] {
                Resp::BulkString(Some(b)) => match String::from_utf8_lossy(b).parse::<usize>() {
                    Ok(n) => n,
                    Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
                },
                Resp::SimpleString(s) => match String::from_utf8_lossy(s).parse::<usize>() {
                    Ok(n) => n,
                    Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
                },
                _ => return Resp::Error("ERR value is not an integer or out of range".to_string()),
            };
            i += 2;
        } else {
            return Resp::Error("ERR syntax error".to_string());
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
                if let Some(pattern) = &match_pattern_str {
                    if !match_pattern(pattern.as_bytes(), member) {
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

pub fn spop(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'SPOP'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let count = if items.len() > 2 {
        match &items[2] {
             Resp::BulkString(Some(b)) => match String::from_utf8_lossy(b).parse::<i64>() {
                 Ok(n) => Some(n),
                 Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
             },
             Resp::SimpleString(s) => match String::from_utf8_lossy(s).parse::<i64>() {
                 Ok(n) => Some(n),
                 Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
             },
             _ => return Resp::Error("ERR value is not an integer or out of range".to_string()),
        }
    } else {
        None
    };

    if let Some(c) = count {
        if c < 0 {
            return Resp::Error("ERR value is out of range, must be positive".to_string());
        }
    }

    if let Some(mut entry) = db.get_mut(&key) {
         if entry.is_expired() {
            drop(entry);
            db.remove(&key);
            if count.is_some() {
                return Resp::Array(Some(Vec::new()));
            } else {
                return Resp::BulkString(None);
            }
         }
         
         if let Value::Set(set) = &mut entry.value {
             if set.is_empty() {
                  if count.is_some() {
                      return Resp::Array(Some(Vec::new()));
                  } else {
                      return Resp::BulkString(None);
                  }
             }

             let mut rng = rand::rng();
             
             match count {
                 None => {
                     if let Some(member) = set.iter().choose(&mut rng).cloned() {
                         set.remove(&member);
                         return Resp::BulkString(Some(member));
                     } else {
                         return Resp::BulkString(None);
                     }
                 },
                 Some(c) => {
                     if c == 0 {
                         return Resp::Array(Some(Vec::new()));
                     }
                     
                     let count_val = c as usize;
                     let members: Vec<_> = set.iter().choose_multiple(&mut rng, count_val).into_iter().cloned().collect();
                     
                     let mut result = Vec::new();
                     for member in members {
                         set.remove(&member);
                         result.push(Resp::BulkString(Some(member)));
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

pub fn smove(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 4 {
        return Resp::Error("ERR wrong number of arguments for 'SMOVE'".to_string());
    }
    let source = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid source key".to_string()),
    };
    let destination = match &items[2] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid destination key".to_string()),
    };
    let member = match &items[3] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid member".to_string()),
    };

    if source == destination {
        if let Some(entry) = db.get(&source) {
            if entry.is_expired() {
                return Resp::Integer(0);
            }
            match &entry.value {
                Value::Set(set) => return Resp::Integer(if set.contains(&member) { 1 } else { 0 }),
                _ => return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            return Resp::Integer(0);
        }
    }

    // Check if destination exists and is of wrong type
    if let Some(entry) = db.get(&destination) {
        if !entry.is_expired() {
            if !matches!(entry.value, Value::Set(_)) {
                return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
            }
        }
    }

    // Remove from source
    let removed = if let Some(mut entry) = db.get_mut(&source) {
        if entry.is_expired() {
            drop(entry);
            db.remove(&source);
            false
        } else {
            match &mut entry.value {
                Value::Set(set) => {
                    let res = set.remove(&member);
                    if set.is_empty() {
                        drop(entry);
                        db.remove(&source);
                    }
                    res
                }
                _ => return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        }
    } else {
        false
    };

    if !removed {
        return Resp::Integer(0);
    }

    // Add to destination
    let mut entry = db
        .entry(destination)
        .or_insert_with(|| Entry::new(Value::Set(HashSet::new()), None));
    
    if entry.is_expired() {
        entry.value = Value::Set(HashSet::new());
        entry.expires_at = None;
    }

    match &mut entry.value {
        Value::Set(set) => {
            set.insert(member);
            Resp::Integer(1)
        }
        _ => {
            // This should theoretically not happen due to the check above, 
            // but if it does (race condition), we already removed from source.
            // In a robust implementation, we might try to add back to source,
            // but for now we'll just return the error.
             Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        }
    }
}

pub fn srandmember(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'SRANDMEMBER'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let count = if items.len() > 2 {
        match &items[2] {
             Resp::BulkString(Some(b)) => match String::from_utf8_lossy(b).parse::<i64>() {
                 Ok(n) => Some(n),
                 Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
             },
             Resp::SimpleString(s) => match String::from_utf8_lossy(s).parse::<i64>() {
                 Ok(n) => Some(n),
                 Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
             },
             _ => return Resp::Error("ERR value is not an integer or out of range".to_string()),
        }
    } else {
        None
    };

    if let Some(entry) = db.get(&key) {
         if entry.is_expired() {
            drop(entry);
            db.remove(&key);
            if count.is_some() {
                return Resp::Array(Some(Vec::new()));
            } else {
                return Resp::BulkString(None);
            }
         }
         
         if let Value::Set(set) = &entry.value {
             if set.is_empty() {
                  if count.is_some() {
                      return Resp::Array(Some(Vec::new()));
                  } else {
                      return Resp::BulkString(None);
                  }
             }

             let mut rng = rand::rng();

             match count {
                 None => {
                     if let Some(member) = set.iter().choose(&mut rng).cloned() {
                         return Resp::BulkString(Some(member));
                     } else {
                         return Resp::BulkString(None);
                     }
                 },
                 Some(c) => {
                     if c == 0 {
                         return Resp::Array(Some(Vec::new()));
                     }
                     
                     let count_val = c.abs() as usize;
                     
                     if c > 0 {
                         // Distinct elements
                         let members: Vec<_> = set.iter().choose_multiple(&mut rng, count_val).into_iter().cloned().collect();
                         let result = members.into_iter().map(|m| Resp::BulkString(Some(m))).collect();
                         return Resp::Array(Some(result));
                     } else {
                         // Allow duplicates (negative count)
                         let members_vec: Vec<_> = set.iter().collect();
                         let mut result = Vec::with_capacity(count_val);
                         for _ in 0..count_val {
                             if let Some(member) = members_vec.choose(&mut rng) {
                                 result.push(Resp::BulkString(Some((**member).clone())));
                             }
                         }
                         return Resp::Array(Some(result));
                     }
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


