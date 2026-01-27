use crate::cmd::{ConnectionContext, ServerContext};
use crate::db::{Db, Entry, Value};
use crate::resp::Resp;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::time::timeout;

pub fn lpush(items: &[Resp], conn_ctx: &ConnectionContext, server_ctx: &ServerContext) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'LPUSH'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let db = &server_ctx.databases[conn_ctx.db_index];

    let mut count = 0;
    for i in 2..items.len() {
        let val = match &items[i] {
            Resp::BulkString(Some(b)) => b.clone(),
            Resp::SimpleString(s) => s.clone(),
            _ => return Resp::Error("ERR invalid value".to_string()),
        };

        // Check for blocking waiters
        let mut handled = false;
        let map_key = (conn_ctx.db_index, key.to_vec());
        
        // We need to loop because the first waiter might be dead (dropped receiver)
        loop {
            // Scope the lock
            let mut sender_opt = None;
            if let Some(mut waiters) = server_ctx.blocking_waiters.get_mut(&map_key) {
                if let Some(sender) = waiters.pop_front() {
                    sender_opt = Some(sender);
                }
            }

            if let Some(sender) = sender_opt {
                // Try to send to the waiter
                // We send (key, value)
                // Use try_send for synchronous sending
                match sender.try_send((key.to_vec(), val.to_vec())) {
                    Ok(_) => {
                        handled = true;
                        break;
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        // Channel full, receiver not ready? Should not happen with size 1 if receiver is waiting.
                        // But if it happens, we treat it as not handled by this waiter?
                        // Or we can't block. So we assume this waiter is busy and try next?
                        // But strictly BLPOP waiters should be ready.
                        // If full, maybe another push filled it?
                        // If so, this waiter is effectively "served" by another push.
                        // So we should try next waiter.
                        continue;
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        // Receiver dropped, try next waiter
                        continue;
                    }
                }
            } else {
                // No more waiters
                break;
            }
        }

        if !handled {
            let mut entry = db
                .entry(key.clone())
                .or_insert_with(|| Entry::new(Value::List(VecDeque::new()), None));
            
            if entry.is_expired() {
                entry.value = Value::List(VecDeque::new());
                entry.expires_at = None;
            }

            if let Value::List(list) = &mut entry.value {
                list.push_front(val);
                count = list.len();
            } else {
                return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
            }
        } else {
            // Value was sent to a waiter, so list length might not increase?
            // Redis says: "The command returns the length of the list after the push operations."
            // If a value is delivered to a waiter, it is effectively pushed and then popped.
            // So the length is the current length.
            // But if the list was empty and we sent to waiter, length is 0?
            // Redis docs: "RPUSH mylist a b c" -> returns 3.
            // If "BLPOP mylist 0" is waiting.
            // "RPUSH mylist a" -> returns 1? Or 0?
            // Redis `LPUSH` returns the length of the list *after* the push.
            // If `BLPOP` consumes it, the list is empty (len 0).
            // Let's verify standard Redis behavior if possible.
            // Assuming 0 if consumed.
            if let Some(entry) = db.get(&key) {
                 if let Value::List(list) = &entry.value {
                     count = list.len();
                 }
            } else {
                count = 0;
            }
        }
    }
    Resp::Integer(count as i64)
}

pub fn rpush(items: &[Resp], conn_ctx: &ConnectionContext, server_ctx: &ServerContext) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'RPUSH'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let db = &server_ctx.databases[conn_ctx.db_index];

    let mut count = 0;
    for i in 2..items.len() {
        let val = match &items[i] {
            Resp::BulkString(Some(b)) => b.clone(),
            Resp::SimpleString(s) => s.clone(),
            _ => return Resp::Error("ERR invalid value".to_string()),
        };

        // Check for blocking waiters
        let mut handled = false;
        let map_key = (conn_ctx.db_index, key.to_vec());
        
        // We need to loop because the first waiter might be dead (dropped receiver)
        loop {
            // Scope the lock
            let mut sender_opt = None;
            if let Some(mut waiters) = server_ctx.blocking_waiters.get_mut(&map_key) {
                if let Some(sender) = waiters.pop_front() {
                    sender_opt = Some(sender);
                }
            }

            if let Some(sender) = sender_opt {
                // Try to send to the waiter
                // We send (key, value)
                // Use try_send for synchronous sending
                match sender.try_send((key.to_vec(), val.to_vec())) {
                    Ok(_) => {
                        handled = true;
                        break;
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        continue;
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        continue;
                    }
                }
            } else {
                // No more waiters
                break;
            }
        }

        if !handled {
            let mut entry = db
                .entry(key.clone())
                .or_insert_with(|| Entry::new(Value::List(VecDeque::new()), None));
            
            if entry.is_expired() {
                entry.value = Value::List(VecDeque::new());
                entry.expires_at = None;
            }

            if let Value::List(list) = &mut entry.value {
                list.push_back(val);
                count = list.len();
            } else {
                return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
            }
        } else {
            if let Some(entry) = db.get(&key) {
                 if let Value::List(list) = &entry.value {
                     count = list.len();
                 }
            } else {
                count = 0;
            }
        }
    }
    Resp::Integer(count as i64)
}

pub fn lpop(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'LPOP'".to_string());
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
            return Resp::BulkString(None);
        }
        match &mut entry.value {
            Value::List(list) => match list.pop_front() {
                Some(v) => Resp::BulkString(Some(v)),
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

pub fn rpop(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'RPOP'".to_string());
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
            return Resp::BulkString(None);
        }
        match &mut entry.value {
            Value::List(list) => match list.pop_back() {
                Some(v) => Resp::BulkString(Some(v)),
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

pub fn llen(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'LLEN'".to_string());
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
            Value::List(list) => Resp::Integer(list.len() as i64),
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
        }
    } else {
        Resp::Integer(0)
    }
}

pub fn lrange(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 4 {
        return Resp::Error("ERR wrong number of arguments for 'LRANGE'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    
    let start = match &items[2] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).parse::<i64>(),
        Resp::SimpleString(s) => String::from_utf8_lossy(s).parse::<i64>(),
        _ => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    
    let stop = match &items[3] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).parse::<i64>(),
        Resp::SimpleString(s) => String::from_utf8_lossy(s).parse::<i64>(),
        _ => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    if let (Ok(start), Ok(stop)) = (start, stop) {
        if let Some(entry) = db.get(&key) {
            if entry.is_expired() {
                return Resp::Array(Some(vec![]));
            }
            match &entry.value {
                Value::List(list) => {
                    let len = list.len() as i64;
                    let start = if start < 0 { len + start } else { start };
                    let stop = if stop < 0 { len + stop } else { stop };
                    
                    let start = if start < 0 { 0 } else { start } as usize;
                    let stop = if stop < 0 { 0 } else { stop } as usize;
                    
                    if start >= list.len() {
                        return Resp::Array(Some(vec![]));
                    }
                    
                    let stop = if stop >= list.len() { list.len() - 1 } else { stop };
                    
                    if start > stop {
                        return Resp::Array(Some(vec![]));
                    }
                    
                    let mut result = Vec::new();
                    for i in start..=stop {
                        if let Some(val) = list.get(i) {
                            result.push(Resp::BulkString(Some(val.clone())));
                        }
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
    } else {
        Resp::Error("ERR value is not an integer or out of range".to_string())
    }
}

#[derive(Copy, Clone)]
enum PopDirection {
    Left,
    Right,
}

async fn blocking_pop_generic(
    items: &[Resp],
    conn_ctx: &ConnectionContext,
    server_ctx: &ServerContext,
    direction: PopDirection,
) -> Resp {
    if items.len() < 3 {
        let cmd = match direction {
            PopDirection::Left => "BLPOP",
            PopDirection::Right => "BRPOP",
        };
        return Resp::Error(format!("ERR wrong number of arguments for '{}'", cmd));
    }

    let timeout_arg = match &items[items.len() - 1] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).parse::<f64>(),
        Resp::SimpleString(s) => String::from_utf8_lossy(s).parse::<f64>(),
        _ => return Resp::Error("ERR timeout is not a float or out of range".to_string()),
    };

    let timeout_secs = match timeout_arg {
        Ok(v) => v,
        Err(_) => return Resp::Error("ERR timeout is not a float or out of range".to_string()),
    };

    let db = &server_ctx.databases[conn_ctx.db_index];
    let mut keys = Vec::new();

    // 1. Try to serve from existing lists immediately
    for i in 1..items.len() - 1 {
        let key = match &items[i] {
            Resp::BulkString(Some(b)) => b.clone(),
            Resp::SimpleString(s) => s.clone(),
            _ => continue,
        };
        keys.push(key.clone());

        if let Some(mut entry) = db.get_mut(&key) {
             if entry.is_expired() {
                 drop(entry);
                 db.remove(&key);
                 continue;
             }
             if let Value::List(list) = &mut entry.value {
                 let val_opt = match direction {
                     PopDirection::Left => list.pop_front(),
                     PopDirection::Right => list.pop_back(),
                 };
                 if let Some(val) = val_opt {
                     // Found item, return immediately
                     return Resp::Array(Some(vec![
                         Resp::BulkString(Some(key)),
                         Resp::BulkString(Some(val)),
                     ]));
                 }
             }
        }
    }

    // 2. If no data, block
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(Vec<u8>, Vec<u8>)>(1);
    
    // Register waiter for all keys
    for key in &keys {
        let map_key = (conn_ctx.db_index, key.to_vec());
        let mut queue = server_ctx.blocking_waiters.entry(map_key).or_insert_with(VecDeque::new);
        queue.push_back(tx.clone());
    }

    // Wait
    let result = if timeout_secs > 0.0 {
        let duration = Duration::from_secs_f64(timeout_secs);
        match timeout(duration, rx.recv()).await {
            Ok(Some((key, val))) => Some((key, val)),
            Ok(None) => None,
            Err(_) => None, // Timeout
        }
    } else {
        // Infinite wait
        rx.recv().await
    };

    match result {
        Some((key, val)) => Resp::Array(Some(vec![
            Resp::BulkString(Some(bytes::Bytes::from(key))),
            Resp::BulkString(Some(bytes::Bytes::from(val))),
        ])),
        None => Resp::BulkString(None), // Timeout
    }
}

pub async fn blpop(items: &[Resp], conn_ctx: &ConnectionContext, server_ctx: &ServerContext) -> Resp {
    blocking_pop_generic(items, conn_ctx, server_ctx, PopDirection::Left).await
}

pub async fn brpop(items: &[Resp], conn_ctx: &ConnectionContext, server_ctx: &ServerContext) -> Resp {
    blocking_pop_generic(items, conn_ctx, server_ctx, PopDirection::Right).await
}

fn parse_direction(arg: &Resp) -> Result<PopDirection, Resp> {
    let bytes = match arg {
        Resp::BulkString(Some(b)) => b,
        Resp::SimpleString(s) => s,
        _ => return Err(Resp::Error("ERR syntax error".to_string())),
    };
    let s = String::from_utf8_lossy(bytes).to_ascii_uppercase();
    match s.as_str() {
        "LEFT" => Ok(PopDirection::Left),
        "RIGHT" => Ok(PopDirection::Right),
        _ => Err(Resp::Error("ERR syntax error".to_string())),
    }
}

pub fn lmove(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 5 {
        return Resp::Error("ERR wrong number of arguments for 'LMOVE'".to_string());
    }

    let src_key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let dst_key = match &items[2] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let where_from = match parse_direction(&items[3]) {
        Ok(d) => d,
        Err(e) => return e,
    };

    let where_to = match parse_direction(&items[4]) {
        Ok(d) => d,
        Err(e) => return e,
    };

    let db_ref = db;

    match lmove_execute(db_ref, &src_key, &dst_key, where_from, where_to) {
        Ok(Some(v)) => Resp::BulkString(Some(v)),
        Ok(None) => Resp::BulkString(None),
        Err(e) => e,
    }
}

fn lmove_execute(
    db: &Db,
    src_key: &bytes::Bytes,
    dst_key: &bytes::Bytes,
    where_from: PopDirection,
    where_to: PopDirection,
) -> Result<Option<bytes::Bytes>, Resp> {
    let src = src_key.clone();
    let dst = dst_key.clone();

    if src == dst {
        if let Some(mut entry) = db.get_mut(&src) {
            if entry.is_expired() {
                drop(entry);
                db.remove(&src);
                return Ok(None);
            }
            match &mut entry.value {
                Value::List(list) => {
                    let val = match where_from {
                        PopDirection::Left => list.pop_front(),
                        PopDirection::Right => list.pop_back(),
                    };
                    match val {
                        Some(v) => {
                            let pushed = v.clone();
                            match where_to {
                                PopDirection::Left => list.push_front(pushed),
                                PopDirection::Right => list.push_back(pushed),
                            }
                            Ok(Some(v))
                        }
                        None => Ok(None),
                    }
                }
                _ => Err(Resp::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                )),
            }
        } else {
            Ok(None)
        }
    } else {
        {
            if let Some(entry) = db.get(&dst) {
                if !entry.is_expired() {
                    match &entry.value {
                        Value::List(_) => {}
                        _ => {
                            return Err(Resp::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            ))
                        }
                    }
                }
            }
        }

        let mut val: Option<bytes::Bytes> = None;

        {
            if let Some(mut entry) = db.get_mut(&src) {
                if entry.is_expired() {
                    drop(entry);
                    db.remove(&src);
                } else {
                    match &mut entry.value {
                        Value::List(list) => {
                            let v = match where_from {
                                PopDirection::Left => list.pop_front(),
                                PopDirection::Right => list.pop_back(),
                            };
                            if let Some(v) = v {
                                val = Some(v);
                            } else {
                                return Ok(None);
                            }
                        }
                        _ => {
                            return Err(Resp::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            ))
                        }
                    }
                }
            } else {
                return Ok(None);
            }
        }

        let v = match val {
            Some(v) => v,
            None => return Ok(None),
        };

        let mut need_new_entry = false;
        let mut expired = false;
        {
            if let Some(entry) = db.get(&dst) {
                if entry.is_expired() {
                    expired = true;
                }
            } else {
                need_new_entry = true;
            }
        }

        if expired {
            db.remove(&dst);
            need_new_entry = true;
        }

        let mut entry = if need_new_entry {
            db.entry(dst.clone())
                .or_insert_with(|| Entry::new(Value::List(VecDeque::new()), None))
        } else {
            db.get_mut(&dst).unwrap()
        };

        match &mut entry.value {
            Value::List(list) => {
                let pushed = v.clone();
                match where_to {
                    PopDirection::Left => list.push_front(pushed),
                    PopDirection::Right => list.push_back(pushed),
                }
                Ok(Some(v))
            }
            _ => Err(Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            )),
        }
    }
}

fn blmove_push_to_dest(
    db: &Db,
    dst_key: &bytes::Bytes,
    where_to: PopDirection,
    value: bytes::Bytes,
) -> Result<(), Resp> {
    let dst = dst_key.clone();

    let mut need_new_entry = false;
    let mut expired = false;
    {
        if let Some(entry) = db.get(&dst) {
            if entry.is_expired() {
                expired = true;
            }
        } else {
            need_new_entry = true;
        }
    }

    if expired {
        db.remove(&dst);
        need_new_entry = true;
    }

    let mut entry = if need_new_entry {
        db.entry(dst.clone())
            .or_insert_with(|| Entry::new(Value::List(VecDeque::new()), None))
    } else {
        db.get_mut(&dst).unwrap()
    };

    match &mut entry.value {
        Value::List(list) => {
            match where_to {
                PopDirection::Left => list.push_front(value),
                PopDirection::Right => list.push_back(value),
            }
            Ok(())
        }
        _ => Err(Resp::Error(
            "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
        )),
    }
}

pub async fn blmove(
    items: &[Resp],
    conn_ctx: &ConnectionContext,
    server_ctx: &ServerContext,
) -> Resp {
    if items.len() != 6 {
        return Resp::Error("ERR wrong number of arguments for 'BLMOVE'".to_string());
    }

    let src_key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let dst_key = match &items[2] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let where_from = match parse_direction(&items[3]) {
        Ok(d) => d,
        Err(e) => return e,
    };

    let where_to = match parse_direction(&items[4]) {
        Ok(d) => d,
        Err(e) => return e,
    };

    let timeout_arg = match &items[5] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).parse::<f64>(),
        Resp::SimpleString(s) => String::from_utf8_lossy(s).parse::<f64>(),
        _ => return Resp::Error("ERR timeout is not a float or out of range".to_string()),
    };

    let timeout_secs = match timeout_arg {
        Ok(v) => v,
        Err(_) => return Resp::Error("ERR timeout is not a float or out of range".to_string()),
    };

    let db = &server_ctx.databases[conn_ctx.db_index];

    match lmove_execute(db, &src_key, &dst_key, where_from, where_to) {
        Ok(Some(v)) => return Resp::BulkString(Some(v)),
        Ok(None) => {}
        Err(e) => return e,
    }

    let (tx, mut rx) = tokio::sync::mpsc::channel::<(Vec<u8>, Vec<u8>)>(1);

    let map_key = (conn_ctx.db_index, src_key.to_vec());
    let mut queue = server_ctx
        .blocking_waiters
        .entry(map_key)
        .or_insert_with(VecDeque::new);
    queue.push_back(tx);

    let result = if timeout_secs > 0.0 {
        let duration = Duration::from_secs_f64(timeout_secs);
        match timeout(duration, rx.recv()).await {
            Ok(Some((_key, val))) => Some(val),
            Ok(None) => None,
            Err(_) => None,
        }
    } else {
        match rx.recv().await {
            Some((_key, val)) => Some(val),
            None => None,
        }
    };

    match result {
        Some(v) => {
            let value = bytes::Bytes::from(v);
            let db = &server_ctx.databases[conn_ctx.db_index];
            match blmove_push_to_dest(db, &dst_key, where_to, value.clone()) {
                Ok(()) => Resp::BulkString(Some(value)),
                Err(e) => e,
            }
        }
        None => Resp::BulkString(None),
    }
}
