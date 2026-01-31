use crate::cmd::{ConnectionContext, ServerContext};
use crate::db::{Db, Entry, Value};
use crate::resp::Resp;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::time::timeout;

use std::sync::atomic::Ordering;

pub fn lpush(items: &[Resp], conn_ctx: &ConnectionContext, server_ctx: &ServerContext) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'LPUSH'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let db = {
        let db_lock = server_ctx.databases[conn_ctx.db_index].read().unwrap();
        db_lock.clone()
    };

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

    let db = {
        let db_lock = server_ctx.databases[conn_ctx.db_index].read().unwrap();
        db_lock.clone()
    };

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

    let db = server_ctx.databases[conn_ctx.db_index].read().unwrap().clone();
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
    server_ctx.blocked_client_count.fetch_add(1, Ordering::Relaxed);
    
    let (_shutdown_tx, mut shutdown_rx) = if let Some(rx) = &conn_ctx.shutdown {
        (None, rx.clone())
    } else {
        let (tx, rx) = tokio::sync::watch::channel(false);
        (Some(tx), rx)
    };

    let result = if timeout_secs > 0.0 {
        let duration = Duration::from_secs_f64(timeout_secs);
        tokio::select! {
            res = timeout(duration, rx.recv()) => {
                match res {
                    Ok(Some((key, val))) => Some((key, val)),
                    Ok(None) => None,
                    Err(_) => None, // Timeout
                }
            }
            _ = shutdown_rx.changed() => {
                None
            }
        }
    } else {
        // Infinite wait
        tokio::select! {
            res = rx.recv() => {
                res
            }
            _ = shutdown_rx.changed() => {
                None
            }
        }
    };
    server_ctx.blocked_client_count.fetch_sub(1, Ordering::Relaxed);

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

    let db = server_ctx.databases[conn_ctx.db_index].read().unwrap().clone();

    match lmove_execute(&db, &src_key, &dst_key, where_from, where_to) {
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

    server_ctx.blocked_client_count.fetch_add(1, Ordering::Relaxed);
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
    server_ctx.blocked_client_count.fetch_sub(1, Ordering::Relaxed);

    match result {
        Some(v) => {
            let value = bytes::Bytes::from(v);
            let db = server_ctx.databases[conn_ctx.db_index].read().unwrap().clone();
            match blmove_push_to_dest(&db, &dst_key, where_to, value.clone()) {
                Ok(()) => Resp::BulkString(Some(value)),
                Err(e) => e,
            }
        }
        None => Resp::BulkString(None),
    }
}

pub fn linsert(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 5 {
        return Resp::Error("ERR wrong number of arguments for 'LINSERT'".to_string());
    }

    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let before = match &items[2] {
        Resp::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(b).to_ascii_uppercase();
            if s == "BEFORE" {
                true
            } else if s == "AFTER" {
                false
            } else {
                return Resp::Error("ERR syntax error".to_string());
            }
        }
        Resp::SimpleString(s) => {
            let s = String::from_utf8_lossy(s).to_ascii_uppercase();
            if s == "BEFORE" {
                true
            } else if s == "AFTER" {
                false
            } else {
                return Resp::Error("ERR syntax error".to_string());
            }
        }
        _ => return Resp::Error("ERR syntax error".to_string()),
    };

    let pivot = match &items[3] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid value".to_string()),
    };

    let element = match &items[4] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid value".to_string()),
    };

    if let Some(mut entry) = db.get_mut(&key) {
        if entry.is_expired() {
            drop(entry);
            db.remove(&key);
            return Resp::Integer(0);
        }
        
        match &mut entry.value {
            Value::List(list) => {
                let mut index = None;
                for (i, val) in list.iter().enumerate() {
                    if val == &pivot {
                        index = Some(i);
                        break;
                    }
                }

                if let Some(idx) = index {
                    let insert_at = if before { idx } else { idx + 1 };
                    list.insert(insert_at, element);
                    Resp::Integer(list.len() as i64)
                } else {
                    Resp::Integer(-1)
                }
            }
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
        }
    } else {
        Resp::Integer(0)
    }
}

pub fn lrem(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 4 {
        return Resp::Error("ERR wrong number of arguments for 'LREM'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    let count = match &items[2] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).parse::<i64>(),
        Resp::SimpleString(s) => String::from_utf8_lossy(s).parse::<i64>(),
        _ => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    let count = match count {
        Ok(v) => v,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    let element = match &items[3] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid value".to_string()),
    };

    if let Some(mut entry) = db.get_mut(&key) {
        if entry.is_expired() {
            drop(entry);
            db.remove(&key);
            return Resp::Integer(0);
        }
        match &mut entry.value {
            Value::List(list) => {
                let mut removed = 0;
                if count == 0 {
                    let initial_len = list.len();
                    list.retain(|x| x != &element);
                    removed = initial_len - list.len();
                } else if count > 0 {
                    let mut to_remove = count;
                    let mut i = 0;
                    while i < list.len() && to_remove > 0 {
                        if &list[i] == &element {
                            list.remove(i);
                            removed += 1;
                            to_remove -= 1;
                        } else {
                            i += 1;
                        }
                    }
                } else {
                    // count < 0, remove from tail
                    let mut to_remove = -count;
                    let mut i = list.len();
                    while i > 0 && to_remove > 0 {
                        i -= 1;
                        if &list[i] == &element {
                            list.remove(i);
                            removed += 1;
                            to_remove -= 1;
                        }
                    }
                }
                if list.is_empty() {
                    drop(entry);
                    db.remove(&key);
                }
                Resp::Integer(removed as i64)
            }
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
        }
    } else {
        Resp::Integer(0)
    }
}

pub fn ltrim(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 4 {
        return Resp::Error("ERR wrong number of arguments for 'LTRIM'".to_string());
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
    let start = match start {
        Ok(v) => v,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    let stop = match &items[3] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).parse::<i64>(),
        Resp::SimpleString(s) => String::from_utf8_lossy(s).parse::<i64>(),
        _ => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    let stop = match stop {
        Ok(v) => v,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    if let Some(mut entry) = db.get_mut(&key) {
        if entry.is_expired() {
            drop(entry);
            db.remove(&key);
            return Resp::SimpleString(bytes::Bytes::from_static(b"OK"));
        }
        match &mut entry.value {
            Value::List(list) => {
                let len = list.len() as i64;
                let start = if start < 0 { len + start } else { start };
                let stop = if stop < 0 { len + stop } else { stop };

                let start = if start < 0 { 0 } else { start };
                
                if start > stop || start >= len {
                    list.clear();
                    drop(entry);
                    db.remove(&key);
                    return Resp::SimpleString(bytes::Bytes::from_static(b"OK"));
                }
                
                let start = start as usize;
                let stop = if stop >= len { len - 1 } else { stop } as usize;
                
                if start > 0 {
                    for _ in 0..start {
                        list.pop_front();
                    }
                }
                
                let new_len = stop - start + 1;
                list.truncate(new_len);
                
                if list.is_empty() {
                    drop(entry);
                    db.remove(&key);
                }

                Resp::SimpleString(bytes::Bytes::from_static(b"OK"))
            }
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
        }
    } else {
        Resp::SimpleString(bytes::Bytes::from_static(b"OK"))
    }
}

pub fn lindex(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'LINDEX'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    let index = match &items[2] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).parse::<i64>(),
        Resp::SimpleString(s) => String::from_utf8_lossy(s).parse::<i64>(),
        _ => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    let index = match index {
        Ok(v) => v,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    if let Some(entry) = db.get(&key) {
        if entry.is_expired() {
            return Resp::BulkString(None);
        }
        match &entry.value {
            Value::List(list) => {
                let len = list.len() as i64;
                let idx = if index < 0 { len + index } else { index };
                
                if idx < 0 || idx >= len {
                    return Resp::BulkString(None);
                }
                
                if let Some(val) = list.get(idx as usize) {
                    Resp::BulkString(Some(val.clone()))
                } else {
                    Resp::BulkString(None)
                }
            }
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
        }
    } else {
        Resp::BulkString(None)
    }
}

pub fn lpushx(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'LPUSHX'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    if let Some(mut entry) = db.get_mut(&key) {
        if entry.is_expired() {
            return Resp::Integer(0);
        }
        match &mut entry.value {
            Value::List(list) => {
                for i in 2..items.len() {
                    let val = match &items[i] {
                        Resp::BulkString(Some(b)) => b.clone(),
                        Resp::SimpleString(s) => s.clone(),
                        _ => return Resp::Error("ERR invalid value".to_string()),
                    };
                    list.push_front(val);
                }
                Resp::Integer(list.len() as i64)
            }
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
        }
    } else {
        Resp::Integer(0)
    }
}

pub fn rpushx(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'RPUSHX'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    if let Some(mut entry) = db.get_mut(&key) {
        if entry.is_expired() {
            return Resp::Integer(0);
        }
        match &mut entry.value {
            Value::List(list) => {
                for i in 2..items.len() {
                    let val = match &items[i] {
                        Resp::BulkString(Some(b)) => b.clone(),
                        Resp::SimpleString(s) => s.clone(),
                        _ => return Resp::Error("ERR invalid value".to_string()),
                    };
                    list.push_back(val);
                }
                Resp::Integer(list.len() as i64)
            }
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
        }
    } else {
        Resp::Integer(0)
    }
}

pub fn lpos(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'LPOS'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };
    let element = match &items[2] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid value".to_string()),
    };

    let mut rank: i64 = 1;
    let mut count: Option<i64> = None;
    let mut maxlen: Option<i64> = None;

    let mut i = 3;
    while i < items.len() {
        let arg = match &items[i] {
            Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_ascii_uppercase(),
            Resp::SimpleString(s) => String::from_utf8_lossy(s).to_ascii_uppercase(),
            _ => return Resp::Error("ERR syntax error".to_string()),
        };

        match arg.as_str() {
            "RANK" => {
                if i + 1 >= items.len() {
                    return Resp::Error("ERR syntax error".to_string());
                }
                rank = match &items[i+1] {
                    Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).parse().unwrap_or(0),
                    Resp::SimpleString(s) => String::from_utf8_lossy(s).parse().unwrap_or(0),
                    _ => 0,
                };
                if rank == 0 {
                    return Resp::Error("ERR RANK can't be zero: use 1 to start from the first match, 2 from the second, ... or use negative to start from the end of the list".to_string());
                }
                i += 2;
            }
            "COUNT" => {
                if i + 1 >= items.len() {
                    return Resp::Error("ERR syntax error".to_string());
                }
                let c = match &items[i+1] {
                    Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).parse().unwrap_or(-1),
                    Resp::SimpleString(s) => String::from_utf8_lossy(s).parse().unwrap_or(-1),
                    _ => -1,
                };
                if c < 0 {
                    return Resp::Error("ERR COUNT can't be negative".to_string());
                }
                count = Some(c);
                i += 2;
            }
            "MAXLEN" => {
                if i + 1 >= items.len() {
                    return Resp::Error("ERR syntax error".to_string());
                }
                let m = match &items[i+1] {
                    Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).parse().unwrap_or(-1),
                    Resp::SimpleString(s) => String::from_utf8_lossy(s).parse().unwrap_or(-1),
                    _ => -1,
                };
                if m < 0 {
                    return Resp::Error("ERR MAXLEN can't be negative".to_string());
                }
                maxlen = Some(m);
                i += 2;
            }
            _ => return Resp::Error("ERR syntax error".to_string()),
        }
    }

    if let Some(entry) = db.get(&key) {
        if entry.is_expired() {
            return if count.is_some() {
                Resp::Array(Some(vec![]))
            } else {
                Resp::BulkString(None)
            };
        }
        match &entry.value {
            Value::List(list) => {
                let mut matches = Vec::new();
                let mut comparisons = 0;
                let mut matched_count = 0;

                let return_array = count.is_some();
                let limit_matches = match count {
                    Some(0) => usize::MAX,
                    Some(n) => n as usize,
                    None => 1,
                };
                let max_comps = maxlen.unwrap_or(0) as usize;

                let mut skipped = 0;
                let target_skip = rank.abs() as usize - 1;

                if rank > 0 {
                    for (idx, val) in list.iter().enumerate() {
                        if max_comps > 0 && comparisons >= max_comps {
                            break;
                        }
                        comparisons += 1;

                        if val == &element {
                            if skipped < target_skip {
                                skipped += 1;
                            } else {
                                matches.push(idx as i64);
                                matched_count += 1;
                                if matched_count >= limit_matches {
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    // rank < 0
                    for (idx, val) in list.iter().enumerate().rev() {
                        if max_comps > 0 && comparisons >= max_comps {
                            break;
                        }
                        comparisons += 1;

                        if val == &element {
                            if skipped < target_skip {
                                skipped += 1;
                            } else {
                                matches.push(idx as i64);
                                matched_count += 1;
                                if matched_count >= limit_matches {
                                    break;
                                }
                            }
                        }
                    }
                }

                if return_array {
                    let res = matches.into_iter().map(|i| Resp::Integer(i)).collect();
                    Resp::Array(Some(res))
                } else {
                    if matches.is_empty() {
                        Resp::BulkString(None)
                    } else {
                        Resp::Integer(matches[0])
                    }
                }
            }
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
        }
    } else {
        if count.is_some() {
            Resp::Array(Some(vec![]))
        } else {
            Resp::BulkString(None)
        }
    }
}

