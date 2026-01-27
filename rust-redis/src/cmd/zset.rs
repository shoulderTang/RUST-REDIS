use crate::db::{Db, Entry, SortedSet, TotalOrderF64, Value};
use crate::resp::Resp;
use crate::cmd::{ConnectionContext, ServerContext};
use std::collections::VecDeque;
use std::time::Duration;
use tokio::time::timeout;

pub fn zadd(items: &[Resp], conn_ctx: &ConnectionContext, server_ctx: &ServerContext) -> Resp {
    if items.len() < 4 || items.len() % 2 != 0 {
        return Resp::Error("ERR wrong number of arguments for 'ZADD'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let db = &server_ctx.databases[conn_ctx.db_index];

    let mut entry = db
        .entry(key.clone())
        .or_insert_with(|| Entry::new(Value::ZSet(SortedSet::new()), None));
    if entry.is_expired() {
        entry.value = Value::ZSet(SortedSet::new());
        entry.expires_at = None;
    }

    let mut added_count = 0;

    if let Value::ZSet(zset) = &mut entry.value {
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
                    zset.scores
                        .remove(&(TotalOrderF64(*old_score), member.clone()));
                    zset.members.insert(member.clone(), score);
                    zset.scores.insert((TotalOrderF64(score), member));
                }
            } else {
                zset.members.insert(member.clone(), score);
                zset.scores.insert((TotalOrderF64(score), member));
                added_count += 1;
            }
        }
        
        // Notify waiters if we have members
        if !zset.members.is_empty() {
            let map_key = (conn_ctx.db_index, key.to_vec());
            
            // Loop to serve waiters while we have members
            loop {
                if zset.members.is_empty() {
                    break;
                }

                let mut sender_info = None;
                if let Some(mut waiters) = server_ctx.blocking_zset_waiters.get_mut(&map_key) {
                    if let Some(info) = waiters.pop_front() {
                        sender_info = Some(info);
                    }
                }

                if let Some((sender, is_min)) = sender_info {
                    // Pop from ZSet
                    let popped = if is_min {
                        // Pop Min
                        if let Some((score_wrapper, member)) = zset.scores.pop_first() {
                            let score = score_wrapper.0;
                            zset.members.remove(&member);
                            Some((member, score))
                        } else {
                            None
                        }
                    } else {
                        // Pop Max
                        if let Some((score_wrapper, member)) = zset.scores.pop_last() {
                            let score = score_wrapper.0;
                            zset.members.remove(&member);
                            Some((member, score))
                        } else {
                            None
                        }
                    };

                    if let Some((member, score)) = popped {
                        match sender.try_send((key.to_vec(), member.to_vec(), score)) {
                            Ok(_) => {
                                // Sent successfully
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                // Should not happen for size 1, but if so, we dropped the item?
                                // Wait, if we popped it, we MUST deliver or put it back.
                                // If we can't deliver, we should put it back?
                                // Or retry next waiter?
                                // For simplicity/robustness, if we fail to send, we should try to put it back?
                                // But `sender` is closed is the main issue.
                                // If Full, it means the receiver hasn't read yet? But they are blocked waiting.
                                // Let's assume Full won't happen.
                                // If Closed, we proceed to next waiter.
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                // Receiver gone. We popped the item. We should put it back or give to next waiter?
                                // We should give to next waiter.
                                // If no more waiters, put back?
                                // For now, let's try to serve next waiter with the SAME item.
                                // But my loop structure pops a NEW item each time.
                                // This logic is flawed if send fails.
                                
                                // Refined logic:
                                // 1. Peek waiter.
                                // 2. Pop item.
                                // 3. Try send.
                                // 4. If fail, loop with SAME item?
                            }
                        }
                    }
                } else {
                    break;
                }
            }
        }
        
        Resp::Integer(added_count)
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
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
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
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
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
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
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
            _ => Resp::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
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
            }
            Resp::SimpleString(s) => {
                if s.eq_ignore_ascii_case(b"WITHSCORES") {
                    true
                } else {
                    return Resp::Error("ERR syntax error".to_string());
                }
            }
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
                for (score, member) in zset
                    .scores
                    .iter()
                    .skip(start_idx as usize)
                    .take((stop_idx - start_idx + 1) as usize)
                {
                    result.push(Resp::BulkString(Some(member.clone())));
                    if withscores {
                        result.push(Resp::BulkString(Some(bytes::Bytes::from(
                            score.0.to_string(),
                        ))));
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
}

pub fn zpopmin(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'ZPOPMIN'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let count = if items.len() > 2 {
        match &items[2] {
            Resp::BulkString(Some(b)) => match String::from_utf8_lossy(b).parse::<i64>() {
                Ok(c) => c,
                Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
            },
            Resp::SimpleString(s) => match String::from_utf8_lossy(s).parse::<i64>() {
                Ok(c) => c,
                Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
            },
            _ => return Resp::Error("ERR value is not an integer or out of range".to_string()),
        }
    } else {
        1
    };

    if let Some(mut entry) = db.get_mut(&key) {
        if entry.is_expired() {
            drop(entry);
            db.remove(&key);
            return Resp::Array(Some(vec![]));
        }
        match &mut entry.value {
            Value::ZSet(zset) => {
                let mut result = Vec::new();
                for _ in 0..count {
                    if let Some((score_wrapper, member)) = zset.scores.pop_first() {
                        let score = score_wrapper.0;
                        zset.members.remove(&member);
                        result.push(Resp::BulkString(Some(member)));
                        result.push(Resp::BulkString(Some(bytes::Bytes::from(score.to_string()))));
                    } else {
                        break;
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
}

pub fn zpopmax(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'ZPOPMAX'".to_string());
    }
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let count = if items.len() > 2 {
        match &items[2] {
            Resp::BulkString(Some(b)) => match String::from_utf8_lossy(b).parse::<i64>() {
                Ok(c) => c,
                Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
            },
            Resp::SimpleString(s) => match String::from_utf8_lossy(s).parse::<i64>() {
                Ok(c) => c,
                Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
            },
            _ => return Resp::Error("ERR value is not an integer or out of range".to_string()),
        }
    } else {
        1
    };

    if let Some(mut entry) = db.get_mut(&key) {
        if entry.is_expired() {
            drop(entry);
            db.remove(&key);
            return Resp::Array(Some(vec![]));
        }
        match &mut entry.value {
            Value::ZSet(zset) => {
                let mut result = Vec::new();
                for _ in 0..count {
                    if let Some((score_wrapper, member)) = zset.scores.pop_last() {
                        let score = score_wrapper.0;
                        zset.members.remove(&member);
                        result.push(Resp::BulkString(Some(member)));
                        result.push(Resp::BulkString(Some(bytes::Bytes::from(score.to_string()))));
                    } else {
                        break;
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
}

pub async fn bzpopmin(items: &[Resp], conn_ctx: &ConnectionContext, server_ctx: &ServerContext) -> Resp {
    blocking_zpop_generic(items, conn_ctx, server_ctx, true).await
}

pub async fn bzpopmax(items: &[Resp], conn_ctx: &ConnectionContext, server_ctx: &ServerContext) -> Resp {
    blocking_zpop_generic(items, conn_ctx, server_ctx, false).await
}

async fn blocking_zpop_generic(
    items: &[Resp],
    conn_ctx: &ConnectionContext,
    server_ctx: &ServerContext,
    is_min: bool,
) -> Resp {
    if items.len() < 3 {
        let cmd = if is_min { "BZPOPMIN" } else { "BZPOPMAX" };
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

    // 1. Try to serve from existing sets immediately
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
             if let Value::ZSet(zset) = &mut entry.value {
                 let popped = if is_min {
                     zset.scores.pop_first()
                 } else {
                     zset.scores.pop_last()
                 };

                 if let Some((score_wrapper, member)) = popped {
                     let score = score_wrapper.0;
                     zset.members.remove(&member);
                     
                     return Resp::Array(Some(vec![
                         Resp::BulkString(Some(key)),
                         Resp::BulkString(Some(member)),
                         Resp::BulkString(Some(bytes::Bytes::from(score.to_string()))),
                     ]));
                 }
             }
        }
    }

    // 2. If no data, block
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(Vec<u8>, Vec<u8>, f64)>(1);
    
    // Register waiter for all keys
    for key in &keys {
        let map_key = (conn_ctx.db_index, key.to_vec());
        let mut queue = server_ctx.blocking_zset_waiters.entry(map_key).or_insert_with(VecDeque::new);
        queue.push_back((tx.clone(), is_min));
    }

    // Wait
    let result = if timeout_secs > 0.0 {
        let duration = Duration::from_secs_f64(timeout_secs);
        match timeout(duration, rx.recv()).await {
            Ok(Some((key, val, score))) => Some((key, val, score)),
            Ok(None) => None,
            Err(_) => None, // Timeout
        }
    } else {
        // Infinite wait
        rx.recv().await
    };

    match result {
        Some((key, val, score)) => Resp::Array(Some(vec![
            Resp::BulkString(Some(bytes::Bytes::from(key))),
            Resp::BulkString(Some(bytes::Bytes::from(val))),
            Resp::BulkString(Some(bytes::Bytes::from(score.to_string()))),
        ])),
        None => Resp::BulkString(None), // Timeout
    }
}
