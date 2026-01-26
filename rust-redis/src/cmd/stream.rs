use crate::db::{Db, Value};
use crate::resp::Resp;
use crate::stream::{Stream, StreamID, ConsumerGroup, Consumer, PendingEntry};
use bytes::Bytes;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

fn as_bytes(resp: &Resp) -> Option<Bytes> {
    match resp {
        Resp::BulkString(Some(b)) => Some(b.clone()),
        Resp::SimpleString(b) => Some(b.clone()),
        _ => None,
    }
}

pub fn xadd(args: &[Resp], db: &Db) -> (Resp, Option<Resp>) {
    if args.len() < 5 || (args.len() - 3) % 2 != 0 {
        return (Resp::Error("ERR wrong number of arguments for 'xadd' command".to_string()), None);
    }

    let key = match as_bytes(&args[1]) {
        Some(b) => b,
        None => return (Resp::Error("ERR invalid key".to_string()), None),
    };

    let id_str = match as_bytes(&args[2]) {
        Some(b) => String::from_utf8_lossy(&b).to_string(),
        None => return (Resp::Error("ERR invalid ID".to_string()), None),
    };

    let mut entry_fields = Vec::new();
    let mut i = 3;
    while i < args.len() {
        let field = match as_bytes(&args[i]) {
            Some(b) => b,
            None => return (Resp::Error("ERR invalid field".to_string()), None),
        };
        let value = match as_bytes(&args[i + 1]) {
            Some(b) => b,
            None => return (Resp::Error("ERR invalid value".to_string()), None),
        };
        entry_fields.push((field, value));
        i += 2;
    }

    let mut stream = if let Some(mut entry) = db.get_mut(&key) {
        if let Value::Stream(s) = &mut entry.value {
            s.clone()
        } else {
            return (Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()), None);
        }
    } else {
        Stream::new()
    };

    let id = if id_str == "*" {
        let last_id = stream.last_id;
        if last_id.ms == u64::MAX && last_id.seq == u64::MAX {
             return (Resp::Error("ERR The stream has exhausted the last possible ID".to_string()), None);
        }
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        if now < last_id.ms {
             let ms = last_id.ms;
             let seq = last_id.seq + 1;
             StreamID::new(ms, seq)
        } else if now == last_id.ms {
             StreamID::new(now, last_id.seq + 1)
        } else {
             StreamID::new(now, 0)
        }
    } else {
        match StreamID::from_str(&id_str) {
            Ok(id) => id,
            Err(_) => return (Resp::Error("ERR invalid stream ID".to_string()), None),
        }
    };

    match stream.insert(id, entry_fields) {
        Ok(new_id) => {
            db.insert(key.clone(), crate::db::Entry::new(Value::Stream(stream), None));
            
            // Construct log command
            let mut log_args = Vec::with_capacity(args.len());
            log_args.push(Resp::BulkString(Some(Bytes::from("XADD"))));
            log_args.push(args[1].clone()); // key
            log_args.push(Resp::BulkString(Some(Bytes::from(new_id.to_string())))); // concrete ID
            
            // fields
            for i in 3..args.len() {
                log_args.push(args[i].clone());
            }
            
            (Resp::BulkString(Some(Bytes::from(new_id.to_string()))), Some(Resp::Array(Some(log_args))))
        }
        Err(e) => (Resp::Error(e.to_string()), None),
    }
}

pub fn xlen(args: &[Resp], db: &Db) -> Resp {
    if args.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'xlen' command".to_string());
    }

    let key = match as_bytes(&args[1]) {
        Some(b) => b,
        None => return Resp::Error("ERR invalid key".to_string()),
    };

    if let Some(entry) = db.get(&key) {
        if let Value::Stream(stream) = &entry.value {
            return Resp::Integer(stream.len() as i64);
        } else {
            return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
        }
    }

    Resp::Integer(0)
}

pub fn xrange(args: &[Resp], db: &Db) -> Resp {
    if args.len() < 4 {
        return Resp::Error("ERR wrong number of arguments for 'xrange' command".to_string());
    }

    let key = match as_bytes(&args[1]) {
        Some(b) => b,
        None => return Resp::Error("ERR invalid key".to_string()),
    };

    let start_str = match as_bytes(&args[2]) {
        Some(b) => String::from_utf8_lossy(&b).to_string(),
        None => return Resp::Error("ERR invalid start ID".to_string()),
    };

    let end_str = match as_bytes(&args[3]) {
        Some(b) => String::from_utf8_lossy(&b).to_string(),
        None => return Resp::Error("ERR invalid end ID".to_string()),
    };

    // Parse options: [COUNT count]
    let mut count = None;
    if args.len() > 4 {
        if args.len() == 6 {
             let opt = match as_bytes(&args[4]) {
                Some(b) => String::from_utf8_lossy(&b).to_string().to_uppercase(),
                None => return Resp::Error("ERR syntax error".to_string()),
            };
            if opt == "COUNT" {
                 if let Some(val) = as_bytes(&args[5]) {
                     if let Ok(c) = String::from_utf8_lossy(&val).parse::<usize>() {
                         count = Some(c);
                     } else {
                         return Resp::Error("ERR invalid count".to_string());
                     }
                 } else {
                     return Resp::Error("ERR invalid count".to_string());
                 }
            } else {
                return Resp::Error("ERR syntax error".to_string());
            }
        } else {
             return Resp::Error("ERR syntax error".to_string());
        }
    }

    if let Some(entry) = db.get(&key) {
        if let Value::Stream(stream) = &entry.value {
            let start_id = if start_str == "-" {
                StreamID::new(0, 0)
            } else {
                 match StreamID::from_str(&start_str) {
                    Ok(id) => id,
                    Err(_) => return Resp::Error("ERR invalid start ID".to_string()),
                }
            };

            let end_id = if end_str == "+" {
                StreamID::new(u64::MAX, u64::MAX)
            } else {
                 match StreamID::from_str(&end_str) {
                    Ok(id) => id,
                    Err(_) => return Resp::Error("ERR invalid end ID".to_string()),
                }
            };

            let entries = stream.range(&start_id, &end_id);
            let mut arr = Vec::new();
            let take_count = count.unwrap_or(entries.len());
            
            for entry in entries.iter().take(take_count) {
                let mut entry_arr = Vec::new();
                entry_arr.push(Resp::BulkString(Some(Bytes::from(entry.id.to_string()))));
                
                let mut fields_arr = Vec::new();
                for (field, value) in &entry.fields {
                    fields_arr.push(Resp::BulkString(Some(field.clone())));
                    fields_arr.push(Resp::BulkString(Some(value.clone())));
                }
                entry_arr.push(Resp::Array(Some(fields_arr)));
                
                arr.push(Resp::Array(Some(entry_arr)));
            }
            
            return Resp::Array(Some(arr));
        } else {
            return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
        }
    }

    Resp::Array(Some(Vec::new()))
}

pub fn xrevrange(args: &[Resp], db: &Db) -> Resp {
    if args.len() < 4 {
        return Resp::Error("ERR wrong number of arguments for 'xrevrange' command".to_string());
    }

    let key = match as_bytes(&args[1]) {
        Some(b) => b,
        None => return Resp::Error("ERR invalid key".to_string()),
    };

    let end_str = match as_bytes(&args[2]) {
        Some(b) => String::from_utf8_lossy(&b).to_string(),
        None => return Resp::Error("ERR invalid end ID".to_string()),
    };

    let start_str = match as_bytes(&args[3]) {
        Some(b) => String::from_utf8_lossy(&b).to_string(),
        None => return Resp::Error("ERR invalid start ID".to_string()),
    };

    // Parse options: [COUNT count]
    let mut count = None;
    if args.len() > 4 {
        if args.len() == 6 {
             let opt = match as_bytes(&args[4]) {
                Some(b) => String::from_utf8_lossy(&b).to_string().to_uppercase(),
                None => return Resp::Error("ERR syntax error".to_string()),
            };
            if opt == "COUNT" {
                 if let Some(val) = as_bytes(&args[5]) {
                     if let Ok(c) = String::from_utf8_lossy(&val).parse::<usize>() {
                         count = Some(c);
                     } else {
                         return Resp::Error("ERR invalid count".to_string());
                     }
                 } else {
                     return Resp::Error("ERR invalid count".to_string());
                 }
            } else {
                return Resp::Error("ERR syntax error".to_string());
            }
        } else {
             return Resp::Error("ERR syntax error".to_string());
        }
    }

    if let Some(entry) = db.get(&key) {
        if let Value::Stream(stream) = &entry.value {
            let start_id = if start_str == "-" {
                StreamID::new(0, 0)
            } else {
                 match StreamID::from_str(&start_str) {
                    Ok(id) => id,
                    Err(_) => return Resp::Error("ERR invalid start ID".to_string()),
                }
            };

            let end_id = if end_str == "+" {
                StreamID::new(u64::MAX, u64::MAX)
            } else {
                 match StreamID::from_str(&end_str) {
                    Ok(id) => id,
                    Err(_) => return Resp::Error("ERR invalid end ID".to_string()),
                }
            };

            // rev_range expects (start, end) where start <= end usually, but rev_range implementation
            // in Stream might handle (end, start) or expects min, max.
            // Redis XREVRANGE end start [COUNT] -> from higher ID to lower ID.
            // My Stream::rev_range implementation takes (start, end) as (min, max) and iterates backwards?
            // Let's check Stream::rev_range implementation. 
            // It calls rax.rev_range(&start_bytes, &end_bytes).
            // Usually range queries take (min, max).
            
            let entries = stream.rev_range(&start_id, &end_id);
            let mut arr = Vec::new();
            let take_count = count.unwrap_or(entries.len());
            
            for entry in entries.iter().take(take_count) {
                let mut entry_arr = Vec::new();
                entry_arr.push(Resp::BulkString(Some(Bytes::from(entry.id.to_string()))));
                
                let mut fields_arr = Vec::new();
                for (field, value) in &entry.fields {
                    fields_arr.push(Resp::BulkString(Some(field.clone())));
                    fields_arr.push(Resp::BulkString(Some(value.clone())));
                }
                entry_arr.push(Resp::Array(Some(fields_arr)));
                
                arr.push(Resp::Array(Some(entry_arr)));
            }
            
            return Resp::Array(Some(arr));
        } else {
            return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
        }
    }

    Resp::Array(Some(Vec::new()))
}

pub fn xdel(args: &[Resp], db: &Db) -> (Resp, Option<Resp>) {
    if args.len() < 3 {
        return (Resp::Error("ERR wrong number of arguments for 'xdel' command".to_string()), None);
    }

    let key = match as_bytes(&args[1]) {
        Some(b) => b,
        None => return (Resp::Error("ERR invalid key".to_string()), None),
    };

    let mut deleted = 0;
    
    if let Some(mut entry) = db.get_mut(&key) {
        if let Value::Stream(stream) = &mut entry.value {
            for i in 2..args.len() {
                 let id_str = match as_bytes(&args[i]) {
                    Some(b) => String::from_utf8_lossy(&b).to_string(),
                    None => continue,
                };
                
                if let Ok(id) = StreamID::from_str(&id_str) {
                    if stream.remove(&id).is_some() {
                        deleted += 1;
                    }
                }
            }
        } else {
             return (Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()), None);
        }
    } else {
        return (Resp::Integer(0), None);
    }

    // Log command
    let mut log_args = Vec::with_capacity(args.len());
    for arg in args {
        log_args.push(arg.clone());
    }

    (Resp::Integer(deleted), Some(Resp::Array(Some(log_args))))
}

pub fn xread(args: &[Resp], db: &Db) -> Resp {
    // XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
    if args.len() < 4 {
        return Resp::Error("ERR wrong number of arguments for 'xread' command".to_string());
    }

    let mut arg_idx = 1;
    let mut count = None;
    let mut _block = None;

    while arg_idx < args.len() {
        let arg = match as_bytes(&args[arg_idx]) {
            Some(b) => String::from_utf8_lossy(&b).to_string().to_uppercase(),
            None => return Resp::Error("ERR syntax error".to_string()),
        };

        if arg == "COUNT" {
            arg_idx += 1;
             if arg_idx >= args.len() {
                 return Resp::Error("ERR syntax error".to_string());
             }
             if let Some(val) = as_bytes(&args[arg_idx]) {
                 if let Ok(c) = String::from_utf8_lossy(&val).parse::<usize>() {
                     count = Some(c);
                 } else {
                     return Resp::Error("ERR invalid count".to_string());
                 }
             } else {
                 return Resp::Error("ERR invalid count".to_string());
             }
             arg_idx += 1;
        } else if arg == "BLOCK" {
            arg_idx += 1;
             if arg_idx >= args.len() {
                 return Resp::Error("ERR syntax error".to_string());
             }
             if let Some(val) = as_bytes(&args[arg_idx]) {
                 if let Ok(c) = String::from_utf8_lossy(&val).parse::<u64>() {
                     _block = Some(c);
                 } else {
                     return Resp::Error("ERR invalid block time".to_string());
                 }
             } else {
                 return Resp::Error("ERR invalid block time".to_string());
             }
             arg_idx += 1;
        } else if arg == "STREAMS" {
            arg_idx += 1;
            break;
        } else {
            return Resp::Error("ERR syntax error".to_string());
        }
    }

    let remaining_args = args.len() - arg_idx;
    if remaining_args % 2 != 0 {
        return Resp::Error("ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified.".to_string());
    }

    let num_streams = remaining_args / 2;
    let mut result_arr = Vec::new();

    for i in 0..num_streams {
        let key_idx = arg_idx + i;
        let id_idx = arg_idx + num_streams + i;

        let key = match as_bytes(&args[key_idx]) {
            Some(b) => b,
            None => return Resp::Error("ERR invalid key".to_string()),
        };

        let id_str = match as_bytes(&args[id_idx]) {
            Some(b) => String::from_utf8_lossy(&b).to_string(),
            None => return Resp::Error("ERR invalid ID".to_string()),
        };

        if let Some(entry) = db.get(&key) {
            if let Value::Stream(stream) = &entry.value {
                let start_id = if id_str == "$" {
                    stream.last_id
                } else {
                    match StreamID::from_str(&id_str) {
                        Ok(id) => id,
                        Err(_) => return Resp::Error("ERR invalid stream ID".to_string()),
                    }
                };

                // XREAD returns entries with ID > start_id
                let range_start = if start_id.seq == u64::MAX {
                    if start_id.ms == u64::MAX {
                        // Impossible to have > MAX-MAX
                        continue;
                    } else {
                        StreamID::new(start_id.ms + 1, 0)
                    }
                } else {
                    StreamID::new(start_id.ms, start_id.seq + 1)
                };

                let range_end = StreamID::new(u64::MAX, u64::MAX);
                let entries = stream.range(&range_start, &range_end);

                if !entries.is_empty() {
                    let mut stream_res = Vec::new();
                    stream_res.push(Resp::BulkString(Some(key.clone())));

                    let mut entries_arr = Vec::new();
                    let take_count = count.unwrap_or(entries.len());

                    for entry in entries.iter().take(take_count) {
                        let mut entry_arr = Vec::new();
                        entry_arr.push(Resp::BulkString(Some(Bytes::from(entry.id.to_string()))));

                        let mut fields_arr = Vec::new();
                        for (field, value) in &entry.fields {
                            fields_arr.push(Resp::BulkString(Some(field.clone())));
                            fields_arr.push(Resp::BulkString(Some(value.clone())));
                        }
                        entry_arr.push(Resp::Array(Some(fields_arr)));

                        entries_arr.push(Resp::Array(Some(entry_arr)));
                    }
                    stream_res.push(Resp::Array(Some(entries_arr)));
                    result_arr.push(Resp::Array(Some(stream_res)));
                }
            } else {
                return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
            }
        }
    }

    if result_arr.is_empty() {
        Resp::BulkString(None)
    } else {
        Resp::Array(Some(result_arr))
    }
}

pub fn xgroup(args: &[Resp], db: &Db) -> (Resp, Option<Resp>) {
    if args.len() < 2 {
        return (Resp::Error("ERR wrong number of arguments for 'xgroup' command".to_string()), None);
    }

    let subcommand = match as_bytes(&args[1]) {
        Some(b) => String::from_utf8_lossy(&b).to_string().to_uppercase(),
        None => return (Resp::Error("ERR syntax error".to_string()), None),
    };

    if subcommand == "CREATE" {
        if args.len() < 5 {
             return (Resp::Error("ERR wrong number of arguments for 'xgroup' command".to_string()), None);
        }
        let key = match as_bytes(&args[2]) {
            Some(b) => b,
            None => return (Resp::Error("ERR invalid key".to_string()), None),
        };
        let group_name = match as_bytes(&args[3]) {
            Some(b) => String::from_utf8_lossy(&b).to_string(),
            None => return (Resp::Error("ERR invalid group name".to_string()), None),
        };
        let id_str = match as_bytes(&args[4]) {
            Some(b) => String::from_utf8_lossy(&b).to_string(),
            None => return (Resp::Error("ERR invalid ID".to_string()), None),
        };
        
        let mut mkstream = false;
        if args.len() > 5 {
             let opt = match as_bytes(&args[5]) {
                Some(b) => String::from_utf8_lossy(&b).to_string().to_uppercase(),
                None => return (Resp::Error("ERR syntax error".to_string()), None),
            };
            if opt == "MKSTREAM" {
                mkstream = true;
            } else {
                 return (Resp::Error("ERR syntax error".to_string()), None);
            }
        }

        if !db.contains_key(&key) {
            if mkstream {
                 let stream = Stream::new();
                 db.insert(key.clone(), crate::db::Entry::new(Value::Stream(stream), None));
            } else {
                return (Resp::Error("ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.".to_string()), None);
            }
        }

        if let Some(mut entry) = db.get_mut(&key) {
             if let Value::Stream(stream) = &mut entry.value {
                 if stream.groups.contains_key(&group_name) {
                     return (Resp::Error("BUSYGROUP Consumer Group name already exists".to_string()), None);
                 }
                 
                 let id = if id_str == "$" {
                     stream.last_id
                 } else {
                      match StreamID::from_str(&id_str) {
                        Ok(id) => id,
                        Err(_) => return (Resp::Error("ERR invalid stream ID".to_string()), None),
                    }
                 };

                 let group = ConsumerGroup::new(group_name.clone(), id);
                 stream.groups.insert(group_name, group);
                 
                 // Log command
                 let mut log_args = Vec::with_capacity(args.len());
                 log_args.push(args[0].clone()); // XGROUP
                 log_args.push(args[1].clone()); // CREATE
                 log_args.push(args[2].clone()); // key
                 log_args.push(args[3].clone()); // groupname
                 log_args.push(Resp::BulkString(Some(Bytes::from(id.to_string())))); // resolved ID
                 
                 for i in 5..args.len() {
                     log_args.push(args[i].clone());
                 }
                 
                 return (Resp::SimpleString(Bytes::from("OK")), Some(Resp::Array(Some(log_args))));
             } else {
                 return (Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()), None);
             }
        }
        (Resp::SimpleString(Bytes::from("OK")), None)
    } else if subcommand == "DESTROY" {
        if args.len() < 4 {
             return (Resp::Error("ERR wrong number of arguments for 'xgroup' command".to_string()), None);
        }
        let key = match as_bytes(&args[2]) {
            Some(b) => b,
            None => return (Resp::Error("ERR invalid key".to_string()), None),
        };
        let group_name = match as_bytes(&args[3]) {
            Some(b) => String::from_utf8_lossy(&b).to_string(),
            None => return (Resp::Error("ERR invalid group name".to_string()), None),
        };

        if let Some(mut entry) = db.get_mut(&key) {
            if let Value::Stream(stream) = &mut entry.value {
                if stream.groups.remove(&group_name).is_some() {
                    return (Resp::Integer(1), None);
                } else {
                    return (Resp::Integer(0), None);
                }
            } else {
                return (Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()), None);
            }
        } else {
             return (Resp::Integer(0), None);
        }
    } else {
        (Resp::Error("ERR unknown subcommand".to_string()), None)
    }
}

pub fn xreadgroup(args: &[Resp], db: &Db) -> (Resp, Option<Resp>) {
    // XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] id [id ...]
    if args.len() < 7 {
         return (Resp::Error("ERR wrong number of arguments for 'xreadgroup' command".to_string()), None);
    }

    let mut arg_idx = 1;
    let mut count = None;
    let mut _block = None;
    let mut _noack = false;

    // First arg must be GROUP
    let arg1 = match as_bytes(&args[arg_idx]) {
        Some(b) => String::from_utf8_lossy(&b).to_string().to_uppercase(),
        None => return (Resp::Error("ERR syntax error".to_string()), None),
    };
    if arg1 != "GROUP" {
         return (Resp::Error("ERR syntax error".to_string()), None);
    }
    arg_idx += 1;
    
    let group_name = match as_bytes(&args[arg_idx]) {
        Some(b) => String::from_utf8_lossy(&b).to_string(),
        None => return (Resp::Error("ERR syntax error".to_string()), None),
    };
    arg_idx += 1;
    
    let consumer_name = match as_bytes(&args[arg_idx]) {
        Some(b) => String::from_utf8_lossy(&b).to_string(),
        None => return (Resp::Error("ERR syntax error".to_string()), None),
    };
    arg_idx += 1;

    while arg_idx < args.len() {
        let arg = match as_bytes(&args[arg_idx]) {
            Some(b) => String::from_utf8_lossy(&b).to_string().to_uppercase(),
            None => return (Resp::Error("ERR syntax error".to_string()), None),
        };

        if arg == "COUNT" {
            arg_idx += 1;
             if arg_idx >= args.len() {
                 return (Resp::Error("ERR syntax error".to_string()), None);
             }
             if let Some(val) = as_bytes(&args[arg_idx]) {
                 if let Ok(c) = String::from_utf8_lossy(&val).parse::<usize>() {
                     count = Some(c);
                 } else {
                     return (Resp::Error("ERR invalid count".to_string()), None);
                 }
             } else {
                 return (Resp::Error("ERR invalid count".to_string()), None);
             }
             arg_idx += 1;
        } else if arg == "BLOCK" {
            arg_idx += 1;
             if arg_idx >= args.len() {
                 return (Resp::Error("ERR syntax error".to_string()), None);
             }
             if let Some(val) = as_bytes(&args[arg_idx]) {
                 if let Ok(c) = String::from_utf8_lossy(&val).parse::<u64>() {
                     _block = Some(c);
                 } else {
                     return (Resp::Error("ERR invalid block time".to_string()), None);
                 }
             } else {
                 return (Resp::Error("ERR invalid block time".to_string()), None);
             }
             arg_idx += 1;
        } else if arg == "NOACK" {
            _noack = true;
            arg_idx += 1;
        } else if arg == "STREAMS" {
            arg_idx += 1;
            break;
        } else {
            return (Resp::Error("ERR syntax error".to_string()), None);
        }
    }

    let remaining_args = args.len() - arg_idx;
    if remaining_args % 2 != 0 {
        return (Resp::Error("ERR Unbalanced XREADGROUP list of streams: for each stream key an ID or '$' must be specified.".to_string()), None);
    }

    let num_streams = remaining_args / 2;
    let mut result_arr = Vec::new();
    let mut needs_log = false;

    for i in 0..num_streams {
        let key_idx = arg_idx + i;
        let id_idx = arg_idx + num_streams + i;

        let key = match as_bytes(&args[key_idx]) {
            Some(b) => b,
            None => return (Resp::Error("ERR invalid key".to_string()), None),
        };

        let id_str = match as_bytes(&args[id_idx]) {
            Some(b) => String::from_utf8_lossy(&b).to_string(),
            None => return (Resp::Error("ERR invalid ID".to_string()), None),
        };

        if let Some(mut entry) = db.get_mut(&key) {
            if let Value::Stream(stream) = &mut entry.value {
                // Find group and prepare
                let start_id_opt = if let Some(group) = stream.groups.get_mut(&group_name) {
                    // Ensure consumer exists
                    if !group.consumers.contains_key(&consumer_name) {
                        group.consumers.insert(consumer_name.clone(), Consumer::new(consumer_name.clone()));
                    }
                    let consumer = group.consumers.get_mut(&consumer_name).unwrap();
                    consumer.seen_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();

                    if id_str == ">" {
                        Some(group.last_id)
                    } else {
                         match StreamID::from_str(&id_str) {
                            Ok(id) => Some(id),
                            Err(_) => return (Resp::Error("ERR invalid stream ID".to_string()), None),
                        }
                    }
                } else {
                    None
                };

                if start_id_opt.is_none() {
                     return (Resp::Error("NOGROUP No such key 'group_name' or consumer group 'group_name' in key 'key'".to_string()), None);
                }
                let start_id = start_id_opt.unwrap();

                let mut entries_to_process = Vec::new();

                if id_str == ">" {
                    // Range logic
                    let range_start = if start_id.seq == u64::MAX {
                        if start_id.ms == u64::MAX {
                             continue;
                        } else {
                            StreamID::new(start_id.ms + 1, 0)
                        }
                    } else {
                        StreamID::new(start_id.ms, start_id.seq + 1)
                    };
                    let range_end = StreamID::new(u64::MAX, u64::MAX);

                    let entries = stream.range(&range_start, &range_end);
                    let take_count = count.unwrap_or(entries.len());
                    entries_to_process = entries.into_iter().take(take_count).collect();

                    // If reading new messages (>), update last_id and add to PEL
                    if !entries_to_process.is_empty() {
                        needs_log = true;
                        if let Some(group) = stream.groups.get_mut(&group_name) {
                            let consumer = group.consumers.get_mut(&consumer_name).unwrap();
                            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();

                            for entry in &entries_to_process {
                                let pe = PendingEntry {
                                    id: entry.id,
                                    delivery_time: now,
                                    delivery_count: 1,
                                    owner: consumer_name.clone(),
                                };
                                group.pel.insert(entry.id, pe);
                                consumer.pending_ids.insert(entry.id);
                                
                                // Update group last_id
                                if entry.id > group.last_id {
                                    group.last_id = entry.id;
                                }
                            }
                        }
                    }
                } else {
                    // History logic: read from PEL
                    if let Some(group) = stream.groups.get(&group_name) {
                         if let Some(consumer) = group.consumers.get(&consumer_name) {
                             let mut pending_ids: Vec<StreamID> = consumer.pending_ids.iter()
                                 .filter(|&id| *id > start_id)
                                 .cloned()
                                 .collect();
                             pending_ids.sort();
                             
                             let take_count = count.unwrap_or(pending_ids.len());
                             
                             for id in pending_ids.into_iter().take(take_count) {
                                 if let Some(entry) = stream.get(&id) {
                                      entries_to_process.push(entry.clone());
                                 }
                             }
                         }
                    }
                }
                
                if !entries_to_process.is_empty() {
                     let mut stream_res = Vec::new();
                     stream_res.push(Resp::BulkString(Some(key.clone())));
                     let mut entries_arr = Vec::new();

                     for entry in &entries_to_process {
                         let mut entry_arr = Vec::new();
                         entry_arr.push(Resp::BulkString(Some(Bytes::from(entry.id.to_string()))));
                         
                         let mut fields_arr = Vec::new();
                         for (field, value) in &entry.fields {
                             fields_arr.push(Resp::BulkString(Some(field.clone())));
                             fields_arr.push(Resp::BulkString(Some(value.clone())));
                         }
                         entry_arr.push(Resp::Array(Some(fields_arr)));
                         entries_arr.push(Resp::Array(Some(entry_arr)));
                     }
                     stream_res.push(Resp::Array(Some(entries_arr)));
                     result_arr.push(Resp::Array(Some(stream_res)));
                }
            } else {
                return (Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()), None);
            }
        }
    }

    let response = if result_arr.is_empty() {
        Resp::BulkString(None)
    } else {
        Resp::Array(Some(result_arr))
    };

    if needs_log {
        let mut log_args = Vec::with_capacity(args.len());
        let mut i = 0;
        let mut streams_found = false;
        while i < args.len() {
             let arg = if !streams_found {
                 match as_bytes(&args[i]) {
                    Some(b) => {
                        let s = String::from_utf8_lossy(&b).to_string().to_uppercase();
                        if s == "STREAMS" {
                            streams_found = true;
                        }
                        Some(s)
                    },
                    None => None,
                }
             } else {
                 None
             };

             if !streams_found && arg.as_deref() == Some("BLOCK") {
                 i += 2; // Skip BLOCK and its arg
             } else {
                 log_args.push(args[i].clone());
                 i += 1;
             }
        }
        (response, Some(Resp::Array(Some(log_args))))
    } else {
        (response, None)
    }
}

pub fn xack(args: &[Resp], db: &Db) -> (Resp, Option<Resp>) {
    // XACK key group id [id ...]
    if args.len() < 4 {
        return (Resp::Error("ERR wrong number of arguments for 'xack' command".to_string()), None);
    }

    let key = match as_bytes(&args[1]) {
        Some(b) => b,
        None => return (Resp::Error("ERR invalid key".to_string()), None),
    };

    let group_name = match as_bytes(&args[2]) {
        Some(b) => String::from_utf8_lossy(&b).to_string(),
        None => return (Resp::Error("ERR invalid group name".to_string()), None),
    };

    let mut acked = 0;

    if let Some(mut entry) = db.get_mut(&key) {
        if let Value::Stream(stream) = &mut entry.value {
            if let Some(group) = stream.groups.get_mut(&group_name) {
                for i in 3..args.len() {
                     let id_str = match as_bytes(&args[i]) {
                        Some(b) => String::from_utf8_lossy(&b).to_string(),
                        None => continue,
                    };
                    if let Ok(id) = StreamID::from_str(&id_str) {
                        if let Some(pe) = group.pel.remove(&id) {
                            acked += 1;
                            if let Some(consumer) = group.consumers.get_mut(&pe.owner) {
                                consumer.pending_ids.remove(&id);
                            }
                        }
                    }
                }
            } else {
                 return (Resp::Integer(0), None);
            }
        } else {
            return (Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()), None);
        }
    } else {
        return (Resp::Integer(0), None);
    }

    // Log command
    let mut log_args = Vec::with_capacity(args.len());
    for arg in args {
        log_args.push(arg.clone());
    }

    (Resp::Integer(acked), Some(Resp::Array(Some(log_args))))
}
