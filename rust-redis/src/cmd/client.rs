use bytes::Bytes;
use crate::resp::Resp;
use crate::cmd::{ConnectionContext, ServerContext};

pub fn client(items: &[Resp], _conn_ctx: &mut ConnectionContext, server_ctx: &ServerContext) -> (Resp, Option<Resp>) {
    if items.len() < 2 {
        return (Resp::Error("ERR wrong number of arguments for 'client' command".to_string()), None);
    }
    let subcmd = match &items[1] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_uppercase(),
        Resp::SimpleString(s) => String::from_utf8_lossy(s).to_uppercase(),
        _ => String::new(),
    };
    match subcmd.as_str() {
        "LIST" => {
            let now = std::time::Instant::now();
            let mut lines = Vec::new();
            for kv in server_ctx.clients.iter() {
                let ci = kv.value();
                let age = now.duration_since(ci.connect_time).as_secs();
                let idle = now.duration_since(ci.last_activity).as_secs();
                let line = format!(
                    "id={} addr={} name={} age={} idle={} flags={} db={} sub={} psub={} cmd={}",
                    ci.id,
                    ci.addr,
                    ci.name,
                    age,
                    idle,
                    ci.flags,
                    ci.db,
                    ci.sub,
                    ci.psub,
                    ci.cmd
                );
                lines.push(line);
            }
            let out = if lines.is_empty() {
                String::new()
            } else {
                let mut s = lines.join("\n");
                s.push_str("\r\n");
                s
            };
            (Resp::BulkString(Some(Bytes::from(out))), None)
        }
        "KILL" => {
            if items.len() < 3 {
                 return (Resp::Error("ERR wrong number of arguments for 'client kill' command".to_string()), None);
            }
            
            let arg2 = match &items[2] {
                Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
                Resp::SimpleString(s) => String::from_utf8_lossy(s).to_string(),
                 _ => return (Resp::Error("ERR syntax error".to_string()), None),
            };
            
            let mut kill_id: Option<u64> = None;
            let mut kill_addr: Option<String> = None;
            let mut is_filter = false;
            let mut skip_me = true; // Default for filter mode is yes, but we are keeping it simple

            // Check if it's the filter syntax: CLIENT KILL [ID id] [ADDR addr] ...
            // Or legacy: CLIENT KILL ip:port
            
            if arg2.eq_ignore_ascii_case("ID") {
                if items.len() != 4 {
                    return (Resp::Error("ERR syntax error".to_string()), None);
                }
                let id_str = match &items[3] {
                    Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
                    Resp::SimpleString(s) => String::from_utf8_lossy(s).to_string(),
                    _ => return (Resp::Error("ERR syntax error".to_string()), None),
                };
                if let Ok(id) = id_str.parse::<u64>() {
                    kill_id = Some(id);
                    is_filter = true;
                } else {
                     return (Resp::Error("ERR value is not an integer or out of range".to_string()), None);
                }
            } else if arg2.eq_ignore_ascii_case("ADDR") {
                 if items.len() != 4 {
                    return (Resp::Error("ERR syntax error".to_string()), None);
                }
                let addr_str = match &items[3] {
                    Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
                    Resp::SimpleString(s) => String::from_utf8_lossy(s).to_string(),
                    _ => return (Resp::Error("ERR syntax error".to_string()), None),
                };
                kill_addr = Some(addr_str);
                is_filter = true;
            } else {
                // Legacy: CLIENT KILL ip:port
                 if items.len() != 3 {
                    return (Resp::Error("ERR syntax error".to_string()), None);
                }
                kill_addr = Some(arg2);
                // is_filter remains false
                skip_me = false; // Legacy kills even self if address matches
            }
            
            let mut killed_count = 0;
            
            if let Some(id) = kill_id {
                if let Some(ci) = server_ctx.clients.get(&id) {
                     // With ID, SKIPME is not relevant unless explicitly set (which we don't support yet)
                     // But usually ID is specific enough.
                     if let Some(tx) = &ci.shutdown_tx {
                         let _ = tx.send(true);
                         killed_count += 1;
                     }
                }
            } else if let Some(addr) = kill_addr {
                for kv in server_ctx.clients.iter() {
                    let ci = kv.value();
                    if ci.addr == addr {
                        // Check skip_me for current connection
                        if is_filter && skip_me && ci.id == _conn_ctx.id {
                            continue;
                        }
                        
                        if let Some(tx) = &ci.shutdown_tx {
                             let _ = tx.send(true);
                             killed_count += 1;
                        }
                    }
                }
            }
            
            if is_filter {
                (Resp::Integer(killed_count), None)
            } else {
                if killed_count > 0 {
                    (Resp::SimpleString(Bytes::from("OK")), None)
                } else {
                    (Resp::Error("ERR No such client".to_string()), None)
                }
            }
        }
        "SETNAME" => {
            if items.len() != 3 {
                return (Resp::Error("ERR wrong number of arguments for 'client setname' command".to_string()), None);
            }
            let name = match &items[2] {
                Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
                Resp::SimpleString(s) => String::from_utf8_lossy(s).to_string(),
                _ => return (Resp::Error("ERR syntax error".to_string()), None),
            };

            // Validate name (no spaces)
            if name.chars().any(char::is_whitespace) {
                 return (Resp::Error("ERR Client names cannot contain spaces, newlines or special characters.".to_string()), None);
            }

            if let Some(mut ci) = server_ctx.clients.get_mut(&_conn_ctx.id) {
                ci.name = name;
            } else {
                 return (Resp::Error("ERR client not found".to_string()), None);
            }
            (Resp::SimpleString(Bytes::from("OK")), None)
        }
        "TRACKING" => {
            if items.len() < 3 {
                return (Resp::Error("ERR wrong number of arguments for 'client tracking' command".to_string()), None);
            }
            let mode = match &items[2] {
                Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_uppercase(),
                Resp::SimpleString(s) => String::from_utf8_lossy(s).to_uppercase(),
                _ => return (Resp::Error("ERR syntax error".to_string()), None),
            };

            match mode.as_str() {
                "ON" => {
                    _conn_ctx.client_tracking = true;
                    // Handle options like REDIRECT, BCAST, PREFIX etc.
                    let mut i = 3;
                    while i < items.len() {
                        let opt = match &items[i] {
                            Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_uppercase(),
                            Resp::SimpleString(s) => String::from_utf8_lossy(s).to_uppercase(),
                            _ => return (Resp::Error("ERR syntax error".to_string()), None),
                        };
                        match opt.as_str() {
                            "REDIRECT" => {
                                if i + 1 >= items.len() {
                                    return (Resp::Error("ERR syntax error".to_string()), None);
                                }
                                let id_str = match &items[i+1] {
                                    Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
                                    Resp::SimpleString(s) => String::from_utf8_lossy(s).to_string(),
                                    _ => return (Resp::Error("ERR syntax error".to_string()), None),
                                };
                                if let Ok(id) = id_str.parse::<i64>() {
                                    _conn_ctx.client_redir_id = id;
                                } else {
                                    return (Resp::Error("ERR value is not an integer or out of range".to_string()), None);
                                }
                                i += 2;
                            }
                            "BCAST" | "PREFIX" | "OPTIN" | "OPTOUT" | "NOLOOP" => {
                                // For now, we only support basic tracking (default mode)
                                // We can implement these more complex modes later if needed.
                                i += 1;
                            }
                            _ => return (Resp::Error(format!("ERR unknown option for 'CLIENT TRACKING ON': {}", opt)), None),
                        }
                    }
                    (Resp::SimpleString(Bytes::from("OK")), None)
                }
                "OFF" => {
                    _conn_ctx.client_tracking = false;
                    _conn_ctx.client_redir_id = -1;
                    // Cleanup tracked keys for this client
                    for mut entry in server_ctx.tracking_clients.iter_mut() {
                        entry.value_mut().remove(&_conn_ctx.id);
                    }
                    (Resp::SimpleString(Bytes::from("OK")), None)
                }
                _ => (Resp::Error("ERR syntax error".to_string()), None),
            }
        }
        "CACHING" => {
            if items.len() != 3 {
                return (Resp::Error("ERR wrong number of arguments for 'client caching' command".to_string()), None);
            }
            let val = match &items[2] {
                Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_uppercase(),
                Resp::SimpleString(s) => String::from_utf8_lossy(s).to_uppercase(),
                _ => return (Resp::Error("ERR syntax error".to_string()), None),
            };
            match val.as_str() {
                "YES" => _conn_ctx.client_caching = true,
                "NO" => _conn_ctx.client_caching = false,
                _ => return (Resp::Error("ERR syntax error".to_string()), None),
            }
            (Resp::SimpleString(Bytes::from("OK")), None)
        }
        "GETREDIR" => {
            (Resp::Integer(_conn_ctx.client_redir_id), None)
        }
        _ => (Resp::Error("ERR Unsupported CLIENT subcommand".to_string()), None),
    }
}
