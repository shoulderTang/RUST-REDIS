use crate::db::{Db, Value, Entry};
use crate::resp::{as_bytes, Resp};

pub fn process_frame(
    frame: Resp,
    db: &Db,
) -> Resp {
    match frame {
        Resp::Array(Some(items)) => {
            if items.is_empty() {
                return Resp::Error("ERR empty command".to_string());
            }
            let cmd_raw = match as_bytes(&items[0]) {
                Some(b) => b,
                None => return Resp::Error("ERR invalid command".to_string()),
            };
            match command_name(cmd_raw) {
                Command::Ping => {
                    if items.len() == 1 {
                        Resp::SimpleString(bytes::Bytes::from_static(b"PONG"))
                    } else if items.len() == 2 {
                        match &items[1] {
                            Resp::BulkString(Some(b)) => Resp::BulkString(Some(b.clone())),
                            Resp::SimpleString(s) => Resp::BulkString(Some(s.clone())),
                            _ => Resp::BulkString(None),
                        }
                    } else {
                        Resp::Error("ERR wrong number of arguments for 'PING'".to_string())
                    }
                }
                Command::Set => {
                    if items.len() != 3 {
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
                    db.insert(key, Entry::new(Value::String(val), None));
                    Resp::SimpleString(bytes::Bytes::from_static(b"OK"))
                }
                Command::Get => {
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
                            Resp::BulkString(None)
                        } else {
                            match &entry.value {
                                Value::String(b) => Resp::BulkString(Some(b.clone())),
                            }
                        }
                    } else {
                        Resp::BulkString(None)
                    }
                }
                Command::Expire => {
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
                Command::Ttl => {
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
                Command::Dbsize => {
                    if items.len() != 1 {
                        return Resp::Error("ERR wrong number of arguments for 'DBSIZE'".to_string());
                    }
                    Resp::Integer(db.len() as i64)
                }
                Command::Shutdown => {
                    std::process::exit(0);
                }
                Command::Unknown => Resp::Error("ERR unknown command".to_string()),
            }
        }
        _ => Resp::Error("ERR protocol error: expected array".to_string()),
    }
}

enum Command {
    Ping,
    Set,
    Get,
    Expire,
    Ttl,
    Dbsize,
    Shutdown,
    Unknown,
}

fn command_name(raw: &[u8]) -> Command {
    if equals_ignore_ascii_case(raw, b"PING") {
        Command::Ping
    } else if equals_ignore_ascii_case(raw, b"SET") {
        Command::Set
    } else if equals_ignore_ascii_case(raw, b"GET") {
        Command::Get
    } else if equals_ignore_ascii_case(raw, b"EXPIRE") {
        Command::Expire
    } else if equals_ignore_ascii_case(raw, b"TTL") {
        Command::Ttl
    } else if equals_ignore_ascii_case(raw, b"DBSIZE") {
        Command::Dbsize
    } else if equals_ignore_ascii_case(raw, b"SHUTDOWN") {
        Command::Shutdown
    } else {
        Command::Unknown
    }
}

fn equals_ignore_ascii_case(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    for i in 0..a.len() {
        let ca = a[i];
        let cb = b[i];
        if ca == cb {
            continue;
        }
        if ca.to_ascii_lowercase() != cb.to_ascii_lowercase() {
            return false;
        }
    }
    true
}

#[cfg(test)]
#[path = "cmd_test.rs"]
mod tests;
