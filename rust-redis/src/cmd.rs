use crate::db::Db;
use crate::resp::{as_bytes, Resp};

pub fn process_frame(
    frame: Resp,
    db: &mut Db,
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
                        Resp::SimpleString("PONG".to_string())
                    } else if items.len() == 2 {
                        match as_bytes(&items[1]) {
                            Some(b) => Resp::BulkString(Some(b.to_vec())),
                            None => Resp::BulkString(None),
                        }
                    } else {
                        Resp::Error("ERR wrong number of arguments for 'PING'".to_string())
                    }
                }
                Command::Set => {
                    if items.len() != 3 {
                        return Resp::Error("ERR wrong number of arguments for 'SET'".to_string());
                    }
                    let key_bytes = match as_bytes(&items[1]) {
                        Some(b) => b,
                        None => return Resp::Error("ERR invalid key".to_string()),
                    };
                    let val_bytes = match as_bytes(&items[2]) {
                        Some(b) => b,
                        None => return Resp::BulkString(None),
                    };
                    let key = match std::str::from_utf8(key_bytes) {
                        Ok(s) => s.to_string(),
                        Err(_) => return Resp::Error("ERR key must be UTF-8".to_string()),
                    };
                    db.insert(key, bytes::Bytes::copy_from_slice(val_bytes));
                    Resp::SimpleString("OK".to_string())
                }
                Command::Get => {
                    if items.len() != 2 {
                        return Resp::Error("ERR wrong number of arguments for 'GET'".to_string());
                    }
                    let key_bytes = match as_bytes(&items[1]) {
                        Some(b) => b,
                        None => return Resp::Error("ERR invalid key".to_string()),
                    };
                    let key = match std::str::from_utf8(key_bytes) {
                        Ok(s) => s.to_string(),
                        Err(_) => return Resp::Error("ERR key must be UTF-8".to_string()),
                    };
                    match db.get(&key) {
                        Some(v) => Resp::BulkString(Some(v.to_vec())),
                        None => Resp::BulkString(None),
                    }
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
