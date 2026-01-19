use crate::db::{Db, Value};
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
                    db.insert(key, Value::String(val));
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

                    let resp = match db.get(&key) {
                        Some(v) => match &*v {
                            Value::String(b) => Resp::BulkString(Some(b.clone())),
                        },
                        None => Resp::BulkString(None),
                    };

                    resp
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
