use std::sync::Arc;
use tokio::sync::Mutex;

use crate::db::Db;
use crate::resp::{as_bytes, Resp};

pub async fn process_frame(
    frame: Resp,
    db: &Arc<Mutex<Db>>,
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
            let cmd = String::from_utf8_lossy(cmd_raw).to_uppercase();
            match cmd.as_str() {
                "PING" => {
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
                "SET" => {
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
                    let guard = &mut *db.lock().await;
                    guard.insert(key, val_bytes.to_vec());
                    Resp::SimpleString("OK".to_string())
                }
                "GET" => {
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
                    let guard = &mut *db.lock().await;
                    match guard.get_clone(&key) {
                        Some(v) => Resp::BulkString(Some(v)),
                        None => Resp::BulkString(None),
                    }
                }
                _ => Resp::Error("ERR unknown command".to_string()),
            }
        }
        _ => Resp::Error("ERR protocol error: expected array".to_string()),
    }
}
