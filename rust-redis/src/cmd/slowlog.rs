use crate::resp::Resp;
use super::ServerContext;
use bytes::Bytes;
use std::sync::atomic::Ordering;

pub async fn slowlog(items: &[Resp], server_ctx: &ServerContext) -> (Resp, Option<Resp>) {
    if items.len() < 2 {
        return (Resp::Error("ERR wrong number of arguments for 'SLOWLOG' command".to_string()), None);
    }
    let sub = match &items[1] {
        Resp::BulkString(Some(b)) | Resp::SimpleString(b) => {
            String::from_utf8_lossy(&b[..]).to_uppercase()
        }
        _ => return (Resp::Error("ERR subcommand must be a string".to_string()), None),
    };
    match sub.as_str() {
        "GET" => {
            let count = if items.len() >= 3 {
                match &items[2] {
                    Resp::BulkString(Some(b)) | Resp::SimpleString(b) => {
                        if let Ok(s) = std::str::from_utf8(&b[..]) {
                            s.parse::<usize>().unwrap_or(10_000)
                        } else {
                            10_000
                        }
                    }
                    _ => 10_000,
                }
            } else {
                10_000
            };
            let log = server_ctx.slowlog.lock().await;
            let mut resp_entries = Vec::new();
            for entry in log.iter().take(count) {
                let mut args_resp = Vec::new();
                for a in &entry.args {
                    args_resp.push(Resp::BulkString(Some(a.clone())));
                }
                // Format per Redis: [id, timestamp, micros, [args], client_addr, client_name]
                resp_entries.push(Resp::Array(Some(vec![
                    Resp::Integer(entry.id as i64),
                    Resp::Integer(entry.timestamp),
                    Resp::Integer(entry.microseconds),
                    Resp::Array(Some(args_resp)),
                    Resp::BulkString(Some(Bytes::from(entry.client_addr.clone()))),
                    Resp::BulkString(Some(Bytes::from(entry.client_name.clone()))),
                ])));
            }
            (Resp::Array(Some(resp_entries)), None)
        }
        "LEN" => {
            let log = server_ctx.slowlog.lock().await;
            (Resp::Integer(log.len() as i64), None)
        }
        "RESET" => {
            let mut log = server_ctx.slowlog.lock().await;
            log.clear();
            (Resp::SimpleString(Bytes::from("OK")), None)
        }
        _ => (Resp::Error("ERR unknown SLOWLOG subcommand".to_string()), None),
    }
}
