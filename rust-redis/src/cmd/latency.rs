use crate::resp::Resp;
use crate::cmd::{ServerContext, LatencyEvent};
use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn latency(items: &[Resp], server_ctx: &ServerContext) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'latency' command".to_string());
    }

    let subcommand = match &items[1] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_uppercase(),
        Resp::SimpleString(s) => String::from_utf8_lossy(s).to_uppercase(),
        _ => return Resp::Error("ERR syntax error".to_string()),
    };

    match subcommand.as_str() {
        "LATEST" => {
            let mut results = Vec::new();
            for entry in server_ctx.latency_events.iter() {
                let event_name = entry.key();
                let events = entry.value();
                if let Some(latest) = events.back() {
                    let mut event_info = Vec::new();
                    event_info.push(Resp::BulkString(Some(Bytes::from(event_name.clone()))));
                    event_info.push(Resp::Integer(latest.timestamp as i64));
                    event_info.push(Resp::Integer(latest.duration as i64));
                    // Max duration in history - for simplicity using latest as max for now or scanning
                    let max = events.iter().map(|e| e.duration).max().unwrap_or(0);
                    event_info.push(Resp::Integer(max as i64));
                    results.push(Resp::Array(Some(event_info)));
                }
            }
            Resp::Array(Some(results))
        }
        "HISTORY" => {
            if items.len() < 3 {
                return Resp::Error("ERR wrong number of arguments for 'latency history' command".to_string());
            }
            let event_name = match &items[2] {
                Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
                Resp::SimpleString(s) => String::from_utf8_lossy(s).to_string(),
                _ => return Resp::Error("ERR syntax error".to_string()),
            };

            if let Some(events) = server_ctx.latency_events.get(&event_name) {
                let mut results = Vec::new();
                for event in events.iter() {
                    let mut event_info = Vec::new();
                    event_info.push(Resp::Integer(event.timestamp as i64));
                    event_info.push(Resp::Integer(event.duration as i64));
                    results.push(Resp::Array(Some(event_info)));
                }
                Resp::Array(Some(results))
            } else {
                Resp::Array(Some(Vec::new()))
            }
        }
        "GRAPH" => {
            if items.len() < 3 {
                return Resp::Error("ERR wrong number of arguments for 'latency graph' command".to_string());
            }
            // Simple ASCII graph implementation or just a placeholder
            Resp::BulkString(Some(Bytes::from("ASCII graph not implemented yet")))
        }
        "RESET" => {
            if items.len() == 2 {
                server_ctx.latency_events.clear();
            } else {
                for i in 2..items.len() {
                    let name = match &items[i] {
                        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
                        Resp::SimpleString(s) => String::from_utf8_lossy(s).to_string(),
                        _ => continue,
                    };
                    server_ctx.latency_events.remove(&name);
                }
            }
            Resp::SimpleString(Bytes::from("OK"))
        }
        "HELP" => {
            let help = vec![
                "LATENCY LATEST - Return the latest latency samples for all events.",
                "LATENCY HISTORY <event> - Return historical latency samples for <event>.",
                "LATENCY GRAPH <event> - Render an ASCII graph of latency for <event>.",
                "LATENCY RESET [<event> ...] - Reset latency data for one or more events.",
                "LATENCY HELP - Prints this help message.",
            ];
            let mut res = Vec::new();
            for line in help {
                res.push(Resp::SimpleString(Bytes::from(line)));
            }
            Resp::Array(Some(res))
        }
        _ => Resp::Error(format!("ERR unknown subcommand for 'LATENCY {}'", subcommand)),
    }
}

pub fn record_latency(server_ctx: &ServerContext, event: &str, duration_ms: u64) {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let mut events = server_ctx.latency_events.entry(event.to_string()).or_insert_with(std::collections::VecDeque::new);
    events.push_back(LatencyEvent {
        timestamp: now,
        duration: duration_ms,
    });
    // Keep only last 160 samples (Redis default)
    if events.len() > 160 {
        events.pop_front();
    }
}
