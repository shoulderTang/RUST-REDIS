use crate::aof::AppendFsync;
use crate::cmd::ServerContext;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::atomic::Ordering;

pub async fn config(items: &[Resp], ctx: &ServerContext) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'config' command".to_string());
    }

    let subcommand = match items.get(1) {
        Some(Resp::BulkString(Some(b))) => String::from_utf8_lossy(b).to_uppercase(),
        Some(Resp::SimpleString(b)) => String::from_utf8_lossy(b).to_uppercase(),
        _ => return Resp::Error("ERR syntax error".to_string()),
    };

    match subcommand.as_str() {
        "GET" => config_get(items, ctx).await,
        "SET" => config_set(items, ctx).await,
        _ => Resp::Error(format!("ERR unknown subcommand '{}'. Try GET, SET, HELP.", subcommand)),
    }
}

async fn config_get(items: &[Resp], ctx: &ServerContext) -> Resp {
    let parameter = match items.get(2) {
        Some(Resp::BulkString(Some(b))) => String::from_utf8_lossy(b).to_string(),
        Some(Resp::SimpleString(b)) => String::from_utf8_lossy(b).to_string(),
        _ => return Resp::Error("ERR syntax error".to_string()),
    };

    let mut response = Vec::new();
    let param_lower = parameter.to_lowercase();

    // Helper to add pair
    let mut add_pair = |k: &str, v: &str| {
        response.push(Resp::BulkString(Some(Bytes::from(k.to_string()))));
        response.push(Resp::BulkString(Some(Bytes::from(v.to_string()))));
    };

    let cfg = &ctx.config;

    let appendfsync_str = match cfg.appendfsync {
        AppendFsync::Always => "always",
        AppendFsync::EverySec => "everysec",
        AppendFsync::No => "no",
    };

    let slowlog_threshold = ctx.slowlog_threshold_us.load(Ordering::Relaxed);
    let slowlog_max_len = ctx.slowlog_max_len.load(Ordering::Relaxed);
    let maxmemory = ctx.maxmemory.load(Ordering::Relaxed);

    let configs = vec![
        //("save", "3600 1 300 100 60 10000".to_string()),
        (
            "appendonly",
            if cfg.appendonly {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        ("appendfilename", cfg.appendfilename.clone()),
        ("appendfsync", appendfsync_str.to_string()),
        ("bind", cfg.bind.clone()),
        ("port", cfg.port.to_string()),
        ("databases", cfg.databases.to_string()),
        ("slowlog-log-slower-than", slowlog_threshold.to_string()),
        ("slowlog-max-len", slowlog_max_len.to_string()),
        ("maxmemory", maxmemory.to_string()),
    ];

    if param_lower == "*" {
        for (k, v) in configs {
            add_pair(k, &v);
        }
    } else {
        for (k, v) in configs {
            if k == param_lower {
                add_pair(k, &v);
            }
        }
    }

    Resp::Array(Some(response))
}

async fn config_set(items: &[Resp], ctx: &ServerContext) -> Resp {
    if items.len() != 4 {
        return Resp::Error("ERR wrong number of arguments for 'config set' command".to_string());
    }

    let parameter = match items.get(2) {
        Some(Resp::BulkString(Some(b))) => String::from_utf8_lossy(b).to_string(),
        Some(Resp::SimpleString(b)) => String::from_utf8_lossy(b).to_string(),
        _ => return Resp::Error("ERR syntax error".to_string()),
    };

    let value = match items.get(3) {
        Some(Resp::BulkString(Some(b))) => String::from_utf8_lossy(b).to_string(),
        Some(Resp::SimpleString(b)) => String::from_utf8_lossy(b).to_string(),
        Some(Resp::Integer(i)) => i.to_string(),
        _ => return Resp::Error("ERR syntax error".to_string()),
    };

    let param_lower = parameter.to_lowercase();

    match param_lower.as_str() {
        "slowlog-log-slower-than" => {
            match value.parse::<i64>() {
                Ok(v) => {
                    ctx.slowlog_threshold_us.store(v, Ordering::Relaxed);
                    Resp::SimpleString(Bytes::from("OK"))
                }
                Err(_) => Resp::Error("ERR value is not an integer or out of range".to_string()),
            }
        }
        "slowlog-max-len" => {
            match value.parse::<usize>() {
                Ok(v) => {
                    ctx.slowlog_max_len.store(v, Ordering::Relaxed);
                    // Trim the slowlog queue immediately
                    let mut logq = ctx.slowlog.lock().await;
                    while logq.len() > v {
                        logq.pop_back();
                    }
                    Resp::SimpleString(Bytes::from("OK"))
                }
                Err(_) => Resp::Error("ERR value is not an integer or out of range".to_string()),
            }
        }
        "maxmemory" => {
            // Need to parse with units potentially, but simple integer for now or reuse parse_memory logic?
            // Since parse_memory is in conf module and private or public, I should check.
            // It is private in previous toolcall but I made it public? No, I added it inside conf.rs but didn't make it public.
            // Let's check conf.rs again or just duplicate/move the logic.
            // Actually I should make `parse_memory` public in `conf.rs` to reuse it.
            // For now, I'll try to use a simple parse and if it fails, maybe implement unit parsing here or call a helper.
            // Re-implementing unit parsing here is safer if I can't easily access the other one.
            
            let s = value.to_lowercase();
            let (num, unit) = if s.ends_with("gb") {
                (s.trim_end_matches("gb"), 1024 * 1024 * 1024)
            } else if s.ends_with("mb") {
                (s.trim_end_matches("mb"), 1024 * 1024)
            } else if s.ends_with("kb") {
                (s.trim_end_matches("kb"), 1024)
            } else if s.ends_with("b") {
                (s.trim_end_matches("b"), 1)
            } else {
                (s.as_str(), 1)
            };
            
            match num.parse::<u64>() {
                Ok(n) => {
                    let bytes = n * unit;
                    ctx.maxmemory.store(bytes, Ordering::Relaxed);
                    Resp::SimpleString(Bytes::from("OK"))
                }
                Err(_) => Resp::Error("ERR value is not an integer or out of range".to_string()),
            }
        }
        _ => Resp::Error("ERR Unsupported CONFIG parameter".to_string()),
    }
}
