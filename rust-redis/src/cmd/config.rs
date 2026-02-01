use crate::aof::AppendFsync;
use crate::cmd::ServerContext;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::atomic::Ordering;

pub async fn config(items: &[Resp], ctx: &ServerContext) -> Resp {
    if items.len() < 2 {
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
        "REWRITE" => config_rewrite(items, ctx).await,
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
    let maxmemory_policy = *ctx.maxmemory_policy.read().unwrap();
    let maxmemory_samples = ctx.maxmemory_samples.load(Ordering::Relaxed);
    let notify_flags = ctx.notify_keyspace_events.load(Ordering::Relaxed);
    let notify_str = crate::cmd::notify::flags_to_string(notify_flags);
    let rdbcompression = ctx.rdbcompression.load(Ordering::Relaxed);
    let rdbchecksum = ctx.rdbchecksum.load(Ordering::Relaxed);
    let stop_writes_on_bgsave_error = ctx.stop_writes_on_bgsave_error.load(Ordering::Relaxed);
    let save_params = ctx.save_params.read().unwrap();
    let save_str = save_params.iter().map(|(s, c)| format!("{} {}", s, c)).collect::<Vec<_>>().join(" ");

    let configs = vec![
        ("save", save_str),
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
        ("maxmemory-policy", maxmemory_policy.as_str().to_string()),
        ("maxmemory-samples", maxmemory_samples.to_string()),
        ("notify-keyspace-events", notify_str),
        ("rdbcompression", if rdbcompression { "yes".to_string() } else { "no".to_string() }),
        ("rdbchecksum", if rdbchecksum { "yes".to_string() } else { "no".to_string() }),
        ("stop-writes-on-bgsave-error", if stop_writes_on_bgsave_error { "yes".to_string() } else { "no".to_string() }),
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
        "maxmemory-policy" => {
            if let Some(p) = crate::conf::EvictionPolicy::from_str(&value) {
                let mut policy = ctx.maxmemory_policy.write().unwrap();
                *policy = p;
                Resp::SimpleString(Bytes::from("OK"))
            } else {
                Resp::Error("ERR Invalid maxmemory-policy".to_string())
            }
        }
        "maxmemory-samples" => {
            match value.parse::<usize>() {
                Ok(v) => {
                    ctx.maxmemory_samples.store(v, Ordering::Relaxed);
                    Resp::SimpleString(Bytes::from("OK"))
                }
                Err(_) => Resp::Error("ERR value is not an integer or out of range".to_string()),
            }
        }
        "notify-keyspace-events" => {
            let flags = crate::cmd::notify::parse_notify_flags(&value);
            ctx.notify_keyspace_events.store(flags, Ordering::Relaxed);
            Resp::SimpleString(Bytes::from("OK"))
        }
        "rdbcompression" => {
            ctx.rdbcompression.store(value.eq_ignore_ascii_case("yes"), Ordering::Relaxed);
            Resp::SimpleString(Bytes::from("OK"))
        }
        "rdbchecksum" => {
            ctx.rdbchecksum.store(value.eq_ignore_ascii_case("yes"), Ordering::Relaxed);
            Resp::SimpleString(Bytes::from("OK"))
        }
        "stop-writes-on-bgsave-error" => {
            ctx.stop_writes_on_bgsave_error.store(value.eq_ignore_ascii_case("yes"), Ordering::Relaxed);
            Resp::SimpleString(Bytes::from("OK"))
        }
        "save" => {
            let mut new_params = Vec::new();
            if !value.is_empty() {
                let parts: Vec<&str> = value.split_whitespace().collect();
                if parts.len() % 2 != 0 {
                    return Resp::Error("ERR Invalid save parameters".to_string());
                }
                for i in (0..parts.len()).step_by(2) {
                    if let (Ok(s), Ok(c)) = (parts[i].parse::<u64>(), parts[i+1].parse::<u64>()) {
                        new_params.push((s, c));
                    } else {
                        return Resp::Error("ERR Invalid save parameters".to_string());
                    }
                }
            }
            let mut params = ctx.save_params.write().unwrap();
            *params = new_params;
            Resp::SimpleString(Bytes::from("OK"))
        }
        _ => Resp::Error("ERR Unsupported CONFIG parameter".to_string()),
    }
}

async fn config_rewrite(_items: &[Resp], ctx: &ServerContext) -> Resp {
    if let Some(config_file) = &ctx.config.config_file {
        // Construct config content
        let mut content = String::new();
        let cfg = &ctx.config;
        
        // Helper to append config line
        let mut append_cfg = |k: &str, v: &str| {
            content.push_str(&format!("{} {}\n", k, v));
        };

        // port
        append_cfg("port", &cfg.port.to_string());
        // bind
        append_cfg("bind", &cfg.bind);
        // databases
        append_cfg("databases", &cfg.databases.to_string());
        // maxmemory
        append_cfg("maxmemory", &ctx.maxmemory.load(Ordering::Relaxed).to_string());
        // maxmemory-policy
        append_cfg("maxmemory-policy", ctx.maxmemory_policy.read().unwrap().as_str());
        // maxmemory-samples
        append_cfg("maxmemory-samples", &ctx.maxmemory_samples.load(Ordering::Relaxed).to_string());
        // notify-keyspace-events
        let notify_flags = ctx.notify_keyspace_events.load(Ordering::Relaxed);
        append_cfg("notify-keyspace-events", &crate::cmd::notify::flags_to_string(notify_flags));
        // rdbcompression
        append_cfg("rdbcompression", if ctx.rdbcompression.load(Ordering::Relaxed) { "yes" } else { "no" });
        // rdbchecksum
        append_cfg("rdbchecksum", if ctx.rdbchecksum.load(Ordering::Relaxed) { "yes" } else { "no" });
        // stop-writes-on-bgsave-error
        append_cfg("stop-writes-on-bgsave-error", if ctx.stop_writes_on_bgsave_error.load(Ordering::Relaxed) { "yes" } else { "no" });
        // save
        {
            let params = ctx.save_params.read().unwrap();
            for (s, c) in params.iter() {
                append_cfg("save", &format!("{} {}", s, c));
            }
            if params.is_empty() {
                append_cfg("save", "\"\"");
            }
        }
        // appendonly
        append_cfg("appendonly", if cfg.appendonly { "yes" } else { "no" });
        // appendfilename
        append_cfg("appendfilename", &cfg.appendfilename);
        // appendfsync
        let appendfsync_str = match cfg.appendfsync {
            AppendFsync::Always => "always",
            AppendFsync::EverySec => "everysec",
            AppendFsync::No => "no",
        };
        append_cfg("appendfsync", appendfsync_str);
        
        // slowlog
        append_cfg("slowlog-log-slower-than", &ctx.slowlog_threshold_us.load(Ordering::Relaxed).to_string());
        append_cfg("slowlog-max-len", &ctx.slowlog_max_len.load(Ordering::Relaxed).to_string());
        
        // maxclients
        append_cfg("maxclients", &cfg.maxclients.to_string());

        // Write to file
        match std::fs::write(config_file, content) {
            Ok(_) => Resp::SimpleString(Bytes::from("OK")),
            Err(e) => Resp::Error(format!("ERR Rewriting config file: {}", e)),
        }
    } else {
        Resp::Error("ERR The server is running without a config file".to_string())
    }
}
