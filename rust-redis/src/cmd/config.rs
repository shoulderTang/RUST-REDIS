use crate::aof::AppendFsync;
use crate::conf::Config;
use crate::resp::Resp;
use bytes::Bytes;

pub fn config(items: &[Resp], cfg: &Config) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'config' command".to_string());
    }

    let subcommand = match items.get(1) {
        Some(Resp::BulkString(Some(b))) => String::from_utf8_lossy(b).to_uppercase(),
        Some(Resp::SimpleString(b)) => String::from_utf8_lossy(b).to_uppercase(),
        _ => return Resp::Error("ERR syntax error".to_string()),
    };

    if subcommand != "GET" {
        // For now only support GET
        return Resp::Error("ERR unsupported CONFIG subcommand".to_string());
    }

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

    // We can add more configs here as needed
    let appendfsync_str = match cfg.appendfsync {
        AppendFsync::Always => "always",
        AppendFsync::EverySec => "everysec",
        AppendFsync::No => "no",
    };

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
