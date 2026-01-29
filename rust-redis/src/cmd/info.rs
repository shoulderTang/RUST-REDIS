use crate::cmd::ServerContext;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::atomic::Ordering;

pub fn info(items: &[Resp], ctx: &ServerContext) -> Resp {
    let section = if items.len() > 1 {
        match items.get(1) {
            Some(Resp::BulkString(Some(b))) => String::from_utf8_lossy(b).to_lowercase(),
            Some(Resp::SimpleString(b)) => String::from_utf8_lossy(b).to_lowercase(),
            _ => "default".to_string(),
        }
    } else {
        "default".to_string()
    };

    let mut info = String::new();

    if section == "default" || section == "all" || section == "server" {
        info.push_str(&get_server_info(ctx));
    }

    if section == "default" || section == "all" || section == "clients" {
        if !info.is_empty() {
            info.push_str("\r\n");
        }
        info.push_str(&get_clients_info(ctx));
    }

    if section == "default" || section == "all" || section == "memory" {
        if !info.is_empty() {
            info.push_str("\r\n");
        }
        info.push_str(&get_memory_info(ctx));
    }

    if section == "default" || section == "all" || section == "keyspace" {
        if !info.is_empty() {
            info.push_str("\r\n");
        }
        info.push_str(&get_keyspace_info(ctx));
    }

    Resp::BulkString(Some(Bytes::from(info)))
}

fn get_server_info(_ctx: &ServerContext) -> String {
    let mut s = String::new();
    s.push_str("# Server\r\n");
    s.push_str("redis_version:6.2.5\r\n");
    s.push_str(&format!("os:{}\r\n", std::env::consts::OS));
    s.push_str(&format!("process_id:{}\r\n", std::process::id()));
    s.push_str(&format!("tcp_port:{}\r\n", _ctx.config.port));
    if let Some(config_file) = &_ctx.config.config_file {
        s.push_str(&format!("config_file:{}\r\n", config_file));
    } else {
        s.push_str("config_file:\r\n");
    }
    s
}

fn get_clients_info(ctx: &ServerContext) -> String {
    let mut s = String::new();
    s.push_str("# Clients\r\n");
    let connected = ctx.client_count.load(Ordering::Relaxed);
    s.push_str(&format!("connected_clients:{}\r\n", connected));
    
    let blocked = ctx.blocked_client_count.load(Ordering::Relaxed);
    s.push_str(&format!("blocked_clients:{}\r\n", blocked));
    
    s.push_str(&format!("maxclients:{}\r\n", ctx.config.maxclients));
    s
}

fn get_memory_info(ctx: &ServerContext) -> String {
    let (rss, external_peak) = get_memory_usage();
    let used_memory = rss; // Approximation
    // Update and read persistent peak from context
    let stored_peak = ctx.mem_peak_rss.load(Ordering::Relaxed);
    //for test //stored_peak = 89999999;
    let mut new_peak = stored_peak.max(external_peak).max(used_memory);
    if new_peak > stored_peak {
        ctx.mem_peak_rss.store(new_peak, Ordering::Relaxed);
    } else {
        new_peak = stored_peak;
    }

    // Lua memory
    let lua_mem = if let Ok(lua) = ctx.script_manager.lua.lock() {
        lua.used_memory() as u64
    } else {
        0
    };

    let mut s = String::new();
    s.push_str("# Memory\r\n");
    s.push_str(&format!("used_memory:{}\r\n", used_memory));
    s.push_str(&format!("used_memory_human:{}\r\n", bytes_to_human(used_memory)));
    s.push_str(&format!("used_memory_rss:{}\r\n", rss));
    s.push_str(&format!("used_memory_rss_human:{}\r\n", bytes_to_human(rss)));
    s.push_str(&format!("used_memory_peak:{}\r\n", new_peak));
    s.push_str(&format!("used_memory_peak_human:{}\r\n", bytes_to_human(new_peak)));
    s.push_str(&format!("used_memory_lua:{}\r\n", lua_mem));
    s.push_str(&format!("used_memory_lua_human:{}\r\n", bytes_to_human(lua_mem)));
    let maxmemory = ctx.maxmemory.load(Ordering::Relaxed);
    s.push_str(&format!("maxmemory:{}\r\n", maxmemory));
    s.push_str(&format!("maxmemory_human:{}\r\n", bytes_to_human(maxmemory)));
    s.push_str("maxmemory_policy:noeviction\r\n");
    //s.push_str("mem_fragmentation_ratio:1.00\r\n");
    //s.push_str("mem_allocator:libc\r\n");
    s
}

fn get_memory_usage() -> (u64, u64) {
    let mut current_rss = 0;
    //let mut peak_rss = 0;

    if let Some(usage) = memory_stats::memory_stats() {
        current_rss = usage.physical_mem as u64;
    }

    // #[cfg(unix)]
    // {
    //     use std::mem;
    //     unsafe {
    //         let mut rusage: libc::rusage = mem::zeroed();
    //         if libc::getrusage(libc::RUSAGE_SELF, &mut rusage) == 0 {
    //             let maxrss = rusage.ru_maxrss as u64;
    //             #[cfg(target_os = "macos")]
    //             {
    //                 peak_rss = maxrss;
    //             }
    //             #[cfg(not(target_os = "macos"))]
    //             {
    //                 peak_rss = maxrss * 1024;
    //             }
    //         }
    //     }
    // }

    // Fallback: if peak is 0 (e.g. Windows or getrusage failed), assume peak >= current
    // if pre_peak_rss < current_rss {
    //     pre_peak_rss = current_rss;
    // }

    (current_rss, current_rss)
}

fn bytes_to_human(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    
    if bytes >= GB {
        format!("{:.2}G", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2}M", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2}K", bytes as f64 / KB as f64)
    } else {
        format!("{}B", bytes)
    }
}

fn get_keyspace_info(ctx: &ServerContext) -> String {
    let mut s = String::new();
    s.push_str("# Keyspace\r\n");
    for (i, db) in ctx.databases.iter().enumerate() {
        let keys = db.len();
        if keys > 0 {
            let mut expires = 0;
            let mut total_ttl = 0;
            let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("Time went backwards").as_millis() as u64;

            for entry in db.iter() {
                if let Some(expires_at) = entry.value().expires_at {
                    expires += 1;
                    if expires_at > now {
                        total_ttl += expires_at - now;
                    }
                }
            }
            let avg_ttl = if expires > 0 { total_ttl / expires } else { 0 };
            s.push_str(&format!("db{}:keys={},expires={},avg_ttl={}\r\n", i, keys, expires, avg_ttl));
        }
    }
    s
}
