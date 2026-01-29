use crate::cmd::ServerContext;
use crate::resp::Resp;
use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn info(args: &[Resp], ctx: &ServerContext) -> Resp {
    let section = if args.len() > 1 {
        match &args[1] {
            Resp::BulkString(Some(s)) => String::from_utf8_lossy(s).to_lowercase(),
            Resp::SimpleString(s) => String::from_utf8_lossy(s).to_lowercase(),
            _ => "default".to_string(),
        }
    } else {
        "default".to_string()
    };

    let mut info = String::new();

    if section == "default" || section == "all" || section == "server" {
        info.push_str(get_server_info(ctx).as_str());
    }

    if section == "default" || section == "all" || section == "clients" {
        info.push_str(get_clients_info(ctx).as_str());
    }

    if section == "default" || section == "all" || section == "keyspace" {
        info.push_str(get_keyspace_info(ctx).as_str());
    }

    // TODO: Add other sections

    Resp::BulkString(Some(Bytes::from(info)))
}

fn get_server_info(ctx: &ServerContext) -> String {
    let mut s = String::new();
    s.push_str("# Server\r\n");
    s.push_str("redis_version:6.2.5\r\n");
    s.push_str("redis_git_sha1:00000000\r\n");
    s.push_str("redis_git_dirty:0\r\n");
    s.push_str("redis_build_id:0000000000000000\r\n");
    s.push_str("redis_mode:standalone\r\n");
    s.push_str(&format!("os:{} {}\r\n", std::env::consts::OS, std::env::consts::ARCH));
    s.push_str(&format!("arch_bits:{}\r\n", std::mem::size_of::<usize>() * 8));
    
    // Detect multiplexing API
    let multiplexing_api = if cfg!(target_os = "linux") {
        "epoll"
    } else if cfg!(target_os = "macos") || cfg!(target_os = "freebsd") || cfg!(target_os = "openbsd") || cfg!(target_os = "netbsd") {
        "kqueue"
    } else if cfg!(target_os = "windows") {
        "iocp"
    } else {
        "unknown"
    };
    s.push_str(&format!("multiplexing_api:{}\r\n", multiplexing_api));
    
    s.push_str("atomicvar_api:atomic-builtin\r\n");
    s.push_str("gcc_version:0.0.0\r\n");
    s.push_str(&format!("process_id:{}\r\n", std::process::id()));
    s.push_str(&format!("run_id:{}\r\n", ctx.run_id));
    s.push_str(&format!("tcp_port:{}\r\n", ctx.config.port));
    
    let uptime = ctx.start_time.elapsed().as_secs();
    let uptime_days = uptime / (24 * 3600);
    s.push_str(&format!("uptime_in_seconds:{}\r\n", uptime));
    s.push_str(&format!("uptime_in_days:{}\r\n", uptime_days));
    
    s.push_str("hz:10\r\n");
    s.push_str("configured_hz:10\r\n");
    s.push_str("lru_clock:0\r\n");
    s.push_str(&format!("executable:{}\r\n", std::env::current_exe().map(|p| p.to_string_lossy().into_owned()).unwrap_or_else(|_| "unknown".to_string())));
    s.push_str(&format!("config_file:{}\r\n", ctx.config.config_file.as_deref().unwrap_or("")));
    s.push_str("io_threads_active:0\r\n");
    s
}

fn get_clients_info(ctx: &ServerContext) -> String {
    let mut s = String::new();
    s.push_str("# Clients\r\n");
    let connected_clients = ctx.client_count.load(std::sync::atomic::Ordering::Relaxed);
    let blocked_clients = ctx.blocked_client_count.load(std::sync::atomic::Ordering::Relaxed);
    s.push_str(&format!("connected_clients:{}\r\n", connected_clients));
    s.push_str("cluster_connections:0\r\n");
    s.push_str(&format!("maxclients:{}\r\n", ctx.config.maxclients));
    s.push_str("client_recent_max_input_buffer:0\r\n");
    s.push_str("client_recent_max_output_buffer:0\r\n");
    s.push_str(&format!("blocked_clients:{}\r\n", blocked_clients));
    s.push_str("tracking_clients:0\r\n");
    s.push_str("clients_in_timeout_table:0\r\n");
    s
}

fn get_keyspace_info(ctx: &ServerContext) -> String {
    let mut s = String::new();
    s.push_str("# Keyspace\r\n");
    
    for (i, db) in ctx.databases.iter().enumerate() {
        if db.is_empty() {
            continue;
        }
        
        let mut keys = 0;
        let mut expires = 0;
        let mut ttl_sum = 0;
        
        for entry in db.iter() {
            if entry.value().is_expired() {
                continue;
            }
            keys += 1;
            if let Some(expires_at) = entry.value().expires_at {
                expires += 1;
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_millis() as u64;
                if expires_at > now {
                    ttl_sum += expires_at - now;
                }
            }
        }
        
        if keys == 0 {
            continue;
        }
        
        let avg_ttl = if expires > 0 { ttl_sum / expires } else { 0 };
        
        s.push_str(&format!("db{}:keys={},expires={},avg_ttl={}\r\n", i, keys, expires, avg_ttl));
    }
    s
}
