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

    if section == "default" || section == "all" || section == "replication" {
        if !info.is_empty() {
            info.push_str("\r\n");
        }
        info.push_str(&get_replication_info(ctx));
    }

    if section == "default" || section == "all" || section == "keyspace" {
        if !info.is_empty() {
            info.push_str("\r\n");
        }
        info.push_str(&get_keyspace_info(ctx));
    }

    if section == "default" || section == "all" || section == "cluster" {
        if !info.is_empty() {
            info.push_str("\r\n");
        }
        info.push_str(&get_cluster_info(ctx));
    }

    Resp::BulkString(Some(Bytes::from(info)))
}

pub fn role(_items: &[Resp], ctx: &ServerContext) -> Resp {
    let role = *ctx.repl.replication_role.read().unwrap();
    match role {
        crate::cmd::ReplicationRole::Master => {
            let mut role_info = Vec::new();
            role_info.push(Resp::BulkString(Some(Bytes::from("master"))));
            role_info.push(Resp::Integer(0));
            role_info.push(Resp::Array(Some(Vec::new())));
            Resp::Array(Some(role_info))
        }
        crate::cmd::ReplicationRole::Slave => {
            let mh = ctx
                .repl.master_host
                .read()
                .unwrap()
                .clone()
                .unwrap_or_else(|| "unknown".to_string());
            let mp = ctx.repl.master_port.read().unwrap().unwrap_or(0) as i64;
            let mut role_info = Vec::new();
            role_info.push(Resp::BulkString(Some(Bytes::from("slave"))));
            role_info.push(Resp::BulkString(Some(Bytes::from(mh))));
            role_info.push(Resp::Integer(mp));
            role_info.push(Resp::BulkString(Some(Bytes::from("connect"))));
            role_info.push(Resp::Integer(0));
            Resp::Array(Some(role_info))
        }
    }
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
    let connected = ctx.clients_ctx.client_count.load(Ordering::Relaxed);
    s.push_str(&format!("connected_clients:{}\r\n", connected));

    let blocked = ctx.clients_ctx.blocked_client_count.load(Ordering::Relaxed);
    s.push_str(&format!("blocked_clients:{}\r\n", blocked));

    s.push_str(&format!("maxclients:{}\r\n", ctx.config.maxclients));
    s
}

fn get_memory_info(ctx: &ServerContext) -> String {
    let (rss, external_peak) = get_memory_usage();
    let used_memory = rss; // Approximation
    // Update and read persistent peak from context
    let stored_peak = ctx.mem.mem_peak_rss.load(Ordering::Relaxed);
    //for test //stored_peak = 89999999;
    let mut new_peak = stored_peak.max(external_peak).max(used_memory);
    if new_peak > stored_peak {
        ctx.mem.mem_peak_rss.store(new_peak, Ordering::Relaxed);
    } else {
        new_peak = stored_peak;
    }

    // Lua memory: each EVAL uses a per-call VM, so no shared instance to query.
    let lua_mem = 0u64;

    let mut s = String::new();
    s.push_str("# Memory\r\n");
    s.push_str(&format!("used_memory:{}\r\n", used_memory));
    s.push_str(&format!(
        "used_memory_human:{}\r\n",
        bytes_to_human(used_memory)
    ));
    s.push_str(&format!("used_memory_rss:{}\r\n", rss));
    s.push_str(&format!(
        "used_memory_rss_human:{}\r\n",
        bytes_to_human(rss)
    ));
    s.push_str(&format!("used_memory_peak:{}\r\n", new_peak));
    s.push_str(&format!(
        "used_memory_peak_human:{}\r\n",
        bytes_to_human(new_peak)
    ));
    s.push_str(&format!("used_memory_lua:{}\r\n", lua_mem));
    s.push_str(&format!(
        "used_memory_lua_human:{}\r\n",
        bytes_to_human(lua_mem)
    ));
    let maxmemory = ctx.mem.maxmemory.load(Ordering::Relaxed);
    s.push_str(&format!("maxmemory:{}\r\n", maxmemory));
    s.push_str(&format!(
        "maxmemory_human:{}\r\n",
        bytes_to_human(maxmemory)
    ));
    let policy = *ctx.mem.maxmemory_policy.read().unwrap();
    s.push_str(&format!("maxmemory_policy:{}\r\n", policy.as_str()));
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

fn get_replication_info(ctx: &ServerContext) -> String {
    let mut s = String::new();
    s.push_str("# Replication\r\n");
    let role = *ctx.repl.replication_role.read().unwrap();
    match role {
        crate::cmd::ReplicationRole::Master => {
            s.push_str("role:master\r\n");
            let connected_slaves = ctx.repl.replicas.len();
            s.push_str(&format!("connected_slaves:{}\r\n", connected_slaves));
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as u64;
            let mut idx = 0;
            for entry in ctx.repl.replicas.iter() {
                let id = *entry.key();
                let addr = if let Some(ci) = ctx.clients_ctx.clients.get(&id) {
                    ci.addr.clone()
                } else {
                    String::from("unknown:0")
                };
                let mut ip = String::from("unknown");
                let mut port: u16 = 0;
                if let Some((host, port_s)) = addr.rsplit_once(':') {
                    ip = host.to_string();
                    port = port_s.parse::<u16>().unwrap_or(0);
                }
                if let Some(p) = ctx.repl.replica_listening_port.get(&id) {
                    port = *p.value();
                }
                let offset = if let Some(v) = ctx.repl.replica_ack.get(&id) {
                    *v.value()
                } else {
                    0
                };
                let ack_time = if let Some(t) = ctx.repl.replica_ack_time.get(&id) {
                    *t.value()
                } else {
                    now
                };
                let lag = now.saturating_sub(ack_time);
                s.push_str(&format!(
                    "slave{}:ip={},port={},state=online,offset={},lag={}\r\n",
                    idx, ip, port, offset, lag
                ));
                idx += 1;
            }
            let master_offset = ctx.repl.repl_offset.load(Ordering::Relaxed);
            let repl_backlog_size = ctx.repl.repl_backlog_size.load(Ordering::Relaxed) as u64;
            let (first_offset, histlen) = {
                // Use try_lock: INFO is best-effort; skip if briefly contended.
                if let Ok(q) = ctx.repl.repl_backlog.try_lock() {
                    if let Some((off, _)) = q.front() {
                        let first = *off;
                        let hist = master_offset.saturating_sub(first).saturating_add(1);
                        (first, hist)
                    } else {
                        (0, 0)
                    }
                } else {
                    (0, 0)
                }
            };
            s.push_str(&format!("master_replid:{}\r\n", ctx.repl.run_id.read().unwrap()));
            s.push_str(&format!(
                "master_replid2:{}\r\n",
                ctx.repl.replid2.read().unwrap()
            ));
            s.push_str(&format!(
                "master_repl_offset:{}\r\n",
                ctx.repl.repl_offset.load(std::sync::atomic::Ordering::Relaxed)
            ));
            s.push_str(&format!(
                "second_repl_offset:{}\r\n",
                ctx.repl.second_repl_offset
                    .load(std::sync::atomic::Ordering::Relaxed)
            ));
            let backlog_active = if repl_backlog_size > 0 { 1 } else { 0 };
            s.push_str(&format!("repl_backlog_active:{}\r\n", backlog_active));
            s.push_str(&format!("repl_backlog_size:{}\r\n", repl_backlog_size));
            s.push_str(&format!(
                "repl_backlog_first_byte_offset:{}\r\n",
                first_offset
            ));
            s.push_str(&format!("repl_backlog_histlen:{}\r\n", histlen));
        }
        crate::cmd::ReplicationRole::Slave => {
            s.push_str("role:slave\r\n");
            let mh = ctx
                .repl.master_host
                .read()
                .unwrap()
                .clone()
                .unwrap_or_else(|| "unknown".to_string());
            let mp = ctx.repl.master_port.read().unwrap().unwrap_or(0);
            s.push_str(&format!("master_host:{}\r\n", mh));
            s.push_str(&format!("master_port:{}\r\n", mp));
            let status = if ctx.repl.master_link_established.load(Ordering::Relaxed) {
                "up"
            } else {
                "down"
            };
            s.push_str(&format!("master_link_status:{}\r\n", status));
            s.push_str("master_last_io_seconds_ago:0\r\n");
            s.push_str("master_sync_in_progress:0\r\n");
            s.push_str("slave_read_only:1\r\n");
            let offset = ctx.repl.repl_offset.load(Ordering::Relaxed);
            s.push_str(&format!("slave_repl_offset:{}\r\n", offset));
        }
    }
    s
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
    for (i, db_lock) in ctx.databases.iter().enumerate() {
        let db = db_lock.read().unwrap();
        let keys = db.len();
        if keys > 0 {
            let mut expires = 0;
            let mut total_ttl = 0;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis() as u64;

            for r in db.iter() {
                if let Some(expires_at) = r.value().expires_at {
                    expires += 1;
                    if expires_at > now {
                        total_ttl += expires_at - now;
                    }
                }
            }
            let avg_ttl = if expires > 0 { total_ttl / expires } else { 0 };
            s.push_str(&format!(
                "db{}:keys={},expires={},avg_ttl={}\r\n",
                i, keys, expires, avg_ttl
            ));
        }
    }
    s
}

fn get_cluster_info(ctx: &ServerContext) -> String {
    let mut s = String::new();
    s.push_str("# Cluster\r\n");
    s.push_str(&format!(
        "cluster_enabled:{}\r\n",
        if ctx.config.cluster_enabled { "1" } else { "0" }
    ));
    if ctx.config.cluster_enabled {
        if let Ok(cluster) = ctx.cluster_ctx.state.read() {
            let (assigned, full) = cluster.coverage_ok();
            let cluster_state = if full { "ok" } else { "fail" };
            s.push_str(&format!("cluster_state:{}\r\n", cluster_state));
            s.push_str(&format!(
                "cluster_current_epoch:{}\r\n",
                cluster.current_epoch
            ));
            s.push_str(&format!("cluster_slots_assigned:{}\r\n", assigned));
            s.push_str(&format!("cluster_slots_ok:{}\r\n", assigned));
            s.push_str("cluster_slots_pfail:0\r\n");
            s.push_str("cluster_slots_fail:0\r\n");
            let known_nodes = cluster.nodes.len();
            s.push_str(&format!("cluster_known_nodes:{}\r\n", known_nodes));
            let mut masters = 0usize;
            for n in cluster.nodes.values() {
                if n.role == crate::cluster::NodeRole::Master {
                    masters += 1;
                }
            }
            s.push_str(&format!("cluster_size:{}\r\n", masters));
            let my_epoch = cluster
                .nodes
                .get(&cluster.myself)
                .map(|n| n.epoch)
                .unwrap_or(0);
            s.push_str(&format!("cluster_my_epoch:{}\r\n", my_epoch));
        } else {
            s.push_str("cluster_state:fail\r\n");
        }
    }
    s
}
