
#![allow(unexpected_cfgs)]
#![allow(unused_imports)]
#![allow(dead_code)]

use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::io::{AsyncWriteExt, BufReader};
use tracing::{info, warn, error};
use tokio::io::AsyncWriteExt as _;

use crate::resp::Resp;
use crate::sentinel::{SentinelState, start_manual_failover, reset_failover_state};

#[path = "../aof.rs"]
mod aof;
#[path = "../cmd/mod.rs"]
mod cmd;
#[path = "../conf.rs"]
mod conf;
#[path = "../db.rs"]
mod db;
#[path = "../rdb.rs"]
mod rdb;
#[path = "../rax.rs"]
mod rax;
#[path = "../hll.rs"]
mod hll;
#[path = "../stream.rs"]
mod stream;
#[path = "../resp.rs"]
mod resp;
#[path = "../geo.rs"]
mod geo;
#[path = "../acl.rs"]
pub mod acl;

#[path = "../sentinel.rs"]
mod sentinel;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 {
        if args[1] == "-v" || args[1] == "--version" {
            println!("redis-sentinel v0.1.0");
            return;
        }
    }

    let cfg_path = args.get(1);
    let cfg = match conf::load_config(cfg_path.map(|s| s.as_str())) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to load config {:?}, using default: {}", cfg_path, e);
            conf::Config::default()
        }
    };

    // Initialize logging
    if let Some(path) = &cfg.logfile {
        let file_appender = tracing_appender::rolling::never(".", path);
        let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(non_blocking)
            .init();
        run_sentinel(cfg, Some(_guard)).await;
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .init();
        run_sentinel(cfg, None).await;
    }
}

async fn run_sentinel(cfg: conf::Config, _guard: Option<tracing_appender::non_blocking::WorkerGuard>) {
    let addr: String = cfg.address();
    info!("Sentinel starting on {}", addr);

    let sentinel_ip = "127.0.0.1".to_string();
    let sentinel_port = 26379;
    let sentinel_state = sentinel::SentinelState::new(sentinel_ip, sentinel_port);
    sentinel_state.set_config_path(cfg.config_file.clone());
    
    // Initialize monitors from config
    {
        let mut masters = sentinel_state.masters.write().unwrap();
        for (name, ip, port, quorum) in &cfg.sentinel_monitors {
             let mut master = sentinel::MasterInstance::new(name.clone(), ip.clone(), *port, *quorum);
             
             // Apply down-after-milliseconds
             for (n, ms) in &cfg.sentinel_down_after_milliseconds {
                 if n == name {
                     master.down_after_period = *ms;
                 }
             }
             
             // Apply failover-timeout
             for (n, ms) in &cfg.sentinel_failover_timeout {
                 if n == name {
                     master.failover_timeout = *ms;
                 }
             }
             
             // Apply parallel-syncs
             for (n, count) in &cfg.sentinel_parallel_syncs {
                 if n == name {
                     master.parallel_syncs = *count;
                 }
             }

             masters.insert(name.clone(), Arc::new(RwLock::new(master)));
             info!("Monitoring master {} at {}:{} quorum {}", name, ip, port, quorum);
        }
    }

    let listener = TcpListener::bind(&addr).await.unwrap();
    info!("Sentinel listening on {}", addr);

    // Start background monitoring task
    let sentinel_state_clone = sentinel_state.clone();
    tokio::spawn(async move {
        sentinel::sentinel_cron(sentinel_state_clone).await;
    });
    
    loop {
        let (socket, addr) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                error!("Accept error: {}", e);
                continue;
            }
        };
        
        info!("Accepted connection from {}", addr);
        
        let sentinel_state_clone = sentinel_state.clone();
        tokio::spawn(async move {
            let (read_half, mut write_half) = socket.into_split();
            let mut reader = BufReader::new(read_half);

            loop {
                let frame = match resp::read_frame(&mut reader).await {
                    Ok(Some(frame)) => frame,
                    Ok(None) => {
                        info!("Connection closed by peer");
                        break;
                    },
                    Err(e) => {
                        error!("Error reading frame: {}", e);
                        break;
                    }
                };

                // Minimal SUBSCRIBE support for client notifications
                if let Resp::Array(Some(items)) = &frame {
                    if !items.is_empty() {
                        if let Resp::BulkString(Some(cmd_bs)) = &items[0] {
                            let cmd_s = String::from_utf8_lossy(cmd_bs).to_uppercase();
                            if cmd_s == "SUBSCRIBE" && items.len() >= 2 {
                                let channel = match &items[1] {
                                    Resp::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
                                    _ => {
                                        let err_bytes = Resp::Error("ERR invalid channel".to_string()).as_bytes();
                                        let _ = write_half.write_all(&err_bytes).await;
                                        break;
                                    }
                                };
                                // Send subscription confirmation: ["subscribe", channel, 1]
                                let resp = Resp::Array(Some(vec![
                                    Resp::BulkString(Some("subscribe".into())),
                                    Resp::BulkString(Some(channel.clone().into())),
                                    Resp::Integer(1),
                                ]));
                                let ack_bytes = resp.as_bytes();
                                if let Err(e) = write_half.write_all(&ack_bytes).await {
                                    error!("Error writing subscribe ack: {}", e);
                                    break;
                                }
                                // Create receiver and stream messages
                                let tx = sentinel_state_clone.get_or_create_channel(&channel);
                                let mut rx = tx.subscribe();
                                loop {
                                    match rx.recv().await {
                                        Ok(payload) => {
                                            let msg = Resp::Array(Some(vec![
                                                Resp::BulkString(Some("message".into())),
                                                Resp::BulkString(Some(channel.clone().into())),
                                                Resp::BulkString(Some(payload.into())),
                                            ]));
                                            let msg_bytes = msg.as_bytes();
                                            if let Err(e) = write_half.write_all(&msg_bytes).await {
                                                error!("Error writing pubsub message: {}", e);
                                                break;
                                            }
                                        }
                                        Err(_) => break,
                                    }
                                }
                                break;
                            }
                        }
                    }
                }

                let response = process_sentinel_command(frame, &sentinel_state_clone).await;
                if let Err(e) = write_half.write_all(&response).await {
                    error!("Error writing response: {}", e);
                    break;
                }
            }
        });
    }
}

async fn process_sentinel_command(frame: Resp, state: &sentinel::SentinelState) -> Vec<u8> {
    match frame {
        Resp::Array(Some(args)) => {
            if args.is_empty() {
                return Resp::Error("ERR empty command".to_string()).as_bytes();
            }
            let command = match &args[0] {
                Resp::BulkString(Some(s)) => String::from_utf8_lossy(s).to_uppercase(),
                _ => return Resp::Error("ERR invalid command".to_string()).as_bytes(),
            };

            match command.as_str() {
                "PING" => Resp::SimpleString("PONG".to_string().into()).as_bytes(),
                "SENTINEL" => {
                    if args.len() < 2 {
                        return Resp::Error("ERR wrong number of arguments for 'sentinel' command".to_string()).as_bytes();
                    }
                    let subcommand = match &args[1] {
                        Resp::BulkString(Some(s)) => String::from_utf8_lossy(s).to_uppercase(),
                        _ => return Resp::Error("ERR invalid sentinel subcommand".to_string()).as_bytes(),
                    };

                    match subcommand.as_str() {
                        "FLUSHCONFIG" => {
                            let path = { state.config_path.read().ok().and_then(|p| p.clone()) };
                            if let Some(p) = path {
                                match state.write_config_to_path(&p) {
                                    Ok(()) => {
                                        info!(path = p.as_str(), "flushconfig_ok");
                                        Resp::SimpleString("OK".to_string().into()).as_bytes()
                                    }
                                    Err(e) => {
                                        error!(path = p.as_str(), err = e.as_str(), "flushconfig_err");
                                        Resp::Error(e).as_bytes()
                                    }
                                }
                            } else {
                                Resp::Error("ERR No config file specified for Sentinel".to_string()).as_bytes()
                            }
                        }
                        "MONITOR" => {
                            if args.len() != 6 {
                                return Resp::Error("ERR wrong number of arguments for 'sentinel monitor' command".to_string()).as_bytes();
                            }
                            let name = match &args[2] { Resp::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(), _ => return Resp::Error("ERR invalid name".to_string()).as_bytes() };
                            let ip = match &args[3] { Resp::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(), _ => return Resp::Error("ERR invalid ip".to_string()).as_bytes() };
                            let port = match &args[4] {
                                Resp::BulkString(Some(s)) => match String::from_utf8_lossy(s).parse::<u16>() {
                                    Ok(v) => v,
                                    Err(_) => return Resp::Error("ERR invalid port".to_string()).as_bytes(),
                                },
                                _ => return Resp::Error("ERR invalid port".to_string()).as_bytes(),
                            };
                            let quorum = match &args[5] {
                                Resp::BulkString(Some(s)) => match String::from_utf8_lossy(s).parse::<u32>() {
                                    Ok(v) => v,
                                    Err(_) => return Resp::Error("ERR invalid quorum".to_string()).as_bytes(),
                                },
                                _ => return Resp::Error("ERR invalid quorum".to_string()).as_bytes(),
                            };
                            match state.monitor_master(&name, &ip, port, quorum) {
                                Ok(()) => {
                                    info!(master = name.as_str(), ip = ip.as_str(), port = port, quorum = quorum, "monitor_ok");
                                    Resp::SimpleString("OK".to_string().into()).as_bytes()
                                }
                                Err(e) => {
                                    error!(master = name.as_str(), err = e.as_str(), "monitor_err");
                                    Resp::Error(e).as_bytes()
                                }
                            }
                        }
                        "REMOVE" => {
                            if args.len() != 3 {
                                return Resp::Error("ERR wrong number of arguments for 'sentinel remove' command".to_string()).as_bytes();
                            }
                            let name = match &args[2] { Resp::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(), _ => return Resp::Error("ERR invalid name".to_string()).as_bytes() };
                            match state.remove_master(&name) {
                                Ok(()) => {
                                    info!(master = name.as_str(), "remove_ok");
                                    Resp::SimpleString("OK".to_string().into()).as_bytes()
                                }
                                Err(e) => {
                                    error!(master = name.as_str(), err = e.as_str(), "remove_err");
                                    Resp::Error(e).as_bytes()
                                }
                            }
                        }
                        "SET" => {
                            if args.len() < 4 {
                                return Resp::Error("ERR wrong number of arguments for 'sentinel set' command".to_string()).as_bytes();
                            }
                            let name = match &args[2] { Resp::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(), _ => return Resp::Error("ERR invalid name".to_string()).as_bytes() };
                            if (args.len() - 3) % 2 != 1 {
                                return Resp::Error("ERR Invalid number of arguments for SENTINEL SET".to_string()).as_bytes();
                            }
                            let mut options: Vec<(String, String)> = Vec::new();
                            let mut i = 3;
                            while i + 1 < args.len() {
                                let opt = match &args[i] { Resp::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(), _ => return Resp::Error("ERR invalid option".to_string()).as_bytes() };
                                let val = match &args[i+1] { Resp::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(), _ => return Resp::Error("ERR invalid value".to_string()).as_bytes() };
                                options.push((opt, val));
                                i += 2;
                            }
                            match state.set_master_options(&name, &options) {
                                Ok(()) => {
                                    info!(master = name.as_str(), opts = options.len(), "set_ok");
                                    Resp::SimpleString("OK".to_string().into()).as_bytes()
                                }
                                Err(e) => {
                                    error!(master = name.as_str(), err = e.as_str(), "set_err");
                                    Resp::Error(e).as_bytes()
                                }
                            }
                        }
                        "CKQUORUM" => {
                            if args.len() != 3 {
                                return Resp::Error("ERR wrong number of arguments for 'sentinel ckquorum' command".to_string()).as_bytes();
                            }
                            let master_name = match &args[2] {
                                Resp::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
                                _ => return Resp::Error("ERR invalid master name".to_string()).as_bytes(),
                            };
                            match sentinel::check_quorum_for_master(&state, &master_name) {
                                Ok((total, reachable, majority, quorum, ok)) => {
                                    let status = if ok { "OK" } else { "NOQUORUM" };
                                    info!(master = master_name.as_str(), total = total, reachable = reachable, majority = majority, quorum = quorum, status = status, "ckquorum");
                                    let response = vec![
                                        Resp::BulkString(Some(status.into())),
                                        Resp::Integer(reachable as i64),
                                        Resp::Integer(quorum as i64),
                                        Resp::Integer(majority as i64),
                                        Resp::Integer(total as i64),
                                    ];
                                    Resp::Array(Some(response)).as_bytes()
                                }
                                Err(e) => {
                                    error!(master = master_name.as_str(), err = e.as_str(), "ckquorum_err");
                                    Resp::Error(e).as_bytes()
                                }
                            }
                        }
                        "INFO-CACHE" => {
                            if args.len() != 3 {
                                return Resp::Error("ERR wrong number of arguments for 'sentinel info-cache' command".to_string()).as_bytes();
                            }
                            let master_name = match &args[2] {
                                Resp::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
                                _ => return Resp::Error("ERR invalid master name".to_string()).as_bytes(),
                            };
                            let masters = state.masters.read().unwrap();
                            if let Some(master_lock) = masters.get(&master_name) {
                                let master = master_lock.read().unwrap();
                                if let Some(cache) = &master.info_cache {
                                    let response = vec![
                                        Resp::BulkString(Some("last-refresh".into())),
                                        Resp::Integer(cache.last_refresh_time as i64),
                                        Resp::BulkString(Some("raw".into())),
                                        Resp::BulkString(Some(cache.raw.clone().into())),
                                    ];
                                    Resp::Array(Some(response)).as_bytes()
                                } else {
                                    Resp::Array(None).as_bytes()
                                }
                            } else {
                                Resp::Error("ERR No such master with that name".to_string()).as_bytes()
                            }
                        }
                        "SIMULATE-FAILURE" => {
                            if args.len() < 4 {
                                return Resp::Error("ERR wrong number of arguments for 'sentinel simulate-failure' command".to_string()).as_bytes();
                            }
                            let master_name = match &args[2] {
                                Resp::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
                                _ => return Resp::Error("ERR invalid master name".to_string()).as_bytes(),
                            };
                            let mode = match &args[3] {
                                Resp::BulkString(Some(s)) => String::from_utf8_lossy(s).to_uppercase(),
                                _ => return Resp::Error("ERR invalid mode".to_string()).as_bytes(),
                            };
                            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                            // For FAILOVER, avoid holding any lock across await
                            if mode == "FAILOVER" {
                                let exists = {
                                    let masters = state.masters.read().unwrap();
                                    masters.contains_key(&master_name)
                                };
                                if !exists {
                                    return Resp::Error("ERR No such master with that name".to_string()).as_bytes();
                                }
                                match sentinel::start_manual_failover(&state, &master_name).await {
                                    Ok(()) => {
                                        info!(master = master_name.as_str(), "simulate_failover_started");
                                        return Resp::SimpleString("OK".to_string().into()).as_bytes();
                                    }
                                    Err(e) => {
                                        error!(master = master_name.as_str(), err = e.as_str(), "simulate_failover_err");
                                        return Resp::Error(format!("ERR {}", e)).as_bytes();
                                    }
                                }
                            }
                            // Non-awaiting modes can mutate state under lock
                            {
                                let masters = state.masters.read().unwrap();
                                if let Some(master_lock) = masters.get(&master_name) {
                                    let mut master = master_lock.write().unwrap();
                                    match mode.as_str() {
                                        "SDOWN" => { master.s_down_since_time = now; info!(master = master_name.as_str(), "simulate_sdown"); }
                                        "CLEAR" => { master.s_down_since_time = 0; info!(master = master_name.as_str(), "simulate_clear"); }
                                        "CONNFAIL" => { master.last_pong_time = 0; info!(master = master_name.as_str(), "simulate_connfail"); }
                                        _ => return Resp::Error("ERR unknown failure mode".to_string()).as_bytes(),
                                    }
                                } else {
                                    return Resp::Error("ERR No such master with that name".to_string()).as_bytes();
                                }
                            }
                            Resp::SimpleString("OK".to_string().into()).as_bytes()
                        }
                        "MASTERS" => {
                            let masters = state.masters.read().unwrap();
                            let mut response = Vec::new();
                            for master in masters.values() {
                                let master = master.read().unwrap();
                                let mut master_info = Vec::new();
                                master_info.push(Resp::BulkString(Some("name".into())));
                                master_info.push(Resp::BulkString(Some(master.name.clone().into())));
                                master_info.push(Resp::BulkString(Some("ip".into())));
                                master_info.push(Resp::BulkString(Some(master.ip.clone().into())));
                                master_info.push(Resp::BulkString(Some("port".into())));
                                master_info.push(Resp::Integer(master.port as i64));
                                // Add more fields as needed
                                response.push(Resp::Array(Some(master_info)));
                            }
                            Resp::Array(Some(response)).as_bytes()
                        }
                        "GET-MASTER-ADDR-BY-NAME" => {
                            if args.len() != 3 {
                                return Resp::Error("ERR wrong number of arguments for 'sentinel get-master-addr-by-name' command".to_string()).as_bytes();
                            }
                            let master_name = match &args[2] {
                                Resp::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
                                _ => return Resp::Error("ERR invalid master name".to_string()).as_bytes(),
                            };

                            let masters = state.masters.read().unwrap();
                            if let Some(master_lock) = masters.get(&master_name) {
                                let master = master_lock.read().unwrap();
                                let response = vec![
                                    Resp::BulkString(Some(master.ip.clone().into())),
                                    Resp::BulkString(Some(master.port.to_string().into())),
                                ];
                                Resp::Array(Some(response)).as_bytes()
                            } else {
                                Resp::Array(None).as_bytes()
                            }
                        }
                        "SLAVES" => {
                            if args.len() != 3 {
                                return Resp::Error("ERR wrong number of arguments for 'sentinel slaves' command".to_string()).as_bytes();
                            }
                            let master_name = match &args[2] {
                                Resp::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
                                _ => return Resp::Error("ERR invalid master name".to_string()).as_bytes(),
                            };

                            let masters = state.masters.read().unwrap();
                            if let Some(master_lock) = masters.get(&master_name) {
                                let master = master_lock.read().unwrap();
                                let mut response = Vec::new();
                                for slave in master.slaves.values() {
                                    let mut slave_info = Vec::new();
                                    slave_info.push(Resp::BulkString(Some("name".into())));
                                    slave_info.push(Resp::BulkString(Some(format!("{}:{}", slave.ip, slave.port).into())));
                                    slave_info.push(Resp::BulkString(Some("ip".into())));
                                    slave_info.push(Resp::BulkString(Some(slave.ip.clone().into())));
                                    slave_info.push(Resp::BulkString(Some("port".into())));
                                    slave_info.push(Resp::Integer(slave.port as i64));
                                    slave_info.push(Resp::BulkString(Some("flags".into())));
                                    slave_info.push(Resp::BulkString(Some(slave.flags.clone().into())));
                                    slave_info.push(Resp::BulkString(Some("runid".into())));
                                    slave_info.push(Resp::BulkString(Some(slave.run_id.clone().into())));
                                    response.push(Resp::Array(Some(slave_info)));
                                }
                                Resp::Array(Some(response)).as_bytes()
                            } else {
                                Resp::Array(None).as_bytes()
                            }
                        }
                        "SENTINELS" => {
                            if args.len() != 3 {
                                return Resp::Error("ERR wrong number of arguments for 'sentinel sentinels' command".to_string()).as_bytes();
                            }
                            let master_name = match &args[2] {
                                Resp::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
                                _ => return Resp::Error("ERR invalid master name".to_string()).as_bytes(),
                            };

                            let masters = state.masters.read().unwrap();
                            if let Some(master_lock) = masters.get(&master_name) {
                                let master = master_lock.read().unwrap();
                                let mut response = Vec::new();
                                for sentinel in master.sentinels.values() {
                                    let mut sentinel_info = Vec::new();
                                    sentinel_info.push(Resp::BulkString(Some("name".into())));
                                    sentinel_info.push(Resp::BulkString(Some(format!("{}:{}", sentinel.ip, sentinel.port).into())));
                                    sentinel_info.push(Resp::BulkString(Some("ip".into())));
                                    sentinel_info.push(Resp::BulkString(Some(sentinel.ip.clone().into())));
                                    sentinel_info.push(Resp::BulkString(Some("port".into())));
                                    sentinel_info.push(Resp::Integer(sentinel.port as i64));
                                    sentinel_info.push(Resp::BulkString(Some("runid".into())));
                                    sentinel_info.push(Resp::BulkString(Some(sentinel.run_id.clone().into())));
                                    response.push(Resp::Array(Some(sentinel_info)));
                                }
                                Resp::Array(Some(response)).as_bytes()
                            } else {
                                Resp::Array(None).as_bytes()
                            }
                        }
                        "IS-MASTER-DOWN-BY-ADDR" => {
                            if args.len() < 4 {
                                return Resp::Error("ERR wrong number of arguments for 'sentinel is-master-down-by-addr' command".to_string()).as_bytes();
                            }
                            let ip = match &args[2] {
                                Resp::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
                                _ => return Resp::Error("ERR invalid ip".to_string()).as_bytes(),
                            };
                            let port = match &args[3] {
                                Resp::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
                                _ => return Resp::Error("ERR invalid port".to_string()).as_bytes(),
                            };

                            let masters = state.masters.read().unwrap();
                            let mut is_down = 0i64;
                            let leader = Resp::BulkString(None);
                            let leader_epoch = Resp::Integer(0);

                            // Find master by ip:port and check if it's down
                            for master_lock in masters.values() {
                                let master = master_lock.read().unwrap();
                                if master.ip == ip && master.port.to_string() == port {
                                    // Check if master is in ODOWN state
                                    if master.s_down_since_time > 0 {
                                        is_down = 1;
                                    }
                                    break;
                                }
                            }

                            let response = vec![
                                Resp::Integer(is_down),
                                leader,
                                leader_epoch,
                            ];
                            Resp::Array(Some(response)).as_bytes()
                        }
                        "RESET" => {
                            if args.len() != 3 {
                                return Resp::Error("ERR wrong number of arguments for 'sentinel reset' command".to_string()).as_bytes();
                            }
                            let master_name = match &args[2] {
                                Resp::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
                                _ => return Resp::Error("ERR invalid master name".to_string()).as_bytes(),
                            };

                            let masters = state.masters.read().unwrap();
                            if let Some(master_lock) = masters.get(&master_name) {
                                let mut master = master_lock.write().unwrap();
                                // Reset master state
                                master.s_down_since_time = 0;
                                master.last_ping_time = 0;
                                master.last_pong_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                                master.slaves.clear();
                                master.sentinels.clear();
                                Resp::Integer(1).as_bytes()
                            } else {
                                Resp::Integer(0).as_bytes()
                            }
                        }
                        "HELLO" => {
                            // SENTINEL HELLO - Return sentinel information for discovery
                            // Format: [ip, port, runid, current_epoch, master_name, master_ip, master_port, master_config_epoch]
                            let mut response = Vec::new();
                            
                            // Add this sentinel's info
                            response.push(Resp::BulkString(Some(state.ip.clone().into())));
                            response.push(Resp::BulkString(Some(state.port.to_string().into())));
                            response.push(Resp::BulkString(Some(state.run_id.clone().into())));
                            
                            let current_epoch = state.current_epoch.read().unwrap();
                            response.push(Resp::Integer(*current_epoch as i64));
                            
                            // Add master information for the first master (if any)
                            let masters = state.masters.read().unwrap();
                            if let Some(master_lock) = masters.values().next() {
                                let master = master_lock.read().unwrap();
                                response.push(Resp::BulkString(Some(master.name.clone().into())));
                                response.push(Resp::BulkString(Some(master.ip.clone().into())));
                                response.push(Resp::Integer(master.port as i64));
                                response.push(Resp::Integer(master.failover_epoch as i64));
                            } else {
                                // No masters configured
                                response.push(Resp::BulkString(None));
                                response.push(Resp::BulkString(None));
                                response.push(Resp::Integer(0));
                                response.push(Resp::Integer(0));
                            }
                            
                            Resp::Array(Some(response)).as_bytes()
                        }
                        "FAILOVER" => {
                            if args.len() != 3 {
                                return Resp::Error("ERR wrong number of arguments for 'sentinel failover' command".to_string()).as_bytes();
                            }
                            let master_name = match &args[2] {
                                Resp::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
                                _ => return Resp::Error("ERR invalid master name".to_string()).as_bytes(),
                            };

                            match start_manual_failover(&state, &master_name).await {
                                Ok(()) => {
                                    info!("Manual failover initiated for master {}", master_name);
                                    Resp::SimpleString("OK".to_string().into()).as_bytes()
                                }
                                Err(e) => {
                                    error!("Failed to start manual failover for master {}: {}", master_name, e);
                                    Resp::Error(format!("ERR {}", e)).as_bytes()
                                }
                            }
                        }
                        _ => Resp::Error(format!("ERR Unknown SENTINEL subcommand '{}'", subcommand)).as_bytes(),
                    }
                }
                _ => Resp::Error(format!("ERR unknown command '{}'", command)).as_bytes(),
            }
        }
        _ => Resp::Error("ERR invalid request".to_string()).as_bytes(),
    }
}
