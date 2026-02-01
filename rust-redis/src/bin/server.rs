#![allow(unexpected_cfgs)]
#![allow(unused_imports)]
#![allow(dead_code)]
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::{error, info, warn};
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

#[cfg(test)]
#[path = "../tests/mod.rs"]
mod tests;

use rand::Rng;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 {
        if args[1] == "-v" || args[1] == "--version" {
            println!("v0.1.0");
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

    if let Some(path) = &cfg.logfile {
        let file_appender = tracing_appender::rolling::never(".", path);
        let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(non_blocking)
            .init();
        // The guard must be held for the lifetime of the application
        // We move it into a long-lived async block or just keep it in main scope,
        // but main is async. _guard drop will flush logs.
        // However, we enter a loop at the end of main, so _guard will be dropped only when main returns.
        run_server(cfg, Some(_guard)).await;
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .init();
        run_server(cfg, None).await;
    }
}


async fn run_server(
    cfg: conf::Config,
    _guard: Option<tracing_appender::non_blocking::WorkerGuard>,
) {
    let addr = cfg.address();
    info!("starting server, listen on {}", addr);
    if let Some(path) = &cfg.logfile {
        info!("logging to file: {}", path);
    }

    let listener = TcpListener::bind(&addr).await.unwrap();
    
    // Initialize multiple databases
    let mut dbs = Vec::with_capacity(cfg.databases as usize);
    for _ in 0..cfg.databases {
        dbs.push(std::sync::RwLock::new(db::Db::default()));
    }
    let databases = Arc::new(dbs);

    if !cfg.appendonly {
        if let Err(e) = rdb::rdb_load(&databases, &cfg) {
            warn!("Failed to load RDB: {}", e);
        }
    }

    // Create script cache
    let script_manager = cmd::scripting::create_script_manager();
    
    // Initialize ACL
    let mut acl_store = acl::Acl::new();
    
    // Load from ACL file if configured
    if let Some(acl_file) = &cfg.aclfile {
        // If the file doesn't exist, we just start with default ACL.
        // If it exists, we try to load it.
        if std::path::Path::new(acl_file).exists() {
            if let Err(e) = acl_store.load_from_file(acl_file) {
                warn!("Failed to load ACL file {}: {}", acl_file, e);
            } else {
                info!("Loaded ACL from file: {}", acl_file);
            }
        }
    }

    // Apply requirepass to default user if set (compatibility)
    if let Some(pass) = &cfg.requirepass {
        if let Some(default_user_arc) = acl_store.users.get("default") {
             let mut default_user = (**default_user_arc).clone();
             // Add the password
             default_user.passwords.insert(pass.clone());
             acl_store.set_user(default_user);
        }
    }

    let acl = Arc::new(std::sync::RwLock::new(acl_store));
    let mut rng = rand::rng();
    let run_id: String = (0..40).map(|_| rng.sample(rand::distr::Alphanumeric) as char).collect();
    let aof = if cfg.appendonly {
        info!("AOF enabled, file: {}", cfg.appendfilename);
        let aof = aof::Aof::new(&cfg.appendfilename, cfg.appendfsync)
            .await
            .expect("failed to open AOF file"); 
        //aof.load(&cfg.appendfilename, &databases, &cfg, &script_manager)
        Some(Arc::new(Mutex::new(aof)))
    } else {
        None
    };
    let server_ctx = cmd::ServerContext {
        databases: databases,
        acl: acl,
        aof: aof,
        config: Arc::new(cfg.clone()),
        script_manager: script_manager.clone(),
        blocking_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
        blocking_zset_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
        pubsub_channels: std::sync::Arc::new(dashmap::DashMap::new()),
        pubsub_patterns: std::sync::Arc::new(dashmap::DashMap::new()),
        run_id,
        start_time: std::time::Instant::now(),
        client_count: Arc::new(AtomicU64::new(0)),
        blocked_client_count: Arc::new(AtomicU64::new(0)),
        clients: std::sync::Arc::new(dashmap::DashMap::new()),
        monitors: std::sync::Arc::new(dashmap::DashMap::new()),
        slowlog: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::new())),
        slowlog_next_id: std::sync::Arc::new(AtomicU64::new(1)),
        slowlog_max_len: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(cfg.slowlog_max_len as usize)),
        slowlog_threshold_us: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(cfg.slowlog_log_slower_than)),
        mem_peak_rss: std::sync::Arc::new(AtomicU64::new(0)),
        maxmemory: std::sync::Arc::new(AtomicU64::new(cfg.maxmemory)),
        notify_keyspace_events: std::sync::Arc::new(std::sync::atomic::AtomicU32::new(cmd::notify::parse_notify_flags(&cfg.notify_keyspace_events))),
        rdbcompression: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(cfg.rdbcompression)),
        rdbchecksum: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(cfg.rdbchecksum)),
        stop_writes_on_bgsave_error: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(cfg.stop_writes_on_bgsave_error)),
        maxmemory_policy: Arc::new(RwLock::new(cfg.maxmemory_policy)),
        maxmemory_samples: Arc::new(std::sync::atomic::AtomicUsize::new(cfg.maxmemory_samples)),
        save_params: Arc::new(RwLock::new(cfg.save_params.clone())),
        last_bgsave_ok: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
        dirty: Arc::new(AtomicU64::new(0)),
        last_save_time: Arc::new(std::sync::atomic::AtomicI64::new(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64)),
        watched_clients: std::sync::Arc::new(dashmap::DashMap::new()),
        client_watched_dirty: std::sync::Arc::new(dashmap::DashMap::new()),
        tracking_clients: std::sync::Arc::new(dashmap::DashMap::new()),
        acl_log: Arc::new(RwLock::new(std::collections::VecDeque::new())),
        latency_events: Arc::new(dashmap::DashMap::new()),
    };
    
    if let Some(ref aof) = server_ctx.aof {
            //let aof_guard = aof.lock().await;
            let aof_guard = aof.lock().await;
            aof_guard.load(&server_ctx).await.expect("failed to load AOF");
    }

    // Background task to clean up expired keys
    let databases_for_cleanup = server_ctx.databases.clone();
    let server_ctx_for_cleanup = server_ctx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(100));
        loop {
            interval.tick().await;
            for (db_idx, db_lock) in databases_for_cleanup.iter().enumerate() {
                let mut expired_keys = Vec::new();
                {
                    let db = db_lock.write().unwrap();
                    db.retain(|k, v| {
                        if v.is_expired() {
                            expired_keys.push(k.clone());
                            false
                        } else {
                            true
                        }
                    });
                }
                
                for key in expired_keys {
                    cmd::notify::notify_keyspace_event(&server_ctx_for_cleanup, cmd::notify::NOTIFY_EXPIRED, "expired", &key, db_idx).await;
                }
            }
        }
    });

    // Background task for periodic RDB save
    let server_ctx_for_save = server_ctx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            
            let dirty = server_ctx_for_save.dirty.load(Ordering::Relaxed);
            let last_save = server_ctx_for_save.last_save_time.load(Ordering::Relaxed);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            let elapsed = now - last_save;

            let mut trigger_save = false;
            for (secs, changes) in &server_ctx_for_save.config.save_params {
                if elapsed >= (*secs as i64) && dirty >= *changes {
                    trigger_save = true;
                    break;
                }
            }

            if trigger_save && dirty > 0 {
                info!("Configured save reached ({} changes, {} seconds). Starting background save.", dirty, elapsed);
                cmd::save::bgsave(&[], &server_ctx_for_save);
            }
        }
    });

    let next_connection_id = Arc::new(AtomicU64::new(1));

    loop {
        let (mut socket, addr) = listener.accept().await.unwrap();
        info!("accepted connection from {}", addr);

        let current_clients = server_ctx.client_count.load(Ordering::Relaxed);
        if current_clients >= server_ctx.config.maxclients {
            warn!("max number of clients reached, rejecting connection from {}", addr);
            let _ = socket.write_all(b"-ERR max number of clients reached\r\n").await;
            continue;
        }

        server_ctx.client_count.fetch_add(1, Ordering::Relaxed);
        let server_ctx_cloned: cmd::ServerContext = server_ctx.clone();
        let connection_id = next_connection_id.fetch_add(1, Ordering::Relaxed);

        tokio::spawn(async move {
            // Shutdown signal
            let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
            let (tx, mut rx) = tokio::sync::mpsc::channel(32);
            let tx_for_conn = tx.clone();

            {
                let flags = String::from("N");
                let ci = cmd::ClientInfo {
                    id: connection_id,
                    addr: addr.to_string(),
                    name: "".to_string(),
                    db: 0,
                    sub: 0,
                    psub: 0,
                    flags,
                    cmd: "".to_string(),
                    connect_time: std::time::Instant::now(),
                    last_activity: std::time::Instant::now(),
                    shutdown_tx: Some(shutdown_tx.clone()),
                    msg_sender: Some(tx_for_conn.clone()),
                };
                server_ctx_cloned.clients.insert(connection_id, ci);
            }
            let (read_half, write_half) = socket.into_split();
            
            // Writer task
            tokio::spawn(async move {
                let mut writer = BufWriter::new(write_half);
                while let Some(resp) = rx.recv().await {
                    if resp::write_frame(&mut writer, &resp).await.is_err() {
                        break;
                    }
                    if writer.flush().await.is_err() {
                        break;
                    }
                }
            });
            
            // Frame channel
            let (frame_tx, mut frame_rx) = tokio::sync::mpsc::channel(32);

            // Reader Task
            tokio::spawn(async move {
                let mut reader = BufReader::new(read_half);
                loop {
                    match resp::read_frame(&mut reader).await {
                        Ok(Some(frame)) => {
                            if frame_tx.send(frame).await.is_err() {
                                break;
                            }
                        }
                        Ok(None) => break, // EOF
                        Err(_) => break, // Error
                    }
                }
                // Signal shutdown
                let _ = shutdown_tx.send(true);
            });

            let mut conn_ctx = cmd::ConnectionContext::new(connection_id, Some(tx_for_conn), Some(shutdown_rx.clone()));
            server_ctx_cloned.client_watched_dirty.insert(connection_id, conn_ctx.watched_keys_dirty.clone());

            loop {
                tokio::select! {
                    frame_opt = frame_rx.recv() => {
                        match frame_opt {
                            Some(frame) => {
                                let cmd_name = match &frame {
                                    resp::Resp::Array(Some(items)) => {
                                        if !items.is_empty() {
                                            match &items[0] {
                                                resp::Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
                                                resp::Resp::SimpleString(s) => String::from_utf8_lossy(s).to_string(),
                                                _ => String::new(),
                                            }
                                        } else {
                                            String::new()
                                        }
                                    }
                                    _ => String::new(),
                                };
                                let (response, cmd_to_log) = cmd::process_frame(
                                    frame,
                                    &mut conn_ctx,
                                    &server_ctx_cloned,
                                ).await;
                
                                if tx.send(response).await.is_err() {
                                    break;
                                }
                
                                if let Some(cmd) = cmd_to_log {
                                    if let Some(aof) = &server_ctx_cloned.aof {
                                        // Use a timeout to prevent hanging if AOF lock is held for too long
                                        let aof_op = async {
                                            let mut guard = aof.lock().await;
                                            guard.append(&cmd).await
                                        };

                                        match tokio::time::timeout(Duration::from_millis(500), aof_op).await {
                                            Ok(Ok(_)) => {},
                                            Ok(Err(e)) => error!("failed to append to AOF: {}", e),
                                            Err(_) => error!("timeout appending to AOF"),
                                        }
                                    }
                                }
                                if let Some(mut ci) = server_ctx_cloned.clients.get_mut(&connection_id) {
                                    let mut flags = String::from("N");
                                    if conn_ctx.in_multi {
                                        flags.push('M');
                                    }
                                    if !conn_ctx.subscriptions.is_empty() || !conn_ctx.psubscriptions.is_empty() {
                                        flags.push('P');
                                    }
                                    ci.db = conn_ctx.db_index;
                                    ci.sub = conn_ctx.subscriptions.len();
                                    ci.psub = conn_ctx.psubscriptions.len();
                                    ci.flags = flags;
                                    ci.cmd = cmd_name;
                                    ci.last_activity = std::time::Instant::now();
                                }
                            }
                            None => break, // Reader closed
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        break;
                    }
                }
            }
            
            // Cleanup subscriptions on disconnect
            for channel in conn_ctx.subscriptions.iter() {
                if let Some(subscribers) = server_ctx_cloned.pubsub_channels.get(channel) {
                    subscribers.remove(&conn_ctx.id);
                }
            }
            for pattern in conn_ctx.psubscriptions.iter() {
                if let Some(subscribers) = server_ctx_cloned.pubsub_patterns.get(pattern) {
                    subscribers.remove(&conn_ctx.id);
                }
            }
            // Cleanup watched keys
            for (db_idx, keys) in conn_ctx.watched_keys.iter() {
                for key in keys {
                    if let Some(mut clients) = server_ctx_cloned.watched_clients.get_mut(&(*db_idx, key.clone())) {
                        clients.remove(&conn_ctx.id);
                    }
                }
            }
            server_ctx_cloned.client_watched_dirty.remove(&conn_ctx.id);
            server_ctx_cloned.client_count.fetch_sub(1, Ordering::Relaxed);
            server_ctx_cloned.clients.remove(&conn_ctx.id);
            server_ctx_cloned.monitors.remove(&conn_ctx.id);
        });
    }
}
