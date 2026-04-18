#![allow(unexpected_cfgs)]
#![allow(unused_imports)]
#![allow(dead_code)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpListener;
use tracing::{error, info, warn};
#[path = "../acl.rs"]
pub mod acl;
#[path = "../aof.rs"]
mod aof;
#[path = "../clock.rs"]
pub mod clock;
#[path = "../cmd/mod.rs"]
mod cmd;
#[path = "../conf.rs"]
mod conf;
#[path = "../db.rs"]
mod db;
#[path = "../geo.rs"]
mod geo;
#[path = "../hll.rs"]
mod hll;
#[path = "../rax.rs"]
mod rax;
#[path = "../rdb.rs"]
mod rdb;
#[path = "../resp.rs"]
mod resp;
#[path = "../stream.rs"]
mod stream;

#[cfg(test)]
#[path = "../tests/mod.rs"]
mod tests;

use rand::Rng;

use std::os::unix::io::AsRawFd;

use crate::resp::Resp;

#[path = "../cluster.rs"]
pub mod cluster;

#[tokio::main(flavor = "multi_thread")]
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
    clock::start_clock_task();
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

    let acl = Arc::new(arc_swap::ArcSwap::from_pointee(acl_store));
    let mut rng = rand::rng();
    let run_id: String = (0..40)
        .map(|_| rng.sample(rand::distr::Alphanumeric) as char)
        .collect();
    // Hold the raw Aof before the task starts so we can load it first.
    let raw_aof = if cfg.appendonly {
        info!("AOF enabled, file: {}", cfg.appendfilename);
        let aof = aof::Aof::new(&cfg.appendfilename, cfg.appendfsync)
            .await
            .expect("failed to open AOF file");
        Some(aof)
    } else {
        None
    };
    // Initialize Cluster State
    let my_run_id = run_id.clone();
    let cluster_state = {
        let node_id = cluster::NodeId(my_run_id.clone());
        Arc::new(RwLock::new(cluster::ClusterState::new(
            node_id,
            cfg.bind.clone(),
            cfg.port,
        )))
    };
    let mut server_ctx = cmd::ServerContext {
        databases: databases,
        acl: acl,
        aof: None, // filled in after AOF load below
        config: Arc::new(cfg.clone()),
        script_manager: script_manager.clone(),
        blocking_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
        blocking_zset_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
        pubsub: std::sync::Arc::new(cmd::PubSubCtx::new()),
        start_time: std::time::Instant::now(),
        clients_ctx: std::sync::Arc::new(cmd::ClientCtx::new()),
        repl: std::sync::Arc::new(cmd::ReplicationCtx::new(
            run_id,
            cfg.repl_backlog_size,
            cfg.repl_ping_replica_period,
            cfg.repl_timeout,
            cfg.replica_read_only,
            cfg.min_replicas_to_write,
            cfg.min_replicas_max_lag,
            cfg.repl_diskless_sync,
            cfg.repl_diskless_sync_delay,
        )),
        slowlog: std::sync::Arc::new(cmd::SlowLogCtx::new(
            cfg.slowlog_max_len as usize,
            cfg.slowlog_log_slower_than,
        )),
        mem: std::sync::Arc::new(cmd::MemoryCtx::new(
            cfg.maxmemory,
            cfg.maxmemory_policy,
            cfg.maxmemory_samples,
            cmd::notify::parse_notify_flags(&cfg.notify_keyspace_events),
        )),
        persist: std::sync::Arc::new(cmd::PersistenceCtx::new(
            cfg.rdbcompression,
            cfg.rdbchecksum,
            cfg.stop_writes_on_bgsave_error,
            cfg.save_params.clone(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
        )),
        cluster_ctx: std::sync::Arc::new(cmd::ClusterCtx::new(cluster_state.clone())),
    };

    if cfg.cluster_enabled {
        let p = std::path::Path::new(&cfg.dir).join(&cfg.cluster_config_file);
        if p.exists() {
            if let Ok(text) = std::fs::read_to_string(&p) {
                if let Ok(mut st) = server_ctx.cluster_ctx.state.write() {
                    let _ = st.load_config_text(&text, &cfg.bind, cfg.port);
                }
            }
        }
    }

    // Load AOF while we still have exclusive ownership of the Aof struct,
    // then hand it off to the background task.
    if let Some(aof) = raw_aof {
        aof.load(&server_ctx).await.expect("failed to load AOF");
        server_ctx.aof = Some(aof::start_aof_task(aof));
    }

    // Background task to clean up expired keys
    cmd::start_expiration_task(server_ctx.clone());
    cmd::start_cluster_topology_task(server_ctx.clone());
    cmd::start_cluster_failover_task(server_ctx.clone());

    // Background task for periodic RDB save
    let server_ctx_for_save = server_ctx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(100)); // Check more frequently for child exit
        loop {
            interval.tick().await;

            let dirty = server_ctx_for_save.persist.dirty.load(Ordering::Relaxed);
            let last_save = server_ctx_for_save.persist.last_save_time.load(Ordering::Relaxed);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            let elapsed = now - last_save;

            let mut trigger_save = false;
            for (secs, changes) in &*server_ctx_for_save.persist.save_params.read().unwrap() {
                if elapsed >= (*secs as i64) && dirty >= *changes {
                    trigger_save = true;
                    break;
                }
            }

            // Only trigger if no child process is running
            if trigger_save
                && dirty > 0
                && server_ctx_for_save.persist.rdb_child_pid.load(Ordering::Relaxed) == -1
            {
                info!(
                    "Configured save reached ({} changes, {} seconds). Starting background save.",
                    dirty, elapsed
                );
                cmd::save::bgsave(&[], &server_ctx_for_save);
            }
        }
    });

    let next_connection_id = Arc::new(AtomicU64::new(1));

    loop {
        let (mut socket, addr) = listener.accept().await.unwrap();
        let client_fd = Some(socket.as_raw_fd()); // Capture FD
        info!("accepted connection from {}", addr);

        let current_clients = server_ctx.clients_ctx.client_count.load(Ordering::Relaxed);
        if current_clients >= server_ctx.config.maxclients {
            warn!(
                "max number of clients reached, rejecting connection from {}",
                addr
            );
            let _ = socket
                .write_all(b"-ERR max number of clients reached\r\n")
                .await;
            continue;
        }

        server_ctx.clients_ctx.client_count.fetch_add(1, Ordering::Relaxed);
        let server_ctx_cloned: cmd::ServerContext = server_ctx.clone();
        let connection_id = next_connection_id.fetch_add(1, Ordering::Relaxed);

        tokio::spawn(async move {
            // Shutdown signal
            let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
            // 256 slots: supports deep pipelining without premature back-pressure.
            // 32 was too small for clients sending 100+ pipelined commands at once.
            let (tx, mut rx) = tokio::sync::mpsc::channel(256);
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
                server_ctx_cloned.clients_ctx.clients.insert(connection_id, ci);
            }
            let (read_half, write_half) = socket.into_split();

            // Writer task
            tokio::spawn(async move {
                let mut writer = BufWriter::new(write_half);
                let mut buffer: Vec<Resp> = Vec::new();
                let mut buffering = false;

                'outer: while let Some(resp) = rx.recv().await {
                    match resp {
                        Resp::Control(ref s) if s == "START_RDB_TRANSFER" => {
                            buffering = true;
                        }
                        Resp::Control(ref s) if s == "RDB_FINISHED" => {
                            buffering = false;
                            for item in buffer.drain(..) {
                                if resp::write_frame(&mut writer, &item).await.is_err() {
                                    break 'outer;
                                }
                            }
                            if writer.flush().await.is_err() {
                                break 'outer;
                            }
                        }
                        resp => {
                            if buffering {
                                buffer.push(resp);
                            } else {
                                // Write the first frame
                                if resp::write_frame(&mut writer, &resp).await.is_err() {
                                    break 'outer;
                                }
                                // Drain any additional pending frames before flushing once.
                                // This batches multiple responses into a single syscall.
                                loop {
                                    match rx.try_recv() {
                                        Ok(Resp::Control(ref s)) if s == "START_RDB_TRANSFER" => {
                                            buffering = true;
                                            break;
                                        }
                                        Ok(next) => {
                                            if buffering {
                                                buffer.push(next);
                                            } else if resp::write_frame(&mut writer, &next)
                                                .await
                                                .is_err()
                                            {
                                                break 'outer;
                                            }
                                        }
                                        Err(_) => break, // channel empty or closed
                                    }
                                }
                                if writer.flush().await.is_err() {
                                    break 'outer;
                                }
                            }
                        }
                    }
                }
                // Signal shutdown
                let _ = shutdown_tx.send(true);
            });

            // Frame channel
            let (frame_tx, mut frame_rx) = tokio::sync::mpsc::channel(256);
            let _conn_ctx_id = connection_id;
            let mut conn_ctx = cmd::ConnectionContext::new(
                connection_id,
                client_fd,
                Some(tx_for_conn),
                Some(shutdown_rx.clone()),
            );
            server_ctx_cloned
                .clients_ctx.client_watched_dirty
                .insert(connection_id, conn_ctx.watched_keys_dirty.clone());

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
                        Err(_) => break,   // Error
                    }
                }
            });

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
                                        aof.append(&cmd).await;
                                    }
                                    let next_off = server_ctx_cloned.repl.repl_offset.fetch_add(1, Ordering::Relaxed) + 1;
                                    {
                                        {
                                            let mut q = server_ctx_cloned.repl.repl_backlog.lock().await;
                                            q.push_back((next_off, cmd.clone()));
                                            let max = server_ctx_cloned.repl.repl_backlog_size.load(Ordering::Relaxed);
                                            while q.len() > max {
                                                q.pop_front();
                                            }
                                        }
                                    }
                                    for entry in server_ctx_cloned.repl.replicas.iter() {
                                        let _ = entry.value().try_send(cmd.clone());
                                    }
                                }
                                if let Some(mut ci) = server_ctx_cloned.clients_ctx.clients.get_mut(&connection_id) {
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
                if let Some(subscribers) = server_ctx_cloned.pubsub.channels.get(channel) {
                    subscribers.remove(&conn_ctx.id);
                }
            }
            for pattern in conn_ctx.psubscriptions.iter() {
                if let Some(subscribers) = server_ctx_cloned.pubsub.patterns.get(pattern) {
                    subscribers.remove(&conn_ctx.id);
                }
            }
            // Cleanup watched keys
            for (db_idx, keys) in conn_ctx.watched_keys.iter() {
                for key in keys {
                    if let Some(mut clients) = server_ctx_cloned
                        .clients_ctx.watched_clients
                        .get_mut(&(*db_idx, key.clone()))
                    {
                        clients.remove(&conn_ctx.id);
                    }
                }
            }
            server_ctx_cloned.clients_ctx.client_watched_dirty.remove(&conn_ctx.id);
            server_ctx_cloned
                .clients_ctx.client_count
                .fetch_sub(1, Ordering::Relaxed);
            server_ctx_cloned.clients_ctx.clients.remove(&conn_ctx.id);
            server_ctx_cloned.clients_ctx.monitors.remove(&conn_ctx.id);
            server_ctx_cloned.repl.replicas.remove(&conn_ctx.id);
        });
    }
}
