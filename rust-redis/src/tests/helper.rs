use crate::cmd::{ConnectionContext, ServerContext, process_frame};
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::{Arc, RwLock};
use dashmap::DashMap;

use rand::Rng;

pub async fn run_cmd(args: Vec<&str>, conn_ctx: &mut ConnectionContext, server_ctx: &ServerContext) -> Resp {
    let mut resp_args = Vec::new();
    for arg in args {
        resp_args.push(Resp::BulkString(Some(Bytes::from(arg.to_string()))));
    }
    let req = Resp::Array(Some(resp_args));
    let (res, _) = process_frame(req, conn_ctx, server_ctx).await;
    res
}

pub fn create_server_context() -> ServerContext {
    let mut dbs = Vec::new();
    for _ in 0..16 {
        dbs.push(RwLock::new(Db::default()));
    }
    let db = Arc::new(dbs);
    let config = Config::default();
    let script_manager = crate::cmd::scripting::create_script_manager();
    let acl = Arc::new(RwLock::new(crate::acl::Acl::new()));
    
    let mut rng = rand::rng();
    let run_id: String = (0..40).map(|_| rng.sample(rand::distr::Alphanumeric) as char).collect();

    let save_params = config.save_params.clone();
    let maxmemory_policy = config.maxmemory_policy;
    let maxmemory_samples = config.maxmemory_samples;
    let node_id = crate::cluster::NodeId(run_id.clone());
    let cluster_state = Arc::new(RwLock::new(crate::cluster::ClusterState::new(node_id, config.bind.clone(), config.port)));
    ServerContext {
        databases: db,
        acl: acl,
        aof: None,
        config: Arc::new(config),
        script_manager: script_manager,
        blocking_waiters: Arc::new(DashMap::new()),
        blocking_zset_waiters: Arc::new(DashMap::new()),
        pubsub_channels: Arc::new(DashMap::new()),
        pubsub_patterns: Arc::new(DashMap::new()),
        run_id: Arc::new(RwLock::new(run_id)),
        replid2: Arc::new(RwLock::new("0000000000000000000000000000000000000000".to_string())),
        second_repl_offset: Arc::new(std::sync::atomic::AtomicI64::new(-1)),
        start_time: std::time::Instant::now(),
        client_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        blocked_client_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        clients: Arc::new(DashMap::new()),
        monitors: Arc::new(DashMap::new()),
        replicas: Arc::new(DashMap::new()),
        repl_backlog: Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new())),
        repl_backlog_size: Arc::new(std::sync::atomic::AtomicUsize::new(1024)),
        repl_ping_replica_period: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        repl_timeout: Arc::new(std::sync::atomic::AtomicU64::new(60)),
        repl_offset: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        replica_ack: Arc::new(DashMap::new()),
        replica_ack_time: Arc::new(DashMap::new()),
        replica_listening_port: Arc::new(DashMap::new()),
        slowlog: Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::new())),
        slowlog_next_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        slowlog_max_len: Arc::new(std::sync::atomic::AtomicUsize::new(128)),
        slowlog_threshold_us: Arc::new(std::sync::atomic::AtomicI64::new(10_000)),
        mem_peak_rss: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        maxmemory: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        notify_keyspace_events: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        rdbcompression: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        rdbchecksum: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        stop_writes_on_bgsave_error: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        replica_read_only: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        min_replicas_to_write: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        min_replicas_max_lag: Arc::new(std::sync::atomic::AtomicU64::new(10)),
        repl_diskless_sync: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        repl_diskless_sync_delay: Arc::new(std::sync::atomic::AtomicU64::new(5)),
        maxmemory_policy: Arc::new(RwLock::new(maxmemory_policy)),
        maxmemory_samples: Arc::new(std::sync::atomic::AtomicUsize::new(maxmemory_samples)),
        save_params: Arc::new(RwLock::new(save_params)),
        last_bgsave_ok: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        dirty: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        last_save_time: Arc::new(std::sync::atomic::AtomicI64::new(0)),
        watched_clients: Arc::new(DashMap::new()),
        client_watched_dirty: Arc::new(DashMap::new()),
        tracking_clients: Arc::new(DashMap::new()),
        acl_log: Arc::new(RwLock::new(std::collections::VecDeque::new())),
        latency_events: Arc::new(DashMap::new()),
        replication_role: Arc::new(RwLock::new(crate::cmd::ReplicationRole::Master)),
        master_host: Arc::new(RwLock::new(None)),
        master_port: Arc::new(RwLock::new(None)),
        repl_waiters: Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new())),
        rdb_child_pid: Arc::new(std::sync::atomic::AtomicI32::new(-1)),
        rdb_sync_client_id: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        master_link_established: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        cluster: cluster_state,
        node_conns: Arc::new(DashMap::new()),
    }
}

pub fn create_connection_context() -> ConnectionContext {
    let mut ctx = ConnectionContext::new(0, None, None, None);
    ctx.authenticated = true;
    ctx
}

pub fn create_server_context_with_cluster() -> ServerContext {
    let mut dbs = Vec::new();
    for _ in 0..16 {
        dbs.push(RwLock::new(Db::default()));
    }
    let db = Arc::new(dbs);
    let mut cfg = Config::default();
    cfg.cluster_enabled = true;
    let script_manager = crate::cmd::scripting::create_script_manager();
    let acl = Arc::new(RwLock::new(crate::acl::Acl::new()));
    
    let mut rng = rand::rng();
    let run_id: String = (0..40).map(|_| rng.sample(rand::distr::Alphanumeric) as char).collect();
    // Put cluster config into a per-test temp directory to avoid clutter
    let tmp_dir = std::env::temp_dir().join(format!("rust-redis-tests-{}", run_id));
    let _ = std::fs::create_dir_all(&tmp_dir);
    cfg.dir = tmp_dir.to_string_lossy().into_owned();
    cfg.cluster_config_file = "node.conf".to_string();
    let node_id = crate::cluster::NodeId(run_id.clone());
    let cluster_state = Arc::new(RwLock::new(crate::cluster::ClusterState::new(node_id, cfg.bind.clone(), cfg.port)));
    
    let save_params = cfg.save_params.clone();
    let maxmemory_policy = cfg.maxmemory_policy;
    let maxmemory_samples = cfg.maxmemory_samples;
    ServerContext {
        databases: db,
        acl: acl,
        aof: None,
        config: Arc::new(cfg),
        script_manager: script_manager,
        blocking_waiters: Arc::new(DashMap::new()),
        blocking_zset_waiters: Arc::new(DashMap::new()),
        pubsub_channels: Arc::new(DashMap::new()),
        pubsub_patterns: Arc::new(DashMap::new()),
        run_id: Arc::new(RwLock::new(run_id)),
        replid2: Arc::new(RwLock::new("0000000000000000000000000000000000000000".to_string())),
        second_repl_offset: Arc::new(std::sync::atomic::AtomicI64::new(-1)),
        start_time: std::time::Instant::now(),
        client_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        blocked_client_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        clients: Arc::new(DashMap::new()),
        monitors: Arc::new(DashMap::new()),
        replicas: Arc::new(DashMap::new()),
        repl_backlog: Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new())),
        repl_backlog_size: Arc::new(std::sync::atomic::AtomicUsize::new(1024)),
        repl_ping_replica_period: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        repl_timeout: Arc::new(std::sync::atomic::AtomicU64::new(60)),
        repl_offset: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        replica_ack: Arc::new(DashMap::new()),
        replica_ack_time: Arc::new(DashMap::new()),
        replica_listening_port: Arc::new(DashMap::new()),
        slowlog: Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::new())),
        slowlog_next_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        slowlog_max_len: Arc::new(std::sync::atomic::AtomicUsize::new(128)),
        slowlog_threshold_us: Arc::new(std::sync::atomic::AtomicI64::new(10_000)),
        mem_peak_rss: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        maxmemory: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        notify_keyspace_events: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        rdbcompression: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        rdbchecksum: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        stop_writes_on_bgsave_error: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        replica_read_only: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        min_replicas_to_write: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        min_replicas_max_lag: Arc::new(std::sync::atomic::AtomicU64::new(10)),
        repl_diskless_sync: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        repl_diskless_sync_delay: Arc::new(std::sync::atomic::AtomicU64::new(5)),
        maxmemory_policy: Arc::new(RwLock::new(maxmemory_policy)),
        maxmemory_samples: Arc::new(std::sync::atomic::AtomicUsize::new(maxmemory_samples)),
        save_params: Arc::new(RwLock::new(save_params)),
        last_bgsave_ok: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        dirty: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        last_save_time: Arc::new(std::sync::atomic::AtomicI64::new(0)),
        watched_clients: Arc::new(DashMap::new()),
        client_watched_dirty: Arc::new(DashMap::new()),
        tracking_clients: Arc::new(DashMap::new()),
        acl_log: Arc::new(RwLock::new(std::collections::VecDeque::new())),
        latency_events: Arc::new(DashMap::new()),
        replication_role: Arc::new(RwLock::new(crate::cmd::ReplicationRole::Master)),
        master_host: Arc::new(RwLock::new(None)),
        master_port: Arc::new(RwLock::new(None)),
        repl_waiters: Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new())),
        rdb_child_pid: Arc::new(std::sync::atomic::AtomicI32::new(-1)),
        rdb_sync_client_id: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        master_link_established: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        cluster: cluster_state,
        node_conns: Arc::new(DashMap::new()),
    }
}
