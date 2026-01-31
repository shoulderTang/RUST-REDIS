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
    let db = Arc::new(vec![RwLock::new(Db::default())]);
    let config = Config::default();
    let script_manager = crate::cmd::scripting::create_script_manager();
    let acl = Arc::new(RwLock::new(crate::acl::Acl::new()));
    
    let mut rng = rand::rng();
    let run_id: String = (0..40).map(|_| rng.sample(rand::distr::Alphanumeric) as char).collect();

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
        run_id,
        start_time: std::time::Instant::now(),
        client_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        blocked_client_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        clients: Arc::new(DashMap::new()),
        monitors: Arc::new(DashMap::new()),
        slowlog: Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::new())),
        slowlog_next_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        slowlog_max_len: Arc::new(std::sync::atomic::AtomicUsize::new(128)),
        slowlog_threshold_us: Arc::new(std::sync::atomic::AtomicI64::new(10_000)),
        mem_peak_rss: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        maxmemory: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        watched_clients: Arc::new(DashMap::new()),
        client_watched_dirty: Arc::new(DashMap::new()),
    }
}

pub fn create_connection_context() -> ConnectionContext {
    let mut ctx = ConnectionContext::new(0, None, None);
    ctx.authenticated = true;
    ctx
}
