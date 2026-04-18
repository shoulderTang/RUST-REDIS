use crate::cmd::{ConnectionContext, ServerContext, process_frame};
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::{Arc, RwLock};

use rand::Rng;

pub async fn run_cmd(
    args: Vec<&str>,
    conn_ctx: &mut ConnectionContext,
    server_ctx: &ServerContext,
) -> Resp {
    let mut resp_args = Vec::new();
    for arg in args {
        resp_args.push(Resp::BulkString(Some(Bytes::from(arg.to_string()))));
    }
    let req = Resp::Array(Some(resp_args));
    let (res, _) = process_frame(req, conn_ctx, server_ctx).await;
    // Normalize StaticError -> Error so tests written against Resp::Error(String) keep working.
    // In production the response goes over the wire and is decoded as Resp::Error(String) anyway.
    match res {
        Resp::StaticError(s) => Resp::Error(s.to_string()),
        other => other,
    }
}

pub fn create_server_context() -> ServerContext {
    let mut dbs = Vec::new();
    for _ in 0..16 {
        dbs.push(RwLock::new(Db::default()));
    }
    let db = Arc::new(dbs);
    let config = Config::default();
    let script_manager = crate::cmd::scripting::create_script_manager();
    let acl = Arc::new(arc_swap::ArcSwap::from_pointee(crate::acl::Acl::new()));

    let mut rng = rand::rng();
    let run_id: String = (0..40)
        .map(|_| rng.sample(rand::distr::Alphanumeric) as char)
        .collect();

    let save_params = config.save_params.clone();
    let maxmemory_policy = config.maxmemory_policy;
    let maxmemory_samples = config.maxmemory_samples;
    let node_id = crate::cluster::NodeId(run_id.clone());
    let cluster_state = Arc::new(RwLock::new(crate::cluster::ClusterState::new(
        node_id,
        config.bind.clone(),
        config.port,
    )));
    ServerContext {
        databases: db,
        acl: acl,
        aof: None,
        config: Arc::new(config),
        script_manager: script_manager,
        blocking_waiters: Arc::new(DashMap::new()),
        blocking_zset_waiters: Arc::new(DashMap::new()),
        pubsub: Arc::new(crate::cmd::PubSubCtx::new()),
        repl: Arc::new(crate::cmd::ReplicationCtx::new(
            run_id, 1024, 1, 60, true, 0, 10, false, 5,
        )),
        start_time: std::time::Instant::now(),
        clients_ctx: Arc::new(crate::cmd::ClientCtx::new()),
        slowlog: Arc::new(crate::cmd::SlowLogCtx::new(128, 10_000)),
        mem: Arc::new(crate::cmd::MemoryCtx::new(0, maxmemory_policy, maxmemory_samples, 0)),
        persist: Arc::new(crate::cmd::PersistenceCtx::new(true, true, true, save_params, 0)),
        cluster_ctx: Arc::new(crate::cmd::ClusterCtx::new(cluster_state)),
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
    let acl = Arc::new(arc_swap::ArcSwap::from_pointee(crate::acl::Acl::new()));

    let mut rng = rand::rng();
    let run_id: String = (0..40)
        .map(|_| rng.sample(rand::distr::Alphanumeric) as char)
        .collect();
    // Put cluster config into a per-test temp directory to avoid clutter
    let tmp_dir = std::env::temp_dir().join(format!("rust-redis-tests-{}", run_id));
    let _ = std::fs::create_dir_all(&tmp_dir);
    cfg.dir = tmp_dir.to_string_lossy().into_owned();
    cfg.cluster_config_file = "node.conf".to_string();
    let node_id = crate::cluster::NodeId(run_id.clone());
    let cluster_state = Arc::new(RwLock::new(crate::cluster::ClusterState::new(
        node_id,
        cfg.bind.clone(),
        cfg.port,
    )));

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
        pubsub: Arc::new(crate::cmd::PubSubCtx::new()),
        repl: Arc::new(crate::cmd::ReplicationCtx::new(
            run_id, 1024, 1, 60, true, 0, 10, false, 5,
        )),
        start_time: std::time::Instant::now(),
        clients_ctx: Arc::new(crate::cmd::ClientCtx::new()),
        slowlog: Arc::new(crate::cmd::SlowLogCtx::new(128, 10_000)),
        mem: Arc::new(crate::cmd::MemoryCtx::new(0, maxmemory_policy, maxmemory_samples, 0)),
        persist: Arc::new(crate::cmd::PersistenceCtx::new(true, true, true, save_params, 0)),
        cluster_ctx: Arc::new(crate::cmd::ClusterCtx::new(cluster_state)),
    }
}
