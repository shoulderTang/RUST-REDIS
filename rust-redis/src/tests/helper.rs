use crate::cmd::{ConnectionContext, ServerContext};
use crate::conf::Config;
use crate::db::Db;
use std::sync::{Arc, RwLock};
use dashmap::DashMap;

use rand::Rng;

pub fn create_server_context() -> ServerContext {
    let db = Arc::new(vec![Db::default()]);
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
    }
}

pub fn create_connection_context() -> ConnectionContext {
    ConnectionContext {
        db_index: 0,
        authenticated: true,
        current_username: "default".to_string(),
        in_multi: false,
        multi_queue: Vec::new(),
        msg_sender: None,
        subscriptions: std::collections::HashSet::new(),
        psubscriptions: std::collections::HashSet::new(),
        id: 0,
        shutdown: None,
    }
}
