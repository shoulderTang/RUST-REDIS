use crate::cmd::{ConnectionContext, ServerContext};
use crate::conf::Config;
use crate::db::Db;
use std::sync::{Arc, RwLock};
use dashmap::DashMap;

pub fn create_server_context() -> ServerContext {
    let db = Arc::new(vec![Db::default()]);
    let config = Config::default();
    let script_manager = crate::cmd::scripting::create_script_manager();
    let acl = Arc::new(RwLock::new(crate::acl::Acl::new()));

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
    }
}
