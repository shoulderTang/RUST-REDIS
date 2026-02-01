use crate::resp::Resp;
use crate::tests::helper::run_cmd;
use bytes::Bytes;

#[tokio::test]
async fn test_move() {
    // Create a server context with 2 databases
    let server_ctx = crate::tests::helper::create_server_context();
    let cfg = crate::conf::Config::default();
    let mut dbs = Vec::new();
    for _ in 0..2 {
        dbs.push(std::sync::RwLock::new(crate::db::Db::default()));
    }
    let server_ctx = crate::cmd::ServerContext {
        databases: std::sync::Arc::new(dbs),
        acl: server_ctx.acl.clone(),
        aof: None,
        config: std::sync::Arc::new(cfg),
        script_manager: server_ctx.script_manager.clone(),
        blocking_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
        blocking_zset_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
        pubsub_channels: std::sync::Arc::new(dashmap::DashMap::new()),
        pubsub_patterns: std::sync::Arc::new(dashmap::DashMap::new()),
        run_id: "test".to_string(),
        start_time: std::time::Instant::now(),
        client_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        blocked_client_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        clients: std::sync::Arc::new(dashmap::DashMap::new()),
        monitors: std::sync::Arc::new(dashmap::DashMap::new()),
        slowlog: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::new())),
        slowlog_next_id: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1)),
        slowlog_max_len: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(128)),
        slowlog_threshold_us: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(10_000)),
        mem_peak_rss: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        maxmemory: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        notify_keyspace_events: std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0)),
        rdbcompression: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
        rdbchecksum: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
        stop_writes_on_bgsave_error: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
        maxmemory_policy: std::sync::Arc::new(std::sync::RwLock::new(crate::conf::EvictionPolicy::NoEviction)),
        maxmemory_samples: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(5)),
        save_params: std::sync::Arc::new(std::sync::RwLock::new(vec![(3600, 1), (300, 100), (60, 10000)])),
        last_bgsave_ok: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
        dirty: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        last_save_time: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0)),
        watched_clients: std::sync::Arc::new(dashmap::DashMap::new()),
        client_watched_dirty: std::sync::Arc::new(dashmap::DashMap::new()),
        tracking_clients: std::sync::Arc::new(dashmap::DashMap::new()),
        acl_log: std::sync::Arc::new(std::sync::RwLock::new(std::collections::VecDeque::new())),
        latency_events: std::sync::Arc::new(dashmap::DashMap::new()),
    };

    let mut conn = crate::tests::helper::create_connection_context();
    conn.db_index = 0;

    // 1. SET key in DB 0
    run_cmd(vec!["SET", "foo", "bar"], &mut conn, &server_ctx).await;
    
    // 2. MOVE to DB 1
    let res = run_cmd(vec!["MOVE", "foo", "1"], &mut conn, &server_ctx).await;
    assert_eq!(res, Resp::Integer(1));

    // 3. Verify DB 0 is empty
    let res = run_cmd(vec!["EXISTS", "foo"], &mut conn, &server_ctx).await;
    assert_eq!(res, Resp::Integer(0));

    // 4. Verify DB 1 has the key
    conn.db_index = 1;
    let res = run_cmd(vec!["GET", "foo"], &mut conn, &server_ctx).await;
    assert_eq!(res, Resp::BulkString(Some(Bytes::from("bar"))));
}

#[tokio::test]
async fn test_swapdb() {
    let cfg = crate::conf::Config::default();
    let mut dbs = Vec::new();
    for _ in 0..2 {
        dbs.push(std::sync::RwLock::new(crate::db::Db::default()));
    }
    let server_ctx = crate::cmd::ServerContext {
        databases: std::sync::Arc::new(dbs),
        acl: std::sync::Arc::new(std::sync::RwLock::new(crate::acl::Acl::new())),
        aof: None,
        config: std::sync::Arc::new(cfg),
        script_manager: crate::cmd::scripting::create_script_manager(),
        blocking_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
        blocking_zset_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
        pubsub_channels: std::sync::Arc::new(dashmap::DashMap::new()),
        pubsub_patterns: std::sync::Arc::new(dashmap::DashMap::new()),
        run_id: "test".to_string(),
        start_time: std::time::Instant::now(),
        client_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        blocked_client_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        clients: std::sync::Arc::new(dashmap::DashMap::new()),
        monitors: std::sync::Arc::new(dashmap::DashMap::new()),
        slowlog: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::new())),
        slowlog_next_id: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1)),
        slowlog_max_len: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(128)),
        slowlog_threshold_us: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(10_000)),
        mem_peak_rss: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        maxmemory: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        notify_keyspace_events: std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0)),
        rdbcompression: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
        rdbchecksum: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
        stop_writes_on_bgsave_error: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
        maxmemory_policy: std::sync::Arc::new(std::sync::RwLock::new(crate::conf::EvictionPolicy::NoEviction)),
        maxmemory_samples: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(5)),
        save_params: std::sync::Arc::new(std::sync::RwLock::new(vec![(3600, 1), (300, 100), (60, 10000)])),
        last_bgsave_ok: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
        dirty: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        last_save_time: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0)),
        watched_clients: std::sync::Arc::new(dashmap::DashMap::new()),
        client_watched_dirty: std::sync::Arc::new(dashmap::DashMap::new()),
        tracking_clients: std::sync::Arc::new(dashmap::DashMap::new()),
        acl_log: std::sync::Arc::new(std::sync::RwLock::new(std::collections::VecDeque::new())),
        latency_events: std::sync::Arc::new(dashmap::DashMap::new()),
    };

    let mut conn = crate::tests::helper::create_connection_context();
    
    // 1. SET in DB 0 and DB 1
    conn.db_index = 0;
    run_cmd(vec!["SET", "db0_key", "val0"], &mut conn, &server_ctx).await;
    conn.db_index = 1;
    run_cmd(vec!["SET", "db1_key", "val1"], &mut conn, &server_ctx).await;

    // 2. SWAPDB 0 1
    let res = run_cmd(vec!["SWAPDB", "0", "1"], &mut conn, &server_ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    // 3. Verify DB 0 now has DB 1's key
    conn.db_index = 0;
    let res = run_cmd(vec!["GET", "db1_key"], &mut conn, &server_ctx).await;
    assert_eq!(res, Resp::BulkString(Some(Bytes::from("val1"))));
    let res = run_cmd(vec!["EXISTS", "db0_key"], &mut conn, &server_ctx).await;
    assert_eq!(res, Resp::Integer(0));

    // 4. Verify DB 1 now has DB 0's key
    conn.db_index = 1;
    let res = run_cmd(vec!["GET", "db0_key"], &mut conn, &server_ctx).await;
    assert_eq!(res, Resp::BulkString(Some(Bytes::from("val0"))));
}
