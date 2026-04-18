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
        pubsub: std::sync::Arc::new(crate::cmd::PubSubCtx::new()),
        repl: std::sync::Arc::new(crate::cmd::ReplicationCtx::new(
            "test".to_string(), 1024, 1, 60, true, 0, 10, false, 5,
        )),
        start_time: std::time::Instant::now(),
        clients_ctx: std::sync::Arc::new(crate::cmd::ClientCtx::new()),
        slowlog: std::sync::Arc::new(crate::cmd::SlowLogCtx::new(128, 10_000)),
        mem: std::sync::Arc::new(crate::cmd::MemoryCtx::new(
            0,
            crate::conf::EvictionPolicy::NoEviction,
            5,
            0,
        )),
        persist: std::sync::Arc::new(crate::cmd::PersistenceCtx::new(
            true, true, true,
            vec![(3600, 1), (300, 100), (60, 10000)],
            0,
        )),
        cluster_ctx: std::sync::Arc::new(crate::cmd::ClusterCtx::new(
            std::sync::Arc::new(std::sync::RwLock::new(crate::cluster::ClusterState::new(
                crate::cluster::NodeId("test".to_string()),
                "127.0.0.1".to_string(),
                6380,
            )))
        )),
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
        acl: std::sync::Arc::new(arc_swap::ArcSwap::from_pointee(crate::acl::Acl::new())),
        aof: None,
        config: std::sync::Arc::new(cfg),
        script_manager: crate::cmd::scripting::create_script_manager(),
        blocking_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
        blocking_zset_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
        pubsub: std::sync::Arc::new(crate::cmd::PubSubCtx::new()),
        repl: std::sync::Arc::new(crate::cmd::ReplicationCtx::new(
            "test".to_string(), 1024, 1, 60, true, 0, 10, false, 5,
        )),
        start_time: std::time::Instant::now(),
        clients_ctx: std::sync::Arc::new(crate::cmd::ClientCtx::new()),
        slowlog: std::sync::Arc::new(crate::cmd::SlowLogCtx::new(128, 10_000)),
        mem: std::sync::Arc::new(crate::cmd::MemoryCtx::new(
            0,
            crate::conf::EvictionPolicy::NoEviction,
            5,
            0,
        )),
        persist: std::sync::Arc::new(crate::cmd::PersistenceCtx::new(
            true, true, true,
            vec![(3600, 1), (300, 100), (60, 10000)],
            0,
        )),
        cluster_ctx: std::sync::Arc::new(crate::cmd::ClusterCtx::new(
            std::sync::Arc::new(std::sync::RwLock::new(crate::cluster::ClusterState::new(
                crate::cluster::NodeId("test".to_string()),
                "127.0.0.1".to_string(),
                6380,
            )))
        )),
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
