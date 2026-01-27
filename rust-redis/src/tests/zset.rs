use crate::cmd::process_frame;
use crate::cmd::scripting;
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::Arc;

#[tokio::test]
async fn test_zset_ops() {
    let db = Arc::new(vec![Db::default()]);
    let cfg = Arc::new(Config::default());
    let acl = Arc::new(std::sync::RwLock::new(crate::acl::Acl::new()));
    let mut conn_ctx = crate::cmd::ConnectionContext {
        db_index: 0,
        authenticated: true,
        current_username: "default".to_string(),
    };
    let server_ctx = crate::cmd::ServerContext {
        databases: db.clone(),
        acl: acl.clone(),
        aof: None,
        config: cfg.clone(),
        script_manager: scripting::create_script_manager(),
        blocking_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
        blocking_zset_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
    };

    // ZADD zset 1 m1 2 m2 -> 2
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZADD"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
        Resp::BulkString(Some(Bytes::from("1"))),
        Resp::BulkString(Some(Bytes::from("m1"))),
        Resp::BulkString(Some(Bytes::from("2"))),
        Resp::BulkString(Some(Bytes::from("m2"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 2),
        _ => panic!("expected Integer(2)"),
    }

    // ZSCORE zset m1 -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZSCORE"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
        Resp::BulkString(Some(Bytes::from("m1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("1")),
        _ => panic!("expected BulkString(1)"),
    }

    // ZRANK zset m1 -> 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZRANK"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
        Resp::BulkString(Some(Bytes::from("m1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("expected Integer(0)"),
    }

    // ZRANGE zset 0 -1 -> [m1, m2]
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZRANGE"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
        Resp::BulkString(Some(Bytes::from("0"))),
        Resp::BulkString(Some(Bytes::from("-1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("m1")),
                _ => panic!("expected BulkString(m1)"),
            }
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("m2")),
                _ => panic!("expected BulkString(m2)"),
            }
        }
        _ => panic!("expected Array"),
    }

    // ZRANGE zset 0 -1 WITHSCORES -> [m1, 1, m2, 2]
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZRANGE"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
        Resp::BulkString(Some(Bytes::from("0"))),
        Resp::BulkString(Some(Bytes::from("-1"))),
        Resp::BulkString(Some(Bytes::from("WITHSCORES"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 4);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("m1")),
                _ => panic!("expected BulkString(m1)"),
            }
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("1")),
                _ => panic!("expected BulkString(1)"),
            }
            match &items[2] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("m2")),
                _ => panic!("expected BulkString(m2)"),
            }
            match &items[3] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("2")),
                _ => panic!("expected BulkString(2)"),
            }
        }
        _ => panic!("expected Array"),
    }

    // ZCARD zset -> 2
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZCARD"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 2),
        _ => panic!("expected Integer(2)"),
    }

    // ZREM zset m1 -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZREM"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
        Resp::BulkString(Some(Bytes::from("m1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // ZCARD zset -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZCARD"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }
}

#[tokio::test]
async fn test_zpopmin_ops() {
    let db = Arc::new(vec![Db::default()]);
    let cfg = Arc::new(Config::default());
    let acl = Arc::new(std::sync::RwLock::new(crate::acl::Acl::new()));
    let mut conn_ctx = crate::cmd::ConnectionContext {
        db_index: 0,
        authenticated: true,
        current_username: "default".to_string(),
    };
    let server_ctx = crate::cmd::ServerContext {
        databases: db.clone(),
        acl: acl.clone(),
        aof: None,
        config: cfg.clone(),
        script_manager: scripting::create_script_manager(),
        blocking_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
        blocking_zset_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
    };

    // ZADD zset 1 m1 2 m2 3 m3
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZADD"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
        Resp::BulkString(Some(Bytes::from("1"))),
        Resp::BulkString(Some(Bytes::from("m1"))),
        Resp::BulkString(Some(Bytes::from("2"))),
        Resp::BulkString(Some(Bytes::from("m2"))),
        Resp::BulkString(Some(Bytes::from("3"))),
        Resp::BulkString(Some(Bytes::from("m3"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // ZPOPMIN zset 2 -> [m1, 1, m2, 2]
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZPOPMIN"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
        Resp::BulkString(Some(Bytes::from("2"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 4);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("m1")),
                _ => panic!("expected m1"),
            }
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("1")),
                _ => panic!("expected 1"),
            }
            match &items[2] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("m2")),
                _ => panic!("expected m2"),
            }
            match &items[3] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("2")),
                _ => panic!("expected 2"),
            }
        }
        _ => panic!("expected Array"),
    }
}

#[tokio::test]
async fn test_bzpopmin_ops() {
    let db = Arc::new(vec![Db::default()]);
    let cfg = Arc::new(Config::default());
    let acl = Arc::new(std::sync::RwLock::new(crate::acl::Acl::new()));
    let server_ctx = crate::cmd::ServerContext {
        databases: db.clone(),
        acl: acl.clone(),
        aof: None,
        config: cfg.clone(),
        script_manager: scripting::create_script_manager(),
        blocking_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
        blocking_zset_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
    };

    // 1. Blocking wait
    let server_ctx_clone = server_ctx.clone();
    let handle = tokio::spawn(async move {
        let mut conn_ctx = crate::cmd::ConnectionContext {
            db_index: 0,
            authenticated: true,
            current_username: "default".to_string(),
        };
        let req = Resp::Array(Some(vec![
            Resp::BulkString(Some(Bytes::from("BZPOPMIN"))),
            Resp::BulkString(Some(Bytes::from("zset_block"))),
            Resp::BulkString(Some(Bytes::from("0"))),
        ]));
        let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx_clone).await;
        res
    });

    // Wait a bit to ensure blocking
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Push data
    let mut conn_ctx = crate::cmd::ConnectionContext {
        db_index: 0,
        authenticated: true,
        current_username: "default".to_string(),
    };
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZADD"))),
        Resp::BulkString(Some(Bytes::from("zset_block"))),
        Resp::BulkString(Some(Bytes::from("10"))),
        Resp::BulkString(Some(Bytes::from("m_block"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // Check result
    let res = handle.await.unwrap();
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 3);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("zset_block")),
                _ => panic!("expected zset_block"),
            }
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("m_block")),
                _ => panic!("expected m_block"),
            }
            match &items[2] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("10")),
                _ => panic!("expected 10"),
            }
        }
        _ => panic!("expected Array"),
    }
}

#[tokio::test]
async fn test_zpopmax_ops() {
    let db = Arc::new(vec![Db::default()]);
    let cfg = Arc::new(Config::default());
    let acl = Arc::new(std::sync::RwLock::new(crate::acl::Acl::new()));
    let mut conn_ctx = crate::cmd::ConnectionContext {
        db_index: 0,
        authenticated: true,
        current_username: "default".to_string(),
    };
    let server_ctx = crate::cmd::ServerContext {
        databases: db.clone(),
        acl: acl.clone(),
        aof: None,
        config: cfg.clone(),
        script_manager: scripting::create_script_manager(),
        blocking_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
        blocking_zset_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
    };

    // ZADD zset 1 m1 2 m2 3 m3
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZADD"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
        Resp::BulkString(Some(Bytes::from("1"))),
        Resp::BulkString(Some(Bytes::from("m1"))),
        Resp::BulkString(Some(Bytes::from("2"))),
        Resp::BulkString(Some(Bytes::from("m2"))),
        Resp::BulkString(Some(Bytes::from("3"))),
        Resp::BulkString(Some(Bytes::from("m3"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // ZPOPMAX zset 2 -> [m3, 3, m2, 2]
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZPOPMAX"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
        Resp::BulkString(Some(Bytes::from("2"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 4);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("m3")),
                _ => panic!("expected m3"),
            }
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("3")),
                _ => panic!("expected 3"),
            }
            match &items[2] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("m2")),
                _ => panic!("expected m2"),
            }
            match &items[3] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("2")),
                _ => panic!("expected 2"),
            }
        }
        _ => panic!("expected Array"),
    }
}

#[tokio::test]
async fn test_bzpopmax_ops() {
    let db = Arc::new(vec![Db::default()]);
    let cfg = Arc::new(Config::default());
    let acl = Arc::new(std::sync::RwLock::new(crate::acl::Acl::new()));
    let server_ctx = crate::cmd::ServerContext {
        databases: db.clone(),
        acl: acl.clone(),
        aof: None,
        config: cfg.clone(),
        script_manager: scripting::create_script_manager(),
        blocking_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
        blocking_zset_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
    };

    // 1. Blocking wait
    let server_ctx_clone = server_ctx.clone();
    let handle = tokio::spawn(async move {
        let mut conn_ctx = crate::cmd::ConnectionContext {
            db_index: 0,
            authenticated: true,
            current_username: "default".to_string(),
        };
        let req = Resp::Array(Some(vec![
            Resp::BulkString(Some(Bytes::from("BZPOPMAX"))),
            Resp::BulkString(Some(Bytes::from("zset_block_max"))),
            Resp::BulkString(Some(Bytes::from("0"))),
        ]));
        let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx_clone).await;
        res
    });

    // Wait a bit to ensure blocking
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Push data
    let mut conn_ctx = crate::cmd::ConnectionContext {
        db_index: 0,
        authenticated: true,
        current_username: "default".to_string(),
    };
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZADD"))),
        Resp::BulkString(Some(Bytes::from("zset_block_max"))),
        Resp::BulkString(Some(Bytes::from("10"))),
        Resp::BulkString(Some(Bytes::from("m_block_max"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // Check result
    let res = handle.await.unwrap();
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 3);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("zset_block_max")),
                _ => panic!("expected zset_block_max"),
            }
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("m_block_max")),
                _ => panic!("expected m_block_max"),
            }
            match &items[2] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("10")),
                _ => panic!("expected 10"),
            }
        }
        _ => panic!("expected Array"),
    }
}
