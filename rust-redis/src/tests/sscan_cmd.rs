use crate::cmd::{process_frame, ConnectionContext, ServerContext};
use crate::cmd::scripting;
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::{Arc, RwLock};

#[tokio::test]
async fn test_sscan_basic() {
    let db = Arc::new(vec![Db::default()]);
    let config = Config::default();
    let script_manager = scripting::create_script_manager();
    let acl = Arc::new(RwLock::new(crate::acl::Acl::new()));

    let server_ctx = ServerContext {
        databases: db.clone(),
        acl: acl,
        aof: None,
        config: Arc::new(config),
        script_manager: script_manager,
        blocking_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
        blocking_zset_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
        pubsub_channels: std::sync::Arc::new(dashmap::DashMap::new()),
        pubsub_patterns: std::sync::Arc::new(dashmap::DashMap::new()),
    };

    let mut conn_ctx = ConnectionContext {
        db_index: 0,
        authenticated: true,
        current_username: "default".to_string(),
        in_multi: false,
        multi_queue: Vec::new(),
        msg_sender: None,
        subscriptions: std::collections::HashSet::new(),
        psubscriptions: std::collections::HashSet::new(),
        id: 0,
    };

    // Prepare data: SADD myset member1 member2 ... member20
    let mut args = vec![
        Resp::BulkString(Some(Bytes::from("SADD"))),
        Resp::BulkString(Some(Bytes::from("myset"))),
    ];
    for i in 1..=20 {
        args.push(Resp::BulkString(Some(Bytes::from(format!("member{}", i)))));
    }
    let req = Resp::Array(Some(args));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 20),
        _ => panic!("expected Integer(20)"),
    }

    // SSCAN myset 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SSCAN"))),
        Resp::BulkString(Some(Bytes::from("myset"))),
        Resp::BulkString(Some(Bytes::from("0"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            // Check cursor
            match &items[0] {
                Resp::BulkString(Some(_)) => {},
                _ => panic!("expected BulkString cursor"),
            }
            // Check elements
            match &items[1] {
                Resp::Array(Some(elements)) => {
                    assert!(elements.len() > 0);
                },
                _ => panic!("expected Array elements"),
            }
        },
        _ => panic!("expected Array response"),
    }
}

#[tokio::test]
async fn test_sscan_match() {
    let db = Arc::new(vec![Db::default()]);
    let config = Config::default();
    let script_manager = scripting::create_script_manager();
    let acl = Arc::new(RwLock::new(crate::acl::Acl::new()));

    let server_ctx = ServerContext {
        databases: db.clone(),
        acl: acl,
        aof: None,
        config: Arc::new(config),
        script_manager: script_manager,
        blocking_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
        blocking_zset_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
        pubsub_channels: std::sync::Arc::new(dashmap::DashMap::new()),
        pubsub_patterns: std::sync::Arc::new(dashmap::DashMap::new()),
    };

    let mut conn_ctx = ConnectionContext {
        db_index: 0,
        authenticated: true,
        current_username: "default".to_string(),
        in_multi: false,
        multi_queue: Vec::new(),
        msg_sender: None,
        subscriptions: std::collections::HashSet::new(),
        psubscriptions: std::collections::HashSet::new(),
        id: 0,
    };

    // Prepare data
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SADD"))),
        Resp::BulkString(Some(Bytes::from("myset"))),
        Resp::BulkString(Some(Bytes::from("aa"))),
        Resp::BulkString(Some(Bytes::from("ab"))),
        Resp::BulkString(Some(Bytes::from("ac"))),
        Resp::BulkString(Some(Bytes::from("bb"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // SSCAN myset 0 MATCH a*
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SSCAN"))),
        Resp::BulkString(Some(Bytes::from("myset"))),
        Resp::BulkString(Some(Bytes::from("0"))),
        Resp::BulkString(Some(Bytes::from("MATCH"))),
        Resp::BulkString(Some(Bytes::from("a*"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            match &items[1] {
                Resp::Array(Some(elements)) => {
                    // Should find aa, ab, ac
                    assert_eq!(elements.len(), 3);
                },
                _ => panic!("expected Array elements"),
            }
        },
        _ => panic!("expected Array response"),
    }
}
