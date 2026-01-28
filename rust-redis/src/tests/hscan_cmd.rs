use crate::cmd::{process_frame, ConnectionContext, ServerContext};
use crate::cmd::scripting;
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::{Arc, RwLock};

#[tokio::test]
async fn test_hscan_basic() {
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

    // Prepare data: HMSET myhash field1 value1 field2 value2 ... field20 value20
    let mut args = vec![
        Resp::BulkString(Some(Bytes::from("HMSET"))),
        Resp::BulkString(Some(Bytes::from("myhash"))),
    ];
    for i in 1..=20 {
        args.push(Resp::BulkString(Some(Bytes::from(format!("field{}", i)))));
        args.push(Resp::BulkString(Some(Bytes::from(format!("value{}", i)))));
    }
    let req = Resp::Array(Some(args));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }

    // HSCAN myhash 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("HSCAN"))),
        Resp::BulkString(Some(Bytes::from("myhash"))),
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
                    assert_eq!(elements.len() % 2, 0); // Key-value pairs
                },
                _ => panic!("expected Array elements"),
            }
        },
        _ => panic!("expected Array response"),
    }
}

#[tokio::test]
async fn test_hscan_match() {
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
        Resp::BulkString(Some(Bytes::from("HMSET"))),
        Resp::BulkString(Some(Bytes::from("myhash"))),
        Resp::BulkString(Some(Bytes::from("aa"))),
        Resp::BulkString(Some(Bytes::from("1"))),
        Resp::BulkString(Some(Bytes::from("ab"))),
        Resp::BulkString(Some(Bytes::from("2"))),
        Resp::BulkString(Some(Bytes::from("ac"))),
        Resp::BulkString(Some(Bytes::from("3"))),
        Resp::BulkString(Some(Bytes::from("bb"))),
        Resp::BulkString(Some(Bytes::from("4"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // HSCAN myhash 0 MATCH a*
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("HSCAN"))),
        Resp::BulkString(Some(Bytes::from("myhash"))),
        Resp::BulkString(Some(Bytes::from("0"))),
        Resp::BulkString(Some(Bytes::from("MATCH"))),
        Resp::BulkString(Some(Bytes::from("a*"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            match &items[1] {
                Resp::Array(Some(elements)) => {
                    // Should find aa, ab, ac (6 elements total with values)
                    assert_eq!(elements.len(), 6);
                },
                _ => panic!("expected Array elements"),
            }
        },
        _ => panic!("expected Array response"),
    }
}

#[tokio::test]
async fn test_hscan_count() {
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
    let mut args = vec![
        Resp::BulkString(Some(Bytes::from("HMSET"))),
        Resp::BulkString(Some(Bytes::from("myhash"))),
    ];
    for i in 0..100 {
        args.push(Resp::BulkString(Some(Bytes::from(format!("k{}", i)))));
        args.push(Resp::BulkString(Some(Bytes::from(format!("v{}", i)))));
    }
    process_frame(Resp::Array(Some(args)), &mut conn_ctx, &server_ctx).await;

    // HSCAN myhash 0 COUNT 5
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("HSCAN"))),
        Resp::BulkString(Some(Bytes::from("myhash"))),
        Resp::BulkString(Some(Bytes::from("0"))),
        Resp::BulkString(Some(Bytes::from("COUNT"))),
        Resp::BulkString(Some(Bytes::from("5"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            match &items[1] {
                Resp::Array(Some(elements)) => {
                    // Should return around 5 items (10 elements)
                    // Note: Implementation sorts keys, so it's deterministic
                    assert_eq!(elements.len(), 10);
                },
                _ => panic!("expected Array elements"),
            }
        },
        _ => panic!("expected Array response"),
    }
}

#[tokio::test]
async fn test_hscan_wrong_type() {
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

    // SET string_key value
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("string_key"))),
        Resp::BulkString(Some(Bytes::from("value"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // HSCAN string_key 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("HSCAN"))),
        Resp::BulkString(Some(Bytes::from("string_key"))),
        Resp::BulkString(Some(Bytes::from("0"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(msg) => assert!(msg.contains("WRONGTYPE"), "Expected WRONGTYPE error, got: {}", msg),
        _ => panic!("expected WRONGTYPE error, got: {:?}", res),
    }
}
