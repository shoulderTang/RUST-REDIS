use std::sync::{Arc, RwLock};
use crate::db::Db;
use crate::conf::Config;
use crate::cmd::{self, scripting, ServerContext, process_frame};
use crate::resp::Resp;
use bytes::Bytes;

#[tokio::test]
async fn test_scan() {
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

    let mut conn_ctx = crate::cmd::ConnectionContext::new(0, None);
    conn_ctx.authenticated = true;

    // Create 100 keys
    for i in 0..100 {
        let req = Resp::Array(Some(vec![
            Resp::BulkString(Some(Bytes::from("SET"))),
            Resp::BulkString(Some(Bytes::from(format!("key:{:03}", i)))), // key:000, key:001... to ensure sort order matches creation slightly better, though not strictly required
            Resp::BulkString(Some(Bytes::from("val"))),
        ]));
        process_frame(req, &mut conn_ctx, &server_ctx).await;
    }

    // SCAN 0 COUNT 20
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SCAN"))),
        Resp::BulkString(Some(Bytes::from("0"))),
        Resp::BulkString(Some(Bytes::from("COUNT"))),
        Resp::BulkString(Some(Bytes::from("20"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    
    let mut cursor: usize;
    let mut keys_found = std::collections::HashSet::new();

    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            // Parse new cursor
            match &items[0] {
                Resp::BulkString(Some(b)) => {
                    let s = std::str::from_utf8(b).unwrap();
                    cursor = s.parse().unwrap();
                },
                _ => panic!("expected BulkString cursor"),
            }
            // Parse keys
            match &items[1] {
                Resp::Array(Some(keys)) => {
                    for key in keys {
                        match key {
                            Resp::BulkString(Some(b)) => {
                                keys_found.insert(b.clone());
                            },
                            _ => panic!("expected BulkString key"),
                        }
                    }
                },
                _ => panic!("expected Array of keys"),
            }
        },
        _ => panic!("expected Array result"),
    }

    // Continue scanning until cursor is 0
    while cursor != 0 {
         let req = Resp::Array(Some(vec![
            Resp::BulkString(Some(Bytes::from("SCAN"))),
            Resp::BulkString(Some(Bytes::from(cursor.to_string()))),
            Resp::BulkString(Some(Bytes::from("COUNT"))),
            Resp::BulkString(Some(Bytes::from("20"))),
        ]));
        let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
         match res {
            Resp::Array(Some(items)) => {
                match &items[0] {
                    Resp::BulkString(Some(b)) => {
                        let s = std::str::from_utf8(b).unwrap();
                        cursor = s.parse().unwrap();
                    },
                    _ => panic!("expected BulkString cursor"),
                }
                match &items[1] {
                    Resp::Array(Some(keys)) => {
                        for key in keys {
                             match key {
                                Resp::BulkString(Some(b)) => {
                                    keys_found.insert(b.clone());
                                },
                                _ => panic!("expected BulkString key"),
                            }
                        }
                    },
                    _ => panic!("expected Array of keys"),
                }
            },
            _ => panic!("expected Array result"),
        }
    }

    assert_eq!(keys_found.len(), 100);

    // Test MATCH
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SCAN"))),
        Resp::BulkString(Some(Bytes::from("0"))),
        Resp::BulkString(Some(Bytes::from("MATCH"))),
        Resp::BulkString(Some(Bytes::from("key:01*"))), // Should match key:010 to key:019
        Resp::BulkString(Some(Bytes::from("COUNT"))),
        Resp::BulkString(Some(Bytes::from("1000"))),
    ]));
    
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
             match &items[1] {
                Resp::Array(Some(keys)) => {
                    assert!(!keys.is_empty());
                    for key in keys {
                         match key {
                            Resp::BulkString(Some(b)) => {
                                let s = std::str::from_utf8(b).unwrap();
                                assert!(s.starts_with("key:01"));
                            },
                            _ => panic!("expected BulkString key"),
                        }
                    }
                },
                _ => panic!("expected Array of keys"),
            }
        },
        _ => panic!("expected Array result"),
    }
}

#[tokio::test]
async fn test_rename_renamenx_persist() {
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

    let mut conn_ctx = crate::cmd::ConnectionContext::new(0, None);
    conn_ctx.authenticated = true;

    // Helper to run command
    async fn run_cmd(args: Vec<&str>, conn: &mut crate::cmd::ConnectionContext, ctx: &ServerContext) -> Resp {
        let req_items: Vec<Resp> = args.iter().map(|s| Resp::BulkString(Some(Bytes::from(s.to_string())))).collect();
        let req = Resp::Array(Some(req_items));
        let (res, _) = process_frame(req, conn, ctx).await;
        res
    }

    // Test RENAME
    run_cmd(vec!["SET", "k1", "v1"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["RENAME", "k1", "k2"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    let res = run_cmd(vec!["GET", "k1"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::BulkString(None));

    let res = run_cmd(vec!["GET", "k2"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::BulkString(Some(Bytes::from("v1"))));

    // Test RENAME same key
    let res = run_cmd(vec!["RENAME", "k2", "k2"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    // Test RENAME non-existent key
    let res = run_cmd(vec!["RENAME", "nx", "k3"], &mut conn_ctx, &server_ctx).await;
    assert!(matches!(res, Resp::Error(_)));

    // Test RENAMENX
    run_cmd(vec!["SET", "k3", "v3"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["RENAMENX", "k2", "k3"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(0)); // k3 exists, should fail

    let res = run_cmd(vec!["RENAMENX", "k2", "k4"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(1)); // k4 doesn't exist, should succeed

    let res = run_cmd(vec!["GET", "k2"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::BulkString(None));
    let res = run_cmd(vec!["GET", "k4"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::BulkString(Some(Bytes::from("v1"))));

    // Test PERSIST
    run_cmd(vec!["SET", "k5", "v5", "EX", "100"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["TTL", "k5"], &mut conn_ctx, &server_ctx).await;
    if let Resp::Integer(ttl) = res {
        assert!(ttl > 0);
    } else {
        panic!("Expected Integer TTL");
    }

    let res = run_cmd(vec!["PERSIST", "k5"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(1));

    let res = run_cmd(vec!["TTL", "k5"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(-1)); // No expiry

    let res = run_cmd(vec!["PERSIST", "k5"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(0)); // Already persisted
}

#[tokio::test]
async fn test_keys() {
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

    let mut conn_ctx = crate::cmd::ConnectionContext::new(0, None);
    conn_ctx.authenticated = true;

    // Setup keys
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("key1"))),
        Resp::BulkString(Some(Bytes::from("val"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("key2"))),
        Resp::BulkString(Some(Bytes::from("val"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("other"))),
        Resp::BulkString(Some(Bytes::from("val"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // KEYS *
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("KEYS"))),
        Resp::BulkString(Some(Bytes::from("*"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 3);
        }
        _ => panic!("expected Array"),
    }

    // KEYS key*
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("KEYS"))),
        Resp::BulkString(Some(Bytes::from("key*"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
        }
        _ => panic!("expected Array"),
    }

    // KEYS ?ey?
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("KEYS"))),
        Resp::BulkString(Some(Bytes::from("?ey?"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
        }
        _ => panic!("expected Array"),
    }
}

#[tokio::test]
async fn test_expire_ttl() {
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

    let mut conn_ctx = crate::cmd::ConnectionContext::new(0, None);
    conn_ctx.authenticated = true;

    // SET key val
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
        Resp::BulkString(Some(Bytes::from("bar"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // TTL key -> -1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("TTL"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, -1),
        _ => panic!("expected Integer(-1)"),
    }

    // EXPIRE key 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("EXPIRE"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
        Resp::BulkString(Some(Bytes::from("1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // TTL key -> ~1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("TTL"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert!(i >= 0 && i <= 1),
        _ => panic!("expected Integer(>=0)"),
    }

    // Sleep 1.1s
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

    // GET key -> Nil
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(None) => {}
        _ => panic!("expected BulkString(None)"),
    }

    // TTL key -> -2
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("TTL"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, -2),
        _ => panic!("expected Integer(-2)"),
    }
}

#[tokio::test]
async fn test_dbsize() {
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

    let mut conn_ctx = crate::cmd::ConnectionContext::new(0, None);
    conn_ctx.authenticated = true;

    // DBSIZE -> 0
    let req = Resp::Array(Some(vec![Resp::BulkString(Some(Bytes::from("DBSIZE")))]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("expected Integer(0)"),
    }

    // SET key val
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
        Resp::BulkString(Some(Bytes::from("bar"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // DBSIZE -> 1
    let req = Resp::Array(Some(vec![Resp::BulkString(Some(Bytes::from("DBSIZE")))]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }
}

#[tokio::test]
async fn test_del() {
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

    let mut conn_ctx = crate::cmd::ConnectionContext::new(0, None);
    conn_ctx.authenticated = true;

    // Setup keys
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("MSET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
        Resp::BulkString(Some(Bytes::from("k2"))),
        Resp::BulkString(Some(Bytes::from("v2"))),
        Resp::BulkString(Some(Bytes::from("k3"))),
        Resp::BulkString(Some(Bytes::from("v3"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // DEL k1 k2 k_missing
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("DEL"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("k2"))),
        Resp::BulkString(Some(Bytes::from("k_missing"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;

    match res {
        Resp::Integer(i) => assert_eq!(i, 2),
        _ => panic!("expected Integer(2)"),
    }

    // Verify deletion
    assert!(!db[0].contains_key(&Bytes::from("k1")));
    assert!(!db[0].contains_key(&Bytes::from("k2")));
    assert!(db[0].contains_key(&Bytes::from("k3")));
}
