use crate::cmd::{process_frame, ConnectionContext, ServerContext};
use crate::cmd::scripting;
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::{Arc, RwLock};

#[tokio::test]
async fn test_list_ops() {
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
    };

    let mut conn_ctx = ConnectionContext {
        db_index: 0,
        authenticated: true,
        current_username: "default".to_string(),
        in_multi: false,
        multi_queue: Vec::new(),
    };

    // LPUSH list 1 -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LPUSH"))),
        Resp::BulkString(Some(Bytes::from("list"))),
        Resp::BulkString(Some(Bytes::from("1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // RPUSH list 2 -> 2
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("RPUSH"))),
        Resp::BulkString(Some(Bytes::from("list"))),
        Resp::BulkString(Some(Bytes::from("2"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 2),
        _ => panic!("expected Integer(2)"),
    }

    // LRANGE list 0 -1 -> ["1", "2"]
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LRANGE"))),
        Resp::BulkString(Some(Bytes::from("list"))),
        Resp::BulkString(Some(Bytes::from("0"))),
        Resp::BulkString(Some(Bytes::from("-1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("1")),
                _ => panic!("expected BulkString(1)"),
            }
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("2")),
                _ => panic!("expected BulkString(2)"),
            }
        }
        _ => panic!("expected Array"),
    }

    // LPOP list -> "1"
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LPOP"))),
        Resp::BulkString(Some(Bytes::from("list"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("1")),
        _ => panic!("expected BulkString(1)"),
    }

    // RPOP list -> "2"
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("RPOP"))),
        Resp::BulkString(Some(Bytes::from("list"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("2")),
        _ => panic!("expected BulkString(2)"),
    }

    // LLEN list -> 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LLEN"))),
        Resp::BulkString(Some(Bytes::from("list"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("expected Integer(0)"),
    }

    // BLPOP test (immediate)
    // LPUSH list 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LPUSH"))),
        Resp::BulkString(Some(Bytes::from("list"))),
        Resp::BulkString(Some(Bytes::from("1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("LPUSH failed: {:?}", res),
    }

    // BLPOP list 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("BLPOP"))),
        Resp::BulkString(Some(Bytes::from("list"))),
        Resp::BulkString(Some(Bytes::from("0"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            // key
             match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("list")),
                _ => panic!("expected BulkString(list)"),
            }
            // value
             match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("1")),
                _ => panic!("expected BulkString(1)"),
            }
        }
        _ => panic!("expected Array, got {:?}", res),
    }

    // BLPOP test (blocking) - simple timeout check
    // BLPOP list 0.1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("BLPOP"))),
        Resp::BulkString(Some(Bytes::from("list"))),
        Resp::BulkString(Some(Bytes::from("0.1"))),
    ]));
    let start = std::time::Instant::now();
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    let elapsed = start.elapsed();
    assert!(elapsed.as_millis() >= 100);
    match res {
        Resp::BulkString(None) => {}, // timeout
        _ => panic!("expected BulkString(None)"),
    }
}

#[tokio::test]
async fn test_brpop_ops() {
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
    };

    let mut conn_ctx = ConnectionContext {
        db_index: 0,
        authenticated: true,
        current_username: "default".to_string(),
        in_multi: false,
        multi_queue: Vec::new(),
    };

    // LPUSH list a b c -> ["c", "b", "a"]
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LPUSH"))),
        Resp::BulkString(Some(Bytes::from("list"))),
        Resp::BulkString(Some(Bytes::from("a"))),
        Resp::BulkString(Some(Bytes::from("b"))),
        Resp::BulkString(Some(Bytes::from("c"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // BRPOP list 0 -> "a" (from right)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("BRPOP"))),
        Resp::BulkString(Some(Bytes::from("list"))),
        Resp::BulkString(Some(Bytes::from("0"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("a")),
                _ => panic!("expected BulkString(a)"),
            }
        }
        _ => panic!("expected Array, got {:?}", res),
    }

    // Remaining: ["c", "b"]
    
    // BLPOP list 0 -> "c" (from left)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("BLPOP"))),
        Resp::BulkString(Some(Bytes::from("list"))),
        Resp::BulkString(Some(Bytes::from("0"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("c")),
                _ => panic!("expected BulkString(c)"),
            }
        }
        _ => panic!("expected Array, got {:?}", res),
    }

    // Remaining: ["b"]

    // BRPOP blocking test
    // BRPOP empty_list 0.1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("BRPOP"))),
        Resp::BulkString(Some(Bytes::from("empty_list"))),
        Resp::BulkString(Some(Bytes::from("0.1"))),
    ]));
    let start = std::time::Instant::now();
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    let elapsed = start.elapsed();
    assert!(elapsed.as_millis() >= 100);
    match res {
        Resp::BulkString(None) => {}, // timeout
        _ => panic!("expected BulkString(None)"),
    }
}

#[tokio::test]
async fn test_blmove_ops() {
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
    };

    let mut conn_ctx = ConnectionContext {
        db_index: 0,
        authenticated: true,
        current_username: "default".to_string(),
        in_multi: false,
        multi_queue: Vec::new(),
    };

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LPUSH"))),
        Resp::BulkString(Some(Bytes::from("src"))),
        Resp::BulkString(Some(Bytes::from("a"))),
        Resp::BulkString(Some(Bytes::from("b"))),
    ]));
    let (_res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("BLMOVE"))),
        Resp::BulkString(Some(Bytes::from("src"))),
        Resp::BulkString(Some(Bytes::from("dst"))),
        Resp::BulkString(Some(Bytes::from("LEFT"))),
        Resp::BulkString(Some(Bytes::from("RIGHT"))),
        Resp::BulkString(Some(Bytes::from("1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("b")),
        _ => panic!("expected BulkString(b), got {:?}", res),
    }

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LRANGE"))),
        Resp::BulkString(Some(Bytes::from("src"))),
        Resp::BulkString(Some(Bytes::from("0"))),
        Resp::BulkString(Some(Bytes::from("-1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("a")),
                _ => panic!("expected BulkString(a)"),
            }
        }
        _ => panic!("expected Array, got {:?}", res),
    }

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LRANGE"))),
        Resp::BulkString(Some(Bytes::from("dst"))),
        Resp::BulkString(Some(Bytes::from("0"))),
        Resp::BulkString(Some(Bytes::from("-1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("b")),
                _ => panic!("expected BulkString(b)"),
            }
        }
        _ => panic!("expected Array, got {:?}", res),
    }

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("BLMOVE"))),
        Resp::BulkString(Some(Bytes::from("empty_src"))),
        Resp::BulkString(Some(Bytes::from("empty_dst"))),
        Resp::BulkString(Some(Bytes::from("LEFT"))),
        Resp::BulkString(Some(Bytes::from("RIGHT"))),
        Resp::BulkString(Some(Bytes::from("0.1"))),
    ]));
    let start = std::time::Instant::now();
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    let elapsed = start.elapsed();
    assert!(elapsed.as_millis() >= 100);
    match res {
        Resp::BulkString(None) => {}
        _ => panic!("expected BulkString(None)"),
    }
}

#[tokio::test]
async fn test_lmove_ops() {
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
    };

    let mut conn_ctx = ConnectionContext {
        db_index: 0,
        authenticated: true,
        current_username: "default".to_string(),
        in_multi: false,
        multi_queue: Vec::new(),
    };

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LPUSH"))),
        Resp::BulkString(Some(Bytes::from("src"))),
        Resp::BulkString(Some(Bytes::from("a"))),
        Resp::BulkString(Some(Bytes::from("b"))),
    ]));
    let (_res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LMOVE"))),
        Resp::BulkString(Some(Bytes::from("src"))),
        Resp::BulkString(Some(Bytes::from("dst"))),
        Resp::BulkString(Some(Bytes::from("LEFT"))),
        Resp::BulkString(Some(Bytes::from("RIGHT"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("b")),
        _ => panic!("expected BulkString(b), got {:?}", res),
    }

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LRANGE"))),
        Resp::BulkString(Some(Bytes::from("src"))),
        Resp::BulkString(Some(Bytes::from("0"))),
        Resp::BulkString(Some(Bytes::from("-1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("a")),
                _ => panic!("expected BulkString(a)"),
            }
        }
        _ => panic!("expected Array, got {:?}", res),
    }

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LRANGE"))),
        Resp::BulkString(Some(Bytes::from("dst"))),
        Resp::BulkString(Some(Bytes::from("0"))),
        Resp::BulkString(Some(Bytes::from("-1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("b")),
                _ => panic!("expected BulkString(b)"),
            }
        }
        _ => panic!("expected Array, got {:?}", res),
    }

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LMOVE"))),
        Resp::BulkString(Some(Bytes::from("empty_src"))),
        Resp::BulkString(Some(Bytes::from("empty_dst"))),
        Resp::BulkString(Some(Bytes::from("LEFT"))),
        Resp::BulkString(Some(Bytes::from("RIGHT"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(None) => {}
        _ => panic!("expected BulkString(None)"),
    }
}
