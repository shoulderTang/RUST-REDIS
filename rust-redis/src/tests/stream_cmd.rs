use crate::cmd::{process_frame, ConnectionContext, ServerContext};
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::{Arc, RwLock};

#[tokio::test]
async fn test_xadd() {
    let db = Arc::new(vec![Db::default()]);
    let config = Config::default();
    let script_manager = crate::cmd::scripting::create_script_manager();
    let acl = Arc::new(RwLock::new(crate::acl::Acl::new()));

    let server_ctx = ServerContext {
        databases: db.clone(),
        acl: acl.clone(),
        aof: None,
        config: Arc::new(config),
        script_manager: script_manager,
        blocking_waiters: Arc::new(DashMap::new()),
    };

    let mut conn_ctx = ConnectionContext {
        db_index: 0,
        authenticated: true,
        current_username: "default".to_string(),
    };

    // XADD key * field value
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XADD"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("*"))),
        Resp::BulkString(Some(Bytes::from("name"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ];
    let frame = Resp::Array(Some(args));

    let (resp, _) = process_frame(frame, &mut conn_ctx, &server_ctx).await;

    match resp {
        Resp::BulkString(Some(id)) => {
            let id_str = String::from_utf8(id.to_vec()).unwrap();
            assert!(id_str.contains("-"));
        }
        _ => panic!("Expected BulkString with ID, got {:?}", resp),
    }
}

#[tokio::test]
async fn test_xlen() {
    let db = Arc::new(vec![Db::default()]);
    let config = Config::default();
    let script_manager = crate::cmd::scripting::create_script_manager();
    let acl = Arc::new(RwLock::new(crate::acl::Acl::new()));

    let server_ctx = ServerContext {
        databases: db.clone(),
        acl: acl.clone(),
        aof: None,
        config: Arc::new(config),
        script_manager: script_manager,
        blocking_waiters: Arc::new(DashMap::new()),
    };

    let mut conn_ctx = ConnectionContext {
        db_index: 0,
        authenticated: true,
        current_username: "default".to_string(),
    };

    // XADD key * field value
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XADD"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("*"))),
        Resp::BulkString(Some(Bytes::from("name"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ];
    let frame = Resp::Array(Some(args));
    process_frame(frame, &mut conn_ctx, &server_ctx).await;

    // XLEN key
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XLEN"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
    ];
    let frame = Resp::Array(Some(args));
    let (resp, _) = process_frame(frame, &mut conn_ctx, &server_ctx).await;

    assert_eq!(resp, Resp::Integer(1));
}

#[tokio::test]
async fn test_xlen_bug_repro() {
    let db = Arc::new(vec![Db::default()]);
    let config = Config::default();
    let script_manager = crate::cmd::scripting::create_script_manager();
    let acl = Arc::new(RwLock::new(crate::acl::Acl::new()));

    let server_ctx = ServerContext {
        databases: db.clone(),
        acl: acl.clone(),
        aof: None,
        config: Arc::new(config),
        script_manager: script_manager,
        blocking_waiters: Arc::new(DashMap::new()),
    };

    let mut conn_ctx = ConnectionContext {
        db_index: 0,
        authenticated: true,
        current_username: "default".to_string(),
    };

    // Scenario 1: One XADD with multiple fields
    // XADD mystream1 * f1 v1 f2 v2 f3 v3
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XADD"))),
        Resp::BulkString(Some(Bytes::from("mystream1"))),
        Resp::BulkString(Some(Bytes::from("*"))),
        Resp::BulkString(Some(Bytes::from("f1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
        Resp::BulkString(Some(Bytes::from("f2"))),
        Resp::BulkString(Some(Bytes::from("v2"))),
        Resp::BulkString(Some(Bytes::from("f3"))),
        Resp::BulkString(Some(Bytes::from("v3"))),
    ];
    let frame = Resp::Array(Some(args));
    process_frame(frame, &mut conn_ctx, &server_ctx).await;

    // XLEN mystream1 -> Should be 1
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XLEN"))),
        Resp::BulkString(Some(Bytes::from("mystream1"))),
    ];
    let frame = Resp::Array(Some(args));
    let (resp, _) = process_frame(frame, &mut conn_ctx, &server_ctx).await;
    assert_eq!(resp, Resp::Integer(1), "Single XADD with multiple fields should result in len 1");

    // Scenario 2: Three XADDs with auto ID
    // XADD mystream2 * f1 v1
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XADD"))),
        Resp::BulkString(Some(Bytes::from("mystream2"))),
        Resp::BulkString(Some(Bytes::from("*"))),
        Resp::BulkString(Some(Bytes::from("f1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
    ];
    process_frame(Resp::Array(Some(args)), &mut conn_ctx, &server_ctx).await;

    // XADD mystream2 * f2 v2
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XADD"))),
        Resp::BulkString(Some(Bytes::from("mystream2"))),
        Resp::BulkString(Some(Bytes::from("*"))),
        Resp::BulkString(Some(Bytes::from("f2"))),
        Resp::BulkString(Some(Bytes::from("v2"))),
    ];
    process_frame(Resp::Array(Some(args)), &mut conn_ctx, &server_ctx).await;

    // XADD mystream2 * f3 v3
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XADD"))),
        Resp::BulkString(Some(Bytes::from("mystream2"))),
        Resp::BulkString(Some(Bytes::from("*"))),
        Resp::BulkString(Some(Bytes::from("f3"))),
        Resp::BulkString(Some(Bytes::from("v3"))),
    ];
    process_frame(Resp::Array(Some(args)), &mut conn_ctx, &server_ctx).await;

    // XLEN mystream2 -> Should be 3
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XLEN"))),
        Resp::BulkString(Some(Bytes::from("mystream2"))),
    ];
    let frame = Resp::Array(Some(args));
    let (resp, _) = process_frame(frame, &mut conn_ctx, &server_ctx).await;
    
    // If this assertion fails with 1, then the bug is reproduced.
    assert_eq!(resp, Resp::Integer(3), "Three separate XADDs should result in len 3");
}

#[tokio::test]
async fn test_xrevrange() {
    let db = Arc::new(vec![Db::default()]);
    let config = Config::default();
    let script_manager = crate::cmd::scripting::create_script_manager();
    let acl = Arc::new(RwLock::new(crate::acl::Acl::new()));

    let server_ctx = ServerContext {
        databases: db.clone(),
        acl: acl.clone(),
        aof: None,
        config: Arc::new(config),
        script_manager: script_manager,
        blocking_waiters: Arc::new(DashMap::new()),
    };

    let mut conn_ctx = ConnectionContext {
        db_index: 0,
        authenticated: true,
        current_username: "default".to_string(),
    };

    // XADD key 100-1 field value
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XADD"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("100-1"))),
        Resp::BulkString(Some(Bytes::from("name"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ];
    let frame = Resp::Array(Some(args));
    process_frame(frame, &mut conn_ctx, &server_ctx).await;

    // XADD key 100-2 field value
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XADD"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("100-2"))),
        Resp::BulkString(Some(Bytes::from("name"))),
        Resp::BulkString(Some(Bytes::from("bar"))),
    ];
    let frame = Resp::Array(Some(args));
    process_frame(frame, &mut conn_ctx, &server_ctx).await;

    // XREVRANGE mystream + - COUNT 1
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XREVRANGE"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("+"))),
        Resp::BulkString(Some(Bytes::from("-"))),
        Resp::BulkString(Some(Bytes::from("COUNT"))),
        Resp::BulkString(Some(Bytes::from("1"))),
    ];
    let frame = Resp::Array(Some(args));
    let (resp, _) = process_frame(frame, &mut conn_ctx, &server_ctx).await;

    match resp {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 1);
            if let Resp::Array(Some(entry)) = &arr[0] {
                assert_eq!(entry[0], Resp::BulkString(Some(Bytes::from("100-2"))));
            } else {
                panic!("Expected entry array");
            }
        }
        _ => panic!("Expected array response"),
    }
}

#[tokio::test]
async fn test_xrange() {
    let db = Arc::new(vec![Db::default()]);
    let config = Config::default();
    let script_manager = crate::cmd::scripting::create_script_manager();
    let acl = Arc::new(RwLock::new(crate::acl::Acl::new()));

    let server_ctx = ServerContext {
        databases: db.clone(),
        acl: acl.clone(),
        aof: None,
        config: Arc::new(config),
        script_manager: script_manager,
        blocking_waiters: Arc::new(DashMap::new()),
    };

    let mut conn_ctx = ConnectionContext {
        db_index: 0,
        authenticated: true,
        current_username: "default".to_string(),
    };

    // XADD key 100-1 field value
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XADD"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("100-1"))),
        Resp::BulkString(Some(Bytes::from("name"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ];
    let frame = Resp::Array(Some(args));
    process_frame(frame, &mut conn_ctx, &server_ctx).await;

    // XADD key 100-2 field value
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XADD"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("100-2"))),
        Resp::BulkString(Some(Bytes::from("name"))),
        Resp::BulkString(Some(Bytes::from("bar"))),
    ];
    let frame = Resp::Array(Some(args));
    process_frame(frame, &mut conn_ctx, &server_ctx).await;

    // XRANGE mystream - +
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XRANGE"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("-"))),
        Resp::BulkString(Some(Bytes::from("+"))),
    ];
    let frame = Resp::Array(Some(args));
    let (resp, _) = process_frame(frame, &mut conn_ctx, &server_ctx).await;

    if let Resp::Array(Some(items)) = resp {
        assert_eq!(items.len(), 2);
        // Verify first item
        if let Resp::Array(Some(entry)) = &items[0] {
            // [ID, [fields]]
            assert_eq!(entry.len(), 2);
            if let Resp::BulkString(Some(id)) = &entry[0] {
                assert_eq!(id, &Bytes::from("100-1"));
            } else {
                panic!("Expected ID");
            }
        } else {
            panic!("Expected entry array");
        }
    } else {
        panic!("Expected Array, got {:?}", resp);
    }
}

#[tokio::test]
async fn test_xdel() {
    let db = Arc::new(vec![Db::default()]);
    let config = Config::default();
    let script_manager = crate::cmd::scripting::create_script_manager();
    let acl = Arc::new(RwLock::new(crate::acl::Acl::new()));

    let server_ctx = ServerContext {
        databases: db.clone(),
        acl: acl.clone(),
        aof: None,
        config: Arc::new(config),
        script_manager: script_manager,
        blocking_waiters: Arc::new(DashMap::new()),
    };

    let mut conn_ctx = ConnectionContext {
        db_index: 0,
        authenticated: true,
        current_username: "default".to_string(),
    };

    // XADD key 100-1 field value
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XADD"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("100-1"))),
        Resp::BulkString(Some(Bytes::from("name"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ];
    let frame = Resp::Array(Some(args));
    process_frame(frame, &mut conn_ctx, &server_ctx).await;

    // XADD key 100-2 field value
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XADD"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("100-2"))),
        Resp::BulkString(Some(Bytes::from("name"))),
        Resp::BulkString(Some(Bytes::from("bar"))),
    ];
    let frame = Resp::Array(Some(args));
    process_frame(frame, &mut conn_ctx, &server_ctx).await;

    // XDEL mystream 100-1
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XDEL"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("100-1"))),
    ];
    let frame = Resp::Array(Some(args));
    let (resp, _) = process_frame(frame, &mut conn_ctx, &server_ctx).await;

    assert_eq!(resp, Resp::Integer(1));

    // XLEN mystream
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XLEN"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
    ];
    let frame = Resp::Array(Some(args));
    let (resp, _) = process_frame(frame, &mut conn_ctx, &server_ctx).await;

    assert_eq!(resp, Resp::Integer(1));
}

#[tokio::test]
async fn test_xread() {
    let db = Arc::new(vec![Db::default()]);
    let config = Config::default();
    let script_manager = crate::cmd::scripting::create_script_manager();
    let acl = Arc::new(RwLock::new(crate::acl::Acl::new()));

    let server_ctx = ServerContext {
        databases: db.clone(),
        acl: acl.clone(),
        aof: None,
        config: Arc::new(config),
        script_manager: script_manager,
        blocking_waiters: Arc::new(DashMap::new()),
    };

    let mut conn_ctx = ConnectionContext {
        db_index: 0,
        authenticated: true,
        current_username: "default".to_string(),
    };

    // XADD key 100-1 field value
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XADD"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("100-1"))),
        Resp::BulkString(Some(Bytes::from("name"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ];
    let frame = Resp::Array(Some(args));
    process_frame(frame, &mut conn_ctx, &server_ctx).await;

    // XADD key 100-2 field value
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XADD"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("100-2"))),
        Resp::BulkString(Some(Bytes::from("name"))),
        Resp::BulkString(Some(Bytes::from("bar"))),
    ];
    let frame = Resp::Array(Some(args));
    process_frame(frame, &mut conn_ctx, &server_ctx).await;

    // XREAD STREAMS mystream 0-0
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XREAD"))),
        Resp::BulkString(Some(Bytes::from("STREAMS"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("0-0"))),
    ];
    let frame = Resp::Array(Some(args));
    let (resp, _) = process_frame(frame, &mut conn_ctx, &server_ctx).await;

    if let Resp::Array(Some(arr)) = resp {
        assert_eq!(arr.len(), 1);
        if let Resp::Array(Some(stream_res)) = &arr[0] {
             // [key, [entries]]
             assert_eq!(stream_res.len(), 2);
             if let Resp::BulkString(Some(key)) = &stream_res[0] {
                 assert_eq!(key, &Bytes::from("mystream"));
             } else {
                 panic!("Expected key");
             }
             if let Resp::Array(Some(entries)) = &stream_res[1] {
                 assert_eq!(entries.len(), 2);
                 if let Resp::Array(Some(entry)) = &entries[0] {
                     if let Resp::BulkString(Some(id)) = &entry[0] {
                         assert_eq!(id, &Bytes::from("100-1"));
                     }
                 }
                 if let Resp::Array(Some(entry)) = &entries[1] {
                     if let Resp::BulkString(Some(id)) = &entry[0] {
                         assert_eq!(id, &Bytes::from("100-2"));
                     }
                 }
             } else {
                 panic!("Expected entries array");
             }
        }
    } else {
        panic!("Expected Array, got {:?}", resp);
    }

    // XREAD COUNT 1 STREAMS mystream 0-0
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XREAD"))),
        Resp::BulkString(Some(Bytes::from("COUNT"))),
        Resp::BulkString(Some(Bytes::from("1"))),
        Resp::BulkString(Some(Bytes::from("STREAMS"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("0-0"))),
    ];
    let frame = Resp::Array(Some(args));
    let (resp, _) = process_frame(frame, &mut conn_ctx, &server_ctx).await;
    
    if let Resp::Array(Some(arr)) = resp {
        if let Resp::Array(Some(stream_res)) = &arr[0] {
             if let Resp::Array(Some(entries)) = &stream_res[1] {
                 assert_eq!(entries.len(), 1);
                 if let Resp::Array(Some(entry)) = &entries[0] {
                     if let Resp::BulkString(Some(id)) = &entry[0] {
                         assert_eq!(id, &Bytes::from("100-1"));
                     }
                 }
             }
        }
    }

    // XREAD STREAMS mystream 100-1
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XREAD"))),
        Resp::BulkString(Some(Bytes::from("STREAMS"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("100-1"))),
    ];
    let frame = Resp::Array(Some(args));
    let (resp, _) = process_frame(frame, &mut conn_ctx, &server_ctx).await;
    
    if let Resp::Array(Some(arr)) = resp {
        if let Resp::Array(Some(stream_res)) = &arr[0] {
             if let Resp::Array(Some(entries)) = &stream_res[1] {
                 assert_eq!(entries.len(), 1);
                 if let Resp::Array(Some(entry)) = &entries[0] {
                     if let Resp::BulkString(Some(id)) = &entry[0] {
                         assert_eq!(id, &Bytes::from("100-2"));
                     }
                 }
             }
        }
    }

    // XREAD STREAMS mystream $
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XREAD"))),
        Resp::BulkString(Some(Bytes::from("STREAMS"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("$"))),
    ];
    let frame = Resp::Array(Some(args));
    let (resp, _) = process_frame(frame, &mut conn_ctx, &server_ctx).await;
    
    // Should be nil because no new items
    if let Resp::BulkString(None) = resp {
        // ok
    } else {
        panic!("Expected Nil, got {:?}", resp);
    }
}
