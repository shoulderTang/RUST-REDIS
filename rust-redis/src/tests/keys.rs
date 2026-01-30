use crate::resp::Resp;
use bytes::Bytes;
use crate::tests::helper::run_cmd;

#[tokio::test]
async fn test_scan() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Create 100 keys
    for i in 0..100 {
        run_cmd(vec!["SET", &format!("key:{:03}", i), "val"], &mut conn_ctx, &server_ctx).await;
    }

    // SCAN 0 COUNT 20
    let res = run_cmd(vec!["SCAN", "0", "COUNT", "20"], &mut conn_ctx, &server_ctx).await;
    
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
        let res = run_cmd(vec!["SCAN", &cursor.to_string(), "COUNT", "20"], &mut conn_ctx, &server_ctx).await;
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
    let res = run_cmd(vec!["SCAN", "0", "MATCH", "key:01*", "COUNT", "1000"], &mut conn_ctx, &server_ctx).await;
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
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

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
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Setup keys
    run_cmd(vec!["SET", "key1", "val"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["SET", "key2", "val"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["SET", "other", "val"], &mut conn_ctx, &server_ctx).await;

    // KEYS *
    let res = run_cmd(vec!["KEYS", "*"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 3);
        }
        _ => panic!("expected Array"),
    }

    // KEYS key*
    let res = run_cmd(vec!["KEYS", "key*"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
        }
        _ => panic!("expected Array"),
    }

    // KEYS ?ey?
    let res = run_cmd(vec!["KEYS", "?ey?"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
        }
        _ => panic!("expected Array"),
    }
}

#[tokio::test]
async fn test_expire_ttl() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // SET key val
    run_cmd(vec!["SET", "foo", "bar"], &mut conn_ctx, &server_ctx).await;

    // TTL key -> -1
    let res = run_cmd(vec!["TTL", "foo"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, -1),
        _ => panic!("expected Integer(-1)"),
    }

    // EXPIRE key 1
    let res = run_cmd(vec!["EXPIRE", "foo", "1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // TTL key -> ~1
    let res = run_cmd(vec!["TTL", "foo"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert!(i >= 0 && i <= 1),
        _ => panic!("expected Integer(>=0)"),
    }

    // Sleep 1.1s
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

    // GET key -> Nil
    let res = run_cmd(vec!["GET", "foo"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(None) => {}
        _ => panic!("expected BulkString(None)"),
    }

    // TTL key -> -2
    let res = run_cmd(vec!["TTL", "foo"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, -2),
        _ => panic!("expected Integer(-2)"),
    }
}

#[tokio::test]
async fn test_dbsize() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // DBSIZE -> 0
    let res = run_cmd(vec!["DBSIZE"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("expected Integer(0)"),
    }

    // SET key val
    run_cmd(vec!["SET", "foo", "bar"], &mut conn_ctx, &server_ctx).await;

    // DBSIZE -> 1
    let res = run_cmd(vec!["DBSIZE"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }
}

#[tokio::test]
async fn test_del() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Setup keys
    run_cmd(vec!["MSET", "k1", "v1", "k2", "v2", "k3", "v3"], &mut conn_ctx, &server_ctx).await;

    // DEL k1 k2 k_missing
    let res = run_cmd(vec!["DEL", "k1", "k2", "k_missing"], &mut conn_ctx, &server_ctx).await;

    match res {
        Resp::Integer(i) => assert_eq!(i, 2),
        _ => panic!("expected Integer(2)"),
    }

    // Verify deletion
    let db = server_ctx.databases.get(0).unwrap();
    assert!(!db.contains_key(&Bytes::from("k1")));
    assert!(!db.contains_key(&Bytes::from("k2")));
    assert!(db.contains_key(&Bytes::from("k3")));
}
