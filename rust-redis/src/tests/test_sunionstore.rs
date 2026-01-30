use crate::resp::Resp;
use bytes::Bytes;
use std::collections::HashSet;
use crate::tests::helper::run_cmd;

#[tokio::test]
async fn test_sunionstore() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Setup
    // s1: {a, b}
    run_cmd(vec!["SADD", "s1", "a", "b"], &mut conn_ctx, &server_ctx).await;
    // s2: {b, c}
    run_cmd(vec!["SADD", "s2", "b", "c"], &mut conn_ctx, &server_ctx).await;

    // 1. SUNIONSTORE dest s1 s2 -> {a, b, c}
    let res = run_cmd(vec!["SUNIONSTORE", "dest", "s1", "s2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 3),
        _ => panic!("Expected Integer 3"),
    }
    // Verify dest
    let res = run_cmd(vec!["SMEMBERS", "dest"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 3);
            let mut members = HashSet::new();
            for item in items {
                match item {
                    Resp::BulkString(Some(b)) => {
                        members.insert(b);
                    }
                    _ => panic!("Expected BulkString"),
                }
            }
            assert!(members.contains(&Bytes::from("a")));
            assert!(members.contains(&Bytes::from("b")));
            assert!(members.contains(&Bytes::from("c")));
        }
        _ => panic!("Expected Array"),
    }

    // 2. SUNIONSTORE with missing key -> treats as empty set
    // s1={a,b}, missing={} -> union={a,b}
    let res = run_cmd(vec!["SUNIONSTORE", "dest_miss", "s1", "missing"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 2),
        _ => panic!("Expected Integer 2"),
    }
    // Verify dest_miss
    let res = run_cmd(vec!["SMEMBERS", "dest_miss"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            let mut members = HashSet::new();
            for item in items {
                match item {
                    Resp::BulkString(Some(b)) => {
                        members.insert(b);
                    }
                    _ => panic!("Expected BulkString"),
                }
            }
            assert!(members.contains(&Bytes::from("a")));
            assert!(members.contains(&Bytes::from("b")));
        }
        _ => panic!("Expected Array"),
    }

    // 3. SUNIONSTORE overwrites destination
    run_cmd(vec!["SADD", "dest", "x", "y"], &mut conn_ctx, &server_ctx).await;
    // s2={b,c}
    let res = run_cmd(vec!["SUNIONSTORE", "dest", "s2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 2),
        _ => panic!("Expected Integer 2"),
    }
    // Verify dest has only {b, c}
    let res = run_cmd(vec!["SMEMBERS", "dest"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            let mut members = HashSet::new();
            for item in items {
                match item {
                    Resp::BulkString(Some(b)) => {
                        members.insert(b);
                    }
                    _ => panic!("Expected BulkString"),
                }
            }
            assert!(members.contains(&Bytes::from("b")));
            assert!(members.contains(&Bytes::from("c")));
            assert!(!members.contains(&Bytes::from("x")));
        }
        _ => panic!("Expected Array"),
    }

    // 4. WRONGTYPE in source key
    run_cmd(vec!["SET", "string_key", "val"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["SUNIONSTORE", "dest", "s1", "string_key"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(msg) => assert!(msg.contains("WRONGTYPE")),
        _ => panic!("Expected Error WRONGTYPE"),
    }

    // 5. Destination overwrite from different type
    run_cmd(vec!["SET", "dest_str", "value"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["SUNIONSTORE", "dest_str", "s1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 2),
        _ => panic!("Expected Integer 2"),
    }
    // Check it's a set now
    let res = run_cmd(vec!["TYPE", "dest_str"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::SimpleString(s) => assert_eq!(s, "set"),
        _ => panic!("Expected SimpleString set"),
    }
}
