use crate::resp::Resp;
use bytes::Bytes;
use std::collections::HashSet;
use crate::tests::helper::run_cmd;

#[tokio::test]
async fn test_sunion() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Setup
    // s1: {a, b, c}
    run_cmd(vec!["SADD", "s1", "a", "b", "c"], &mut conn_ctx, &server_ctx).await;
    // s2: {c, d, e}
    run_cmd(vec!["SADD", "s2", "c", "d", "e"], &mut conn_ctx, &server_ctx).await;
    // s3: {a, e, f}
    run_cmd(vec!["SADD", "s3", "a", "e", "f"], &mut conn_ctx, &server_ctx).await;

    // 1. SUNION s1 (same as SMEMBERS)
    let res = run_cmd(vec!["SUNION", "s1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 3);
            let mut members = HashSet::new();
            for item in items {
                match item {
                    Resp::BulkString(Some(b)) => { members.insert(b); },
                    _ => panic!("Expected BulkString"),
                }
            }
            assert!(members.contains(&Bytes::from("a")));
            assert!(members.contains(&Bytes::from("b")));
            assert!(members.contains(&Bytes::from("c")));
        }
        _ => panic!("Expected Array"),
    }

    // 2. SUNION s1 s2 -> {a, b, c, d, e}
    let res = run_cmd(vec!["SUNION", "s1", "s2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 5);
            let mut members = HashSet::new();
            for item in items {
                match item {
                    Resp::BulkString(Some(b)) => { members.insert(b); },
                    _ => panic!("Expected BulkString"),
                }
            }
            assert!(members.contains(&Bytes::from("a")));
            assert!(members.contains(&Bytes::from("b")));
            assert!(members.contains(&Bytes::from("c")));
            assert!(members.contains(&Bytes::from("d")));
            assert!(members.contains(&Bytes::from("e")));
        }
        _ => panic!("Expected Array"),
    }

    // 3. SUNION s1 s2 s3 -> {a, b, c, d, e, f}
    let res = run_cmd(vec!["SUNION", "s1", "s2", "s3"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 6);
            let mut members = HashSet::new();
            for item in items {
                match item {
                    Resp::BulkString(Some(b)) => { members.insert(b); },
                    _ => panic!("Expected BulkString"),
                }
            }
            assert!(members.contains(&Bytes::from("a")));
            assert!(members.contains(&Bytes::from("b")));
            assert!(members.contains(&Bytes::from("c")));
            assert!(members.contains(&Bytes::from("d")));
            assert!(members.contains(&Bytes::from("e")));
            assert!(members.contains(&Bytes::from("f")));
        }
        _ => panic!("Expected Array"),
    }

    // 4. SUNION s1 missing -> {a, b, c} (missing key is empty set)
    let res = run_cmd(vec!["SUNION", "s1", "missing"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 3);
            let mut members = HashSet::new();
            for item in items {
                match item {
                    Resp::BulkString(Some(b)) => { members.insert(b); },
                    _ => panic!("Expected BulkString"),
                }
            }
            assert!(members.contains(&Bytes::from("a")));
            assert!(members.contains(&Bytes::from("b")));
            assert!(members.contains(&Bytes::from("c")));
        }
        _ => panic!("Expected Array"),
    }

    // 5. SUNION missing s1 -> {a, b, c}
    let res = run_cmd(vec!["SUNION", "missing", "s1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 3);
            let mut members = HashSet::new();
            for item in items {
                match item {
                    Resp::BulkString(Some(b)) => { members.insert(b); },
                    _ => panic!("Expected BulkString"),
                }
            }
            assert!(members.contains(&Bytes::from("a")));
            assert!(members.contains(&Bytes::from("b")));
            assert!(members.contains(&Bytes::from("c")));
        }
        _ => panic!("Expected Array"),
    }

    // 6. WRONGTYPE
    run_cmd(vec!["SET", "string_key", "val"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["SUNION", "string_key", "s1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(msg) => assert!(msg.contains("WRONGTYPE")),
        _ => panic!("Expected Error"),
    }
}
