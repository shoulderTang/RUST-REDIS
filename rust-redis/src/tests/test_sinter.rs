use crate::resp::Resp;
use bytes::Bytes;
use std::collections::HashSet;
use crate::tests::helper::run_cmd;

#[tokio::test]
async fn test_sinter() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Setup
    // s1: {a, b, c, d}
    run_cmd(vec!["SADD", "s1", "a", "b", "c", "d"], &mut conn_ctx, &server_ctx).await;
    // s2: {c}
    run_cmd(vec!["SADD", "s2", "c"], &mut conn_ctx, &server_ctx).await;
    // s3: {a, c, e}
    run_cmd(vec!["SADD", "s3", "a", "c", "e"], &mut conn_ctx, &server_ctx).await;

    // 1. SINTER s1 (should be same as SMEMBERS)
    let res = run_cmd(vec!["SINTER", "s1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 4);
        }
        _ => panic!("Expected Array"),
    }

    // 2. SINTER s1 s2 -> {c}
    let res = run_cmd(vec!["SINTER", "s1", "s2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("c")),
                _ => panic!("Expected BulkString"),
            }
        }
        _ => panic!("Expected Array"),
    }

    // 3. SINTER s2 s1 -> {c} (order shouldn't matter)
    let res = run_cmd(vec!["SINTER", "s2", "s1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("c")),
                _ => panic!("Expected BulkString"),
            }
        }
        _ => panic!("Expected Array"),
    }

    // 4. SINTER s1 s2 s3 -> {c}
    let res = run_cmd(vec!["SINTER", "s1", "s2", "s3"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("c")),
                _ => panic!("Expected BulkString"),
            }
        }
        _ => panic!("Expected Array"),
    }

    // 5. SINTER s1 s3 -> {a, c}
    let res = run_cmd(vec!["SINTER", "s1", "s3"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            let mut members = HashSet::new();
            for item in items {
                match item {
                    Resp::BulkString(Some(b)) => { members.insert(b); },
                    _ => panic!("Expected BulkString"),
                }
            }
            assert!(members.contains(&Bytes::from("a")));
            assert!(members.contains(&Bytes::from("c")));
        }
        _ => panic!("Expected Array"),
    }

    // 6. SINTER s1 missing -> []
    let res = run_cmd(vec!["SINTER", "s1", "missing"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => assert_eq!(items.len(), 0),
        _ => panic!("Expected Array"),
    }

    // 7. SINTER missing s1 -> []
    let res = run_cmd(vec!["SINTER", "missing", "s1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => assert_eq!(items.len(), 0),
        _ => panic!("Expected Array"),
    }

    // 8. WRONGTYPE
    run_cmd(vec!["SET", "string_key", "val"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["SINTER", "string_key", "s1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(msg) => assert!(msg.contains("WRONGTYPE")),
        _ => panic!("Expected Error"),
    }
    
    // 9. WRONGTYPE second key
    let res = run_cmd(vec!["SINTER", "s1", "string_key"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(msg) => assert!(msg.contains("WRONGTYPE")),
        _ => panic!("Expected Error"),
    }
}
