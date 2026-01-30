use crate::cmd::process_frame;
use crate::resp::Resp;
use bytes::Bytes;
use std::collections::HashSet;

#[tokio::test]
async fn test_sdiff() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Helper to run commands
    async fn run_cmd(
        args: Vec<&str>,
        conn_ctx: &mut crate::cmd::ConnectionContext,
        server_ctx: &crate::cmd::ServerContext,
    ) -> Resp {
        let mut resp_args = Vec::new();
        for arg in args {
            resp_args.push(Resp::BulkString(Some(Bytes::from(arg.to_string()))));
        }
        let req = Resp::Array(Some(resp_args));
        let (res, _) = process_frame(req, conn_ctx, server_ctx).await;
        res
    }

    // Setup
    // s1: {a, b, c, d}
    run_cmd(vec!["SADD", "s1", "a", "b", "c", "d"], &mut conn_ctx, &server_ctx).await;
    // s2: {c}
    run_cmd(vec!["SADD", "s2", "c"], &mut conn_ctx, &server_ctx).await;
    // s3: {a, e}
    run_cmd(vec!["SADD", "s3", "a", "e"], &mut conn_ctx, &server_ctx).await;

    // 1. SDIFF s1 (same as SMEMBERS)
    let res = run_cmd(vec!["SDIFF", "s1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 4);
        }
        _ => panic!("Expected Array"),
    }

    // 2. SDIFF s1 s2 -> {a, b, d} (c removed)
    let res = run_cmd(vec!["SDIFF", "s1", "s2"], &mut conn_ctx, &server_ctx).await;
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
            assert!(members.contains(&Bytes::from("d")));
        }
        _ => panic!("Expected Array"),
    }

    // 3. SDIFF s1 s2 s3 -> {b, d} (c removed by s2, a removed by s3)
    let res = run_cmd(vec!["SDIFF", "s1", "s2", "s3"], &mut conn_ctx, &server_ctx).await;
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
            assert!(members.contains(&Bytes::from("b")));
            assert!(members.contains(&Bytes::from("d")));
        }
        _ => panic!("Expected Array"),
    }

    // 4. SDIFF s1 missing -> {a, b, c, d} (missing key is empty set, nothing removed)
    let res = run_cmd(vec!["SDIFF", "s1", "missing"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 4);
        }
        _ => panic!("Expected Array"),
    }

    // 5. SDIFF missing s1 -> [] (missing first key is empty set)
    let res = run_cmd(vec!["SDIFF", "missing", "s1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => assert_eq!(items.len(), 0),
        _ => panic!("Expected Array"),
    }

    // 6. WRONGTYPE first key
    run_cmd(vec!["SET", "string_key", "val"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["SDIFF", "string_key", "s1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(msg) => assert!(msg.contains("WRONGTYPE")),
        _ => panic!("Expected Error"),
    }

    // 7. WRONGTYPE subsequent key
    let res = run_cmd(vec!["SDIFF", "s1", "string_key"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(msg) => assert!(msg.contains("WRONGTYPE")),
        _ => panic!("Expected Error"),
    }
}
