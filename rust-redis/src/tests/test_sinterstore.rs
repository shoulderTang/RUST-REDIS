use crate::cmd::process_frame;
use crate::resp::Resp;
use bytes::Bytes;
use std::collections::HashSet;

#[tokio::test]
async fn test_sinterstore() {
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
    // s3: {a, c, e}
    run_cmd(vec!["SADD", "s3", "a", "c", "e"], &mut conn_ctx, &server_ctx).await;

    // 1. SINTERSTORE dest s1 s2 s3 -> {c}
    let res = run_cmd(vec!["SINTERSTORE", "dest", "s1", "s2", "s3"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 1),
        _ => panic!("Expected Integer"),
    }
    // Verify dest
    let res = run_cmd(vec!["SMEMBERS", "dest"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(b, &Bytes::from("c")),
                _ => panic!("Expected BulkString"),
            }
        }
        _ => panic!("Expected Array"),
    }

    // 2. SINTERSTORE dest s1 s2 (overwrite) -> {c}
    let res = run_cmd(vec!["SINTERSTORE", "dest", "s1", "s2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 1),
        _ => panic!("Expected Integer"),
    }
    // Verify dest
    let res = run_cmd(vec!["SMEMBERS", "dest"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(b, &Bytes::from("c")),
                _ => panic!("Expected BulkString"),
            }
        }
        _ => panic!("Expected Array"),
    }

    // 3. SINTERSTORE with missing key -> empty
    let res = run_cmd(vec!["SINTERSTORE", "dest_empty", "s1", "missing"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 0),
        _ => panic!("Expected Integer"),
    }
    // Verify dest_empty
    let res = run_cmd(vec!["SMEMBERS", "dest_empty"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => assert_eq!(items.len(), 0),
        _ => panic!("Expected Array"),
    }

    // 4. SINTERSTORE overwrites existing non-set key (actually, SINTERSTORE just writes a set, it should work even if dest was string before? No, usually in Redis commands overwrite unless specified otherwise, but types might be tricky. Let's see implementation. db.insert overwrites.)
    run_cmd(vec!["SET", "dest_str", "value"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["SINTERSTORE", "dest_str", "s1", "s2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 1),
        _ => panic!("Expected Integer"),
    }
    let res = run_cmd(vec!["GET", "dest_str"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(msg) => assert!(msg.contains("WRONGTYPE")), // Because it's now a Set
        _ => panic!("Expected Error WRONGTYPE"),
    }

    // 5. WRONGTYPE in source key
    run_cmd(vec!["SET", "string_key", "val"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["SINTERSTORE", "dest", "s1", "string_key"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(msg) => assert!(msg.contains("WRONGTYPE")),
        _ => panic!("Expected Error"),
    }
}
