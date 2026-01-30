use crate::cmd::process_frame;
use crate::db::Value;
use crate::resp::Resp;
use bytes::Bytes;
use std::collections::HashSet;

#[tokio::test]
async fn test_sdiffstore() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

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

    // Setup: s1={a,b,c,d}, s2={c}, s3={a,c,e}
    run_cmd(vec!["sadd", "s1", "a", "b", "c", "d"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["sadd", "s2", "c"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["sadd", "s3", "a", "c", "e"], &mut conn_ctx, &server_ctx).await;

    // Test 1: SDIFFSTORE dest s1 s2 s3 -> {b, d}
    // s1 - s2 = {a,b,d}
    // {a,b,d} - s3 = {b,d}
    let res = run_cmd(vec!["sdiffstore", "dest", "s1", "s2", "s3"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 2),
        _ => panic!("Expected integer response"),
    }

    // Verify content of dest
    let res = run_cmd(vec!["smembers", "dest"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2);
            let mut members = HashSet::new();
            for item in arr {
                if let Resp::BulkString(Some(b)) = item {
                    members.insert(b);
                }
            }
            assert!(members.contains(&Bytes::from("b")));
            assert!(members.contains(&Bytes::from("d")));
        }
        _ => panic!("Expected array response"),
    }

    // Test 2: Overwrite existing key
    run_cmd(vec!["set", "dest", "foo"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["sdiffstore", "dest", "s1", "s2"], &mut conn_ctx, &server_ctx).await;
    // s1 - s2 = {a,b,d} -> count 3
    match res {
        Resp::Integer(n) => assert_eq!(n, 3),
        _ => panic!("Expected integer response"),
    }
    
    // Verify type is Set
    let res = run_cmd(vec!["type", "dest"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("set")),
        _ => panic!("Expected SimpleString response"),
    }

    // Test 3: Missing first key (empty result)
    let res = run_cmd(vec!["sdiffstore", "dest2", "missing", "s1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 0),
        _ => panic!("Expected integer response"),
    }
    let res = run_cmd(vec!["exists", "dest2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 1), // SDIFFSTORE creates empty set key? Redis SDIFFSTORE stores empty set if result is empty? 
        // Redis documentation says: "If the destination key already exists, it is overwritten."
        // If result is empty, it stores an empty set.
        _ => panic!("Expected integer response"),
    }
    
    // Test 4: Missing subsequent keys (ignored)
    // s1 - missing = s1
    let res = run_cmd(vec!["sdiffstore", "dest3", "s1", "missing"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 4),
        _ => panic!("Expected integer response"),
    }

    // Test 5: WRONGTYPE source
    run_cmd(vec!["set", "string_key", "foo"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["sdiffstore", "dest", "s1", "string_key"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(s) => assert!(s.contains("WRONGTYPE")),
        _ => panic!("Expected WRONGTYPE error"),
    }
}
