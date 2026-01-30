use crate::cmd::process_frame;
use crate::resp::Resp;
use bytes::Bytes;

#[tokio::test]
async fn test_zrandmember() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Helper to run command
    async fn run_cmd(args: Vec<&str>, conn_ctx: &mut crate::cmd::ConnectionContext, server_ctx: &crate::cmd::ServerContext) -> Resp {
        let mut resp_args = Vec::new();
        for arg in args {
            resp_args.push(Resp::BulkString(Some(Bytes::from(arg.to_string()))));
        }
        let req = Resp::Array(Some(resp_args));
        let (res, _) = process_frame(req, conn_ctx, server_ctx).await;
        res
    }

    // 1. Setup: ZADD myzset 10 a 20 b 30 c
    run_cmd(vec!["ZADD", "myzset", "10", "a", "20", "b", "30", "c"], &mut conn_ctx, &server_ctx).await;

    // 2. Test ZRANDMEMBER key (single member)
    let res = run_cmd(vec!["ZRANDMEMBER", "myzset"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b).to_string();
            assert!(s == "a" || s == "b" || s == "c");
        }
        _ => panic!("Expected BulkString, got {:?}", res),
    }

    // 3. Test ZRANDMEMBER key 2 (distinct)
    let res = run_cmd(vec!["ZRANDMEMBER", "myzset", "2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2);
            let b1 = match &arr[0] { Resp::BulkString(Some(b)) => b, _ => panic!() };
            let b2 = match &arr[1] { Resp::BulkString(Some(b)) => b, _ => panic!() };
            let s1 = String::from_utf8_lossy(b1).to_string();
            let s2 = String::from_utf8_lossy(b2).to_string();
            assert_ne!(s1, s2);
            assert!(s1 == "a" || s1 == "b" || s1 == "c");
            assert!(s2 == "a" || s2 == "b" || s2 == "c");
        }
        _ => panic!("Expected Array of 2, got {:?}", res),
    }

    // 4. Test ZRANDMEMBER key -5 (repeated)
    let res = run_cmd(vec!["ZRANDMEMBER", "myzset", "-5"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 5);
            for item in arr {
                let b = match item { Resp::BulkString(Some(b)) => b, _ => panic!() };
                let s = String::from_utf8_lossy(&b).to_string();
                assert!(s == "a" || s == "b" || s == "c");
            }
        }
        _ => panic!("Expected Array of 5, got {:?}", res),
    }

    // 5. Test ZRANDMEMBER key 2 WITHSCORES
    let res = run_cmd(vec!["ZRANDMEMBER", "myzset", "2", "WITHSCORES"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 4); // 2 members * 2 (member, score)
            let b1 = match &arr[0] { Resp::BulkString(Some(b)) => b, _ => panic!() };
            let bs1 = match &arr[1] { Resp::BulkString(Some(b)) => b, _ => panic!() };
            let b2 = match &arr[2] { Resp::BulkString(Some(b)) => b, _ => panic!() };
            let bs2 = match &arr[3] { Resp::BulkString(Some(b)) => b, _ => panic!() };
            
            let m1 = String::from_utf8_lossy(b1).to_string();
            let s1 = String::from_utf8_lossy(bs1).to_string();
            let m2 = String::from_utf8_lossy(b2).to_string();
            let s2 = String::from_utf8_lossy(bs2).to_string();
            
            assert_ne!(m1, m2);
            
            let mut found1 = false;
            let mut found2 = false;
            if m1 == "a" { assert_eq!(s1, "10"); found1 = true; }
            if m1 == "b" { assert_eq!(s1, "20"); found1 = true; }
            if m1 == "c" { assert_eq!(s1, "30"); found1 = true; }
            
            if m2 == "a" { assert_eq!(s2, "10"); found2 = true; }
            if m2 == "b" { assert_eq!(s2, "20"); found2 = true; }
            if m2 == "c" { assert_eq!(s2, "30"); found2 = true; }
            
            assert!(found1 && found2);
        }
        _ => panic!("Expected Array of 4, got {:?}", res),
    }

    // 6. Test non-existent key
    let res = run_cmd(vec!["ZRANDMEMBER", "nonexistent"], &mut conn_ctx, &server_ctx).await;
    assert!(matches!(res, Resp::BulkString(None)));

    let res = run_cmd(vec!["ZRANDMEMBER", "nonexistent", "2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => assert_eq!(arr.len(), 0),
        _ => panic!("Expected empty Array, got {:?}", res),
    }
}
