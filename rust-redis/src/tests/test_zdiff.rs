use crate::cmd::process_frame;
use crate::resp::Resp;
use bytes::Bytes;

#[tokio::test]
async fn test_zdiff() {
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

    // Setup: 
    // zset1: a:1, b:2, c:3
    // zset2: b:3, d:4
    run_cmd(vec!["ZADD", "zset1", "1", "a", "2", "b", "3", "c"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["ZADD", "zset2", "3", "b", "4", "d"], &mut conn_ctx, &server_ctx).await;

    // 1. Basic ZDIFF
    let res = run_cmd(vec!["ZDIFF", "2", "zset1", "zset2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2);
            let members: Vec<String> = arr.into_iter().map(|r| match r { Resp::BulkString(Some(b)) => String::from_utf8_lossy(&b).to_string(), _ => panic!() }).collect();
            assert_eq!(members, vec!["a", "c"]); // a and c are in zset1 but not zset2
        }
        _ => panic!("Expected Array, got {:?}", res),
    }

    // 2. ZDIFF with WITHSCORES
    let res = run_cmd(vec!["ZDIFF", "2", "zset1", "zset2", "WITHSCORES"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 4);
            assert_eq!(match &arr[0] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "a");
            assert_eq!(match &arr[1] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "1");
            assert_eq!(match &arr[2] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "c");
            assert_eq!(match &arr[3] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "3");
        }
        _ => panic!("Expected Array of 4, got {:?}", res),
    }

    // 3. ZDIFFSTORE
    let res = run_cmd(vec!["ZDIFFSTORE", "out", "2", "zset1", "zset2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 2),
        _ => panic!("Expected Integer 2, got {:?}", res),
    }
    
    let res = run_cmd(vec!["ZRANGE", "out", "0", "-1", "WITHSCORES"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 4);
            assert_eq!(match &arr[0] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "a");
            assert_eq!(match &arr[1] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "1");
            assert_eq!(match &arr[2] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "c");
            assert_eq!(match &arr[3] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "3");
        }
        _ => panic!("Expected Array, got {:?}", res),
    }

    // 4. Test with non-existent second key
    let res = run_cmd(vec!["ZDIFF", "2", "zset1", "nonexistent"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => assert_eq!(arr.len(), 3),
        _ => panic!("Expected Array of 3, got {:?}", res),
    }
}
