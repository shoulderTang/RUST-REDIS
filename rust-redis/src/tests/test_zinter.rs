use crate::cmd::process_frame;
use crate::resp::Resp;
use bytes::Bytes;

#[tokio::test]
async fn test_zinter() {
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
    // zset1: a:1, b:2
    // zset2: b:3, c:4
    run_cmd(vec!["ZADD", "zset1", "1", "a", "2", "b"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["ZADD", "zset2", "3", "b", "4", "c"], &mut conn_ctx, &server_ctx).await;

    // 1. Basic ZINTER
    let res = run_cmd(vec!["ZINTER", "2", "zset1", "zset2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 1);
            let members: Vec<String> = arr.into_iter().map(|r| match r { Resp::BulkString(Some(b)) => String::from_utf8_lossy(&b).to_string(), _ => panic!() }).collect();
            assert_eq!(members, vec!["b"]); // intersection is only 'b'
        }
        _ => panic!("Expected Array, got {:?}", res),
    }

    // 2. ZINTER with WITHSCORES
    let res = run_cmd(vec!["ZINTER", "2", "zset1", "zset2", "WITHSCORES"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(match &arr[0] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "b");
            assert_eq!(match &arr[1] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "5"); // 2+3
        }
        _ => panic!("Expected Array of 2, got {:?}", res),
    }

    // 3. ZINTER with WEIGHTS
    let res = run_cmd(vec!["ZINTER", "2", "zset1", "zset2", "WEIGHTS", "10", "1", "WITHSCORES"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            // zset1*10: a:10, b:20
            // zset2*1:  b:3, c:4
            // intersection: b:23
            assert_eq!(arr.len(), 2);
            assert_eq!(match &arr[0] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "b");
            assert_eq!(match &arr[1] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "23");
        }
        _ => panic!("Expected Array, got {:?}", res),
    }

    // 4. ZINTER with AGGREGATE MIN
    let res = run_cmd(vec!["ZINTER", "2", "zset1", "zset2", "AGGREGATE", "MIN", "WITHSCORES"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            // b:min(2,3)=2
            assert_eq!(arr.len(), 2);
            assert_eq!(match &arr[0] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "b");
            assert_eq!(match &arr[1] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "2");
        }
        _ => panic!("Expected Array, got {:?}", res),
    }

    // 5. ZINTERSTORE
    let res = run_cmd(vec!["ZINTERSTORE", "out", "2", "zset1", "zset2", "WEIGHTS", "1", "100"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("Expected Integer 1, got {:?}", res),
    }
    
    let res = run_cmd(vec!["ZRANGE", "out", "0", "-1", "WITHSCORES"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            // zset1*1: a:1, b:2
            // zset2*100: b:300, c:400
            // intersection: b:302
            assert_eq!(arr.len(), 2);
            assert_eq!(match &arr[0] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "b");
            assert_eq!(match &arr[1] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "302");
        }
        _ => panic!("Expected Array, got {:?}", res),
    }

    // 6. Test with one non-existent key
    let res = run_cmd(vec!["ZINTER", "2", "zset1", "nonexistent"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => assert_eq!(arr.len(), 0),
        _ => panic!("Expected empty Array, got {:?}", res),
    }
}
