use crate::cmd::process_frame;
use crate::resp::Resp;
use bytes::Bytes;

#[tokio::test]
async fn test_zrangebyscore() {
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

    // Setup: ZADD myzset 1 a 2 b 3 c 4 d 5 e
    run_cmd(vec!["ZADD", "myzset", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e"], &mut conn_ctx, &server_ctx).await;

    // 1. Basic ZRANGEBYSCORE
    let res = run_cmd(vec!["ZRANGEBYSCORE", "myzset", "2", "4"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
            let members: Vec<String> = arr.into_iter().map(|r| match r { Resp::BulkString(Some(b)) => String::from_utf8_lossy(&b).to_string(), _ => panic!() }).collect();
            assert_eq!(members, vec!["b", "c", "d"]);
        }
        _ => panic!("Expected Array, got {:?}", res),
    }

    // 2. WITHSCORES
    let res = run_cmd(vec!["ZRANGEBYSCORE", "myzset", "2", "3", "WITHSCORES"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 4);
            assert_eq!(match &arr[0] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "b");
            assert_eq!(match &arr[1] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "2");
            assert_eq!(match &arr[2] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "c");
            assert_eq!(match &arr[3] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "3");
        }
        _ => panic!("Expected Array, got {:?}", res),
    }

    // 3. LIMIT
    let res = run_cmd(vec!["ZRANGEBYSCORE", "myzset", "1", "5", "LIMIT", "1", "2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(match &arr[0] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "b");
            assert_eq!(match &arr[1] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "c");
        }
        _ => panic!("Expected Array, got {:?}", res),
    }

    // 4. Exclusive bounds
    let res = run_cmd(vec!["ZRANGEBYSCORE", "myzset", "(1", "3"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(match &arr[0] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "b");
            assert_eq!(match &arr[1] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "c");
        }
        _ => panic!("Expected Array, got {:?}", res),
    }
}

#[tokio::test]
async fn test_zrangebylex() {
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

    // Setup: ZADD myzset 0 a 0 b 0 c 0 d 0 e
    run_cmd(vec!["ZADD", "myzset", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e"], &mut conn_ctx, &server_ctx).await;

    // 1. Basic ZRANGEBYLEX
    let res = run_cmd(vec!["ZRANGEBYLEX", "myzset", "[b", "[d"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
            let members: Vec<String> = arr.into_iter().map(|r| match r { Resp::BulkString(Some(b)) => String::from_utf8_lossy(&b).to_string(), _ => panic!() }).collect();
            assert_eq!(members, vec!["b", "c", "d"]);
        }
        _ => panic!("Expected Array, got {:?}", res),
    }

    // 2. LIMIT
    let res = run_cmd(vec!["ZRANGEBYLEX", "myzset", "-", "+", "LIMIT", "1", "2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(match &arr[0] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "b");
            assert_eq!(match &arr[1] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "c");
        }
        _ => panic!("Expected Array, got {:?}", res),
    }
}
