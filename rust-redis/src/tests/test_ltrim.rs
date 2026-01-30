use crate::cmd::process_frame;
use crate::resp::Resp;
use bytes::Bytes;

#[tokio::test]
async fn test_ltrim() {
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

    // RPUSH mylist a b c d e
    run_cmd(vec!["RPUSH", "mylist", "a", "b", "c", "d", "e"], &mut conn_ctx, &server_ctx).await;

    // LTRIM mylist 1 3 -> b c d
    let res = run_cmd(vec!["LTRIM", "mylist", "1", "3"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::SimpleString(b) => assert_eq!(b, Bytes::from("OK")),
        _ => panic!("Expected OK"),
    }

    // LRANGE mylist 0 -1
    let res = run_cmd(vec!["LRANGE", "mylist", "0", "-1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 3);
            match &items[0] { Resp::BulkString(Some(b)) => assert_eq!(b, &Bytes::from("b")), _ => panic!() }
            match &items[1] { Resp::BulkString(Some(b)) => assert_eq!(b, &Bytes::from("c")), _ => panic!() }
            match &items[2] { Resp::BulkString(Some(b)) => assert_eq!(b, &Bytes::from("d")), _ => panic!() }
        }
        _ => panic!("Expected Array"),
    }

    // Reset
    run_cmd(vec!["DEL", "mylist"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["RPUSH", "mylist", "a", "b", "c", "d", "e"], &mut conn_ctx, &server_ctx).await;

    // LTRIM mylist 0 -2 -> a b c d
    let res = run_cmd(vec!["LTRIM", "mylist", "0", "-2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::SimpleString(b) => assert_eq!(b, Bytes::from("OK")),
        _ => panic!("Expected OK"),
    }

    // LRANGE mylist 0 -1
    let res = run_cmd(vec!["LRANGE", "mylist", "0", "-1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 4);
            match &items[3] { Resp::BulkString(Some(b)) => assert_eq!(b, &Bytes::from("d")), _ => panic!() }
        }
        _ => panic!("Expected Array"),
    }

    // LTRIM mylist 2 1 -> empty
    let res = run_cmd(vec!["LTRIM", "mylist", "2", "1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::SimpleString(b) => assert_eq!(b, Bytes::from("OK")),
        _ => panic!("Expected OK"),
    }

    // LRANGE mylist 0 -1
    let res = run_cmd(vec!["LRANGE", "mylist", "0", "-1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => assert_eq!(items.len(), 0),
        _ => panic!("Expected Array"),
    }

    // Reset
    run_cmd(vec!["DEL", "mylist"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["RPUSH", "mylist", "a", "b", "c"], &mut conn_ctx, &server_ctx).await;

    // LTRIM mylist 0 -100 -> empty
    let res = run_cmd(vec!["LTRIM", "mylist", "0", "-100"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::SimpleString(b) => assert_eq!(b, Bytes::from("OK")),
        _ => panic!("Expected OK"),
    }

    // LRANGE mylist 0 -1
    let res = run_cmd(vec!["LRANGE", "mylist", "0", "-1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => assert_eq!(items.len(), 0),
        _ => panic!("Expected Array"),
    }
}
