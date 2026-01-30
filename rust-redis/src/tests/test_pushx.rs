use crate::cmd::process_frame;
use crate::resp::Resp;
use bytes::Bytes;

#[tokio::test]
async fn test_pushx() {
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

    // LPUSHX on non-existent key -> 0
    let res = run_cmd(vec!["LPUSHX", "mylist", "a"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 0),
        _ => panic!("Expected Integer 0"),
    }

    // RPUSHX on non-existent key -> 0
    let res = run_cmd(vec!["RPUSHX", "mylist", "a"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 0),
        _ => panic!("Expected Integer 0"),
    }

    // Create list
    run_cmd(vec!["LPUSH", "mylist", "a"], &mut conn_ctx, &server_ctx).await;

    // LPUSHX on existing list -> 2 (b, a)
    let res = run_cmd(vec!["LPUSHX", "mylist", "b"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 2),
        _ => panic!("Expected Integer 2"),
    }

    // RPUSHX on existing list -> 3 (b, a, c)
    let res = run_cmd(vec!["RPUSHX", "mylist", "c"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 3),
        _ => panic!("Expected Integer 3"),
    }

    // Verify content
    let res = run_cmd(vec!["LRANGE", "mylist", "0", "-1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 3);
            match &items[0] { Resp::BulkString(Some(b)) => assert_eq!(b, &Bytes::from("b")), _ => panic!() }
            match &items[1] { Resp::BulkString(Some(b)) => assert_eq!(b, &Bytes::from("a")), _ => panic!() }
            match &items[2] { Resp::BulkString(Some(b)) => assert_eq!(b, &Bytes::from("c")), _ => panic!() }
        }
        _ => panic!("Expected Array"),
    }
}
