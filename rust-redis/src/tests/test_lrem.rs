use crate::cmd::process_frame;
use crate::resp::Resp;
use bytes::Bytes;

#[tokio::test]
async fn test_lrem() {
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

    // RPUSH mylist a b a c a
    run_cmd(vec!["RPUSH", "mylist", "a", "b", "a", "c", "a"], &mut conn_ctx, &server_ctx).await;

    // LREM mylist 2 a
    let res = run_cmd(vec!["LREM", "mylist", "2", "a"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 2),
        _ => panic!("Expected Integer(2), got {:?}", res),
    }

    // LRANGE mylist 0 -1 -> b c a
    let res = run_cmd(vec!["LRANGE", "mylist", "0", "-1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 3);
            match &items[0] { Resp::BulkString(Some(b)) => assert_eq!(b, &Bytes::from("b")), _ => panic!() }
            match &items[1] { Resp::BulkString(Some(b)) => assert_eq!(b, &Bytes::from("c")), _ => panic!() }
            match &items[2] { Resp::BulkString(Some(b)) => assert_eq!(b, &Bytes::from("a")), _ => panic!() }
        }
        _ => panic!("Expected Array"),
    }

    // Reset: DEL mylist
    run_cmd(vec!["DEL", "mylist"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["RPUSH", "mylist", "a", "b", "a", "c", "a"], &mut conn_ctx, &server_ctx).await;

    // LREM mylist -2 a
    let res = run_cmd(vec!["LREM", "mylist", "-2", "a"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 2),
        _ => panic!("Expected Integer(2)"),
    }

    // LRANGE mylist 0 -1 -> a b c
    let res = run_cmd(vec!["LRANGE", "mylist", "0", "-1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 3);
            match &items[0] { Resp::BulkString(Some(b)) => assert_eq!(b, &Bytes::from("a")), _ => panic!() }
            match &items[1] { Resp::BulkString(Some(b)) => assert_eq!(b, &Bytes::from("b")), _ => panic!() }
            match &items[2] { Resp::BulkString(Some(b)) => assert_eq!(b, &Bytes::from("c")), _ => panic!() }
        }
        _ => panic!("Expected Array"),
    }

    // Reset
    run_cmd(vec!["DEL", "mylist"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["RPUSH", "mylist", "a", "b", "a", "c", "a"], &mut conn_ctx, &server_ctx).await;

    // LREM mylist 0 a
    let res = run_cmd(vec!["LREM", "mylist", "0", "a"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 3),
        _ => panic!("Expected Integer(3)"),
    }

    // LRANGE mylist 0 -1 -> b c
    let res = run_cmd(vec!["LRANGE", "mylist", "0", "-1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            match &items[0] { Resp::BulkString(Some(b)) => assert_eq!(b, &Bytes::from("b")), _ => panic!() }
            match &items[1] { Resp::BulkString(Some(b)) => assert_eq!(b, &Bytes::from("c")), _ => panic!() }
        }
        _ => panic!("Expected Array"),
    }
}
