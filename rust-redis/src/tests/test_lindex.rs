use crate::cmd::process_frame;
use crate::resp::Resp;
use bytes::Bytes;

#[tokio::test]
async fn test_lindex() {
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

    // RPUSH mylist a b c
    run_cmd(vec!["RPUSH", "mylist", "a", "b", "c"], &mut conn_ctx, &server_ctx).await;

    // LINDEX mylist 0 -> a
    let res = run_cmd(vec!["LINDEX", "mylist", "0"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("a")),
        _ => panic!("Expected 'a'"),
    }

    // LINDEX mylist 1 -> b
    let res = run_cmd(vec!["LINDEX", "mylist", "1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("b")),
        _ => panic!("Expected 'b'"),
    }

    // LINDEX mylist -1 -> c
    let res = run_cmd(vec!["LINDEX", "mylist", "-1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("c")),
        _ => panic!("Expected 'c'"),
    }

    // LINDEX mylist -2 -> b
    let res = run_cmd(vec!["LINDEX", "mylist", "-2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("b")),
        _ => panic!("Expected 'b'"),
    }

    // LINDEX mylist 3 -> nil
    let res = run_cmd(vec!["LINDEX", "mylist", "3"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(None) => {},
        _ => panic!("Expected nil, got {:?}", res),
    }

    // LINDEX mylist -4 -> nil
    let res = run_cmd(vec!["LINDEX", "mylist", "-4"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(None) => {},
        _ => panic!("Expected nil, got {:?}", res),
    }
    
    // LINDEX nonexist 0 -> nil
    let res = run_cmd(vec!["LINDEX", "nonexist", "0"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(None) => {},
        _ => panic!("Expected nil, got {:?}", res),
    }
}
