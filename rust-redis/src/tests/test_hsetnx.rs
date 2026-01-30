use crate::cmd::process_frame;
use crate::resp::Resp;
use bytes::Bytes;

#[tokio::test]
async fn test_hsetnx() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    async fn run_cmd(args: Vec<&str>, conn_ctx: &mut crate::cmd::ConnectionContext, server_ctx: &crate::cmd::ServerContext) -> Resp {
        let mut resp_args = Vec::new();
        for arg in args {
            resp_args.push(Resp::BulkString(Some(Bytes::from(arg.to_string()))));
        }
        let req = Resp::Array(Some(resp_args));
        let (res, _) = process_frame(req, conn_ctx, server_ctx).await;
        res
    }

    // 1. HSETNX new key
    let res = run_cmd(vec!["HSETNX", "myhash", "field1", "value1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 1),
        _ => panic!("Expected Integer 1, got {:?}", res),
    }

    // Verify
    let res = run_cmd(vec!["HGET", "myhash", "field1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("value1")),
        _ => panic!("Expected value1"),
    }

    // 2. HSETNX existing field
    let res = run_cmd(vec!["HSETNX", "myhash", "field1", "value2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 0),
        _ => panic!("Expected Integer 0, got {:?}", res),
    }

    // Verify value didn't change
    let res = run_cmd(vec!["HGET", "myhash", "field1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("value1")),
        _ => panic!("Expected value1"),
    }

    // 3. HSETNX new field in existing key
    let res = run_cmd(vec!["HSETNX", "myhash", "field2", "value2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 1),
        _ => panic!("Expected Integer 1, got {:?}", res),
    }

    // Verify
    let res = run_cmd(vec!["HGET", "myhash", "field2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("value2")),
        _ => panic!("Expected value2"),
    }

    // 4. Wrong type
    run_cmd(vec!["SET", "mystring", "foo"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["HSETNX", "mystring", "field", "value"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(e) => assert!(e.contains("WRONGTYPE")),
        _ => panic!("Expected WRONGTYPE, got {:?}", res),
    }
}
