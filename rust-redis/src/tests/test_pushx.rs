use crate::resp::Resp;
use bytes::Bytes;
use crate::tests::helper::run_cmd;

#[tokio::test]
async fn test_pushx() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

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
