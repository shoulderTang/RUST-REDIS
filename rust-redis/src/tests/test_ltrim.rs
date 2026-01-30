use crate::resp::Resp;
use bytes::Bytes;
use crate::tests::helper::run_cmd;

#[tokio::test]
async fn test_ltrim() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

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
