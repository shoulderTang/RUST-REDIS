use crate::resp::Resp;
use bytes::Bytes;
use crate::tests::helper::run_cmd;

#[tokio::test]
async fn test_lindex() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

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
