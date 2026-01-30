use crate::resp::Resp;
use bytes::Bytes;
use crate::tests::helper::run_cmd;

#[tokio::test]
async fn test_hincrby() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // --- HINCRBY Tests ---

    // 1. New key
    let res = run_cmd(vec!["HINCRBY", "myhash", "field1", "1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 1),
        _ => panic!("Expected Integer 1, got {:?}", res),
    }

    // Verify
    let res = run_cmd(vec!["HGET", "myhash", "field1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("1")),
        _ => panic!("Expected 1"),
    }

    // 2. Increment again
    let res = run_cmd(vec!["HINCRBY", "myhash", "field1", "5"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 6),
        _ => panic!("Expected Integer 6, got {:?}", res),
    }

    // 3. Decrement
    let res = run_cmd(vec!["HINCRBY", "myhash", "field1", "-10"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, -4),
        _ => panic!("Expected Integer -4, got {:?}", res),
    }

    // 4. New field
    let res = run_cmd(vec!["HINCRBY", "myhash", "field2", "100"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 100),
        _ => panic!("Expected Integer 100, got {:?}", res),
    }

    // 5. Wrong type
    run_cmd(vec!["SET", "mystring", "foo"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["HINCRBY", "mystring", "field", "1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(e) => assert!(e.contains("WRONGTYPE")),
        _ => panic!("Expected WRONGTYPE, got {:?}", res),
    }

    // 6. Value not integer
    run_cmd(vec!["HSET", "myhash", "field3", "notanumber"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["HINCRBY", "myhash", "field3", "1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(e) => assert!(e.contains("hash value is not an integer")),
        _ => panic!("Expected error about value, got {:?}", res),
    }
}

#[tokio::test]
async fn test_hincrbyfloat() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // --- HINCRBYFLOAT Tests ---

    // 1. New key
    let res = run_cmd(vec!["HINCRBYFLOAT", "myfloat", "field1", "10.5"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("10.5")),
        _ => panic!("Expected BulkString 10.5, got {:?}", res),
    }

    // 2. Increment
    let res = run_cmd(vec!["HINCRBYFLOAT", "myfloat", "field1", "0.1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("10.6")),
        _ => panic!("Expected BulkString 10.6, got {:?}", res),
    }

    // 3. Negative
    let res = run_cmd(vec!["HINCRBYFLOAT", "myfloat", "field1", "-5"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("5.6")),
        _ => panic!("Expected BulkString 5.6, got {:?}", res),
    }

    // 4. Scientific
    let res = run_cmd(vec!["HINCRBYFLOAT", "myfloat", "field1", "2.0e2"], &mut conn_ctx, &server_ctx).await;
    // 5.6 + 200 = 205.6
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("205.6")),
        _ => panic!("Expected BulkString 205.6, got {:?}", res),
    }

    // 5. Wrong type
    run_cmd(vec!["SET", "mystring", "foo"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["HINCRBYFLOAT", "mystring", "field", "1.0"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(e) => assert!(e.contains("WRONGTYPE")),
        _ => panic!("Expected WRONGTYPE, got {:?}", res),
    }

    // 6. Value not float
    run_cmd(vec!["HSET", "myfloat", "field3", "notanumber"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["HINCRBYFLOAT", "myfloat", "field3", "1.0"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(e) => assert!(e.contains("hash value is not a float")),
        _ => panic!("Expected error about value, got {:?}", res),
    }
}
