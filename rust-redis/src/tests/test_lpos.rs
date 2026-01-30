use crate::resp::Resp;
use crate::tests::helper::run_cmd;

#[tokio::test]
async fn test_lpos() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Prepare list: a b c a b c
    run_cmd(vec!["RPUSH", "mylist", "a", "b", "c", "a", "b", "c"], &mut conn_ctx, &server_ctx).await;

    // 1. Basic LPOS
    // LPOS mylist a -> 0
    let res = run_cmd(vec!["LPOS", "mylist", "a"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 0),
        _ => panic!("Expected Integer 0, got {:?}", res),
    }
    // LPOS mylist c -> 2
    let res = run_cmd(vec!["LPOS", "mylist", "c"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 2),
        _ => panic!("Expected Integer 2, got {:?}", res),
    }

    // 2. RANK
    // LPOS mylist a RANK 1 -> 0
    let res = run_cmd(vec!["LPOS", "mylist", "a", "RANK", "1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 0),
        _ => panic!("Expected Integer 0, got {:?}", res),
    }
    // LPOS mylist a RANK 2 -> 3 (index 3 is the second 'a')
    let res = run_cmd(vec!["LPOS", "mylist", "a", "RANK", "2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 3),
        _ => panic!("Expected Integer 3, got {:?}", res),
    }
    // LPOS mylist a RANK -1 -> 3
    let res = run_cmd(vec!["LPOS", "mylist", "a", "RANK", "-1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 3),
        _ => panic!("Expected Integer 3, got {:?}", res),
    }
    // LPOS mylist a RANK -2 -> 0
    let res = run_cmd(vec!["LPOS", "mylist", "a", "RANK", "-2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 0),
        _ => panic!("Expected Integer 0, got {:?}", res),
    }

    // 3. COUNT
    // LPOS mylist a COUNT 2 -> [0, 3]
    let res = run_cmd(vec!["LPOS", "mylist", "a", "COUNT", "2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            match &items[0] { Resp::Integer(n) => assert_eq!(*n, 0), _ => panic!("Expected Integer 0") }
            match &items[1] { Resp::Integer(n) => assert_eq!(*n, 3), _ => panic!("Expected Integer 3") }
        }
        _ => panic!("Expected Array, got {:?}", res),
    }
    // LPOS mylist a COUNT 0 -> [0, 3]
    let res = run_cmd(vec!["LPOS", "mylist", "a", "COUNT", "0"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            match &items[0] { Resp::Integer(n) => assert_eq!(*n, 0), _ => panic!("Expected Integer 0") }
            match &items[1] { Resp::Integer(n) => assert_eq!(*n, 3), _ => panic!("Expected Integer 3") }
        }
        _ => panic!("Expected Array, got {:?}", res),
    }
    // LPOS mylist a RANK -1 COUNT 2 -> [3, 0]
    let res = run_cmd(vec!["LPOS", "mylist", "a", "RANK", "-1", "COUNT", "2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            match &items[0] { Resp::Integer(n) => assert_eq!(*n, 3), _ => panic!("Expected Integer 3") }
            match &items[1] { Resp::Integer(n) => assert_eq!(*n, 0), _ => panic!("Expected Integer 0") }
        }
        _ => panic!("Expected Array [3, 0], got {:?}", res),
    }

    // 4. MAXLEN
    // LPOS mylist a RANK 2 MAXLEN 2 -> nil (because second a is at index 3, which is > maxlen if maxlen counts items checked?)
    let res = run_cmd(vec!["LPOS", "mylist", "a", "RANK", "2", "MAXLEN", "2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(None) => {},
        _ => panic!("Expected Nil, got {:?}", res),
    }

    // 5. Non-existent key
    let res = run_cmd(vec!["LPOS", "nosuchkey", "a"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(None) => {},
        _ => panic!("Expected Nil, got {:?}", res),
    }
    
    // 6. Wrong type
    run_cmd(vec!["SET", "mystring", "foo"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["LPOS", "mystring", "a"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(e) => assert!(e.contains("WRONGTYPE")),
        _ => panic!("Expected WRONGTYPE, got {:?}", res),
    }
}
