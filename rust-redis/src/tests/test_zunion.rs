use crate::resp::Resp;
use crate::tests::helper::run_cmd;

#[tokio::test]
async fn test_zunion() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Setup: 
    // zset1: a:1, b:2
    // zset2: b:3, c:4
    run_cmd(vec!["ZADD", "zset1", "1", "a", "2", "b"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["ZADD", "zset2", "3", "b", "4", "c"], &mut conn_ctx, &server_ctx).await;

    // 1. Basic ZUNION
    let res = run_cmd(vec!["ZUNION", "2", "zset1", "zset2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
            let members: Vec<String> = arr.into_iter().map(|r| match r { Resp::BulkString(Some(b)) => String::from_utf8_lossy(&b).to_string(), _ => panic!() }).collect();
            assert_eq!(members, vec!["a", "c", "b"]); // sorted by score: a:1, c:4, b:5
        }
        _ => panic!("Expected Array, got {:?}", res),
    }

    // 2. ZUNION with WITHSCORES
    let res = run_cmd(vec!["ZUNION", "2", "zset1", "zset2", "WITHSCORES"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 6);
            let s1 = match &arr[1] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() };
            let s3 = match &arr[3] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() };
            let s5 = match &arr[5] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() };
            assert_eq!(s1, "1"); // a
            assert_eq!(s3, "4"); // c
            assert_eq!(s5, "5"); // b (2+3)
        }
        _ => panic!("Expected Array of 6, got {:?}", res),
    }

    // 3. ZUNION with WEIGHTS
    let res = run_cmd(vec!["ZUNION", "2", "zset1", "zset2", "WEIGHTS", "10", "1", "WITHSCORES"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            // zset1*10: a:10, b:20
            // zset2*1:  b:3, c:4
            // union: a:10, c:4, b:23
            // sorted: c:4, a:10, b:23
            assert_eq!(arr.len(), 6);
            assert_eq!(match &arr[1] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "4"); // c
            assert_eq!(match &arr[3] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "10"); // a
            assert_eq!(match &arr[5] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "23"); // b
        }
        _ => panic!("Expected Array, got {:?}", res),
    }

    // 4. ZUNION with AGGREGATE MIN
    let res = run_cmd(vec!["ZUNION", "2", "zset1", "zset2", "AGGREGATE", "MIN", "WITHSCORES"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            // a:1, b:min(2,3)=2, c:4
            // sorted: a:1, b:2, c:4
            assert_eq!(arr.len(), 6);
            assert_eq!(match &arr[1] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "1"); // a
            assert_eq!(match &arr[3] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "2"); // b
            assert_eq!(match &arr[5] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "4"); // c
        }
        _ => panic!("Expected Array, got {:?}", res),
    }

    // 5. ZUNIONSTORE
    let res = run_cmd(vec!["ZUNIONSTORE", "out", "2", "zset1", "zset2", "WEIGHTS", "1", "100"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 3),
        _ => panic!("Expected Integer 3, got {:?}", res),
    }
    
    let res = run_cmd(vec!["ZRANGE", "out", "0", "-1", "WITHSCORES"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            // zset1*1: a:1, b:2
            // zset2*100: b:300, c:400
            // union: a:1, b:302, c:400
            assert_eq!(arr.len(), 6);
            assert_eq!(match &arr[1] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "1"); // a
            assert_eq!(match &arr[3] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "302"); // b
            assert_eq!(match &arr[5] { Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(), _ => panic!() }, "400"); // c
        }
        _ => panic!("Expected Array, got {:?}", res),
    }
}
