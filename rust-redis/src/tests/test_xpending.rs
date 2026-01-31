use crate::resp::Resp;
use crate::tests::helper::run_cmd;
use bytes::Bytes;

#[tokio::test]
async fn test_xpending_basic() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // 1. Setup: XADD and XGROUP
    run_cmd(vec!["XADD", "mystream", "1-0", "f1", "v1"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["XADD", "mystream", "2-0", "f2", "v2"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["XGROUP", "CREATE", "mystream", "mygroup", "0-0"], &mut conn_ctx, &server_ctx).await;

    // 2. XREADGROUP to create pending entries
    run_cmd(vec!["XREADGROUP", "GROUP", "mygroup", "consumer1", "COUNT", "1", "STREAMS", "mystream", ">"], &mut conn_ctx, &server_ctx).await;

    // 3. XPENDING summary
    let res = run_cmd(vec!["XPENDING", "mystream", "mygroup"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 4);
            assert_eq!(arr[0], Resp::Integer(1)); // 1 pending
            assert_eq!(arr[1], Resp::BulkString(Some(Bytes::from("1-0")))); // min ID
            assert_eq!(arr[2], Resp::BulkString(Some(Bytes::from("1-0")))); // max ID
            if let Resp::Array(Some(consumers)) = &arr[3] {
                assert_eq!(consumers.len(), 1);
                if let Resp::Array(Some(c1)) = &consumers[0] {
                    assert_eq!(c1[0], Resp::BulkString(Some(Bytes::from("consumer1"))));
                    assert_eq!(c1[1], Resp::BulkString(Some(Bytes::from("1"))));
                } else { panic!("Expected consumer array"); }
            } else { panic!("Expected consumers list"); }
        }
        _ => panic!("Expected summary array, got {:?}", res),
    }

    // 4. XREADGROUP more
    run_cmd(vec!["XREADGROUP", "GROUP", "mygroup", "consumer2", "COUNT", "1", "STREAMS", "mystream", ">"], &mut conn_ctx, &server_ctx).await;

    // 5. XPENDING summary again
    let res = run_cmd(vec!["XPENDING", "mystream", "mygroup"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr[0], Resp::Integer(2)); // 2 pending
            assert_eq!(arr[1], Resp::BulkString(Some(Bytes::from("1-0"))));
            assert_eq!(arr[2], Resp::BulkString(Some(Bytes::from("2-0"))));
        }
        _ => panic!("Expected summary array"),
    }

    // 6. XPENDING detailed
    let res = run_cmd(vec!["XPENDING", "mystream", "mygroup", "-", "+", "10"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2);
            if let Resp::Array(Some(e1)) = &arr[0] {
                assert_eq!(e1[0], Resp::BulkString(Some(Bytes::from("1-0"))));
                assert_eq!(e1[1], Resp::BulkString(Some(Bytes::from("consumer1"))));
                assert_eq!(e1[3], Resp::Integer(1)); // delivery count
            }
            if let Resp::Array(Some(e2)) = &arr[1] {
                assert_eq!(e2[0], Resp::BulkString(Some(Bytes::from("2-0"))));
                assert_eq!(e2[1], Resp::BulkString(Some(Bytes::from("consumer2"))));
            }
        }
        _ => panic!("Expected detailed array"),
    }

    // 7. XPENDING detailed with consumer filter
    let res = run_cmd(vec!["XPENDING", "mystream", "mygroup", "-", "+", "10", "consumer1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 1);
            if let Resp::Array(Some(e1)) = &arr[0] {
                assert_eq!(e1[1], Resp::BulkString(Some(Bytes::from("consumer1"))));
            }
        }
        _ => panic!("Expected detailed array with filter"),
    }

    // 8. XACK and verify PEL decreases
    run_cmd(vec!["XACK", "mystream", "mygroup", "1-0"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["XPENDING", "mystream", "mygroup"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr[0], Resp::Integer(1));
            assert_eq!(arr[1], Resp::BulkString(Some(Bytes::from("2-0"))));
        }
        _ => panic!("Expected summary array after XACK"),
    }
}

#[tokio::test]
async fn test_xpending_idle() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    run_cmd(vec!["XADD", "mystream", "1-0", "f1", "v1"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["XGROUP", "CREATE", "mystream", "mygroup", "0-0"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["XREADGROUP", "GROUP", "mygroup", "c1", "COUNT", "1", "STREAMS", "mystream", ">"], &mut conn_ctx, &server_ctx).await;

    // XPENDING with IDLE 1000 -> should be empty if we check immediately
    let res = run_cmd(vec!["XPENDING", "mystream", "mygroup", "IDLE", "1000", "-", "+", "10"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => assert_eq!(arr.len(), 0),
        _ => panic!("Expected empty array for IDLE 1000"),
    }

    // XPENDING with IDLE 0 -> should have 1 entry
    let res = run_cmd(vec!["XPENDING", "mystream", "mygroup", "IDLE", "0", "-", "+", "10"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => assert_eq!(arr.len(), 1),
        _ => panic!("Expected 1 entry for IDLE 0"),
    }
}

#[tokio::test]
async fn test_xpending_errors() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // NOGROUP error
    let res = run_cmd(vec!["XPENDING", "nosuchstream", "mygroup"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(e) => assert!(e.contains("NOGROUP")),
        _ => panic!("Expected NOGROUP error, got {:?}", res),
    }

    run_cmd(vec!["XADD", "mystream", "1-0", "f", "v"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["XPENDING", "mystream", "nosuchgroup"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(e) => assert!(e.contains("NOGROUP")),
        _ => panic!("Expected NOGROUP error"),
    }
}
