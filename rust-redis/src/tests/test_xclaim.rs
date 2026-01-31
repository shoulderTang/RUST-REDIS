use crate::resp::Resp;
use crate::tests::helper::run_cmd;
use bytes::Bytes;

#[tokio::test]
async fn test_xclaim_basic() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // 1. Setup: Stream with group and pending entry
    run_cmd(vec!["XADD", "mystream", "1-0", "f1", "v1"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["XGROUP", "CREATE", "mystream", "mygroup", "0-0"], &mut conn_ctx, &server_ctx).await;
    // Consumer1 reads it, making it pending for Consumer1
    run_cmd(vec!["XREADGROUP", "GROUP", "mygroup", "c1", "COUNT", "1", "STREAMS", "mystream", ">"], &mut conn_ctx, &server_ctx).await;

    // Verify it's pending for c1
    let res = run_cmd(vec!["XPENDING", "mystream", "mygroup", "-", "+", "10"], &mut conn_ctx, &server_ctx).await;
    if let Resp::Array(Some(arr)) = res {
        assert_eq!(arr.len(), 1);
        if let Resp::Array(Some(e)) = &arr[0] {
            assert_eq!(e[1], Resp::BulkString(Some(Bytes::from("c1"))));
        } else { panic!(); }
    } else { panic!(); }

    // 2. XCLAIM by c2 (min-idle-time 0)
    let res = run_cmd(vec!["XCLAIM", "mystream", "mygroup", "c2", "0", "1-0"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 1);
            if let Resp::Array(Some(e)) = &arr[0] {
                assert_eq!(e[0], Resp::BulkString(Some(Bytes::from("1-0"))));
            } else { panic!(); }
        }
        _ => panic!("Expected array, got {:?}", res),
    }

    // 3. Verify ownership changed to c2
    let res = run_cmd(vec!["XPENDING", "mystream", "mygroup", "-", "+", "10"], &mut conn_ctx, &server_ctx).await;
    if let Resp::Array(Some(arr)) = res {
        assert_eq!(arr.len(), 1);
        if let Resp::Array(Some(e)) = &arr[0] {
            assert_eq!(e[1], Resp::BulkString(Some(Bytes::from("c2"))));
            assert_eq!(e[3], Resp::Integer(2)); // delivery count incremented (1 from read, 1 from claim)
        } else { panic!(); }
    } else { panic!(); }
}

#[tokio::test]
async fn test_xclaim_min_idle() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    run_cmd(vec!["XADD", "mystream", "1-0", "f1", "v1"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["XGROUP", "CREATE", "mystream", "mygroup", "0-0"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["XREADGROUP", "GROUP", "mygroup", "c1", "COUNT", "1", "STREAMS", "mystream", ">"], &mut conn_ctx, &server_ctx).await;

    // XCLAIM with high min-idle-time (10000ms) -> should return empty
    let res = run_cmd(vec!["XCLAIM", "mystream", "mygroup", "c2", "10000", "1-0"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => assert_eq!(arr.len(), 0),
        _ => panic!(),
    }
}

#[tokio::test]
async fn test_xautoclaim_basic() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    run_cmd(vec!["XADD", "mystream", "1-0", "f1", "v1"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["XADD", "mystream", "2-0", "f2", "v2"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["XGROUP", "CREATE", "mystream", "mygroup", "0-0"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["XREADGROUP", "GROUP", "mygroup", "c1", "COUNT", "2", "STREAMS", "mystream", ">"], &mut conn_ctx, &server_ctx).await;

    // XAUTOCLAIM c2 0 0-0
    let res = run_cmd(vec!["XAUTOCLAIM", "mystream", "mygroup", "c2", "0", "0-0", "COUNT", "1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], Resp::BulkString(Some(Bytes::from("2-0")))); // next start id
            if let Resp::Array(Some(claimed)) = &arr[1] {
                assert_eq!(claimed.len(), 1);
            } else { panic!(); }
        }
        _ => panic!(),
    }

    // Verify c2 owns 1-0
    let res = run_cmd(vec!["XPENDING", "mystream", "mygroup", "-", "+", "10", "c2"], &mut conn_ctx, &server_ctx).await;
    if let Resp::Array(Some(arr)) = res {
        assert_eq!(arr.len(), 1);
    } else { panic!(); }
}
