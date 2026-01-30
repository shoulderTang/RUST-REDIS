use crate::resp::Resp;
use crate::tests::helper::run_cmd;
use bytes::Bytes;

#[tokio::test]
async fn test_xtrim_maxlen() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Setup: XADD mystream 1-0 f1 v1, 2-0 f2 v2, 3-0 f3 v3
    run_cmd(vec!["XADD", "mystream", "1-0", "f1", "v1"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["XADD", "mystream", "2-0", "f2", "v2"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["XADD", "mystream", "3-0", "f3", "v3"], &mut conn_ctx, &server_ctx).await;

    // Verify length is 3
    let res = run_cmd(vec!["XLEN", "mystream"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(3));

    // Trim to MAXLEN 2
    let res = run_cmd(vec!["XTRIM", "mystream", "MAXLEN", "2"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(1)); // 1 element removed (1-0)

    // Verify length is 2
    let res = run_cmd(vec!["XLEN", "mystream"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(2));

    // Verify remaining IDs are 2-0 and 3-0
    let res = run_cmd(vec!["XRANGE", "mystream", "-", "+"], &mut conn_ctx, &server_ctx).await;
    if let Resp::Array(Some(arr)) = res {
        assert_eq!(arr.len(), 2);
        if let Resp::Array(Some(entry1)) = &arr[0] {
             assert_eq!(entry1[0], Resp::BulkString(Some(Bytes::from("2-0"))));
        }
        if let Resp::Array(Some(entry2)) = &arr[1] {
             assert_eq!(entry2[0], Resp::BulkString(Some(Bytes::from("3-0"))));
        }
    } else {
        panic!("Expected Array");
    }
}

#[tokio::test]
async fn test_xtrim_minid() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Setup: XADD mystream 1-0 f1 v1, 2-0 f2 v2, 3-0 f3 v3
    run_cmd(vec!["XADD", "mystream", "1-0", "f1", "v1"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["XADD", "mystream", "2-0", "f2", "v2"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["XADD", "mystream", "3-0", "f3", "v3"], &mut conn_ctx, &server_ctx).await;

    // Trim to MINID 2-0
    let res = run_cmd(vec!["XTRIM", "mystream", "MINID", "2-0"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(1)); // 1 element removed (1-0)

    // Verify length is 2
    let res = run_cmd(vec!["XLEN", "mystream"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(2));

    // Trim to MINID 4-0 (removes everything)
    let res = run_cmd(vec!["XTRIM", "mystream", "MINID", "4-0"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(2)); // 2 elements removed (2-0, 3-0)

    let res = run_cmd(vec!["XLEN", "mystream"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(0));
}
