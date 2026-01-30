use crate::resp::Resp;
use crate::tests::helper::run_cmd;

#[tokio::test]
async fn test_zcount() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Setup: ZADD myzset 1 a 2 b 3 c 4 d 5 e
    run_cmd(vec!["ZADD", "myzset", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e"], &mut conn_ctx, &server_ctx).await;

    // 1. All elements
    let res = run_cmd(vec!["ZCOUNT", "myzset", "-inf", "+inf"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(5));

    // 2. Inclusive range
    let res = run_cmd(vec!["ZCOUNT", "myzset", "2", "4"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(3)); // 2, 3, 4

    // 3. Exclusive min
    let res = run_cmd(vec!["ZCOUNT", "myzset", "(2", "4"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(2)); // 3, 4

    // 4. Exclusive max
    let res = run_cmd(vec!["ZCOUNT", "myzset", "2", "(4"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(2)); // 2, 3

    // 5. Both exclusive
    let res = run_cmd(vec!["ZCOUNT", "myzset", "(2", "(4"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(1)); // 3

    // 6. Non-existent key
    let res = run_cmd(vec!["ZCOUNT", "nonexistent", "0", "10"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(0));

    // 7. min > max
    let res = run_cmd(vec!["ZCOUNT", "myzset", "10", "0"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(0));

    // 8. inf without +/-
    let res = run_cmd(vec!["ZCOUNT", "myzset", "-inf", "inf"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(5));
}
