use crate::resp::Resp;
use crate::tests::helper::run_cmd;

#[tokio::test]
async fn test_zlexcount() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Setup: ZADD myzset 0 a 0 b 0 c 0 d 0 e
    run_cmd(vec!["ZADD", "myzset", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e"], &mut conn_ctx, &server_ctx).await;

    // 1. All elements
    let res = run_cmd(vec!["ZLEXCOUNT", "myzset", "-", "+"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(5));

    // 2. Inclusive range
    let res = run_cmd(vec!["ZLEXCOUNT", "myzset", "[b", "[d"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(3)); // b, c, d

    // 3. Exclusive min
    let res = run_cmd(vec!["ZLEXCOUNT", "myzset", "(b", "[d"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(2)); // c, d

    // 4. Exclusive max
    let res = run_cmd(vec!["ZLEXCOUNT", "myzset", "[b", "(d"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(2)); // b, c

    // 5. Both exclusive
    let res = run_cmd(vec!["ZLEXCOUNT", "myzset", "(b", "(d"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(1)); // c

    // 6. Non-existent key
    let res = run_cmd(vec!["ZLEXCOUNT", "nonexistent", "-", "+"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(0));

    // 7. min > max
    let res = run_cmd(vec!["ZLEXCOUNT", "myzset", "[z", "[a"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(0));
}
