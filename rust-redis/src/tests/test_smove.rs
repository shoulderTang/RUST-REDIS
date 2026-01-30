use crate::resp::Resp;
use crate::tests::helper::run_cmd;

#[tokio::test]
async fn test_smove() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Setup: SADD source m1 m2
    run_cmd(vec!["SADD", "source", "m1", "m2"], &mut conn_ctx, &server_ctx).await;
    // Setup: SADD dest m3
    run_cmd(vec!["SADD", "dest", "m3"], &mut conn_ctx, &server_ctx).await;

    // 1. SMOVE source dest m1 -> 1 (success)
    let res = run_cmd(vec!["SMOVE", "source", "dest", "m1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1, "Expected 1 for successful SMOVE"),
        _ => panic!("Expected Integer(1)"),
    }

    // Verify m1 moved
    let res = run_cmd(vec!["SISMEMBER", "source", "m1"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(0));
    let res = run_cmd(vec!["SISMEMBER", "dest", "m1"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(1));

    // 2. SMOVE source dest non_exist -> 0 (member not in source)
    let res = run_cmd(vec!["SMOVE", "source", "dest", "non_exist"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 0, "Expected 0 for missing member"),
        _ => panic!("Expected Integer(0)"),
    }

    // 3. SMOVE non_exist_key dest m1 -> 0 (source key not exists)
    let res = run_cmd(vec!["SMOVE", "non_exist_key", "dest", "m1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 0, "Expected 0 for missing source key"),
        _ => panic!("Expected Integer(0)"),
    }

    // 4. WRONGTYPE source
    run_cmd(vec!["SET", "wrong_src", "val"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["SMOVE", "wrong_src", "dest", "m1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(msg) => assert!(msg.contains("WRONGTYPE"), "Expected WRONGTYPE error"),
        _ => panic!("Expected Error"),
    }

    // 5. WRONGTYPE dest
    run_cmd(vec!["SET", "wrong_dest", "val"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["SMOVE", "source", "wrong_dest", "m2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(msg) => assert!(msg.contains("WRONGTYPE"), "Expected WRONGTYPE error"),
        _ => panic!("Expected Error"),
    }

    // 6. Source becomes empty
    // source has m2 left (m1 moved). Move m2 to dest.
    let res = run_cmd(vec!["SMOVE", "source", "dest", "m2"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(1));
    
    // Check source is gone (or empty)
    let res = run_cmd(vec!["EXISTS", "source"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(0), "Source key should be removed when empty");
}
