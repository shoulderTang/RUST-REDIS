use crate::resp::Resp;
use crate::tests::helper::run_cmd;
use bytes::Bytes;

#[tokio::test]
async fn test_watch_basic() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn1 = crate::tests::helper::create_connection_context();
    let mut conn2 = crate::tests::helper::create_connection_context();
    conn1.id = 1;
    conn2.id = 2;
    
    // Register dirty flags in server context (as done in server.rs)
    server_ctx.client_watched_dirty.insert(conn1.id, conn1.watched_keys_dirty.clone());
    server_ctx.client_watched_dirty.insert(conn2.id, conn2.watched_keys_dirty.clone());

    // 1. Client 1 watches 'foo'
    run_cmd(vec!["WATCH", "foo"], &mut conn1, &server_ctx).await;

    // 2. Client 2 modifies 'foo'
    run_cmd(vec!["SET", "foo", "bar"], &mut conn2, &server_ctx).await;

    // 3. Client 1 tries to execute a transaction
    run_cmd(vec!["MULTI"], &mut conn1, &server_ctx).await;
    run_cmd(vec!["SET", "foo", "baz"], &mut conn1, &server_ctx).await;
    let res = run_cmd(vec!["EXEC"], &mut conn1, &server_ctx).await;

    // EXEC should return nil array because 'foo' was modified
    assert_eq!(res, Resp::Array(None));
}

#[tokio::test]
async fn test_watch_no_modification() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn1 = crate::tests::helper::create_connection_context();
    conn1.id = 1;
    server_ctx.client_watched_dirty.insert(conn1.id, conn1.watched_keys_dirty.clone());

    run_cmd(vec!["WATCH", "foo"], &mut conn1, &server_ctx).await;
    run_cmd(vec!["MULTI"], &mut conn1, &server_ctx).await;
    run_cmd(vec!["SET", "foo", "bar"], &mut conn1, &server_ctx).await;
    let res = run_cmd(vec!["EXEC"], &mut conn1, &server_ctx).await;

    // EXEC should succeed because 'foo' was not modified by anyone else
    if let Resp::Array(Some(arr)) = res {
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0], Resp::SimpleString(Bytes::from_static(b"OK")));
    } else { panic!("Expected array, got {:?}", res); }
}

#[tokio::test]
async fn test_unwatch() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn1 = crate::tests::helper::create_connection_context();
    let mut conn2 = crate::tests::helper::create_connection_context();
    conn1.id = 1;
    conn2.id = 2;
    server_ctx.client_watched_dirty.insert(conn1.id, conn1.watched_keys_dirty.clone());
    server_ctx.client_watched_dirty.insert(conn2.id, conn2.watched_keys_dirty.clone());

    run_cmd(vec!["WATCH", "foo"], &mut conn1, &server_ctx).await;
    run_cmd(vec!["UNWATCH"], &mut conn1, &server_ctx).await;
    
    run_cmd(vec!["SET", "foo", "bar"], &mut conn2, &server_ctx).await;

    run_cmd(vec!["MULTI"], &mut conn1, &server_ctx).await;
    run_cmd(vec!["SET", "foo", "baz"], &mut conn1, &server_ctx).await;
    let res = run_cmd(vec!["EXEC"], &mut conn1, &server_ctx).await;

    // EXEC should succeed because of UNWATCH
    if let Resp::Array(Some(arr)) = res {
        assert_eq!(arr.len(), 1);
    } else { panic!("Expected array, got {:?}", res); }
}

#[tokio::test]
async fn test_watch_triggered_by_exec() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn1 = crate::tests::helper::create_connection_context();
    let mut conn2 = crate::tests::helper::create_connection_context();
    conn1.id = 1;
    conn2.id = 2;
    server_ctx.client_watched_dirty.insert(conn1.id, conn1.watched_keys_dirty.clone());
    server_ctx.client_watched_dirty.insert(conn2.id, conn2.watched_keys_dirty.clone());

    run_cmd(vec!["WATCH", "foo"], &mut conn1, &server_ctx).await;

    // Client 2 modifies 'foo' via EXEC
    run_cmd(vec!["MULTI"], &mut conn2, &server_ctx).await;
    run_cmd(vec!["SET", "foo", "bar"], &mut conn2, &server_ctx).await;
    run_cmd(vec!["EXEC"], &mut conn2, &server_ctx).await;

    // Client 1 tries to execute a transaction
    run_cmd(vec!["MULTI"], &mut conn1, &server_ctx).await;
    run_cmd(vec!["SET", "foo", "baz"], &mut conn1, &server_ctx).await;
    let res = run_cmd(vec!["EXEC"], &mut conn1, &server_ctx).await;

    assert_eq!(res, Resp::Array(None));
}
