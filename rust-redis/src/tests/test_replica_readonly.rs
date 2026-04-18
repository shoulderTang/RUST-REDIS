use crate::resp::Resp;
use crate::tests::helper::{create_connection_context, create_server_context, run_cmd};
use bytes::Bytes;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn test_replica_read_only() {
    let ctx = create_server_context();
    let mut conn_ctx = create_connection_context();

    // 1. Set as Slave
    {
        let mut role = ctx.repl.replication_role.write().unwrap();
        *role = crate::cmd::ReplicationRole::Slave;
    }
    ctx.repl.replica_read_only.store(true, Ordering::Relaxed);

    // 2. Try write command (SET) -> Should fail
    let res = run_cmd(vec!["SET", "foo", "bar"], &mut conn_ctx, &ctx).await;
    match res {
        Resp::Error(e) => assert!(e.contains("READONLY"), "Expected READONLY error, got {}", e),
        _ => panic!("Expected Error, got {:?}", res),
    }

    // 3. Try read command (GET) -> Should succeed (return null)
    let res = run_cmd(vec!["GET", "foo"], &mut conn_ctx, &ctx).await;
    match res {
        Resp::BulkString(None) => {}
        _ => panic!("Expected Nil, got {:?}", res),
    }

    // 4. Simulate Master connection (is_master = true)
    conn_ctx.is_master = true;

    // 5. Try write command from master -> Should succeed
    let res = run_cmd(vec!["SET", "foo", "bar"], &mut conn_ctx, &ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    // 6. Disable read-only
    ctx.repl.replica_read_only.store(false, Ordering::Relaxed);
    conn_ctx.is_master = false;

    // 7. Try write command -> Should succeed
    let res = run_cmd(vec!["SET", "foo2", "bar2"], &mut conn_ctx, &ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));
}
