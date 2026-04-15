use crate::resp::Resp;
use crate::tests::helper::{create_connection_context, create_server_context, run_cmd};
use bytes::Bytes;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn test_min_replicas_to_write() {
    let ctx = create_server_context();
    let mut conn_ctx = create_connection_context();

    // 1. Enable min-replicas-to-write = 1
    ctx.min_replicas_to_write.store(1, Ordering::Relaxed);
    ctx.min_replicas_max_lag.store(10, Ordering::Relaxed);

    // 2. Try write -> should fail (0 replicas)
    let res = run_cmd(vec!["SET", "k1", "v1"], &mut conn_ctx, &ctx).await;
    match res {
        Resp::Error(e) => assert!(e.contains("NOREPLICAS"), "Expected NOREPLICAS, got {}", e),
        _ => panic!("Expected Error, got {:?}", res),
    }

    // 3. Add a replica and simulate ACK
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    ctx.replica_ack_time.insert(1, now); // Just acknowledged

    // 4. Try write -> should succeed
    let res = run_cmd(vec!["SET", "k1", "v1"], &mut conn_ctx, &ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    // 5. Simulate lag
    let old_time = now - 11; // 11 seconds ago (max lag is 10)
    ctx.replica_ack_time.insert(1, old_time);

    // 6. Try write -> should fail
    let res = run_cmd(vec!["SET", "k2", "v2"], &mut conn_ctx, &ctx).await;
    match res {
        Resp::Error(e) => assert!(e.contains("NOREPLICAS"), "Expected NOREPLICAS, got {}", e),
        _ => panic!("Expected Error, got {:?}", res),
    }
}
