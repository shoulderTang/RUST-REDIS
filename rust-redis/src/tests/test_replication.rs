use crate::resp::Resp;
use crate::tests::helper::{create_connection_context, create_server_context, run_cmd};
use bytes::Bytes;

#[tokio::test]
async fn test_replicaof_and_role() {
    let server_ctx = create_server_context();
    let mut conn_ctx = create_connection_context();

    let res = run_cmd(vec!["ROLE"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(
                arr.get(0),
                Some(&Resp::BulkString(Some(Bytes::from("master"))))
            );
        }
        _ => panic!("Expected ROLE array, got {:?}", res),
    }

    let res = run_cmd(
        vec!["REPLICAOF", "127.0.0.1", "6379"],
        &mut conn_ctx,
        &server_ctx,
    )
    .await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    let res = run_cmd(vec!["ROLE"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(
                arr.get(0),
                Some(&Resp::BulkString(Some(Bytes::from("slave"))))
            );
            assert_eq!(
                arr.get(1),
                Some(&Resp::BulkString(Some(Bytes::from("127.0.0.1"))))
            );
            assert_eq!(arr.get(2), Some(&Resp::Integer(6379)));
        }
        _ => panic!("Expected ROLE array as slave, got {:?}", res),
    }

    let res = run_cmd(vec!["REPLICAOF", "NO", "ONE"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    let res = run_cmd(vec!["ROLE"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(
                arr.get(0),
                Some(&Resp::BulkString(Some(Bytes::from("master"))))
            );
        }
        _ => panic!("Expected ROLE array, got {:?}", res),
    }
}

#[tokio::test]
async fn test_expire_propagation() {
    let ctx = create_server_context();
    // Set as Master
    {
        let mut role = ctx.repl.replication_role.write().unwrap();
        *role = crate::cmd::ReplicationRole::Master;
    }

    // Start background task
    crate::cmd::start_expiration_task(ctx.clone());

    // Create a mock replica
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    ctx.repl.replicas.insert(1, tx);

    // Set a key with short expiration
    let key = "expire_me";
    {
        let db = ctx.databases[0].write().unwrap();
        let val = crate::db::Value::String(bytes::Bytes::from("val"));
        // Expire in 100ms
        let expires_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 100;

        let v = crate::db::Entry::new_with_expire(val, Some(expires_at));
        db.insert(bytes::Bytes::from(key), v);
    }

    // Wait for expiration
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    // Check if we received DEL
    let mut received_del = false;
    let mut received_select = false;

    while let Ok(frame) = rx.try_recv() {
        match frame {
            crate::resp::Resp::Array(Some(items)) => {
                if let Some(crate::resp::Resp::BulkString(Some(cmd))) = items.get(0) {
                    let cmd_str = String::from_utf8_lossy(cmd);
                    if cmd_str == "DEL" {
                        if let Some(crate::resp::Resp::BulkString(Some(k))) = items.get(1) {
                            if String::from_utf8_lossy(k) == key {
                                received_del = true;
                            }
                        }
                    } else if cmd_str == "SELECT" {
                        received_select = true;
                    }
                }
            }
            _ => {}
        }
    }

    assert!(received_select, "Should receive SELECT command");
    assert!(received_del, "Should receive DEL command");
}

#[tokio::test]
async fn test_wait_command() {
    let ctx = create_server_context();
    let mut conn_ctx = create_connection_context();

    // 1. No replicas, WAIT 0 100 -> returns 0
    let res = run_cmd(vec!["WAIT", "0", "100"], &mut conn_ctx, &ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("Expected Integer 0, got {:?}", res),
    }

    // 2. No replicas, WAIT 1 100 -> returns 0 after timeout
    let start = std::time::Instant::now();
    let res = run_cmd(vec!["WAIT", "1", "100"], &mut conn_ctx, &ctx).await;
    let elapsed = start.elapsed().as_millis();
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("Expected Integer 0, got {:?}", res),
    }
    assert!(
        elapsed >= 100,
        "Should wait at least 100ms, waited {}ms",
        elapsed
    );

    // 3. Add a replica and simulate ACK
    // Update offset to 10
    ctx.repl.repl_offset
        .store(10, std::sync::atomic::Ordering::Relaxed);

    // Add fake replica
    let (tx, _rx) = tokio::sync::mpsc::channel(100);
    ctx.repl.replicas.insert(1, tx);

    // Send ACK for offset 10
    ctx.repl.replica_ack.insert(1, 10);

    // WAIT 1 100 -> returns 1 immediately (because offset matched)
    let res = run_cmd(vec!["WAIT", "1", "100"], &mut conn_ctx, &ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("Expected Integer 1, got {:?}", res),
    }

    // 4. Update offset to 20, replica still at 10
    ctx.repl.repl_offset
        .store(20, std::sync::atomic::Ordering::Relaxed);

    // WAIT 1 500 -> blocks
    // We need to simulate ACK coming in asynchronously
    let ctx_clone = ctx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        // Simulate REPLCONF ACK 20
        let mut replica_conn_ctx = create_connection_context();
        replica_conn_ctx.id = 1; // Match the replica ID

        run_cmd(
            vec!["REPLCONF", "ACK", "20"],
            &mut replica_conn_ctx,
            &ctx_clone,
        )
        .await;
    });

    let start = std::time::Instant::now();
    let res = run_cmd(vec!["WAIT", "1", "500"], &mut conn_ctx, &ctx).await;
    let elapsed = start.elapsed().as_millis();

    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("Expected Integer 1, got {:?}", res),
    }
    // It should wait at least ~100ms
    assert!(
        elapsed >= 90,
        "Should wait at least ~100ms, waited {}ms",
        elapsed
    );
}
