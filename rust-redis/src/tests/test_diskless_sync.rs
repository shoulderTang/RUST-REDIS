use crate::tests::helper::{create_server_context, create_connection_context};
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::time::Instant;

#[tokio::test]
async fn test_diskless_sync_delay() {
    let server_ctx = create_server_context();
    let mut conn_ctx = create_connection_context();
    // Enable diskless sync and set delay
    server_ctx.repl_diskless_sync.store(true, Ordering::Relaxed);
    server_ctx.repl_diskless_sync_delay.store(2, Ordering::Relaxed);

    // Prepare PSYNC command
    let args = vec![
        Resp::BulkString(Some(Bytes::from("PSYNC"))),
        Resp::BulkString(Some(Bytes::from("?"))),
        Resp::BulkString(Some(Bytes::from("-1"))),
    ];

    let start = Instant::now();
    let res = crate::cmd::replication::psync(&args, &mut conn_ctx, &server_ctx).await;
    let elapsed = start.elapsed();

    // Verify response is FULLRESYNC
    match res {
        Resp::Multiple(arr) => {
             if let Some(Resp::SimpleString(header)) = arr.get(0) {
                 let s = String::from_utf8_lossy(header);
                 assert!(s.starts_with("FULLRESYNC"));
             } else {
                 panic!("Expected FULLRESYNC header, got {:?}", arr.get(0));
             }
        }
        _ => panic!("Expected Multiple response for PSYNC, got {:?}", res),
    }

    // Verify delay (allow some tolerance, but should be close to 2s)
    assert!(elapsed.as_millis() >= 1900, "Should wait at least 2 seconds, waited {}ms", elapsed.as_millis());
}

#[tokio::test]
async fn test_diskless_sync_no_delay_when_disabled() {
    let server_ctx = create_server_context();
    let mut conn_ctx = create_connection_context();
    // Disable diskless sync
    server_ctx.repl_diskless_sync.store(false, Ordering::Relaxed);
    server_ctx.repl_diskless_sync_delay.store(2, Ordering::Relaxed);

    // Prepare PSYNC command
    let args = vec![
        Resp::BulkString(Some(Bytes::from("PSYNC"))),
        Resp::BulkString(Some(Bytes::from("?"))),
        Resp::BulkString(Some(Bytes::from("-1"))),
    ];

    let start = Instant::now();
    let res = crate::cmd::replication::psync(&args, &mut conn_ctx, &server_ctx).await;
    let elapsed = start.elapsed();

    // Verify response is FULLRESYNC
    match res {
        Resp::Multiple(arr) => {
             if let Some(Resp::SimpleString(header)) = arr.get(0) {
                 let s = String::from_utf8_lossy(header);
                 assert!(s.starts_with("FULLRESYNC"));
             } else {
                 panic!("Expected FULLRESYNC header, got {:?}", arr.get(0));
             }
        }
        _ => panic!("Expected Multiple response for PSYNC, got {:?}", res),
    }

    // Verify no delay
    assert!(elapsed.as_millis() < 500, "Should not wait when disabled, waited {}ms", elapsed.as_millis());
}
