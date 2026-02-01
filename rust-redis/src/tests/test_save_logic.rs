use crate::cmd::{process_frame, ConnectionContext, ServerContext};
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[tokio::test]
async fn test_auto_save() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // 1. Set a very aggressive save policy: 1 second and 1 change
    {
        let mut params = server_ctx.save_params.write().unwrap();
        params.clear();
        params.push((1, 1));
    }
    
    // Set last save time to 2 seconds ago
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    server_ctx.last_save_time.store(now - 2, Ordering::Relaxed);

    // 2. Perform a write command to increment dirty counter
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
        Resp::BulkString(Some(Bytes::from("bar"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;
    
    assert_eq!(server_ctx.dirty.load(Ordering::Relaxed), 1);

    // 3. Wait for the background save task to trigger (it runs every 1s)
    // In our test environment, we haven't started the background task yet because we are using create_server_context.
    // The background task is in run_server in main.rs.
    // For testing, we can manually run the check logic once.
    
    let dirty = server_ctx.dirty.load(Ordering::Relaxed);
    let last_save = server_ctx.last_save_time.load(Ordering::Relaxed);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let elapsed = now - last_save;

    let mut trigger_save = false;
    for (secs, changes) in server_ctx.save_params.read().unwrap().iter() {
        if elapsed >= (*secs as i64) && dirty >= *changes {
            trigger_save = true;
            break;
        }
    }

    assert!(trigger_save);
    
    if trigger_save {
        crate::cmd::save::bgsave(&[], &server_ctx);
    }

    // 4. Wait a bit for the background thread to finish the save
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // 5. Verify dirty is reset and last_save_time is updated
    assert_eq!(server_ctx.dirty.load(Ordering::Relaxed), 0);
    assert!(server_ctx.last_save_time.load(Ordering::Relaxed) >= now);
}
