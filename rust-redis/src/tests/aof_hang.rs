use crate::aof::{Aof, AppendFsync};
use crate::cmd::scripting;
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use crate::cmd::ServerContext;
use crate::cmd::ConnectionContext;
use crate::cmd::process_frame;

fn temp_file() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/tmp/redis_aof_test_hang_{}.aof", now)
}

#[tokio::test]
async fn test_aof_hang_reproduction() {
    let path = temp_file();
    let config = Config {
        appendonly: true,
        appendfilename: path.clone(),
        appendfsync: AppendFsync::EverySec,
        ..Config::default()
    };

    let databases = Arc::new(vec![Db::default()]);
    let script_manager = scripting::create_script_manager();
    
    // Initialize AOF exactly like server.rs
    let aof = Aof::new(&path, config.appendfsync)
            .await
            .expect("failed to open AOF file");
    
    // Create dummy load
    let mut server_ctx : ServerContext = crate::tests::helper::create_server_context();
    Arc::make_mut(&mut server_ctx.config).appendfilename = path.to_string();
    aof.load(&server_ctx)
        .await
        .expect("failed to load AOF");

    let aof_arc = Arc::new(Mutex::new(aof));

    let server_ctx = ServerContext {
        databases: databases.clone(),
        acl: Arc::new(std::sync::RwLock::new(crate::acl::Acl::new())),
        aof: Some(aof_arc.clone()),
        config: Arc::new(config),
        script_manager: script_manager.clone(),
        blocking_waiters: Arc::new(dashmap::DashMap::new()),
        blocking_zset_waiters: Arc::new(dashmap::DashMap::new()),
        pubsub_channels: Arc::new(dashmap::DashMap::new()),
        pubsub_patterns: Arc::new(dashmap::DashMap::new()),
        run_id: "test_run_id".to_string(),
        start_time: std::time::Instant::now(),
        client_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        blocked_client_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        clients: Arc::new(dashmap::DashMap::new()),
        monitors: Arc::new(dashmap::DashMap::new()),
        slowlog: Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::new())),
        slowlog_next_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        slowlog_max_len: Arc::new(std::sync::atomic::AtomicUsize::new(128)),
        slowlog_threshold_us: Arc::new(std::sync::atomic::AtomicI64::new(10_000)),
        mem_peak_rss: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        maxmemory: Arc::new(std::sync::atomic::AtomicU64::new(0)),
    };

    let mut conn_ctx = ConnectionContext::new(1, None, None);

    // Simulate redis-cli command: SET key val
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("key"))),
        Resp::BulkString(Some(Bytes::from("val"))),
    ]));

    // Wrap with timeout to detect hang
    let result = tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
        let (res, cmd_to_log) = process_frame(req, &mut conn_ctx, &server_ctx).await;
        
        if let Some(cmd) = cmd_to_log {
            if let Some(aof) = &server_ctx.aof {
                aof.lock().await.append(&cmd).await.expect("append failed");
            }
        }
        res
    }).await;

    // Cleanup
    let _ = tokio::fs::remove_file(&path).await;

    match result {
        Ok(res) => {
            println!("Command processed successfully: {:?}", res);
        }
        Err(_) => {
            panic!("Test timed out! AOF caused a hang.");
        }
    }
}
