use crate::aof::{Aof, AppendFsync, start_aof_task};
use crate::cmd::ConnectionContext;
use crate::cmd::ServerContext;
use crate::cmd::process_frame;
use crate::cmd::scripting;
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

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

    let databases = Arc::new(vec![RwLock::new(Db::default())]);
    let script_manager = scripting::create_script_manager();

    // Initialize AOF exactly like server.rs: load first, then hand off to task.
    let aof = Aof::new(&path, config.appendfsync)
        .await
        .expect("failed to open AOF file");

    let mut server_ctx: ServerContext = crate::tests::helper::create_server_context();
    Arc::make_mut(&mut server_ctx.config).appendfilename = path.to_string();
    aof.load(&server_ctx).await.expect("failed to load AOF");

    let aof_writer = start_aof_task(aof);

    let server_ctx = ServerContext {
        databases: databases.clone(),
        acl: Arc::new(arc_swap::ArcSwap::from_pointee(crate::acl::Acl::new())),
        aof: Some(aof_writer),
        config: Arc::new(config),
        script_manager: script_manager.clone(),
        blocking_waiters: Arc::new(dashmap::DashMap::new()),
        blocking_zset_waiters: Arc::new(dashmap::DashMap::new()),
        pubsub: Arc::new(crate::cmd::PubSubCtx::new()),
        repl: Arc::new(crate::cmd::ReplicationCtx::new(
            "test_run_id".to_string(), 1024, 1, 60, true, 0, 10, false, 5,
        )),
        start_time: std::time::Instant::now(),
        clients_ctx: Arc::new(crate::cmd::ClientCtx::new()),
        slowlog: Arc::new(crate::cmd::SlowLogCtx::new(128, 10_000)),
        mem: Arc::new(crate::cmd::MemoryCtx::new(
            0,
            crate::conf::EvictionPolicy::NoEviction,
            5,
            0,
        )),
        persist: Arc::new(crate::cmd::PersistenceCtx::new(
            true, true, true,
            vec![(3600, 1), (300, 100), (60, 10000)],
            0,
        )),
        cluster_ctx: Arc::new(crate::cmd::ClusterCtx::new(
            Arc::new(RwLock::new(crate::cluster::ClusterState::new(
                crate::cluster::NodeId("test_run_id".to_string()),
                "127.0.0.1".to_string(),
                6380,
            )))
        )),
    };

    let mut conn_ctx = ConnectionContext::new(1, None, None, None);

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
                aof.append(&cmd).await;
            }
        }
        res
    })
    .await;

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
