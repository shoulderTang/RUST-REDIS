use crate::cmd::{process_frame, ConnectionContext, ServerContext};
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::{Arc, RwLock};

#[tokio::test]
async fn test_info_server() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // INFO SERVER
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("INFO"))),
        Resp::BulkString(Some(Bytes::from("SERVER"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(info_bytes)) => {
            let info = String::from_utf8_lossy(&info_bytes);
            assert!(info.contains("# Server"));
            assert!(info.contains("redis_version:"));
            assert!(info.contains("os:"));
            assert!(info.contains("process_id:"));
            assert!(info.contains("tcp_port:6380"));
            assert!(info.contains("config_file:"));
        }
        _ => panic!("expected BulkString response"),
    }
}

#[tokio::test]
async fn test_info_keyspace() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Use SET command to insert keys
    let req1 = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("key1"))),
        Resp::BulkString(Some(Bytes::from("value1"))),
    ]));
    crate::cmd::string::set(match req1 { Resp::Array(Some(ref items)) => items, _ => unreachable!() }, &server_ctx.databases[0]);

    let req2 = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("key2"))),
        Resp::BulkString(Some(Bytes::from("value2"))),
        Resp::BulkString(Some(Bytes::from("EX"))),
        Resp::BulkString(Some(Bytes::from("100"))),
    ]));
    crate::cmd::string::set(match req2 { Resp::Array(Some(ref items)) => items, _ => unreachable!() }, &server_ctx.databases[0]);

    // INFO KEYSPACE
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("INFO"))),
        Resp::BulkString(Some(Bytes::from("KEYSPACE"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(info_bytes)) => {
            let info = String::from_utf8_lossy(&info_bytes);
            assert!(info.contains("# Keyspace"));
            assert!(info.contains("db0:keys=2,expires=1"));
            // Check if avg_ttl is present and not 0
            assert!(!info.contains("avg_ttl=0"));
        }
        _ => panic!("expected BulkString response"),
    }
}

#[tokio::test]
async fn test_info_clients() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Simulate 1 connected client (ourselves + maybe others if we tracked properly)
    // Since create_server_context initializes client_count to 0, and we are not going through accept loop,
    // we manually increment it to simulate a connection.
    server_ctx.client_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    // INFO CLIENTS
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("INFO"))),
        Resp::BulkString(Some(Bytes::from("CLIENTS"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(info_bytes)) => {
            let info = String::from_utf8_lossy(&info_bytes);
            assert!(info.contains("# Clients"));
            assert!(info.contains("connected_clients:1"));
            assert!(info.contains("blocked_clients:0"));
            assert!(info.contains("maxclients:10000"));
        }
        _ => panic!("expected BulkString response"),
    }
}

#[tokio::test]
async fn test_info_clients_with_config() {
    let mut server_ctx = crate::tests::helper::create_server_context();
    let mut config = crate::conf::Config::default();
    config.maxclients = 5000;
    server_ctx.config = Arc::new(config);
    
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // INFO CLIENTS
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("INFO"))),
        Resp::BulkString(Some(Bytes::from("CLIENTS"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(info_bytes)) => {
            let info = String::from_utf8_lossy(&info_bytes);
            assert!(info.contains("maxclients:5000"));
        }
        _ => panic!("expected BulkString response"),
    }
}

#[tokio::test]
async fn test_info_blocked_clients() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Spawn a blocked client
    let server_ctx_clone = server_ctx.clone();
    tokio::spawn(async move {
        let mut conn_ctx_blocked = crate::tests::helper::create_connection_context();
        let req = Resp::Array(Some(vec![
            Resp::BulkString(Some(Bytes::from("BLPOP"))),
            Resp::BulkString(Some(Bytes::from("list_key"))),
            Resp::BulkString(Some(Bytes::from("0"))),
        ]));
        process_frame(req, &mut conn_ctx_blocked, &server_ctx_clone).await;
    });

    // Wait for the client to block
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // INFO CLIENTS - should have 1 blocked client
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("INFO"))),
        Resp::BulkString(Some(Bytes::from("CLIENTS"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(info_bytes)) => {
            let info = String::from_utf8_lossy(&info_bytes);
            assert!(info.contains("blocked_clients:1"));
        }
        _ => panic!("expected BulkString response"),
    }

    // Unblock the client
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LPUSH"))),
        Resp::BulkString(Some(Bytes::from("list_key"))),
        Resp::BulkString(Some(Bytes::from("value"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // Wait for the blocked client to unblock and process
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // INFO CLIENTS - should have 0 blocked clients
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("INFO"))),
        Resp::BulkString(Some(Bytes::from("CLIENTS"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(info_bytes)) => {
            let info = String::from_utf8_lossy(&info_bytes);
            assert!(info.contains("blocked_clients:0"));
        }
        _ => panic!("expected BulkString response"),
    }
}
