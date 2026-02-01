use crate::tests::helper::{create_connection_context, create_server_context};
use crate::resp::Resp;
use crate::cmd::{ConnectionContext, ServerContext, process_frame, ClientInfo};
use crate::acl::User;
use crate::db::Db;
use bytes::Bytes;
use std::time::Instant;
use std::sync::{Arc, RwLock};

async fn run_cmd_bytes(args: Vec<Bytes>, conn_ctx: &mut ConnectionContext, server_ctx: &ServerContext) -> Resp {
    let mut resp_args = Vec::new();
    for arg in args {
        resp_args.push(Resp::BulkString(Some(arg)));
    }
    let frame = Resp::Array(Some(resp_args));
    let (resp, _) = process_frame(frame, conn_ctx, server_ctx).await;
    resp
}

#[tokio::test]
async fn test_reset_basics() {
    let mut server_ctx = create_server_context();
    
    // Ensure enough databases
    let mut dbs = Vec::new();
    for _ in 0..16 {
        dbs.push(RwLock::new(Db::default()));
    }
    server_ctx.databases = Arc::new(dbs);

    let mut conn_ctx = create_connection_context();

    // Register client in server_ctx
    server_ctx.clients.insert(conn_ctx.id, ClientInfo {
        id: conn_ctx.id,
        addr: "127.0.0.1:1234".to_string(),
        name: String::new(),
        db: 0,
        sub: 0,
        psub: 0,
        flags: String::new(),
        cmd: String::new(),
        connect_time: Instant::now(),
        last_activity: Instant::now(),
        shutdown_tx: None,
    });
    
    // Add user "alice" to ACL so check_access passes
    {
        let mut acl = server_ctx.acl.write().unwrap();
        let mut alice = User::new("alice");
        alice.all_commands = true;
        acl.users.insert("alice".to_string(), Arc::new(alice));
    }

    // Set some state
    conn_ctx.db_index = 5;
    conn_ctx.authenticated = true;
    conn_ctx.current_username = "alice".to_string();
    
    // Set client name
    {
        let mut client_info = server_ctx.clients.get_mut(&conn_ctx.id).unwrap();
        client_info.name = "myclient".to_string();
    }

    // Call RESET
    let resp = run_cmd_bytes(vec![Bytes::from("RESET")], &mut conn_ctx, &server_ctx).await;
    assert_eq!(resp, Resp::SimpleString(Bytes::from("RESET")));

    // Verify state reset
    assert_eq!(conn_ctx.db_index, 0);
    assert_eq!(conn_ctx.authenticated, false);
    assert_eq!(conn_ctx.current_username, "default");
    
    // Verify client name reset
    {
        let client_info = server_ctx.clients.get(&conn_ctx.id).unwrap();
        assert_eq!(client_info.name, "");
    }
}

#[tokio::test]
async fn test_reset_pubsub() {
    let server_ctx = create_server_context();
    let mut conn_ctx = create_connection_context();
    
    // Subscribe
    // We need to simulate subscription manually or via command
    // Using command is better to update global state correctly
    run_cmd_bytes(vec![Bytes::from("SUBSCRIBE"), Bytes::from("chan1")], &mut conn_ctx, &server_ctx).await;
    
    assert!(conn_ctx.subscriptions.contains("chan1"));
    assert!(server_ctx.pubsub_channels.contains_key("chan1"));
    
    // RESET
    let resp = run_cmd_bytes(vec![Bytes::from("RESET")], &mut conn_ctx, &server_ctx).await;
    assert_eq!(resp, Resp::SimpleString(Bytes::from("RESET")));
    
    // Verify subscriptions cleared
    assert!(conn_ctx.subscriptions.is_empty());
    // Verify removed from global map
    // The channel entry might still exist but empty, or be removed?
    // My implementation removes the subscriber from the DashMap inside the channel key.
    if let Some(subs) = server_ctx.pubsub_channels.get("chan1") {
        assert!(!subs.contains_key(&conn_ctx.id));
    }
}

#[tokio::test]
async fn test_reset_multi() {
    let server_ctx = create_server_context();
    let mut conn_ctx = create_connection_context();
    
    // Start MULTI
    run_cmd_bytes(vec![Bytes::from("MULTI")], &mut conn_ctx, &server_ctx).await;
    assert!(conn_ctx.in_multi);
    
    // Queue a command
    run_cmd_bytes(vec![Bytes::from("PING")], &mut conn_ctx, &server_ctx).await;
    assert!(!conn_ctx.multi_queue.is_empty());
    
    // RESET
    let resp = run_cmd_bytes(vec![Bytes::from("RESET")], &mut conn_ctx, &server_ctx).await;
    assert_eq!(resp, Resp::SimpleString(Bytes::from("RESET")));
    
    assert!(!conn_ctx.in_multi);
    assert!(conn_ctx.multi_queue.is_empty());
}
