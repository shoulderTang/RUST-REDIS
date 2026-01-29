use crate::cmd::{process_frame, ConnectionContext};
use crate::resp::Resp;
use bytes::Bytes;
use tokio::sync::mpsc;

#[tokio::test]
async fn test_monitor() {
    let server_ctx = crate::tests::helper::create_server_context();
    
    // Create a monitor client
    let (tx, mut rx) = mpsc::channel(100);
    let mut monitor_ctx = ConnectionContext::new(1, Some(tx), None);
    
    // Enable MONITOR
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("MONITOR"))),
    ]));
    let (res, _) = process_frame(req, &mut monitor_ctx, &server_ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));
    
    // Verify monitor is registered
    assert!(server_ctx.monitors.contains_key(&1));
    
    // Execute another command from a different client
    let mut client_ctx = ConnectionContext::new(2, None, None);
    // Need to insert client info for address resolution in monitor log
    let client_info = crate::cmd::ClientInfo {
        id: 2,
        addr: "127.0.0.1:12345".to_string(),
        name: "".to_string(),
        db: 0,
        sub: 0,
        psub: 0,
        flags: "N".to_string(),
        cmd: "".to_string(),
        connect_time: std::time::Instant::now(),
        last_activity: std::time::Instant::now(),
        shutdown_tx: None,
    };
    server_ctx.clients.insert(2, client_info);

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
        Resp::BulkString(Some(Bytes::from("bar"))),
    ]));
    
    let (res, _) = process_frame(req, &mut client_ctx, &server_ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));
    
    // Check if monitor received the log
    if let Some(log_resp) = rx.recv().await {
        match log_resp {
            Resp::SimpleString(b) => {
                let log = String::from_utf8_lossy(&b);
                println!("Monitor log: {}", log);
                // Log format: timestamp [db addr] "SET" "foo" "bar"
                assert!(log.contains("[0 127.0.0.1:12345]"));
                assert!(log.contains("\"SET\" \"foo\" \"bar\""));
            }
            _ => panic!("Expected SimpleString log"),
        }
    } else {
        panic!("Monitor did not receive log");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_monitor_lua() {
    let server_ctx = crate::tests::helper::create_server_context();
    
    // Create a monitor client
    let (tx, mut rx) = mpsc::channel(100);
    let mut monitor_ctx = ConnectionContext::new(1, Some(tx), None);
    
    // Enable MONITOR
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("MONITOR"))),
    ]));
    let (res, _) = process_frame(req, &mut monitor_ctx, &server_ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));
    
    // Client setup
    let mut client_ctx = ConnectionContext::new(2, None, None);
    let client_info = crate::cmd::ClientInfo {
        id: 2,
        addr: "127.0.0.1:12345".to_string(),
        name: "".to_string(),
        db: 0,
        sub: 0,
        psub: 0,
        flags: "N".to_string(),
        cmd: "".to_string(),
        connect_time: std::time::Instant::now(),
        last_activity: std::time::Instant::now(),
        shutdown_tx: None,
    };
    server_ctx.clients.insert(2, client_info);

    // Execute EVAL script: return redis.call('set', 'lua_key', 'lua_val')
    // Script: return redis.call('set', KEYS[1], ARGV[1])
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("EVAL"))),
        Resp::BulkString(Some(Bytes::from("return redis.call('set', KEYS[1], ARGV[1])"))),
        Resp::BulkString(Some(Bytes::from("1"))),
        Resp::BulkString(Some(Bytes::from("lua_key"))),
        Resp::BulkString(Some(Bytes::from("lua_val"))),
    ]));
    
    let (res, _) = process_frame(req, &mut client_ctx, &server_ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));
    
    // Check monitor logs
    // We expect TWO logs:
    // 1. The EVAL command itself
    // 2. The SET command inside Lua
    
    let mut logs = Vec::new();
    
    // Expect 2 logs
    for _ in 0..2 {
        if let Some(log_resp) = rx.recv().await {
            if let Resp::SimpleString(b) = log_resp {
                logs.push(String::from_utf8_lossy(&b).to_string());
            }
        }
    }
    
    assert_eq!(logs.len(), 2);
    
    let log_eval = &logs[0];
    println!("Log 1: {}", log_eval);
    assert!(log_eval.contains("[0 127.0.0.1:12345]"));
    assert!(log_eval.contains("\"EVAL\""));
    
    let log_set = &logs[1];
    println!("Log 2: {}", log_set);
    // This is the key assertion for this task
    assert!(log_set.contains("[0 lua]"));
    assert!(log_set.contains("\"set\" \"lua_key\" \"lua_val\""));
}
