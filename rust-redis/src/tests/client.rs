use crate::cmd::{process_frame, ConnectionContext, ServerContext, ClientInfo};
use crate::resp::Resp;
use bytes::Bytes;

#[tokio::test]
async fn test_client_list_basic() {
    let server_ctx = crate::tests::helper::create_server_context();
    // Insert a fake client
    let ci = ClientInfo {
        id: 1,
        addr: "127.0.0.1:6380".to_string(),
        name: "cli".to_string(),
        db: 0,
        sub: 0,
        psub: 0,
        flags: "N".to_string(),
        cmd: "PING".to_string(),
        connect_time: std::time::Instant::now() - std::time::Duration::from_secs(2),
        last_activity: std::time::Instant::now() - std::time::Duration::from_secs(1),
        shutdown_tx: None,
        msg_sender: None,
    };
    server_ctx.clients.insert(ci.id, ci);

    let mut conn_ctx = crate::tests::helper::create_connection_context();
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CLIENT"))),
        Resp::BulkString(Some(Bytes::from("LIST"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("id=1"));
            assert!(s.contains("addr=127.0.0.1:6380"));
            assert!(s.contains("name=cli"));
            assert!(s.contains("flags=N"));
            assert!(s.contains("db=0"));
            assert!(s.contains("sub=0"));
            assert!(s.contains("psub=0"));
            assert!(s.contains("cmd=PING"));
            assert!(s.contains("age="));
            assert!(s.contains("idle="));
        }
        _ => panic!("expected BulkString response"),
    }
}

#[tokio::test]
async fn test_client_list_multiple() {
    let server_ctx = crate::tests::helper::create_server_context();
    // Insert two clients
    let ci1 = ClientInfo {
        id: 2,
        addr: "10.0.0.1:1234".to_string(),
        name: "".to_string(),
        db: 1,
        sub: 1,
        psub: 0,
        flags: "NP".to_string(),
        cmd: "SUBSCRIBE".to_string(),
        connect_time: std::time::Instant::now(),
        last_activity: std::time::Instant::now(),
        shutdown_tx: None,
        msg_sender: None,
    };
    let ci2 = ClientInfo {
        id: 3,
        addr: "10.0.0.2:2345".to_string(),
        name: "worker".to_string(),
        db: 0,
        sub: 0,
        psub: 1,
        flags: "NP".to_string(),
        cmd: "PSUBSCRIBE".to_string(),
        connect_time: std::time::Instant::now(),
        last_activity: std::time::Instant::now(),
        shutdown_tx: None,
        msg_sender: None,
    };
    server_ctx.clients.insert(ci1.id, ci1);
    server_ctx.clients.insert(ci2.id, ci2);

    let mut conn_ctx = crate::tests::helper::create_connection_context();
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CLIENT"))),
        Resp::BulkString(Some(Bytes::from("LIST"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            // Each client should be on its own line
            let lines: Vec<&str> = s.split('\n').filter(|l| !l.is_empty()).collect();
            assert_eq!(lines.len(), 2);
            assert!(lines.iter().any(|l| l.contains("id=2") && l.contains("addr=10.0.0.1:1234") && l.contains("cmd=SUBSCRIBE")));
            assert!(lines.iter().any(|l| l.contains("id=3") && l.contains("addr=10.0.0.2:2345") && l.contains("name=worker") && l.contains("cmd=PSUBSCRIBE")));
        }
        _ => panic!("expected BulkString response"),
    }
}

#[tokio::test]
async fn test_client_kill_id() {
    let server_ctx = crate::tests::helper::create_server_context();
    let (tx, rx) = tokio::sync::watch::channel(false);
    
    let ci = ClientInfo {
        id: 10,
        addr: "1.2.3.4:5678".to_string(),
        name: "victim".to_string(),
        db: 0,
        sub: 0,
        psub: 0,
        flags: "N".to_string(),
        cmd: "PING".to_string(),
        connect_time: std::time::Instant::now(),
        last_activity: std::time::Instant::now(),
        shutdown_tx: Some(tx),
        msg_sender: None,
    };
    server_ctx.clients.insert(ci.id, ci);
    
    let mut conn_ctx = crate::tests::helper::create_connection_context();
    
    // Test KILL ID
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CLIENT"))),
        Resp::BulkString(Some(Bytes::from("KILL"))),
        Resp::BulkString(Some(Bytes::from("ID"))),
        Resp::BulkString(Some(Bytes::from("10"))),
    ]));
    
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(1));
    assert_eq!(*rx.borrow(), true);
}

#[tokio::test]
async fn test_client_kill_addr() {
    let server_ctx = crate::tests::helper::create_server_context();
    let (tx, rx) = tokio::sync::watch::channel(false);
    
    let ci = ClientInfo {
        id: 11,
        addr: "5.6.7.8:9999".to_string(),
        name: "victim_addr".to_string(),
        db: 0,
        sub: 0,
        psub: 0,
        flags: "N".to_string(),
        cmd: "PING".to_string(),
        connect_time: std::time::Instant::now(),
        last_activity: std::time::Instant::now(),
        shutdown_tx: Some(tx),
        msg_sender: None,
    };
    server_ctx.clients.insert(ci.id, ci);
    
    let mut conn_ctx = crate::tests::helper::create_connection_context();
    
    // Test KILL ADDR
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CLIENT"))),
        Resp::BulkString(Some(Bytes::from("KILL"))),
        Resp::BulkString(Some(Bytes::from("ADDR"))),
        Resp::BulkString(Some(Bytes::from("5.6.7.8:9999"))),
    ]));
    
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(1));
    assert_eq!(*rx.borrow(), true);
}

#[tokio::test]
async fn test_client_kill_legacy() {
    let server_ctx = crate::tests::helper::create_server_context();
    let (tx, rx) = tokio::sync::watch::channel(false);
    
    let ci = ClientInfo {
        id: 12,
        addr: "9.9.9.9:1111".to_string(),
        name: "victim_legacy".to_string(),
        db: 0,
        sub: 0,
        psub: 0,
        flags: "N".to_string(),
        cmd: "PING".to_string(),
        connect_time: std::time::Instant::now(),
        last_activity: std::time::Instant::now(),
        shutdown_tx: Some(tx),
        msg_sender: None,
    };
    server_ctx.clients.insert(ci.id, ci);
    
    let mut conn_ctx = crate::tests::helper::create_connection_context();
    
    // Test KILL ip:port
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CLIENT"))),
        Resp::BulkString(Some(Bytes::from("KILL"))),
        Resp::BulkString(Some(Bytes::from("9.9.9.9:1111"))),
    ]));
    
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::SimpleString(b) => assert_eq!(b, Bytes::from("OK")),
        _ => panic!("Expected SimpleString OK"),
    }
    assert_eq!(*rx.borrow(), true);
}

#[tokio::test]
async fn test_client_setname() {
    let server_ctx = crate::tests::helper::create_server_context();
    let ci = ClientInfo {
        id: 13,
        addr: "127.0.0.1:6379".to_string(),
        name: "old_name".to_string(),
        db: 0,
        sub: 0,
        psub: 0,
        flags: "N".to_string(),
        cmd: "PING".to_string(),
        connect_time: std::time::Instant::now(),
        last_activity: std::time::Instant::now(),
        shutdown_tx: None,
        msg_sender: None,
    };
    server_ctx.clients.insert(ci.id, ci);

    let mut conn_ctx = crate::tests::helper::create_connection_context();
    conn_ctx.id = 13; 

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CLIENT"))),
        Resp::BulkString(Some(Bytes::from("SETNAME"))),
        Resp::BulkString(Some(Bytes::from("new_name"))),
    ]));

    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::SimpleString(b) => assert_eq!(b, Bytes::from("OK")),
        _ => panic!("Expected SimpleString OK"),
    }

    let updated_ci = server_ctx.clients.get(&13).unwrap();
    assert_eq!(updated_ci.name, "new_name");
}

#[tokio::test]
async fn test_client_setname_invalid() {
    let server_ctx = crate::tests::helper::create_server_context();
    let ci = ClientInfo {
        id: 14,
        addr: "127.0.0.1:6379".to_string(),
        name: "valid".to_string(),
        db: 0,
        sub: 0,
        psub: 0,
        flags: "N".to_string(),
        cmd: "PING".to_string(),
        connect_time: std::time::Instant::now(),
        last_activity: std::time::Instant::now(),
        shutdown_tx: None,
        msg_sender: None,
    };
    server_ctx.clients.insert(ci.id, ci);

    let mut conn_ctx = crate::tests::helper::create_connection_context();
    conn_ctx.id = 14; 

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CLIENT"))),
        Resp::BulkString(Some(Bytes::from("SETNAME"))),
        Resp::BulkString(Some(Bytes::from("invalid name"))),
    ]));

    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(err) => assert!(err.contains("Client names cannot contain spaces")),
        _ => panic!("Expected Error response"),
    }
}
