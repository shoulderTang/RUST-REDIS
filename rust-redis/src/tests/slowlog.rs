use crate::cmd::{process_frame, ConnectionContext};
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn test_slowlog_basic() {
    let server_ctx = crate::tests::helper::create_server_context();
    // Log everything by setting threshold to 0
    server_ctx.slowlog_threshold_us.store(0, Ordering::Relaxed);
    server_ctx.slowlog_max_len.store(10, Ordering::Relaxed);

    // Insert client info for addressing
    let client_info = crate::cmd::ClientInfo {
        id: 1,
        addr: "127.0.0.1:9999".to_string(),
        name: "cli".to_string(),
        db: 0,
        sub: 0,
        psub: 0,
        flags: "N".to_string(),
        cmd: "".to_string(),
        connect_time: std::time::Instant::now(),
        last_activity: std::time::Instant::now(),
        shutdown_tx: None,
    };
    server_ctx.clients.insert(1, client_info);

    let mut conn = ConnectionContext::new(1, None, None);

    // Execute a simple command
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k"))),
        Resp::BulkString(Some(Bytes::from("v"))),
    ]));
    let (res, _) = process_frame(req, &mut conn, &server_ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    // Check LEN
    let len_req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SLOWLOG"))),
        Resp::BulkString(Some(Bytes::from("LEN"))),
    ]));
    let (len_res, _) = process_frame(len_req.clone(), &mut conn, &server_ctx).await;
    match len_res {
        Resp::Integer(i) => assert!(i >= 1),
        _ => panic!("Expected integer for SLOWLOG LEN"),
    }

    // GET entries
    let get_req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SLOWLOG"))),
        Resp::BulkString(Some(Bytes::from("GET"))),
    ]));
    let (get_res, _) = process_frame(get_req, &mut conn, &server_ctx).await;
    match get_res {
        Resp::Array(Some(arr)) => {
            assert!(!arr.is_empty());
            // Entry format: [id, timestamp, micros, [args], addr, name]
            if let Resp::Array(Some(entry)) = &arr[0] {
                assert!(entry.len() >= 6);
                if let Resp::Array(Some(args)) = &entry[3] {
                    // First arg should be "SET"
                    match &args[0] {
                        Resp::BulkString(Some(b)) => assert_eq!(b, &Bytes::from("SET")),
                        Resp::SimpleString(b) => assert_eq!(b, &Bytes::from("SET")),
                        _ => panic!("Expected SET in args"),
                    }
                } else {
                    panic!("Expected args array in entry");
                }
            } else {
                panic!("Expected entry array");
            }
        }
        _ => panic!("Expected array for SLOWLOG GET"),
    }

    // RESET
    let reset_req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SLOWLOG"))),
        Resp::BulkString(Some(Bytes::from("RESET"))),
    ]));
    let (reset_res, _) = process_frame(reset_req, &mut conn, &server_ctx).await;
    assert_eq!(reset_res, Resp::SimpleString(Bytes::from("OK")));

    // LEN should be 0
    let (len_res2, _) = process_frame(len_req.clone(), &mut conn, &server_ctx).await;
    match len_res2 {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("Expected integer for SLOWLOG LEN after RESET"),
    }
}

#[tokio::test]
async fn test_slowlog_config() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn = ConnectionContext::new(1, None, None);

    // Initial check (defaults from create_server_context which uses Config::default())
    // Config::default() -> slowlog_log_slower_than = 10000, slowlog_max_len = 128
    
    // Test CONFIG GET slowlog-log-slower-than
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("slowlog-log-slower-than"))),
    ]));
    let (res, _) = process_frame(req, &mut conn, &server_ctx).await;
    if let Resp::Array(Some(arr)) = res {
        assert_eq!(arr.len(), 2);
        match &arr[1] {
            Resp::BulkString(Some(b)) => assert_eq!(b, &Bytes::from("10000")),
            _ => panic!("Unexpected value type"),
        }
    } else {
        panic!("Expected Array");
    }

    // Test CONFIG SET slowlog-log-slower-than 5000
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("slowlog-log-slower-than"))),
        Resp::BulkString(Some(Bytes::from("5000"))),
    ]));
    let (res, _) = process_frame(req, &mut conn, &server_ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    // Verify change
    assert_eq!(server_ctx.slowlog_threshold_us.load(Ordering::Relaxed), 5000);

    // Test CONFIG GET slowlog-max-len
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("slowlog-max-len"))),
    ]));
    let (res, _) = process_frame(req, &mut conn, &server_ctx).await;
    if let Resp::Array(Some(arr)) = res {
        assert_eq!(arr.len(), 2);
        match &arr[1] {
            Resp::BulkString(Some(b)) => assert_eq!(b, &Bytes::from("128")),
            _ => panic!("Unexpected value type"),
        }
    } else {
        panic!("Expected Array");
    }

    // Test CONFIG SET slowlog-max-len 200
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("slowlog-max-len"))),
        Resp::BulkString(Some(Bytes::from("200"))),
    ]));
    let (res, _) = process_frame(req, &mut conn, &server_ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    // Verify change
    assert_eq!(server_ctx.slowlog_max_len.load(Ordering::Relaxed), 200);
}
