use crate::cmd::{process_frame, ConnectionContext, ServerContext};
use crate::cmd::scripting;
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::{Arc, RwLock};

#[tokio::test]
async fn test_zscan_basic() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Prepare data: ZADD myzset 1 m1 2 m2 ... 20 m20
    let mut args = vec![
        Resp::BulkString(Some(Bytes::from("ZADD"))),
        Resp::BulkString(Some(Bytes::from("myzset"))),
    ];
    for i in 1..=20 {
        args.push(Resp::BulkString(Some(Bytes::from(format!("{}", i))))); // score
        args.push(Resp::BulkString(Some(Bytes::from(format!("m{}", i))))); // member
    }
    let req = Resp::Array(Some(args));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(n) => assert_eq!(n, 20),
        _ => panic!("expected Integer(20)"),
    }

    // ZSCAN myzset 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZSCAN"))),
        Resp::BulkString(Some(Bytes::from("myzset"))),
        Resp::BulkString(Some(Bytes::from("0"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            // Check cursor
            match &items[0] {
                Resp::BulkString(Some(_)) => {},
                _ => panic!("expected BulkString cursor"),
            }
            // Check elements
            match &items[1] {
                Resp::Array(Some(elements)) => {
                    assert!(elements.len() > 0);
                    assert_eq!(elements.len() % 2, 0); // Member-score pairs
                },
                _ => panic!("expected Array elements"),
            }
        },
        _ => panic!("expected Array response"),
    }
}

#[tokio::test]
async fn test_zscan_expired() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // ZADD k1 1 m1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZADD"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("1"))),
        Resp::BulkString(Some(Bytes::from("m1"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // PEXPIRE k1 1 (expire essentially immediately)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PEXPIRE"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("1"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // Wait for expiration
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // ZSCAN k1 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZSCAN"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("0"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            // Should return empty list
            match &items[1] {
                Resp::Array(Some(elements)) => {
                    assert_eq!(elements.len(), 0);
                },
                _ => panic!("expected Array elements"),
            }
        },
        _ => panic!("expected Array response"),
    }
}

#[tokio::test]
async fn test_zscan_match() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Prepare data
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZADD"))),
        Resp::BulkString(Some(Bytes::from("myzset"))),
        Resp::BulkString(Some(Bytes::from("1"))),
        Resp::BulkString(Some(Bytes::from("aa"))),
        Resp::BulkString(Some(Bytes::from("2"))),
        Resp::BulkString(Some(Bytes::from("ab"))),
        Resp::BulkString(Some(Bytes::from("3"))),
        Resp::BulkString(Some(Bytes::from("ac"))),
        Resp::BulkString(Some(Bytes::from("4"))),
        Resp::BulkString(Some(Bytes::from("bb"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // ZSCAN myzset 0 MATCH a*
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZSCAN"))),
        Resp::BulkString(Some(Bytes::from("myzset"))),
        Resp::BulkString(Some(Bytes::from("0"))),
        Resp::BulkString(Some(Bytes::from("MATCH"))),
        Resp::BulkString(Some(Bytes::from("a*"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            match &items[1] {
                Resp::Array(Some(elements)) => {
                    // Should find aa, ab, ac (6 elements total with scores)
                    assert_eq!(elements.len(), 6);
                },
                _ => panic!("expected Array elements"),
            }
        },
        _ => panic!("expected Array response"),
    }
}

#[tokio::test]
async fn test_zscan_count() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();
    
    // Prepare data
    let mut args = vec![
        Resp::BulkString(Some(Bytes::from("ZADD"))),
        Resp::BulkString(Some(Bytes::from("large_zset"))),
    ];
    for i in 0..100 {
        args.push(Resp::BulkString(Some(Bytes::from(format!("{}", i)))));
        args.push(Resp::BulkString(Some(Bytes::from(format!("m{}", i)))));
    }
    let req = Resp::Array(Some(args));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // ZSCAN large_zset 0 COUNT 5
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZSCAN"))),
        Resp::BulkString(Some(Bytes::from("large_zset"))),
        Resp::BulkString(Some(Bytes::from("0"))),
        Resp::BulkString(Some(Bytes::from("COUNT"))),
        Resp::BulkString(Some(Bytes::from("5"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            match &items[1] {
                Resp::Array(Some(elements)) => {
                    // It should return exactly 5 items (10 elements with scores)
                    assert_eq!(elements.len(), 10);
                },
                _ => panic!("expected Array elements"),
            }
        },
        _ => panic!("expected Array response"),
    }
}

