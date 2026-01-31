use std::sync::{Arc, RwLock};
use crate::db::Db;
use crate::conf::Config;
use crate::cmd::{scripting, ServerContext, process_frame};
use crate::resp::Resp;
use bytes::Bytes;

#[tokio::test]
async fn test_exists() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // EXISTS non-existent
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("EXISTS"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("expected Integer(0)"),
    }

    // SET k1 v1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // EXISTS k1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("EXISTS"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // SET k2 v2
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k2"))),
        Resp::BulkString(Some(Bytes::from("v2"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // EXISTS k1 k2 k3
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("EXISTS"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("k2"))),
        Resp::BulkString(Some(Bytes::from("k3"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 2),
        _ => panic!("expected Integer(2)"),
    }
}

#[tokio::test]
async fn test_type() {
     let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // TYPE k1 -> none
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("TYPE"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("none")),
        _ => panic!("expected SimpleString(none)"),
    }

    // SET k1 v1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // TYPE k1 -> string
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("TYPE"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("string")),
        _ => panic!("expected SimpleString(string)"),
    }

    // LPUSH l1 v1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LPUSH"))),
        Resp::BulkString(Some(Bytes::from("l1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // TYPE l1 -> list
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("TYPE"))),
        Resp::BulkString(Some(Bytes::from("l1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("list")),
        _ => panic!("expected SimpleString(list)"),
    }
}

#[tokio::test]
async fn test_flushdb() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // SET k1 v1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // FLUSHDB
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("FLUSHDB"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    // EXISTS k1 -> 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("EXISTS"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("expected Integer(0)"),
    }
}

#[tokio::test]
async fn test_flushall() {
    let db1 = RwLock::new(Db::default());
    let db2 = RwLock::new(Db::default());
    let db = Arc::new(vec![db1, db2]);
    let mut server_ctx = crate::tests::helper::create_server_context();
    server_ctx.databases = db.clone();

    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // SELECT 0
    // SET k1 v1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // SELECT 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SELECT"))),
        Resp::BulkString(Some(Bytes::from("1"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // SET k2 v2
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k2"))),
        Resp::BulkString(Some(Bytes::from("v2"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // FLUSHALL
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("FLUSHALL"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    // SELECT 0
    conn_ctx.db_index = 0;
    // EXISTS k1 -> 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("EXISTS"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("expected Integer(0) for k1"),
    }

    // SELECT 1
    conn_ctx.db_index = 1;
    // EXISTS k2 -> 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("EXISTS"))),
        Resp::BulkString(Some(Bytes::from("k2"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("expected Integer(0) for k2"),
    }
}

#[tokio::test]
async fn test_pexpire() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // SET k1 v1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // PEXPIRE k1 1000
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PEXPIRE"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("1000"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // PTTL k1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PTTL"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert!(i > 0 && i <= 1000),
        _ => panic!("expected Integer(>0)"),
    }
}

#[tokio::test]
async fn test_expireat() {
    let mut conn_ctx = crate::tests::helper::create_connection_context();
    let server_ctx = crate::tests::helper::create_server_context();

    // SET k1 v1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    let timestamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() + 10;
    
    // EXPIREAT k1 timestamp
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("EXPIREAT"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from(timestamp.to_string()))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // TTL k1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("TTL"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert!(i > 0 && i <= 10),
        _ => panic!("expected Integer(>0)"),
    }
}

#[tokio::test]
async fn test_pexpireat() {
     let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // SET k1 v1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    let timestamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64 + 10000;
    
    // PEXPIREAT k1 timestamp
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PEXPIREAT"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from(timestamp.to_string()))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // PTTL k1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PTTL"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert!(i > 0 && i <= 10000),
        _ => panic!("expected Integer(>0)"),
    }
}

#[tokio::test]
async fn test_persist() {
     let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // SET k1 v1 EX 10
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
        Resp::BulkString(Some(Bytes::from("EX"))),
        Resp::BulkString(Some(Bytes::from("10"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // TTL k1 -> > 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("TTL"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert!(i > 0),
        _ => panic!("expected Integer(>0)"),
    }

    // PERSIST k1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PERSIST"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // TTL k1 -> -1 (no expiration)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("TTL"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, -1),
        _ => panic!("expected Integer(-1)"),
    }

    // PERSIST k1 again -> 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PERSIST"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("expected Integer(0)"),
    }
}
