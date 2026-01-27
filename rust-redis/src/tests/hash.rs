use crate::cmd::{process_frame, ConnectionContext, ServerContext};
use crate::cmd::scripting;
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::{Arc, RwLock};

#[tokio::test]
async fn test_hash_ops() {
    let db = Arc::new(vec![Db::default()]);
    let config = Config::default();
    let script_manager = scripting::create_script_manager();
    let acl = Arc::new(RwLock::new(crate::acl::Acl::new()));

    let server_ctx = ServerContext {
        databases: db.clone(),
        acl: acl,
        aof: None,
        config: Arc::new(config),
        script_manager: script_manager,
        blocking_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
    };

    let mut conn_ctx = ConnectionContext {
        db_index: 0,
        authenticated: true,
        current_username: "default".to_string(),
    };

    // HSET hash f1 v1 -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("HSET"))),
        Resp::BulkString(Some(Bytes::from("hash"))),
        Resp::BulkString(Some(Bytes::from("f1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // HGET hash f1 -> "v1"
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("HGET"))),
        Resp::BulkString(Some(Bytes::from("hash"))),
        Resp::BulkString(Some(Bytes::from("f1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("v1")),
        _ => panic!("expected BulkString(v1)"),
    }

    // HMSET hash f2 v2 f3 v3 -> OK
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("HMSET"))),
        Resp::BulkString(Some(Bytes::from("hash"))),
        Resp::BulkString(Some(Bytes::from("f2"))),
        Resp::BulkString(Some(Bytes::from("v2"))),
        Resp::BulkString(Some(Bytes::from("f3"))),
        Resp::BulkString(Some(Bytes::from("v3"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }

    // HMGET hash f1 f2 -> ["v1", "v2"]
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("HMGET"))),
        Resp::BulkString(Some(Bytes::from("hash"))),
        Resp::BulkString(Some(Bytes::from("f1"))),
        Resp::BulkString(Some(Bytes::from("f2"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("v1")),
                _ => panic!("expected BulkString(v1)"),
            }
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("v2")),
                _ => panic!("expected BulkString(v2)"),
            }
        }
        _ => panic!("expected Array"),
    }

    // HLEN hash -> 3
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("HLEN"))),
        Resp::BulkString(Some(Bytes::from("hash"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 3),
        _ => panic!("expected Integer(3)"),
    }

    // HDEL hash f1 -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("HDEL"))),
        Resp::BulkString(Some(Bytes::from("hash"))),
        Resp::BulkString(Some(Bytes::from("f1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }
}
