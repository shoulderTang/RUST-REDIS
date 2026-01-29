use crate::cmd::process_frame;
use crate::cmd::scripting;
use crate::conf::Config;
use crate::db::{Db, Value};
use crate::resp::Resp;
use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::Arc;

#[tokio::test]
async fn test_set_get() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // SET key val
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
        Resp::BulkString(Some(Bytes::from("bar"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }

    // GET key
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("bar")),
        _ => panic!("expected BulkString(bar)"),
    }

    // GET non-exist
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("baz"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(None) => {}
        _ => panic!("expected BulkString(None)"),
    }
}

#[tokio::test]
async fn test_mset_mget() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // MSET k1 v1 k2 v2
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("MSET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
        Resp::BulkString(Some(Bytes::from("k2"))),
        Resp::BulkString(Some(Bytes::from("v2"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }

    // MGET k1 k2 k3
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("MGET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("k2"))),
        Resp::BulkString(Some(Bytes::from("k3"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 3);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("v1")),
                _ => panic!("expected BulkString(v1)"),
            }
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("v2")),
                _ => panic!("expected BulkString(v2)"),
            }
            match &items[2] {
                Resp::BulkString(None) => {}
                _ => panic!("expected BulkString(None)"),
            }
        }
        _ => panic!("expected Array"),
    }
}

#[tokio::test]
async fn test_string_extended() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // SET NX key val -> OK
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("key_nx"))),
        Resp::BulkString(Some(Bytes::from("val"))),
        Resp::BulkString(Some(Bytes::from("NX"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }

    // INCR key_incr -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("INCR"))),
        Resp::BulkString(Some(Bytes::from("key_incr"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // DECR key_decr -> -1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("DECR"))),
        Resp::BulkString(Some(Bytes::from("key_decr"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, -1),
        _ => panic!("expected Integer(-1)"),
    }

    // INCRBY key_ib 10 -> 10
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("INCRBY"))),
        Resp::BulkString(Some(Bytes::from("key_ib"))),
        Resp::BulkString(Some(Bytes::from("10"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 10),
        _ => panic!("expected Integer(10)"),
    }

    // DECRBY key_db 5 -> -5
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("DECRBY"))),
        Resp::BulkString(Some(Bytes::from("key_db"))),
        Resp::BulkString(Some(Bytes::from("5"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, -5),
        _ => panic!("expected Integer(-5)"),
    }

    // APPEND key_app "foo" -> 3
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("APPEND"))),
        Resp::BulkString(Some(Bytes::from("key_app"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 3),
        _ => panic!("expected Integer(3)"),
    }

    // APPEND key_app "bar" -> 6
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("APPEND"))),
        Resp::BulkString(Some(Bytes::from("key_app"))),
        Resp::BulkString(Some(Bytes::from("bar"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 6),
        _ => panic!("expected Integer(6)"),
    }

    // STRLEN key_app -> 6
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("STRLEN"))),
        Resp::BulkString(Some(Bytes::from("key_app"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 6),
        _ => panic!("expected Integer(6)"),
    }
}
