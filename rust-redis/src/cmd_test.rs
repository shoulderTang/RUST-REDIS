use super::*;
use bytes::Bytes;
use crate::db::Db;

#[test]
fn test_ping() {
    let db = Db::default();

    // PING
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PING"))),
    ]));
    let res = process_frame(req, &db);
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("PONG")),
        _ => panic!("expected SimpleString(PONG)"),
    }

    // PING msg
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PING"))),
        Resp::BulkString(Some(Bytes::from("hello"))),
    ]));
    let res = process_frame(req, &db);
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("hello")),
        _ => panic!("expected BulkString(hello)"),
    }
}

#[test]
fn test_set_get() {
    let db = Db::default();

    // SET key val
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
        Resp::BulkString(Some(Bytes::from("bar"))),
    ]));
    let res = process_frame(req, &db);
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }

    // GET key
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ]));
    let res = process_frame(req, &db);
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("bar")),
        _ => panic!("expected BulkString(bar)"),
    }

    // GET non-exist
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("baz"))),
    ]));
    let res = process_frame(req, &db);
    match res {
        Resp::BulkString(None) => {},
        _ => panic!("expected BulkString(None)"),
    }
}

#[test]
fn test_unknown_command() {
    let db = Db::default();
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("NOT_EXIST_CMD"))),
    ]));
    let res = process_frame(req, &db);
    match res {
        Resp::Error(e) => assert_eq!(e, "ERR unknown command"),
        _ => panic!("expected Error"),
    }
}

#[test]
fn test_invalid_args() {
    let db = Db::default();

    // SET missing value
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ]));
    let res = process_frame(req, &db);
    match res {
        Resp::Error(e) => assert!(e.contains("wrong number of arguments")),
        _ => panic!("expected Error"),
    }
}
