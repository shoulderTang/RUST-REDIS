use crate::cmd::process_frame;
use crate::cmd::scripting;
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::Arc;

#[test]
fn test_set_ops() {
    let db = Arc::new(vec![Db::default()]);
    let mut db_index = 0;

    // SADD set m1 -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SADD"))),
        Resp::BulkString(Some(Bytes::from("set"))),
        Resp::BulkString(Some(Bytes::from("m1"))),
    ]));
    let (res, _) = process_frame(
        req,
        &db,
        &mut db_index,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // SADD set m1 -> 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SADD"))),
        Resp::BulkString(Some(Bytes::from("set"))),
        Resp::BulkString(Some(Bytes::from("m1"))),
    ]));
    let (res, _) = process_frame(
        req,
        &db,
        &mut db_index,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("expected Integer(0)"),
    }

    // SISMEMBER set m1 -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SISMEMBER"))),
        Resp::BulkString(Some(Bytes::from("set"))),
        Resp::BulkString(Some(Bytes::from("m1"))),
    ]));
    let (res, _) = process_frame(
        req,
        &db,
        &mut db_index,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // SMEMBERS set -> ["m1"]
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SMEMBERS"))),
        Resp::BulkString(Some(Bytes::from("set"))),
    ]));
    let (res, _) = process_frame(
        req,
        &db,
        &mut db_index,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("m1")),
                _ => panic!("expected BulkString(m1)"),
            }
        }
        _ => panic!("expected Array"),
    }

    // SCARD set -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SCARD"))),
        Resp::BulkString(Some(Bytes::from("set"))),
    ]));
    let (res, _) = process_frame(
        req,
        &db,
        &mut db_index,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // SREM set m1 -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SREM"))),
        Resp::BulkString(Some(Bytes::from("set"))),
        Resp::BulkString(Some(Bytes::from("m1"))),
    ]));
    let (res, _) = process_frame(
        req,
        &db,
        &mut db_index,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }
}
