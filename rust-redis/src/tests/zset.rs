use crate::cmd::process_frame;
use crate::cmd::scripting;
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::Arc;

#[test]
fn test_zset_ops() {
    let db = Arc::new(vec![Db::default()]);
    let mut db_index = 0;

    // ZADD zset 1 m1 2 m2 -> 2
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZADD"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
        Resp::BulkString(Some(Bytes::from("1"))),
        Resp::BulkString(Some(Bytes::from("m1"))),
        Resp::BulkString(Some(Bytes::from("2"))),
        Resp::BulkString(Some(Bytes::from("m2"))),
    ]));
    let mut authenticated = true;
    let (res, _) = process_frame(
        req,
        &db,
        &mut db_index, &mut authenticated, &mut "default".to_string(), &std::sync::Arc::new(std::sync::RwLock::new(crate::acl::Acl::new())),
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
    match res {
        Resp::Integer(i) => assert_eq!(i, 2),
        _ => panic!("expected Integer(2)"),
    }

    // ZSCORE zset m1 -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZSCORE"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
        Resp::BulkString(Some(Bytes::from("m1"))),
    ]));
    let mut authenticated = true;
    let (res, _) = process_frame(
        req,
        &db,
        &mut db_index, &mut authenticated, &mut "default".to_string(), &std::sync::Arc::new(std::sync::RwLock::new(crate::acl::Acl::new())),
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("1")),
        _ => panic!("expected BulkString(1)"),
    }

    // ZRANK zset m1 -> 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZRANK"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
        Resp::BulkString(Some(Bytes::from("m1"))),
    ]));
    let mut authenticated = true;
    let (res, _) = process_frame(
        req,
        &db,
        &mut db_index, &mut authenticated, &mut "default".to_string(), &std::sync::Arc::new(std::sync::RwLock::new(crate::acl::Acl::new())),
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("expected Integer(0)"),
    }

    // ZRANGE zset 0 -1 -> [m1, m2]
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZRANGE"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
        Resp::BulkString(Some(Bytes::from("0"))),
        Resp::BulkString(Some(Bytes::from("-1"))),
    ]));
    let mut authenticated = true;
    let (res, _) = process_frame(
        req,
        &db,
        &mut db_index, &mut authenticated, &mut "default".to_string(), &std::sync::Arc::new(std::sync::RwLock::new(crate::acl::Acl::new())),
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("m1")),
                _ => panic!("expected BulkString(m1)"),
            }
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("m2")),
                _ => panic!("expected BulkString(m2)"),
            }
        }
        _ => panic!("expected Array"),
    }

    // ZRANGE zset 0 -1 WITHSCORES -> [m1, 1, m2, 2]
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZRANGE"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
        Resp::BulkString(Some(Bytes::from("0"))),
        Resp::BulkString(Some(Bytes::from("-1"))),
        Resp::BulkString(Some(Bytes::from("WITHSCORES"))),
    ]));
    let mut authenticated = true;
    let (res, _) = process_frame(
        req,
        &db,
        &mut db_index, &mut authenticated, &mut "default".to_string(), &std::sync::Arc::new(std::sync::RwLock::new(crate::acl::Acl::new())),
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 4);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("m1")),
                _ => panic!("expected BulkString(m1)"),
            }
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("1")),
                _ => panic!("expected BulkString(1)"),
            }
            match &items[2] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("m2")),
                _ => panic!("expected BulkString(m2)"),
            }
            match &items[3] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("2")),
                _ => panic!("expected BulkString(2)"),
            }
        }
        _ => panic!("expected Array"),
    }

    // ZCARD zset -> 2
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZCARD"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
    ]));
    let mut authenticated = true;
    let (res, _) = process_frame(
        req,
        &db,
        &mut db_index, &mut authenticated, &mut "default".to_string(), &std::sync::Arc::new(std::sync::RwLock::new(crate::acl::Acl::new())),
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
    match res {
        Resp::Integer(i) => assert_eq!(i, 2),
        _ => panic!("expected Integer(2)"),
    }

    // ZREM zset m1 -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZREM"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
        Resp::BulkString(Some(Bytes::from("m1"))),
    ]));
    let mut authenticated = true;
    let (res, _) = process_frame(
        req,
        &db,
        &mut db_index, &mut authenticated, &mut "default".to_string(), &std::sync::Arc::new(std::sync::RwLock::new(crate::acl::Acl::new())),
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // ZCARD zset -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZCARD"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
    ]));
    let mut authenticated = true;
    let (res, _) = process_frame(
        req,
        &db,
        &mut db_index, &mut authenticated, &mut "default".to_string(), &std::sync::Arc::new(std::sync::RwLock::new(crate::acl::Acl::new())),
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }
}
