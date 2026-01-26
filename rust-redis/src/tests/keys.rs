use crate::cmd::process_frame;
use crate::cmd::scripting;
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::Arc;

#[test]
fn test_keys() {
    let db = Arc::new(vec![Db::default()]);
    let mut db_index = 0;

    // Setup keys
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("key1"))),
        Resp::BulkString(Some(Bytes::from("val"))),
    ]));
    process_frame(
        req,
        &db,
        &mut db_index,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("key2"))),
        Resp::BulkString(Some(Bytes::from("val"))),
    ]));
    process_frame(
        req,
        &db,
        &mut db_index,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("other"))),
        Resp::BulkString(Some(Bytes::from("val"))),
    ]));
    process_frame(
        req,
        &db,
        &mut db_index,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );

    // KEYS *
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("KEYS"))),
        Resp::BulkString(Some(Bytes::from("*"))),
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
            assert_eq!(items.len(), 3);
        }
        _ => panic!("expected Array"),
    }

    // KEYS key*
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("KEYS"))),
        Resp::BulkString(Some(Bytes::from("key*"))),
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
            assert_eq!(items.len(), 2);
        }
        _ => panic!("expected Array"),
    }

    // KEYS ?ey?
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("KEYS"))),
        Resp::BulkString(Some(Bytes::from("?ey?"))),
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
            assert_eq!(items.len(), 2);
        }
        _ => panic!("expected Array"),
    }
}

#[test]
fn test_expire_ttl() {
    let db = Arc::new(vec![Db::default()]);
    let mut db_index = 0;

    // SET key val
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
        Resp::BulkString(Some(Bytes::from("bar"))),
    ]));
    process_frame(
        req,
        &db,
        &mut db_index,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );

    // TTL key -> -1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("TTL"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
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
        Resp::Integer(i) => assert_eq!(i, -1),
        _ => panic!("expected Integer(-1)"),
    }

    // EXPIRE key 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("EXPIRE"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
        Resp::BulkString(Some(Bytes::from("1"))),
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

    // TTL key -> ~1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("TTL"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
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
        Resp::Integer(i) => assert!(i >= 0 && i <= 1),
        _ => panic!("expected Integer(>=0)"),
    }

    // Sleep 1.1s
    std::thread::sleep(std::time::Duration::from_millis(1100));

    // GET key -> Nil
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
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
        Resp::BulkString(None) => {}
        _ => panic!("expected BulkString(None)"),
    }

    // TTL key -> -2
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("TTL"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
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
        Resp::Integer(i) => assert_eq!(i, -2),
        _ => panic!("expected Integer(-2)"),
    }
}

#[test]
fn test_dbsize() {
    let db = Arc::new(vec![Db::default()]);
    let mut db_index = 0;

    // DBSIZE -> 0
    let req = Resp::Array(Some(vec![Resp::BulkString(Some(Bytes::from("DBSIZE")))]));
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

    // SET key val
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
        Resp::BulkString(Some(Bytes::from("bar"))),
    ]));
    process_frame(
        req,
        &db,
        &mut db_index,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );

    // DBSIZE -> 1
    let req = Resp::Array(Some(vec![Resp::BulkString(Some(Bytes::from("DBSIZE")))]));
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

#[test]
fn test_del() {
    let db = Arc::new(vec![Db::default()]);
    let mut db_index = 0;

    // Setup keys
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("MSET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
        Resp::BulkString(Some(Bytes::from("k2"))),
        Resp::BulkString(Some(Bytes::from("v2"))),
        Resp::BulkString(Some(Bytes::from("k3"))),
        Resp::BulkString(Some(Bytes::from("v3"))),
    ]));
    process_frame(
        req,
        &db,
        &mut db_index,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );

    // DEL k1 k2 k_missing
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("DEL"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("k2"))),
        Resp::BulkString(Some(Bytes::from("k_missing"))),
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
        Resp::Integer(i) => assert_eq!(i, 2),
        _ => panic!("expected Integer(2)"),
    }

    // Verify deletion
    assert!(!db[0].contains_key(&Bytes::from("k1")));
    assert!(!db[0].contains_key(&Bytes::from("k2")));
    assert!(db[0].contains_key(&Bytes::from("k3")));
}
