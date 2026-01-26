use crate::cmd::process_frame;
use crate::cmd::scripting;
use crate::conf::Config;
use crate::db::{Db, Value};
use crate::resp::Resp;
use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::Arc;

#[test]
fn test_set_get() {
    let db = Arc::new(vec![Db::default()]);
    let mut db_index = 0;

    // SET key val
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
        Resp::BulkString(Some(Bytes::from("bar"))),
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
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }

    // GET key
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
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("bar")),
        _ => panic!("expected BulkString(bar)"),
    }

    // GET non-exist
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("baz"))),
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
}

#[test]
fn test_mset_mget() {
    let db = Arc::new(vec![Db::default()]);
    let mut db_index = 0;

    // MSET k1 v1 k2 v2
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("MSET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
        Resp::BulkString(Some(Bytes::from("k2"))),
        Resp::BulkString(Some(Bytes::from("v2"))),
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

#[test]
fn test_string_extended() {
    let db = Arc::new(vec![Db::default()]);
    let mut db_index = 0;

    // SET NX key val -> OK
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("key_nx"))),
        Resp::BulkString(Some(Bytes::from("val"))),
        Resp::BulkString(Some(Bytes::from("NX"))),
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
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }

    // INCR key_incr -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("INCR"))),
        Resp::BulkString(Some(Bytes::from("key_incr"))),
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

    // DECR key_decr -> -1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("DECR"))),
        Resp::BulkString(Some(Bytes::from("key_decr"))),
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

    // INCRBY key_ib 10 -> 10
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("INCRBY"))),
        Resp::BulkString(Some(Bytes::from("key_ib"))),
        Resp::BulkString(Some(Bytes::from("10"))),
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
        Resp::Integer(i) => assert_eq!(i, 10),
        _ => panic!("expected Integer(10)"),
    }

    // DECRBY key_db 5 -> -5
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("DECRBY"))),
        Resp::BulkString(Some(Bytes::from("key_db"))),
        Resp::BulkString(Some(Bytes::from("5"))),
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
        Resp::Integer(i) => assert_eq!(i, -5),
        _ => panic!("expected Integer(-5)"),
    }

    // APPEND key_app "foo" -> 3
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("APPEND"))),
        Resp::BulkString(Some(Bytes::from("key_app"))),
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
        Resp::Integer(i) => assert_eq!(i, 3),
        _ => panic!("expected Integer(3)"),
    }

    // APPEND key_app "bar" -> 6
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("APPEND"))),
        Resp::BulkString(Some(Bytes::from("key_app"))),
        Resp::BulkString(Some(Bytes::from("bar"))),
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
        Resp::Integer(i) => assert_eq!(i, 6),
        _ => panic!("expected Integer(6)"),
    }

    // STRLEN key_app -> 6
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("STRLEN"))),
        Resp::BulkString(Some(Bytes::from("key_app"))),
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
        Resp::Integer(i) => assert_eq!(i, 6),
        _ => panic!("expected Integer(6)"),
    }
}

#[test]
fn test_set_options() {
    let db = Arc::new(vec![Db::default()]);
    let mut db_index = 0;

    // 1. SET k1 v1 EX 10
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
        Resp::BulkString(Some(Bytes::from("EX"))),
        Resp::BulkString(Some(Bytes::from("10"))),
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
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }

    // Check TTL
    if let Some(entry) = db[0].get(&Bytes::from("k1")) {
        assert!(entry.expires_at.is_some());
        // Should be roughly now + 10s
        let exp = entry.expires_at.unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        // Allow some buffer
        assert!(exp > now + 8000 && exp < now + 12000);
    } else {
        panic!("k1 not found");
    }

    // 2. SET k2 v2 PX 10000
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k2"))),
        Resp::BulkString(Some(Bytes::from("v2"))),
        Resp::BulkString(Some(Bytes::from("PX"))),
        Resp::BulkString(Some(Bytes::from("10000"))),
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
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }

    if let Some(entry) = db[0].get(&Bytes::from("k2")) {
        assert!(entry.expires_at.is_some());
        let exp = entry.expires_at.unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert!(exp > now + 8000 && exp < now + 12000);
    }

    // 3. SET k1 v1_new KEEPTTL
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("v1_new"))),
        Resp::BulkString(Some(Bytes::from("KEEPTTL"))),
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
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }

    if let Some(entry) = db[0].get(&Bytes::from("k1")) {
        // TTL should be preserved
        assert!(entry.expires_at.is_some());
        assert_eq!(entry.value, Value::String(Bytes::from("v1_new")));
    }

    // 4. SET k3 v3 GET (k3 missing)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k3"))),
        Resp::BulkString(Some(Bytes::from("v3"))),
        Resp::BulkString(Some(Bytes::from("GET"))),
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
    // k3 should exist
    assert!(db[0].contains_key(&Bytes::from("k3")));

    // 5. SET k3 v3_new GET (k3 exists)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k3"))),
        Resp::BulkString(Some(Bytes::from("v3_new"))),
        Resp::BulkString(Some(Bytes::from("GET"))),
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
        Resp::BulkString(Some(s)) => assert_eq!(s, Bytes::from("v3")),
        _ => panic!("expected BulkString(v3)"),
    }
    // k3 should be updated
    if let Some(entry) = db[0].get(&Bytes::from("k3")) {
        assert_eq!(entry.value, Value::String(Bytes::from("v3_new")));
    }

    // 6. EXAT test
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let exat = now_secs + 20;
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k_exat"))),
        Resp::BulkString(Some(Bytes::from("v"))),
        Resp::BulkString(Some(Bytes::from("EXAT"))),
        Resp::BulkString(Some(Bytes::from(exat.to_string()))),
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
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }
    if let Some(entry) = db[0].get(&Bytes::from("k_exat")) {
        let exp = entry.expires_at.unwrap();
        let expected = exat * 1000;
        assert!(exp >= expected && exp <= expected + 1000);
    }

    // 7. PXAT test
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let pxat = now_ms + 20000;
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k_pxat"))),
        Resp::BulkString(Some(Bytes::from("v"))),
        Resp::BulkString(Some(Bytes::from("PXAT"))),
        Resp::BulkString(Some(Bytes::from(pxat.to_string()))),
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
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }
    if let Some(entry) = db[0].get(&Bytes::from("k_pxat")) {
        let exp = entry.expires_at.unwrap();
        // Allow small difference due to execution time
        assert!(exp >= pxat && exp <= pxat + 1000);
    }
}
