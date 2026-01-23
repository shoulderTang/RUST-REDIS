use super::*;
use bytes::Bytes;
use crate::db::{Db, Value};
use crate::conf::Config;
use crate::aof::AppendFsync;

#[test]
fn test_ping() {
    let db = Db::default();

    // PING
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PING"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("PONG")),
        _ => panic!("expected SimpleString(PONG)"),
    }

    // PING msg
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PING"))),
        Resp::BulkString(Some(Bytes::from("hello"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("hello")),
        _ => panic!("expected BulkString(hello)"),
    }
}

#[test]
fn test_keys() {
    let db = Db::default();

    // Setup keys
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("key1"))),
        Resp::BulkString(Some(Bytes::from("val"))),
    ]));
    process_frame(req, &db, &None, &Config::default());

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("key2"))),
        Resp::BulkString(Some(Bytes::from("val"))),
    ]));
    process_frame(req, &db, &None, &Config::default());

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("other"))),
        Resp::BulkString(Some(Bytes::from("val"))),
    ]));
    process_frame(req, &db, &None, &Config::default());

    // KEYS *
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("KEYS"))),
        Resp::BulkString(Some(Bytes::from("*"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
        }
        _ => panic!("expected Array"),
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }

    // GET key
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("bar")),
        _ => panic!("expected BulkString(bar)"),
    }

    // GET non-exist
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("baz"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::Error(e) => assert!(e.contains("wrong number of arguments")),
        _ => panic!("expected Error"),
    }
}

#[test]
fn test_expire_ttl() {
    let db = Db::default();

    // SET key val
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
        Resp::BulkString(Some(Bytes::from("bar"))),
    ]));
    process_frame(req, &db, &None, &Config::default());

    // TTL key -> -1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("TTL"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // TTL key -> ~1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("TTL"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::BulkString(None) => {},
        _ => panic!("expected BulkString(None)"),
    }

    // TTL key -> -2
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("TTL"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::Integer(i) => assert_eq!(i, -2),
        _ => panic!("expected Integer(-2)"),
    }
}

#[test]
fn test_dbsize() {
    let db = Db::default();

    // DBSIZE -> 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("DBSIZE"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    process_frame(req, &db, &None, &Config::default());

    // DBSIZE -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("DBSIZE"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }
}

#[test]
fn test_mset_mget() {
    let db = Db::default();

    // MSET k1 v1 k2 v2
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("MSET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
        Resp::BulkString(Some(Bytes::from("k2"))),
        Resp::BulkString(Some(Bytes::from("v2"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
                Resp::BulkString(None) => {},
                _ => panic!("expected BulkString(None)"),
            }
        }
        _ => panic!("expected Array"),
    }
}

#[test]
fn test_string_extended() {
    let db = Db::default();

    // SET NX key val -> OK
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("key_nx"))),
        Resp::BulkString(Some(Bytes::from("val"))),
        Resp::BulkString(Some(Bytes::from("NX"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }

    // INCR key_incr -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("INCR"))),
        Resp::BulkString(Some(Bytes::from("key_incr"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // DECR key_decr -> -1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("DECR"))),
        Resp::BulkString(Some(Bytes::from("key_decr"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::Integer(i) => assert_eq!(i, 6),
        _ => panic!("expected Integer(6)"),
    }

    // STRLEN key_app -> 6
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("STRLEN"))),
        Resp::BulkString(Some(Bytes::from("key_app"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::Integer(i) => assert_eq!(i, 6),
        _ => panic!("expected Integer(6)"),
    }
}

#[test]
fn test_set_options() {
    use std::time::{SystemTime, Duration, UNIX_EPOCH};
    let db = Db::default();

    // 1. SET k1 v1 EX 10
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
        Resp::BulkString(Some(Bytes::from("EX"))),
        Resp::BulkString(Some(Bytes::from("10"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }
    
    // Check TTL
    if let Some(entry) = db.get(&Bytes::from("k1")) {
        assert!(entry.expires_at.is_some());
        // Should be roughly now + 10s
        let exp = entry.expires_at.unwrap();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }
    
    if let Some(entry) = db.get(&Bytes::from("k2")) {
        assert!(entry.expires_at.is_some());
        let exp = entry.expires_at.unwrap();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        assert!(exp > now + 8000 && exp < now + 12000);
    }

    // 3. SET k1 v1_new KEEPTTL
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("v1_new"))),
        Resp::BulkString(Some(Bytes::from("KEEPTTL"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }
    
    if let Some(entry) = db.get(&Bytes::from("k1")) {
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::BulkString(None) => {},
        _ => panic!("expected BulkString(None)"),
    }
    // k3 should exist
    assert!(db.contains_key(&Bytes::from("k3")));

    // 5. SET k3 v3_new GET (k3 exists)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k3"))),
        Resp::BulkString(Some(Bytes::from("v3_new"))),
        Resp::BulkString(Some(Bytes::from("GET"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::BulkString(Some(s)) => assert_eq!(s, Bytes::from("v3")),
        _ => panic!("expected BulkString(v3)"),
    }
    // k3 should be updated
    if let Some(entry) = db.get(&Bytes::from("k3")) {
        assert_eq!(entry.value, Value::String(Bytes::from("v3_new")));
    }

    // 6. EXAT test
    let now_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let exat = now_secs + 20;
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k_exat"))),
        Resp::BulkString(Some(Bytes::from("v"))),
        Resp::BulkString(Some(Bytes::from("EXAT"))),
        Resp::BulkString(Some(Bytes::from(exat.to_string()))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }
     if let Some(entry) = db.get(&Bytes::from("k_exat")) {
        let exp = entry.expires_at.unwrap();
        let expected = exat * 1000;
        assert!(exp >= expected && exp <= expected + 1000); 
    }

    // 7. PXAT test
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let pxat = now_ms + 20000;
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k_pxat"))),
        Resp::BulkString(Some(Bytes::from("v"))),
        Resp::BulkString(Some(Bytes::from("PXAT"))),
        Resp::BulkString(Some(Bytes::from(pxat.to_string()))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }
     if let Some(entry) = db.get(&Bytes::from("k_pxat")) {
        let exp = entry.expires_at.unwrap();
        // Allow small difference due to execution time
        assert!(exp >= pxat && exp <= pxat + 1000); 
    }
}

#[test]
fn test_command() {
    let db = Db::default();

    // COMMAND
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("COMMAND"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    
    // Just verify it returns an array and contains some known commands
    match res {
        Resp::Array(Some(items)) => {
            assert!(!items.is_empty());
            
            // Check if "set" command is present
            let mut found_set = false;
            for item in items {
                if let Resp::Array(Some(details)) = item {
                    if let Some(Resp::BulkString(Some(name))) = details.get(0) {
                        if *name == Bytes::from("set") {
                            found_set = true;
                            // Check arity
                            match details.get(1) {
                                Some(Resp::Integer(arity)) => assert_eq!(*arity, -3),
                                _ => panic!("expected Integer arity for set"),
                            }
                            break;
                        }
                    }
                }
            }
            assert!(found_set, "COMMAND output should contain 'set'");
        }
        _ => panic!("expected Array"),
    }
}

#[test]
fn test_del() {
    let db = Db::default();
    
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
    process_frame(req, &db, &None, &Config::default());
    
    // DEL k1 k2 k_missing
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("DEL"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("k2"))),
        Resp::BulkString(Some(Bytes::from("k_missing"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    
    match res {
        Resp::Integer(i) => assert_eq!(i, 2),
        _ => panic!("expected Integer(2)"),
    }
    
    // Verify deletion
    assert!(!db.contains_key(&Bytes::from("k1")));
    assert!(!db.contains_key(&Bytes::from("k2")));
    assert!(db.contains_key(&Bytes::from("k3")));
}

#[test]
fn test_config() {
    let db = Db::default();
    let mut cfg = Config::default();
    cfg.appendonly = true;
    cfg.port = 12345;
    cfg.appendfsync = AppendFsync::Always;
    cfg.appendfilename = "test.aof".to_string();

    // CONFIG GET appendonly
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("appendonly"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &cfg);
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("yes")),
                _ => panic!("expected BulkString(yes)"),
            }
        }
        _ => panic!("expected Array"),
    }

    // CONFIG GET port
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("port"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &cfg);
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("12345")),
                _ => panic!("expected BulkString(12345)"),
            }
        }
        _ => panic!("expected Array"),
    }

    // CONFIG GET appendfsync
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("appendfsync"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &cfg);
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("always")),
                _ => panic!("expected BulkString(always)"),
            }
        }
        _ => panic!("expected Array"),
    }

    // CONFIG GET *
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("*"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &cfg);
    match res {
        Resp::Array(Some(items)) => {
            let mut map = std::collections::HashMap::new();
            for i in (0..items.len()).step_by(2) {
                if let Resp::BulkString(Some(key)) = &items[i] {
                    if let Resp::BulkString(Some(val)) = &items[i+1] {
                        map.insert(key.clone(), val.clone());
                    }
                }
            }
            
            assert_eq!(map.get(&Bytes::from("appendonly")).unwrap(), &Bytes::from("yes"));
            assert_eq!(map.get(&Bytes::from("port")).unwrap(), &Bytes::from("12345"));
            assert_eq!(map.get(&Bytes::from("appendfsync")).unwrap(), &Bytes::from("always"));
            assert_eq!(map.get(&Bytes::from("appendfilename")).unwrap(), &Bytes::from("test.aof"));
        }
        _ => panic!("expected Array"),
    }
}

#[test]
fn test_zset_ops() {
    let db = Db::default();

    // ZADD zset 1 m1 2 m2 -> 2
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZADD"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
        Resp::BulkString(Some(Bytes::from("1"))),
        Resp::BulkString(Some(Bytes::from("m1"))),
        Resp::BulkString(Some(Bytes::from("2"))),
        Resp::BulkString(Some(Bytes::from("m2"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // ZCARD zset -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZCARD"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }
}

#[test]
fn test_list_ops() {
    let db = Db::default();

    // LPUSH list 1 -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LPUSH"))),
        Resp::BulkString(Some(Bytes::from("list"))),
        Resp::BulkString(Some(Bytes::from("1"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // RPUSH list 2 -> 2
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("RPUSH"))),
        Resp::BulkString(Some(Bytes::from("list"))),
        Resp::BulkString(Some(Bytes::from("2"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::Integer(i) => assert_eq!(i, 2),
        _ => panic!("expected Integer(2)"),
    }

    // LRANGE list 0 -1 -> ["1", "2"]
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LRANGE"))),
        Resp::BulkString(Some(Bytes::from("list"))),
        Resp::BulkString(Some(Bytes::from("0"))),
        Resp::BulkString(Some(Bytes::from("-1"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("1")),
                _ => panic!("expected BulkString(1)"),
            }
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("2")),
                _ => panic!("expected BulkString(2)"),
            }
        }
        _ => panic!("expected Array"),
    }

    // LPOP list -> "1"
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LPOP"))),
        Resp::BulkString(Some(Bytes::from("list"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("1")),
        _ => panic!("expected BulkString(1)"),
    }

    // RPOP list -> "2"
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("RPOP"))),
        Resp::BulkString(Some(Bytes::from("list"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("2")),
        _ => panic!("expected BulkString(2)"),
    }

    // LLEN list -> 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LLEN"))),
        Resp::BulkString(Some(Bytes::from("list"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("expected Integer(0)"),
    }
}

#[test]
fn test_hash_ops() {
    let db = Db::default();

    // HSET hash f1 v1 -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("HSET"))),
        Resp::BulkString(Some(Bytes::from("hash"))),
        Resp::BulkString(Some(Bytes::from("f1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }
}

#[test]
fn test_set_ops() {
    let db = Db::default();

    // SADD set m1 -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SADD"))),
        Resp::BulkString(Some(Bytes::from("set"))),
        Resp::BulkString(Some(Bytes::from("m1"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // SMEMBERS set -> ["m1"]
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SMEMBERS"))),
        Resp::BulkString(Some(Bytes::from("set"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
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
    let (res, _) = process_frame(req, &db, &None, &Config::default());
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }
}
