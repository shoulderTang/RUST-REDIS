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
    let (res, _) = process_frame(req, &db);
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("PONG")),
        _ => panic!("expected SimpleString(PONG)"),
    }

    // PING msg
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PING"))),
        Resp::BulkString(Some(Bytes::from("hello"))),
    ]));
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }

    // GET key
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ]));
    let (res, _) = process_frame(req, &db);
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("bar")),
        _ => panic!("expected BulkString(bar)"),
    }

    // GET non-exist
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("baz"))),
    ]));
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
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
    process_frame(req, &db);

    // TTL key -> -1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("TTL"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ]));
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // TTL key -> ~1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("TTL"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ]));
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
    match res {
        Resp::BulkString(None) => {},
        _ => panic!("expected BulkString(None)"),
    }

    // TTL key -> -2
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("TTL"))),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ]));
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
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
    process_frame(req, &db);

    // DBSIZE -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("DBSIZE"))),
    ]));
    let (res, _) = process_frame(req, &db);
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }
}

#[test]
fn test_command() {
    let db = Db::default();

    // COMMAND
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("COMMAND"))),
    ]));
    let (res, _) = process_frame(req, &db);
    
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
                                Some(Resp::Integer(arity)) => assert_eq!(*arity, 3),
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
fn test_config() {
    let db = Db::default();

    // CONFIG GET save
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("save"))),
    ]));
    let (res, _) = process_frame(req, &db);
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("save")),
                _ => panic!("expected BulkString(save)"),
            }
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("3600 1 300 100 60 10000")),
                _ => panic!("expected BulkString(save params)"),
            }
        }
        _ => panic!("expected Array"),
    }

    // CONFIG GET appendonly
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("appendonly"))),
    ]));
    let (res, _) = process_frame(req, &db);
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            match &items[0] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("appendonly")),
                _ => panic!("expected BulkString(appendonly)"),
            }
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("no")),
                _ => panic!("expected BulkString(no)"),
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
    let (res, _) = process_frame(req, &db);
    match res {
        Resp::Array(Some(items)) => {
            // Should contain at least save, appendonly, databases
            assert!(items.len() >= 6); 
            let mut found_save = false;
            let mut found_appendonly = false;
            
            for i in (0..items.len()).step_by(2) {
                if let Resp::BulkString(Some(key)) = &items[i] {
                    if *key == Bytes::from("save") {
                        found_save = true;
                    } else if *key == Bytes::from("appendonly") {
                        found_appendonly = true;
                    }
                }
            }
            assert!(found_save);
            assert!(found_appendonly);
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
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // ZCARD zset -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ZCARD"))),
        Resp::BulkString(Some(Bytes::from("zset"))),
    ]));
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("1")),
        _ => panic!("expected BulkString(1)"),
    }

    // RPOP list -> "2"
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("RPOP"))),
        Resp::BulkString(Some(Bytes::from("list"))),
    ]));
    let (res, _) = process_frame(req, &db);
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("2")),
        _ => panic!("expected BulkString(2)"),
    }

    // LLEN list -> 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LLEN"))),
        Resp::BulkString(Some(Bytes::from("list"))),
    ]));
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // SMEMBERS set -> ["m1"]
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SMEMBERS"))),
        Resp::BulkString(Some(Bytes::from("set"))),
    ]));
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
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
    let (res, _) = process_frame(req, &db);
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }
}
