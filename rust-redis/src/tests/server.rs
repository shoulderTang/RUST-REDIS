use crate::aof::AppendFsync;
use crate::cmd::process_frame;
use crate::cmd::scripting;
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;

#[test]
fn test_ping() {
    let db = Db::default();

    // PING
    let req = Resp::Array(Some(vec![Resp::BulkString(Some(Bytes::from("PING")))]));
    let (res, _) = process_frame(
        req,
        &db,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("PONG")),
        _ => panic!("expected SimpleString(PONG)"),
    }

    // PING msg
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PING"))),
        Resp::BulkString(Some(Bytes::from("hello"))),
    ]));
    let (res, _) = process_frame(
        req,
        &db,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("hello")),
        _ => panic!("expected BulkString(hello)"),
    }
}

#[test]
fn test_unknown_command() {
    let db = Db::default();
    let req = Resp::Array(Some(vec![Resp::BulkString(Some(Bytes::from(
        "NOT_EXIST_CMD",
    )))]));
    let (res, _) = process_frame(
        req,
        &db,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
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
    let (res, _) = process_frame(
        req,
        &db,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
    match res {
        Resp::Error(e) => assert!(e.contains("wrong number of arguments")),
        _ => panic!("expected Error"),
    }
}

#[test]
fn test_command() {
    let db = Db::default();

    // COMMAND
    let req = Resp::Array(Some(vec![Resp::BulkString(Some(Bytes::from("COMMAND")))]));
    let (res, _) = process_frame(
        req,
        &db,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );

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
    let (res, _) = process_frame(
        req,
        &db,
        &None,
        &cfg,
        &scripting::create_script_manager(),
    );
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
    let (res, _) = process_frame(
        req,
        &db,
        &None,
        &cfg,
        &scripting::create_script_manager(),
    );
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

    // CONFIG GET *
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("*"))),
    ]));
    let (res, _) = process_frame(
        req,
        &db,
        &None,
        &cfg,
        &scripting::create_script_manager(),
    );
    match res {
        Resp::Array(Some(items)) => {
            // Should contain multiple pairs
            assert!(items.len() >= 2);
            assert!(items.len() % 2 == 0);

            // Convert to map for easier checking
            let mut map = std::collections::HashMap::new();
            for chunk in items.chunks(2) {
                if let [Resp::BulkString(Some(k)), Resp::BulkString(Some(v))] = chunk {
                    map.insert(k, v);
                }
            }

            assert_eq!(
                **map.get(&Bytes::from("appendonly")).unwrap(),
                Bytes::from("yes")
            );
            assert_eq!(
                **map.get(&Bytes::from("port")).unwrap(),
                Bytes::from("12345")
            );
            assert_eq!(
                **map.get(&Bytes::from("appendfsync")).unwrap(),
                Bytes::from("always")
            );
            assert_eq!(
                **map.get(&Bytes::from("appendfilename")).unwrap(),
                Bytes::from("test.aof")
            );
        }
        _ => panic!("expected Array"),
    }
}
