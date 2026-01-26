use crate::cmd::process_frame;
use crate::aof::AppendFsync;
use crate::conf::Config;
use crate::db::{Db, Value};
use bytes::Bytes;
use crate::cmd::scripting;
use crate::resp::Resp;

#[test]
fn test_eval() {
    let db = Db::default();
    let config = Config::default();

    // Script with keys and redis.call
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
    ]));
    process_frame(
        req,
        &db,
        &None,
        &config,
        &scripting::create_script_manager(),
    );

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("EVAL"))),
        Resp::BulkString(Some(Bytes::from("return redis.call('GET', KEYS[1])"))),
        Resp::BulkString(Some(Bytes::from("1"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
    ]));
    let (res, _) = process_frame(
        req,
        &db,
        &None,
        &config,
        &scripting::create_script_manager(),
    );
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("v1")),
        _ => panic!("expected BulkString(v1)"),
    }
}

#[test]
fn test_eval_pcall() {
    let db = Db::default();
    let config = Config::default();

    // redis.call with error -> raises Lua error
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("EVAL"))),
        Resp::BulkString(Some(Bytes::from("return redis.call('UNKNOWN_CMD')"))),
        Resp::BulkString(Some(Bytes::from("0"))),
    ]));
    let (res, _) = process_frame(
        req,
        &db,
        &None,
        &config,
        &scripting::create_script_manager(),
    );
    match res {
        Resp::Error(e) => assert!(e.contains("ERR error running script")),
        _ => panic!("expected Error, got {:?}", res),
    }

    // redis.pcall with error -> returns error table, which is converted back to Resp::Error by eval
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("EVAL"))),
        Resp::BulkString(Some(Bytes::from("return redis.pcall('UNKNOWN_CMD')"))),
        Resp::BulkString(Some(Bytes::from("0"))),
    ]));
    let (res, _) = process_frame(
        req,
        &db,
        &None,
        &config,
        &scripting::create_script_manager(),
    );
    match res {
        Resp::Error(e) => assert_eq!(e, "ERR unknown command"),
        _ => panic!("expected Error, got {:?}", res),
    }
}

#[test]
fn test_script_commands() {
    let db = Db::default();
    let config = Config::default();
    let script_manager = scripting::create_script_manager();

    // SCRIPT LOAD "return 'hello'"
    let script = "return 'hello'";
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SCRIPT"))),
        Resp::BulkString(Some(Bytes::from("LOAD"))),
        Resp::BulkString(Some(Bytes::from(script))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &config, &script_manager);
    let sha1 = match res {
        Resp::BulkString(Some(b)) => {
            let s = std::str::from_utf8(&b).unwrap();
            assert_eq!(s.len(), 40); // SHA1 length
            s.to_string()
        }
        _ => panic!("expected BulkString(sha1), got {:?}", res),
    };

    // SCRIPT EXISTS sha1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SCRIPT"))),
        Resp::BulkString(Some(Bytes::from("EXISTS"))),
        Resp::BulkString(Some(Bytes::from(sha1.clone()))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &config, &script_manager);
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 1);
            match items[0] {
                Resp::Integer(i) => assert_eq!(i, 1),
                _ => panic!("expected Integer(1)"),
            }
        }
        _ => panic!("expected Array([1])"),
    }

    // EVALSHA sha1 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("EVALSHA"))),
        Resp::BulkString(Some(Bytes::from(sha1.clone()))),
        Resp::BulkString(Some(Bytes::from("0"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &config, &script_manager);
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("hello")),
        _ => panic!("expected BulkString(hello)"),
    }

    // SCRIPT FLUSH
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SCRIPT"))),
        Resp::BulkString(Some(Bytes::from("FLUSH"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &config, &script_manager);
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected SimpleString(OK)"),
    }

    // SCRIPT EXISTS sha1 (should be 0)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SCRIPT"))),
        Resp::BulkString(Some(Bytes::from("EXISTS"))),
        Resp::BulkString(Some(Bytes::from(sha1.clone()))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &config, &script_manager);
    match res {
        Resp::Array(Some(items)) => match items[0] {
            Resp::Integer(i) => assert_eq!(i, 0),
            _ => panic!("expected Integer(0)"),
        },
        _ => panic!("expected Array([0])"),
    }

    // EVALSHA sha1 0 (should fail)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("EVALSHA"))),
        Resp::BulkString(Some(Bytes::from(sha1))),
        Resp::BulkString(Some(Bytes::from("0"))),
    ]));
    let (res, _) = process_frame(req, &db, &None, &config, &script_manager);
    match res {
        Resp::Error(e) => assert!(e.contains("NOSCRIPT")),
        _ => panic!("expected NOSCRIPT error"),
    }
}

#[test]
fn test_lua_state_reuse() {
    let db = Db::default();
    let config = Config::default();
    let script_manager = scripting::create_script_manager();

    // Set a global variable
    let script1 = "my_global = 10; return my_global";
    let req1 = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("EVAL"))),
        Resp::BulkString(Some(Bytes::from(script1))),
        Resp::BulkString(Some(Bytes::from("0"))),
    ]));
    let (res1, _) = process_frame(req1, &db, &None, &config, &script_manager);
    match res1 {
        Resp::Integer(i) => assert_eq!(i, 10),
        _ => panic!("expected Integer(10), got {:?}", res1),
    }

    // Read the global variable in a separate EVAL call
    let script2 = "return my_global";
    let req2 = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("EVAL"))),
        Resp::BulkString(Some(Bytes::from(script2))),
        Resp::BulkString(Some(Bytes::from("0"))),
    ]));
    let (res2, _) = process_frame(req2, &db, &None, &config, &script_manager);
    match res2 {
        Resp::Integer(i) => assert_eq!(i, 10), // Should still be 10 if reused
        _ => panic!("expected Integer(10), got {:?}", res2),
    }
}
