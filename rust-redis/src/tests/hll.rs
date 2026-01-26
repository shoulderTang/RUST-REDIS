use crate::cmd::process_frame;
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use crate::cmd::scripting;
use bytes::Bytes;
use std::sync::Arc;

#[test]
fn test_hll() {
    let db = Arc::new(vec![Db::default()]);
    let mut db_index = 0;
    let config = Config::default();
    let script_manager = scripting::create_script_manager();

    // PFADD hll1 a b c
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PFADD"))),
        Resp::BulkString(Some(Bytes::from("hll1"))),
        Resp::BulkString(Some(Bytes::from("a"))),
        Resp::BulkString(Some(Bytes::from("b"))),
        Resp::BulkString(Some(Bytes::from("c"))),
    ]));
    let (res, _) = process_frame(req, &db, &mut db_index, &None, &config, &script_manager);
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("Expected Integer(1)"),
    }

    // PFCOUNT hll1 -> 3
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PFCOUNT"))),
        Resp::BulkString(Some(Bytes::from("hll1"))),
    ]));
    let (res, _) = process_frame(req, &db, &mut db_index, &None, &config, &script_manager);
    match res {
        Resp::Integer(i) => assert_eq!(i, 3),
        _ => panic!("Expected Integer(3)"),
    }

    // PFADD hll2 c d e
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PFADD"))),
        Resp::BulkString(Some(Bytes::from("hll2"))),
        Resp::BulkString(Some(Bytes::from("c"))),
        Resp::BulkString(Some(Bytes::from("d"))),
        Resp::BulkString(Some(Bytes::from("e"))),
    ]));
    let (res, _) = process_frame(req, &db, &mut db_index, &None, &config, &script_manager);
    assert_eq!(res, Resp::Integer(1));

    // PFCOUNT hll2 -> 3
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PFCOUNT"))),
        Resp::BulkString(Some(Bytes::from("hll2"))),
    ]));
    let (res, _) = process_frame(req, &db, &mut db_index, &None, &config, &script_manager);
    assert_eq!(res, Resp::Integer(3));

    // PFMERGE hll_merge hll1 hll2
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PFMERGE"))),
        Resp::BulkString(Some(Bytes::from("hll_merge"))),
        Resp::BulkString(Some(Bytes::from("hll1"))),
        Resp::BulkString(Some(Bytes::from("hll2"))),
    ]));
    let (res, _) = process_frame(req, &db, &mut db_index, &None, &config, &script_manager);
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("Expected OK"),
    }

    // PFCOUNT hll_merge -> 5 (a, b, c, d, e)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PFCOUNT"))),
        Resp::BulkString(Some(Bytes::from("hll_merge"))),
    ]));
    let (res, _) = process_frame(req, &db, &mut db_index, &None, &config, &script_manager);
    match res {
        Resp::Integer(i) => assert_eq!(i, 5),
        _ => panic!("Expected Integer(5), got {:?}", res),
    }
}

#[test]
fn test_hll_string_promotion() {
    use crate::db::{Entry, Value};
    use crate::hll::HLL_REGISTERS;
    
    let db = Arc::new(vec![Db::default()]);
    let mut db_index = 0;
    let config = Config::default();
    let script_manager = scripting::create_script_manager();

    // Manually insert a String that looks like an HLL (16k zero bytes)
    let key = Bytes::from("hll_str");
    let raw_hll = vec![0u8; HLL_REGISTERS];
    db[0].insert(key.clone(), Entry::new(Value::String(Bytes::from(raw_hll)), None));

    // PFCOUNT should work and return 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PFCOUNT"))),
        Resp::BulkString(Some(key.clone())),
    ]));
    let (res, _) = process_frame(req, &db, &mut db_index, &None, &config, &script_manager);
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("Expected Integer(0) from string promotion"),
    }

    // PFADD should work and promote it
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("PFADD"))),
        Resp::BulkString(Some(key.clone())),
        Resp::BulkString(Some(Bytes::from("foo"))),
    ]));
    let (res, _) = process_frame(req, &db, &mut db_index, &None, &config, &script_manager);
    assert_eq!(res, Resp::Integer(1));

    // Verify it is now Value::HyperLogLog in DB
    if let Some(entry) = db[0].get(&key) {
        match &entry.value {
            Value::HyperLogLog(_) => {}, // Good
            _ => panic!("Value should have been promoted to HyperLogLog"),
        }
    } else {
        panic!("Key missing");
    }
}
