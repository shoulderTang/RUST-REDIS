use crate::cmd::process_frame;
use crate::cmd::scripting;
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;

#[test]
fn test_list_ops() {
    let db = Db::default();

    // LPUSH list 1 -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LPUSH"))),
        Resp::BulkString(Some(Bytes::from("list"))),
        Resp::BulkString(Some(Bytes::from("1"))),
    ]));
    let (res, _) = process_frame(
        req,
        &db,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
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
    let (res, _) = process_frame(
        req,
        &db,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
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
    let (res, _) = process_frame(
        req,
        &db,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
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
    let (res, _) = process_frame(
        req,
        &db,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("1")),
        _ => panic!("expected BulkString(1)"),
    }

    // RPOP list -> "2"
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("RPOP"))),
        Resp::BulkString(Some(Bytes::from("list"))),
    ]));
    let (res, _) = process_frame(
        req,
        &db,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("2")),
        _ => panic!("expected BulkString(2)"),
    }

    // LLEN list -> 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LLEN"))),
        Resp::BulkString(Some(Bytes::from("list"))),
    ]));
    let (res, _) = process_frame(
        req,
        &db,
        &None,
        &Config::default(),
        &scripting::create_script_manager(),
    );
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("expected Integer(0)"),
    }
}
