use crate::cmd::{process_frame, ConnectionContext, ServerContext};
use crate::cmd::scripting;
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::{Arc, RwLock};

#[tokio::test]
async fn test_hash_ops() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // HSET hash f1 v1 -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("HSET"))),
        Resp::BulkString(Some(Bytes::from("hash"))),
        Resp::BulkString(Some(Bytes::from("f1"))),
        Resp::BulkString(Some(Bytes::from("v1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
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
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
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
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
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
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
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
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
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
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }
}

#[tokio::test]
async fn test_hkeys() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    async fn run_cmd(args: Vec<&str>, conn_ctx: &mut crate::cmd::ConnectionContext, server_ctx: &crate::cmd::ServerContext) -> Resp {
        let mut resp_args = Vec::new();
        for arg in args {
            resp_args.push(Resp::BulkString(Some(Bytes::from(arg.to_string()))));
        }
        let req = Resp::Array(Some(resp_args));
        let (res, _) = process_frame(req, conn_ctx, server_ctx).await;
        res
    }

    // 1. HKEYS non-existent key
    let res = run_cmd(vec!["HKEYS", "myhash"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => assert_eq!(arr.len(), 0),
        _ => panic!("Expected empty array, got {:?}", res),
    }

    // 2. Setup hash
    run_cmd(vec!["HSET", "myhash", "field1", "value1"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["HSET", "myhash", "field2", "value2"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["HSET", "myhash", "field3", "value3"], &mut conn_ctx, &server_ctx).await;

    // 3. HKEYS existing hash
    let res = run_cmd(vec!["HKEYS", "myhash"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
            let mut keys: Vec<String> = arr.iter().map(|r| {
                match r {
                    Resp::BulkString(Some(b)) => String::from_utf8(b.to_vec()).unwrap(),
                    _ => panic!("Expected BulkString"),
                }
            }).collect();
            keys.sort();
            assert_eq!(keys, vec!["field1", "field2", "field3"]);
        },
        _ => panic!("Expected array of keys, got {:?}", res),
    }

    // 4. Wrong type
    run_cmd(vec!["SET", "mystring", "foo"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["HKEYS", "mystring"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(e) => assert!(e.contains("WRONGTYPE")),
        _ => panic!("Expected WRONGTYPE, got {:?}", res),
    }
}

#[tokio::test]
async fn test_hvals() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    async fn run_cmd(args: Vec<&str>, conn_ctx: &mut crate::cmd::ConnectionContext, server_ctx: &crate::cmd::ServerContext) -> Resp {
        let mut resp_args = Vec::new();
        for arg in args {
            resp_args.push(Resp::BulkString(Some(Bytes::from(arg.to_string()))));
        }
        let req = Resp::Array(Some(resp_args));
        let (res, _) = process_frame(req, conn_ctx, server_ctx).await;
        res
    }

    // 1. HVALS non-existent key
    let res = run_cmd(vec!["HVALS", "myhash_vals"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => assert_eq!(arr.len(), 0),
        _ => panic!("Expected empty array, got {:?}", res),
    }

    // 2. Setup hash
    run_cmd(vec!["HSET", "myhash_vals", "field1", "value1"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["HSET", "myhash_vals", "field2", "value2"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["HSET", "myhash_vals", "field3", "value3"], &mut conn_ctx, &server_ctx).await;

    // 3. HVALS existing hash
    let res = run_cmd(vec!["HVALS", "myhash_vals"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
            let mut vals: Vec<String> = arr.iter().map(|r| {
                match r {
                    Resp::BulkString(Some(b)) => String::from_utf8(b.to_vec()).unwrap(),
                    _ => panic!("Expected BulkString"),
                }
            }).collect();
            vals.sort();
            assert_eq!(vals, vec!["value1", "value2", "value3"]);
        },
        _ => panic!("Expected array of values, got {:?}", res),
    }

    // 4. Wrong type
    run_cmd(vec!["SET", "mystring_vals", "foo"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["HVALS", "mystring_vals"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(e) => assert!(e.contains("WRONGTYPE")),
        _ => panic!("Expected WRONGTYPE, got {:?}", res),
    }
}

#[tokio::test]
async fn test_hstrlen() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    async fn run_cmd(args: Vec<&str>, conn_ctx: &mut crate::cmd::ConnectionContext, server_ctx: &crate::cmd::ServerContext) -> Resp {
        let mut resp_args = Vec::new();
        for arg in args {
            resp_args.push(Resp::BulkString(Some(Bytes::from(arg.to_string()))));
        }
        let req = Resp::Array(Some(resp_args));
        let (res, _) = process_frame(req, conn_ctx, server_ctx).await;
        res
    }

    // 1. HSTRLEN non-existent key
    let res = run_cmd(vec!["HSTRLEN", "myhash_hstrlen", "field1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("Expected Integer(0), got {:?}", res),
    }

    // 2. Setup hash
    run_cmd(vec!["HSET", "myhash_hstrlen", "field1", "hello"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["HSET", "myhash_hstrlen", "field2", "world!"], &mut conn_ctx, &server_ctx).await;

    // 3. HSTRLEN existing key and field
    let res = run_cmd(vec!["HSTRLEN", "myhash_hstrlen", "field1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 5), // "hello".len()
        _ => panic!("Expected Integer(5), got {:?}", res),
    }

    let res = run_cmd(vec!["HSTRLEN", "myhash_hstrlen", "field2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 6), // "world!".len()
        _ => panic!("Expected Integer(6), got {:?}", res),
    }

    // 4. HSTRLEN existing key but non-existent field
    let res = run_cmd(vec!["HSTRLEN", "myhash_hstrlen", "nonexistent"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("Expected Integer(0), got {:?}", res),
    }

    // 5. Wrong type
    run_cmd(vec!["SET", "mystring_hstrlen", "foo"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["HSTRLEN", "mystring_hstrlen", "field1"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(e) => assert!(e.contains("WRONGTYPE")),
        _ => panic!("Expected WRONGTYPE, got {:?}", res),
    }
}

#[tokio::test]
async fn test_hrandfield() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    async fn run_cmd(args: Vec<&str>, conn_ctx: &mut crate::cmd::ConnectionContext, server_ctx: &crate::cmd::ServerContext) -> Resp {
        let mut resp_args = Vec::new();
        for arg in args {
            resp_args.push(Resp::BulkString(Some(Bytes::from(arg.to_string()))));
        }
        let req = Resp::Array(Some(resp_args));
        let (res, _) = process_frame(req, conn_ctx, server_ctx).await;
        res
    }

    // 1. HRANDFIELD non-existent key
    let res = run_cmd(vec!["HRANDFIELD", "myhash_rand"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(None) => {},
        _ => panic!("Expected Nil, got {:?}", res),
    }

    // 2. HRANDFIELD non-existent key with count
    let res = run_cmd(vec!["HRANDFIELD", "myhash_rand", "5"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => assert_eq!(arr.len(), 0),
        _ => panic!("Expected empty array, got {:?}", res),
    }

    // 3. Setup hash
    run_cmd(vec!["HSET", "myhash_rand", "f1", "v1"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["HSET", "myhash_rand", "f2", "v2"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["HSET", "myhash_rand", "f3", "v3"], &mut conn_ctx, &server_ctx).await;

    // 4. HRANDFIELD no count
    let res = run_cmd(vec!["HRANDFIELD", "myhash_rand"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => {
            let s = String::from_utf8(b.to_vec()).unwrap();
            assert!(["f1", "f2", "f3"].contains(&s.as_str()));
        },
        _ => panic!("Expected BulkString, got {:?}", res),
    }

    // 5. HRANDFIELD positive count
    let res = run_cmd(vec!["HRANDFIELD", "myhash_rand", "2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2);
            let mut keys: Vec<String> = arr.iter().map(|r| {
                match r {
                    Resp::BulkString(Some(b)) => String::from_utf8(b.to_vec()).unwrap(),
                    _ => panic!("Expected BulkString"),
                }
            }).collect();
            // Ensure unique keys
            keys.sort();
            keys.dedup();
            assert_eq!(keys.len(), 2);
            for k in &keys {
                assert!(["f1", "f2", "f3"].contains(&k.as_str()));
            }
        },
        _ => panic!("Expected Array, got {:?}", res),
    }
    
    // 6. HRANDFIELD count >= len
    let res = run_cmd(vec!["HRANDFIELD", "myhash_rand", "5"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
             // Should return all 3 fields, order random
            assert_eq!(arr.len(), 3);
            let mut keys: Vec<String> = arr.iter().map(|r| {
                match r {
                    Resp::BulkString(Some(b)) => String::from_utf8(b.to_vec()).unwrap(),
                    _ => panic!("Expected BulkString"),
                }
            }).collect();
            keys.sort();
            assert_eq!(keys, vec!["f1", "f2", "f3"]);
        },
        _ => panic!("Expected Array, got {:?}", res),
    }

    // 7. HRANDFIELD negative count
    let res = run_cmd(vec!["HRANDFIELD", "myhash_rand", "-5"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 5);
             for r in &arr {
                 match r {
                    Resp::BulkString(Some(b)) => {
                        let s = String::from_utf8(b.to_vec()).unwrap();
                        assert!(["f1", "f2", "f3"].contains(&s.as_str()));
                    },
                    _ => panic!("Expected BulkString"),
                }
             }
        },
        _ => panic!("Expected Array, got {:?}", res),
    }

    // 8. HRANDFIELD with WITHVALUES
    let res = run_cmd(vec!["HRANDFIELD", "myhash_rand", "2", "WITHVALUES"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 4); // 2 keys + 2 values
            for chunk in arr.chunks(2) {
                let k = match &chunk[0] {
                     Resp::BulkString(Some(b)) => String::from_utf8(b.to_vec()).unwrap(),
                     _ => panic!("Expected BulkString Key"),
                };
                let v = match &chunk[1] {
                     Resp::BulkString(Some(b)) => String::from_utf8(b.to_vec()).unwrap(),
                     _ => panic!("Expected BulkString Value"),
                };
                
                match k.as_str() {
                    "f1" => assert_eq!(v, "v1"),
                    "f2" => assert_eq!(v, "v2"),
                    "f3" => assert_eq!(v, "v3"),
                    _ => panic!("Unknown key {}", k),
                }
            }
        },
        _ => panic!("Expected Array, got {:?}", res),
    }

    // 9. Wrong type
    run_cmd(vec!["SET", "mystring_rand", "foo"], &mut conn_ctx, &server_ctx).await;
    let res = run_cmd(vec!["HRANDFIELD", "mystring_rand"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(e) => assert!(e.contains("WRONGTYPE")),
        _ => panic!("Expected WRONGTYPE, got {:?}", res),
    }
}
