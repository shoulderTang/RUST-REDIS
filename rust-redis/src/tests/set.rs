use crate::cmd::{process_frame, ConnectionContext, ServerContext};
use crate::cmd::scripting;
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::Arc;
use std::collections::HashSet;

#[tokio::test]
async fn test_set_ops() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // SADD set m1 -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SADD"))),
        Resp::BulkString(Some(Bytes::from("set"))),
        Resp::BulkString(Some(Bytes::from("m1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
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
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
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
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }

    // SMEMBERS set -> ["m1"]
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SMEMBERS"))),
        Resp::BulkString(Some(Bytes::from("set"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
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
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
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
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 1),
        _ => panic!("expected Integer(1)"),
    }
}

#[tokio::test]
async fn test_srandmember() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    async fn run_cmd(args: Vec<&str>, conn_ctx: &mut crate::cmd::ConnectionContext, server_ctx: &crate::cmd::ServerContext) -> Resp {
        let mut resp_args = Vec::new();
        for arg in args {
            resp_args.push(Resp::BulkString(Some(Bytes::from(arg.to_string()))));
        }
        let (res, _) = process_frame(Resp::Array(Some(resp_args)), conn_ctx, server_ctx).await;
        res
    }

    // SADD set m1 m2 m3
    run_cmd(vec!["SADD", "set", "m1", "m2", "m3"], &mut conn_ctx, &server_ctx).await;

    // SRANDMEMBER set
    let res = run_cmd(vec!["SRANDMEMBER", "set"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(ref b)) => {
            let s = String::from_utf8_lossy(b);
            assert!(s == "m1" || s == "m2" || s == "m3");
        },
        _ => panic!("expected BulkString"),
    }

    // Check set size is still 3 (SRANDMEMBER doesn't remove)
    let res = run_cmd(vec!["SCARD", "set"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 3),
        _ => panic!("expected Integer(3)"),
    }

    // SRANDMEMBER set 2
    let res = run_cmd(vec!["SRANDMEMBER", "set", "2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            // Verify items are distinct
            let mut s: HashSet<Bytes> = HashSet::new();
            for item in items {
                match item {
                    Resp::BulkString(Some(ref b)) => {
                        s.insert(b.clone());
                    },
                    _ => panic!("expected BulkString"),
                }
            }
            assert_eq!(s.len(), 2);
        },
        _ => panic!("expected Array"),
    }

    // SRANDMEMBER set -5
    let res = run_cmd(vec!["SRANDMEMBER", "set", "-5"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 5);
        },
        _ => panic!("expected Array"),
    }
    
    // Non-existent key
    let res = run_cmd(vec!["SRANDMEMBER", "nonexist"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(None) => {},
        _ => panic!("expected BulkString(None)"),
    }

    // Non-existent key with count
    let res = run_cmd(vec!["SRANDMEMBER", "nonexist", "2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => assert_eq!(items.len(), 0),
        _ => panic!("expected empty Array"),
    }
}

#[tokio::test]
async fn test_spop() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    async fn run_cmd(args: Vec<&str>, conn_ctx: &mut crate::cmd::ConnectionContext, server_ctx: &crate::cmd::ServerContext) -> Resp {
        let mut resp_args = Vec::new();
        for arg in args {
            resp_args.push(Resp::BulkString(Some(Bytes::from(arg.to_string()))));
        }
        let (res, _) = process_frame(Resp::Array(Some(resp_args)), conn_ctx, server_ctx).await;
        res
    }

    // SADD set m1 m2 m3
    run_cmd(vec!["SADD", "set", "m1", "m2", "m3"], &mut conn_ctx, &server_ctx).await;

    // SPOP set
    let res = run_cmd(vec!["SPOP", "set"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(ref b)) => {
            let s = String::from_utf8_lossy(b);
            assert!(s == "m1" || s == "m2" || s == "m3");
        },
        _ => panic!("expected BulkString"),
    }

    // Check set size is now 2
    let res = run_cmd(vec!["SCARD", "set"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 2),
        _ => panic!("expected Integer(2)"),
    }

    // SPOP set 2
    let res = run_cmd(vec!["SPOP", "set", "2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            // Verify items are distinct
            let mut s: HashSet<Bytes> = HashSet::new();
            for item in items {
                match item {
                    Resp::BulkString(Some(ref b)) => {
                        s.insert(b.clone());
                    },
                    _ => panic!("expected BulkString"),
                }
            }
            assert_eq!(s.len(), 2);
        },
        _ => panic!("expected Array"),
    }

    // Check set size is now 0
    let res = run_cmd(vec!["SCARD", "set"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("expected Integer(0)"),
    }

    // SPOP empty set
    let res = run_cmd(vec!["SPOP", "set"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(None) => {},
        _ => panic!("expected BulkString(None)"),
    }

    // Non-existent key
    let res = run_cmd(vec!["SPOP", "nonexist"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(None) => {},
        _ => panic!("expected BulkString(None)"),
    }

    // Non-existent key with count
    let res = run_cmd(vec!["SPOP", "nonexist", "2"], &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => assert_eq!(items.len(), 0),
        _ => panic!("expected empty Array"),
    }
}
