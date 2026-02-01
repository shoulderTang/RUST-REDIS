use crate::cmd::{process_frame, ConnectionContext, ServerContext};
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::Arc;

#[tokio::test]
async fn test_rdb_config() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // 1. Check default values via CONFIG GET
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("rdbcompression"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    if let Resp::Array(Some(items)) = res {
        assert_eq!(items[1], Resp::BulkString(Some(Bytes::from("yes"))));
    } else {
        panic!("Expected array response");
    }

    // 2. Modify rdbcompression via CONFIG SET
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("rdbcompression"))),
        Resp::BulkString(Some(Bytes::from("no"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    // 3. Verify modification
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("rdbcompression"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    if let Resp::Array(Some(items)) = res {
        assert_eq!(items[1], Resp::BulkString(Some(Bytes::from("no"))));
    }

    // 4. Verify other RDB configs
    let rdb_configs = vec!["rdbchecksum", "stop-writes-on-bgsave-error"];
    for config in rdb_configs {
        let req = Resp::Array(Some(vec![
            Resp::BulkString(Some(Bytes::from("CONFIG"))),
            Resp::BulkString(Some(Bytes::from("GET"))),
            Resp::BulkString(Some(Bytes::from(config))),
        ]));
        let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
        if let Resp::Array(Some(items)) = res {
            assert_eq!(items[1], Resp::BulkString(Some(Bytes::from("yes"))));
        }

        let req = Resp::Array(Some(vec![
            Resp::BulkString(Some(Bytes::from("CONFIG"))),
            Resp::BulkString(Some(Bytes::from("SET"))),
            Resp::BulkString(Some(Bytes::from(config))),
            Resp::BulkString(Some(Bytes::from("no"))),
        ]));
        let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
        assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

        let req = Resp::Array(Some(vec![
            Resp::BulkString(Some(Bytes::from("CONFIG"))),
            Resp::BulkString(Some(Bytes::from("GET"))),
            Resp::BulkString(Some(Bytes::from(config))),
        ]));
        let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
        if let Resp::Array(Some(items)) = res {
            assert_eq!(items[1], Resp::BulkString(Some(Bytes::from("no"))));
        }
    }
}
