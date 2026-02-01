use crate::cmd::{process_frame, ConnectionContext, ServerContext};
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::Arc;

#[tokio::test]
async fn test_hexists() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // 1. HSET key field val
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("HSET"))),
        Resp::BulkString(Some(Bytes::from("myhash"))),
        Resp::BulkString(Some(Bytes::from("field1"))),
        Resp::BulkString(Some(Bytes::from("val1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(1));

    // 2. HEXISTS existing field
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("HEXISTS"))),
        Resp::BulkString(Some(Bytes::from("myhash"))),
        Resp::BulkString(Some(Bytes::from("field1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(1));

    // 3. HEXISTS non-existing field
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("HEXISTS"))),
        Resp::BulkString(Some(Bytes::from("myhash"))),
        Resp::BulkString(Some(Bytes::from("field2"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(0));

    // 4. HEXISTS non-existing key
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("HEXISTS"))),
        Resp::BulkString(Some(Bytes::from("nosuchkey"))),
        Resp::BulkString(Some(Bytes::from("field1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(0));
}
