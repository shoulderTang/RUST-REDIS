use crate::cmd::{process_frame, ConnectionContext, ServerContext};
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::Arc;

#[tokio::test]
async fn test_config_maxmemory() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // 1. Set maxmemory with units (MB)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("maxmemory"))),
        Resp::BulkString(Some(Bytes::from("100mb"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    // 2. Get maxmemory and verify value (bytes)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("maxmemory"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            match &items[0] {
                 Resp::BulkString(Some(b)) => assert_eq!(String::from_utf8_lossy(b), "maxmemory"),
                 _ => panic!("expected maxmemory key"),
            }
            // 100mb = 100 * 1024 * 1024 = 104857600
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(String::from_utf8_lossy(b), "104857600"),
                _ => panic!("expected maxmemory value 104857600"),
            }
        }
        _ => panic!("expected Array response"),
    }

    // 3. Set maxmemory with units (GB)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("maxmemory"))),
        Resp::BulkString(Some(Bytes::from("1gb"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    // 4. Verify GB value
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("maxmemory"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            // 1gb = 1073741824
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(String::from_utf8_lossy(b), "1073741824"),
                _ => panic!("expected maxmemory value 1073741824"),
            }
        }
        _ => panic!("expected Array response"),
    }
    
    // 5. Set maxmemory raw bytes
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("maxmemory"))),
        Resp::BulkString(Some(Bytes::from("1000"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));
    
    // 6. Verify raw bytes
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("GET"))),
        Resp::BulkString(Some(Bytes::from("maxmemory"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(String::from_utf8_lossy(b), "1000"),
                _ => panic!("expected maxmemory value 1000"),
            }
        }
        _ => panic!("expected Array response"),
    }
}
