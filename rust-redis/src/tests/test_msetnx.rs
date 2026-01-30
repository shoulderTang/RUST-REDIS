use crate::resp::Resp;
use crate::cmd::{process_frame, ServerContext};
use bytes::Bytes;
use tokio;

#[tokio::test]
async fn test_msetnx() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // MSETNX key1 val1 key2 val2 -> 1
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("MSETNX"))),
        Resp::BulkString(Some(Bytes::from("key1"))),
        Resp::BulkString(Some(Bytes::from("val1"))),
        Resp::BulkString(Some(Bytes::from("key2"))),
        Resp::BulkString(Some(Bytes::from("val2"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(1));

    // Verify values
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("MGET"))),
        Resp::BulkString(Some(Bytes::from("key1"))),
        Resp::BulkString(Some(Bytes::from("key2"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], Resp::BulkString(Some(Bytes::from("val1"))));
            assert_eq!(arr[1], Resp::BulkString(Some(Bytes::from("val2"))));
        }
        _ => panic!("Expected array"),
    }

    // MSETNX key2 val3 key3 val4 -> 0 (key2 exists)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("MSETNX"))),
        Resp::BulkString(Some(Bytes::from("key2"))),
        Resp::BulkString(Some(Bytes::from("val3"))),
        Resp::BulkString(Some(Bytes::from("key3"))),
        Resp::BulkString(Some(Bytes::from("val4"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(0));

    // Verify key2 unchanged, key3 not set
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("MGET"))),
        Resp::BulkString(Some(Bytes::from("key2"))),
        Resp::BulkString(Some(Bytes::from("key3"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], Resp::BulkString(Some(Bytes::from("val2"))));
            assert_eq!(arr[1], Resp::BulkString(None));
        }
        _ => panic!("Expected array"),
    }
}
