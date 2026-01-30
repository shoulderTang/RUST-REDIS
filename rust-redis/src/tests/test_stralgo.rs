use crate::resp::Resp;
use crate::cmd::{process_frame, ServerContext};
use bytes::Bytes;
use tokio;

#[tokio::test]
async fn test_stralgo_lcs_basics() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // STRALGO LCS STRINGS "ohmytext" "mynewtext" -> "mytext"
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("STRALGO"))),
        Resp::BulkString(Some(Bytes::from("LCS"))),
        Resp::BulkString(Some(Bytes::from("STRINGS"))),
        Resp::BulkString(Some(Bytes::from("ohmytext"))),
        Resp::BulkString(Some(Bytes::from("mynewtext"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("mytext")),
        _ => panic!("Expected bulk string 'mytext'"),
    }
}

#[tokio::test]
async fn test_stralgo_lcs_len() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // STRALGO LCS STRINGS "ohmytext" "mynewtext" LEN -> 6
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("STRALGO"))),
        Resp::BulkString(Some(Bytes::from("LCS"))),
        Resp::BulkString(Some(Bytes::from("STRINGS"))),
        Resp::BulkString(Some(Bytes::from("ohmytext"))),
        Resp::BulkString(Some(Bytes::from("mynewtext"))),
        Resp::BulkString(Some(Bytes::from("LEN"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Integer(6));
}

#[tokio::test]
async fn test_stralgo_lcs_keys() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Set keys
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("MSET"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("ohmytext"))),
        Resp::BulkString(Some(Bytes::from("k2"))),
        Resp::BulkString(Some(Bytes::from("mynewtext"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // STRALGO LCS KEYS k1 k2 -> "mytext"
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("STRALGO"))),
        Resp::BulkString(Some(Bytes::from("LCS"))),
        Resp::BulkString(Some(Bytes::from("KEYS"))),
        Resp::BulkString(Some(Bytes::from("k1"))),
        Resp::BulkString(Some(Bytes::from("k2"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("mytext")),
        _ => panic!("Expected bulk string 'mytext'"),
    }
}
