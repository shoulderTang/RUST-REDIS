use crate::cmd::{process_frame, ConnectionContext, ServerContext};
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::test]
async fn test_keyspace_notifications() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();
    
    // 1. Enable K$ (Keyspace events for Strings)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("notify-keyspace-events"))),
        Resp::BulkString(Some(Bytes::from("K$"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    // 2. Subscribe to keyspace events for "mykey"
    let (tx, mut rx) = mpsc::channel(32);
    let mut sub_ctx = ConnectionContext::new(1, Some(tx), None);
    sub_ctx.authenticated = true;
    
    let sub_req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SUBSCRIBE"))),
        Resp::BulkString(Some(Bytes::from("__keyspace@0__:mykey"))),
    ]));
    let (sub_res, _) = process_frame(sub_req, &mut sub_ctx, &server_ctx).await;
    assert!(matches!(sub_res, Resp::Array(_)));

    // 3. Perform a SET command to trigger notification
    let set_req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("mykey"))),
        Resp::BulkString(Some(Bytes::from("myval"))),
    ]));
    let (set_res, _) = process_frame(set_req, &mut conn_ctx, &server_ctx).await;
    assert_eq!(set_res, Resp::SimpleString(Bytes::from("OK")));

    // 4. Check if we received the notification
    let msg = rx.recv().await.expect("Expected notification");
    if let Resp::Array(Some(items)) = msg {
        assert_eq!(items[0], Resp::BulkString(Some(Bytes::from("message"))));
        assert_eq!(items[1], Resp::BulkString(Some(Bytes::from("__keyspace@0__:mykey"))));
        assert_eq!(items[2], Resp::BulkString(Some(Bytes::from("set"))));
    } else {
        panic!("Unexpected notification format: {:?}", msg);
    }
}

#[tokio::test]
async fn test_keyevent_notifications() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();
    
    // 1. Enable Eg (Keyevent events for Generic commands)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("notify-keyspace-events"))),
        Resp::BulkString(Some(Bytes::from("Eg"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    // 2. Subscribe to keyevent events for "del"
    let (tx, mut rx) = mpsc::channel(32);
    let mut sub_ctx = ConnectionContext::new(1, Some(tx), None);
    sub_ctx.authenticated = true;
    
    let sub_req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SUBSCRIBE"))),
        Resp::BulkString(Some(Bytes::from("__keyevent@0__:del"))),
    ]));
    let (sub_res, _) = process_frame(sub_req, &mut sub_ctx, &server_ctx).await;
    assert!(matches!(sub_res, Resp::Array(_)));

    // 3. Perform a SET and then a DEL command
    let set_req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("mykey"))),
        Resp::BulkString(Some(Bytes::from("myval"))),
    ]));
    process_frame(set_req, &mut conn_ctx, &server_ctx).await;

    let del_req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("DEL"))),
        Resp::BulkString(Some(Bytes::from("mykey"))),
    ]));
    let (del_res, _) = process_frame(del_req, &mut conn_ctx, &server_ctx).await;
    assert_eq!(del_res, Resp::Integer(1));

    // 4. Check if we received the notification
    let msg = rx.recv().await.expect("Expected notification");
    if let Resp::Array(Some(items)) = msg {
        assert_eq!(items[0], Resp::BulkString(Some(Bytes::from("message"))));
        assert_eq!(items[1], Resp::BulkString(Some(Bytes::from("__keyevent@0__:del"))));
        assert_eq!(items[2], Resp::BulkString(Some(Bytes::from("mykey"))));
    } else {
        panic!("Unexpected notification format: {:?}", msg);
    }
}
