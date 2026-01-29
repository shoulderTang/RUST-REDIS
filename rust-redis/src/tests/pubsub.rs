use crate::cmd::{ConnectionContext, ServerContext};
use crate::resp::Resp;
use crate::cmd::pubsub;
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::test]
async fn test_subscribe_publish() {
    let (tx, mut rx) = mpsc::channel(32);
    
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = ConnectionContext::new(1, Some(tx), None);

    // SUBSCRIBE
    let sub_args = vec![
        Resp::BulkString(Some(Bytes::from("SUBSCRIBE"))),
        Resp::BulkString(Some(Bytes::from("ch1")))
    ];
    let resp = pubsub::subscribe(sub_args, &mut conn_ctx, &server_ctx).await;
    
    // Check SUBSCRIBE response (for the single channel)
    if let Resp::Array(Some(items)) = resp {
        assert_eq!(items[0], Resp::BulkString(Some(Bytes::from("subscribe"))));
        assert_eq!(items[1], Resp::BulkString(Some(Bytes::from("ch1"))));
        assert_eq!(items[2], Resp::Integer(1));
    } else {
        panic!("Unexpected SUBSCRIBE response: {:?}", resp);
    }

    // PUBLISH
    let pub_args = vec![
        Resp::BulkString(Some(Bytes::from("PUBLISH"))),
        Resp::BulkString(Some(Bytes::from("ch1"))),
        Resp::BulkString(Some(Bytes::from("hello")))
    ];
    let resp = pubsub::publish(pub_args, &mut conn_ctx, &server_ctx).await;
    
    // Check PUBLISH response (integer 1)
    if let Resp::Integer(n) = resp {
        assert_eq!(n, 1);
    } else {
        panic!("Unexpected PUBLISH response: {:?}", resp);
    }

    // Check received message
    // The "subscribe" confirmation was returned, not pushed to rx.
    // So the first message in rx should be the PUBLISHED message.
    let msg1 = rx.recv().await.expect("Expected published message");
    if let Resp::Array(Some(items)) = msg1 {
        assert_eq!(items[0], Resp::BulkString(Some(Bytes::from("message"))));
        assert_eq!(items[1], Resp::BulkString(Some(Bytes::from("ch1"))));
        assert_eq!(items[2], Resp::BulkString(Some(Bytes::from("hello"))));
    } else {
        panic!("Unexpected published message: {:?}", msg1);
    }
}

#[tokio::test]
async fn test_pubsub_channels() {
    let (tx, _rx) = mpsc::channel(32);
    
    let server_ctx = crate::tests::helper::create_server_context();

    let mut conn_ctx = ConnectionContext::new(1, Some(tx), None);

    // SUBSCRIBE ch1
    let args = vec![
        Resp::BulkString(Some(Bytes::from("SUBSCRIBE"))),
        Resp::BulkString(Some(Bytes::from("ch1")))
    ];
    pubsub::subscribe(args, &mut conn_ctx, &server_ctx).await;

    // PUBSUB CHANNELS
    let args = vec![
        Resp::BulkString(Some(Bytes::from("PUBSUB"))),
        Resp::BulkString(Some(Bytes::from("CHANNELS")))
    ];
    let resp = pubsub::pubsub_command(args, &mut conn_ctx, &server_ctx).await;

    if let Resp::Array(Some(items)) = resp {
        assert!(!items.is_empty());
        let mut found = false;
        for item in items {
            if let Resp::BulkString(Some(b)) = item {
                if b == "ch1" {
                    found = true;
                    break;
                }
            }
        }
        assert!(found);
    } else {
        panic!("Unexpected response: {:?}", resp);
    }
    
    // Test pattern matching
    let args = vec![
        Resp::BulkString(Some(Bytes::from("PUBSUB"))),
        Resp::BulkString(Some(Bytes::from("CHANNELS"))),
        Resp::BulkString(Some(Bytes::from("ch*")))
    ];
    let resp = pubsub::pubsub_command(args, &mut conn_ctx, &server_ctx).await;
    if let Resp::Array(Some(items)) = resp {
        assert!(!items.is_empty());
        let mut found = false;
        for item in items {
            if let Resp::BulkString(Some(b)) = item {
                if b == "ch1" {
                    found = true;
                    break;
                }
            }
        }
        assert!(found);
    }
}

#[tokio::test]
async fn test_pubsub_channels_filtering() {
    let (tx, _rx) = mpsc::channel(32);
    
    let server_ctx = crate::tests::helper::create_server_context();

    let mut conn_ctx = ConnectionContext::new(1, Some(tx), None);

    // 1. SUBSCRIBE ch1
    let sub_args = vec![
        Resp::BulkString(Some(Bytes::from("SUBSCRIBE"))),
        Resp::BulkString(Some(Bytes::from("ch1")))
    ];
    pubsub::subscribe(sub_args, &mut conn_ctx, &server_ctx).await;

    // 2. UNSUBSCRIBE ch1
    let unsub_args = vec![
        Resp::BulkString(Some(Bytes::from("UNSUBSCRIBE"))),
        Resp::BulkString(Some(Bytes::from("ch1")))
    ];
    pubsub::unsubscribe(unsub_args, &mut conn_ctx, &server_ctx).await;

    // 3. PUBSUB CHANNELS - should be empty (zombie channel filtering)
    let args = vec![
        Resp::BulkString(Some(Bytes::from("PUBSUB"))),
        Resp::BulkString(Some(Bytes::from("CHANNELS")))
    ];
    let resp = pubsub::pubsub_command(args, &mut conn_ctx, &server_ctx).await;
    
    if let Resp::Array(Some(items)) = resp {
        assert!(items.is_empty(), "PUBSUB CHANNELS should be empty after unsubscribe");
    } else {
        panic!("Unexpected response: {:?}", resp);
    }
}
