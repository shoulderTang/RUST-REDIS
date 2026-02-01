use crate::tests::helper::{create_connection_context, create_server_context};
use crate::resp::Resp;
use crate::cmd::{ConnectionContext, ServerContext, process_frame};
use bytes::Bytes;

async fn run_cmd_bytes(args: Vec<Bytes>, conn_ctx: &mut ConnectionContext, server_ctx: &ServerContext) -> Resp {
    let mut resp_args = Vec::new();
    for arg in args {
        resp_args.push(Resp::BulkString(Some(arg)));
    }
    let frame = Resp::Array(Some(resp_args));
    let (resp, _) = process_frame(frame, conn_ctx, server_ctx).await;
    resp
}

#[tokio::test]
async fn test_sort_list_basic() {
    let server_ctx = create_server_context();
    let mut conn_ctx = create_connection_context();

    run_cmd_bytes(vec![Bytes::from("RPUSH"), Bytes::from("mylist"), Bytes::from("3"), Bytes::from("1"), Bytes::from("2")], &mut conn_ctx, &server_ctx).await;

    let resp = run_cmd_bytes(vec![Bytes::from("SORT"), Bytes::from("mylist")], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Resp::BulkString(Some(Bytes::from("1"))));
            assert_eq!(items[1], Resp::BulkString(Some(Bytes::from("2"))));
            assert_eq!(items[2], Resp::BulkString(Some(Bytes::from("3"))));
        }
        _ => panic!("Expected Array"),
    }
}

#[tokio::test]
async fn test_sort_desc_alpha() {
    let server_ctx = create_server_context();
    let mut conn_ctx = create_connection_context();

    run_cmd_bytes(vec![Bytes::from("RPUSH"), Bytes::from("mylist"), Bytes::from("a"), Bytes::from("c"), Bytes::from("b")], &mut conn_ctx, &server_ctx).await;

    let resp = run_cmd_bytes(vec![Bytes::from("SORT"), Bytes::from("mylist"), Bytes::from("ALPHA"), Bytes::from("DESC")], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Resp::BulkString(Some(Bytes::from("c"))));
            assert_eq!(items[1], Resp::BulkString(Some(Bytes::from("b"))));
            assert_eq!(items[2], Resp::BulkString(Some(Bytes::from("a"))));
        }
        _ => panic!("Expected Array"),
    }
}

#[tokio::test]
async fn test_sort_limit() {
    let server_ctx = create_server_context();
    let mut conn_ctx = create_connection_context();

    run_cmd_bytes(vec![Bytes::from("RPUSH"), Bytes::from("mylist"), Bytes::from("1"), Bytes::from("2"), Bytes::from("3"), Bytes::from("4")], &mut conn_ctx, &server_ctx).await;

    let resp = run_cmd_bytes(vec![Bytes::from("SORT"), Bytes::from("mylist"), Bytes::from("LIMIT"), Bytes::from("1"), Bytes::from("2")], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Resp::BulkString(Some(Bytes::from("2"))));
            assert_eq!(items[1], Resp::BulkString(Some(Bytes::from("3"))));
        }
        _ => panic!("Expected Array"),
    }
}

#[tokio::test]
async fn test_sort_by_get() {
    let server_ctx = create_server_context();
    let mut conn_ctx = create_connection_context();

    run_cmd_bytes(vec![Bytes::from("RPUSH"), Bytes::from("mylist"), Bytes::from("1"), Bytes::from("2"), Bytes::from("3")], &mut conn_ctx, &server_ctx).await;
    
    // Weights: w_1=30, w_2=10, w_3=20
    run_cmd_bytes(vec![Bytes::from("MSET"), Bytes::from("w_1"), Bytes::from("30"), Bytes::from("w_2"), Bytes::from("10"), Bytes::from("w_3"), Bytes::from("20")], &mut conn_ctx, &server_ctx).await;
    
    // Objects: o_1=one, o_2=two, o_3=three
    run_cmd_bytes(vec![Bytes::from("MSET"), Bytes::from("o_1"), Bytes::from("one"), Bytes::from("o_2"), Bytes::from("two"), Bytes::from("o_3"), Bytes::from("three")], &mut conn_ctx, &server_ctx).await;

    // Sort by weight: 2 (10), 3 (20), 1 (30)
    // Get object: two, three, one
    let resp = run_cmd_bytes(vec![
        Bytes::from("SORT"), Bytes::from("mylist"), 
        Bytes::from("BY"), Bytes::from("w_*"), 
        Bytes::from("GET"), Bytes::from("o_*")
    ], &mut conn_ctx, &server_ctx).await;

    match resp {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Resp::BulkString(Some(Bytes::from("two"))));
            assert_eq!(items[1], Resp::BulkString(Some(Bytes::from("three"))));
            assert_eq!(items[2], Resp::BulkString(Some(Bytes::from("one"))));
        }
        _ => panic!("Expected Array, got {:?}", resp),
    }
}

#[tokio::test]
async fn test_sort_store() {
    let server_ctx = create_server_context();
    let mut conn_ctx = create_connection_context();

    run_cmd_bytes(vec![Bytes::from("RPUSH"), Bytes::from("mylist"), Bytes::from("3"), Bytes::from("1"), Bytes::from("2")], &mut conn_ctx, &server_ctx).await;

    let resp = run_cmd_bytes(vec![Bytes::from("SORT"), Bytes::from("mylist"), Bytes::from("STORE"), Bytes::from("sorted_list")], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::Integer(n) => assert_eq!(n, 3),
        _ => panic!("Expected Integer"),
    }

    let resp = run_cmd_bytes(vec![Bytes::from("LRANGE"), Bytes::from("sorted_list"), Bytes::from("0"), Bytes::from("-1")], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Resp::BulkString(Some(Bytes::from("1"))));
            assert_eq!(items[1], Resp::BulkString(Some(Bytes::from("2"))));
            assert_eq!(items[2], Resp::BulkString(Some(Bytes::from("3"))));
        }
        _ => panic!("Expected Array"),
    }
}

#[tokio::test]
async fn test_sort_ro() {
    let server_ctx = create_server_context();
    let mut conn_ctx = create_connection_context();

    run_cmd_bytes(vec![Bytes::from("RPUSH"), Bytes::from("mylist"), Bytes::from("3"), Bytes::from("1"), Bytes::from("2")], &mut conn_ctx, &server_ctx).await;

    let resp = run_cmd_bytes(vec![Bytes::from("SORT_RO"), Bytes::from("mylist")], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Resp::BulkString(Some(Bytes::from("1"))));
        }
        _ => panic!("Expected Array"),
    }

    // SORT_RO with STORE should fail
    let resp = run_cmd_bytes(vec![Bytes::from("SORT_RO"), Bytes::from("mylist"), Bytes::from("STORE"), Bytes::from("dest")], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::Error(e) => assert!(e.contains("syntax error") || e.contains("STORE")), // My impl returns syntax error for readonly check
        _ => panic!("Expected Error, got {:?}", resp),
    }
}
