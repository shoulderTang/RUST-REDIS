use crate::cmd::process_frame;
use crate::resp::Resp;
use bytes::Bytes;

#[tokio::test]
async fn test_linsert() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // LPUSH list a b c -> ["c", "b", "a"]
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LPUSH"))),
        Resp::BulkString(Some(Bytes::from("list"))),
        Resp::BulkString(Some(Bytes::from("a"))),
        Resp::BulkString(Some(Bytes::from("b"))),
        Resp::BulkString(Some(Bytes::from("c"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // LINSERT list BEFORE b x -> 4
    // list should be ["c", "x", "b", "a"]
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LINSERT"))),
        Resp::BulkString(Some(Bytes::from("list"))),
        Resp::BulkString(Some(Bytes::from("BEFORE"))),
        Resp::BulkString(Some(Bytes::from("b"))),
        Resp::BulkString(Some(Bytes::from("x"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 4),
        _ => panic!("expected Integer(4)"),
    }

    // Verify list content
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LRANGE"))),
        Resp::BulkString(Some(Bytes::from("list"))),
        Resp::BulkString(Some(Bytes::from("0"))),
        Resp::BulkString(Some(Bytes::from("-1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 4);
            match &items[1] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("x")),
                _ => panic!("expected BulkString(x)"),
            }
        }
        _ => panic!("expected Array"),
    }

    // LINSERT list AFTER b y -> 5
    // list should be ["c", "x", "b", "y", "a"]
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LINSERT"))),
        Resp::BulkString(Some(Bytes::from("list"))),
        Resp::BulkString(Some(Bytes::from("AFTER"))),
        Resp::BulkString(Some(Bytes::from("b"))),
        Resp::BulkString(Some(Bytes::from("y"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 5),
        _ => panic!("expected Integer(5)"),
    }

    // Verify list content
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LRANGE"))),
        Resp::BulkString(Some(Bytes::from("list"))),
        Resp::BulkString(Some(Bytes::from("0"))),
        Resp::BulkString(Some(Bytes::from("-1"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 5);
            match &items[3] {
                Resp::BulkString(Some(b)) => assert_eq!(*b, Bytes::from("y")),
                _ => panic!("expected BulkString(y)"),
            }
        }
        _ => panic!("expected Array"),
    }

    // LINSERT list BEFORE z z -> -1 (pivot not found)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LINSERT"))),
        Resp::BulkString(Some(Bytes::from("list"))),
        Resp::BulkString(Some(Bytes::from("BEFORE"))),
        Resp::BulkString(Some(Bytes::from("z"))),
        Resp::BulkString(Some(Bytes::from("z"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, -1),
        _ => panic!("expected Integer(-1)"),
    }

    // LINSERT non_existent BEFORE a x -> 0
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LINSERT"))),
        Resp::BulkString(Some(Bytes::from("non_existent"))),
        Resp::BulkString(Some(Bytes::from("BEFORE"))),
        Resp::BulkString(Some(Bytes::from("a"))),
        Resp::BulkString(Some(Bytes::from("x"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 0),
        _ => panic!("expected Integer(0)"),
    }

    // Wrong type
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("key"))),
        Resp::BulkString(Some(Bytes::from("value"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("LINSERT"))),
        Resp::BulkString(Some(Bytes::from("key"))),
        Resp::BulkString(Some(Bytes::from("BEFORE"))),
        Resp::BulkString(Some(Bytes::from("a"))),
        Resp::BulkString(Some(Bytes::from("x"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Error(s) => assert!(s.contains("WRONGTYPE")),
        _ => panic!("expected Error WRONGTYPE"),
    }
}
