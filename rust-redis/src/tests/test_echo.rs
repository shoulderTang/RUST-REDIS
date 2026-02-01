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
async fn test_echo() {
    let server_ctx = create_server_context();
    let mut conn_ctx = create_connection_context();

    // ECHO hello
    let resp = run_cmd_bytes(vec![Bytes::from("ECHO"), Bytes::from("hello")], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("hello")),
        _ => panic!("Expected BulkString('hello'), got {:?}", resp),
    }

    // ECHO with spaces
    let resp = run_cmd_bytes(vec![Bytes::from("ECHO"), Bytes::from("hello world")], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::BulkString(Some(b)) => assert_eq!(b, Bytes::from("hello world")),
        _ => panic!("Expected BulkString('hello world'), got {:?}", resp),
    }

    // ECHO wrong number of args
    let resp = run_cmd_bytes(vec![Bytes::from("ECHO")], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::Error(e) => assert!(e.contains("wrong number of arguments")),
        _ => panic!("Expected Error, got {:?}", resp),
    }

    let resp = run_cmd_bytes(vec![Bytes::from("ECHO"), Bytes::from("a"), Bytes::from("b")], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::Error(e) => assert!(e.contains("wrong number of arguments")),
        _ => panic!("Expected Error, got {:?}", resp),
    }
}
