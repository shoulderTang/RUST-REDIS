use crate::cmd::{ConnectionContext, ServerContext, process_frame};
use crate::resp::Resp;
use bytes::Bytes;
use crate::tests::helper::{create_server_context, create_connection_context};

async fn run_cmd_bytes(args: Vec<Bytes>, conn_ctx: &mut ConnectionContext, server_ctx: &ServerContext) -> Resp {
    let mut resp_args = Vec::new();
    for arg in args {
        resp_args.push(Resp::BulkString(Some(arg)));
    }
    let req = Resp::Array(Some(resp_args));
    let (res, _) = process_frame(req, conn_ctx, server_ctx).await;
    res
}

#[tokio::test]
async fn test_dump_restore() {
    let server_ctx = create_server_context();
    let mut conn_ctx = create_connection_context();

    // SET mykey hello
    let _ = run_cmd_bytes(vec![Bytes::from("SET"), Bytes::from("mykey"), Bytes::from("hello")], &mut conn_ctx, &server_ctx).await;

    // DUMP mykey
    let resp = run_cmd_bytes(vec![Bytes::from("DUMP"), Bytes::from("mykey")], &mut conn_ctx, &server_ctx).await;
    let serialized = match resp {
        Resp::BulkString(Some(bytes)) => bytes,
        _ => panic!("Expected BulkString from DUMP, got {:?}", resp),
    };

    // DEL mykey
    let _ = run_cmd_bytes(vec![Bytes::from("DEL"), Bytes::from("mykey")], &mut conn_ctx, &server_ctx).await;

    // RESTORE mykey 0 serialized
    let resp = run_cmd_bytes(vec![Bytes::from("RESTORE"), Bytes::from("mykey"), Bytes::from("0"), serialized.clone()], &mut conn_ctx, &server_ctx).await;
    assert_eq!(resp, Resp::SimpleString(Bytes::from("OK")));

    // GET mykey
    let resp = run_cmd_bytes(vec![Bytes::from("GET"), Bytes::from("mykey")], &mut conn_ctx, &server_ctx).await;
    assert_eq!(resp, Resp::BulkString(Some(Bytes::from("hello"))));

    // RESTORE existing key (fail)
    let resp = run_cmd_bytes(vec![Bytes::from("RESTORE"), Bytes::from("mykey"), Bytes::from("0"), serialized.clone()], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::Error(msg) => assert!(msg.contains("BUSYKEY")),
        _ => panic!("Expected BUSYKEY error, got {:?}", resp),
    }

    // RESTORE REPLACE
    let resp = run_cmd_bytes(vec![Bytes::from("RESTORE"), Bytes::from("mykey"), Bytes::from("0"), serialized.clone(), Bytes::from("REPLACE")], &mut conn_ctx, &server_ctx).await;
    assert_eq!(resp, Resp::SimpleString(Bytes::from("OK")));
    
    // Test Bad Checksum
    let mut bad_serialized = serialized.to_vec();
    let len = bad_serialized.len();
    if len > 0 {
        bad_serialized[len - 1] = bad_serialized[len - 1].wrapping_add(1); // Corrupt CRC
        let resp = run_cmd_bytes(vec![Bytes::from("RESTORE"), Bytes::from("mykey2"), Bytes::from("0"), Bytes::from(bad_serialized)], &mut conn_ctx, &server_ctx).await;
        match resp {
             Resp::Error(msg) => assert!(msg.contains("checksum"), "Expected checksum error, got {}", msg),
             _ => panic!("Expected error for bad checksum, got {:?}", resp),
        }
    }

    // Verify COMMAND count
    // let resp = run_cmd_bytes(vec![Bytes::from("COMMAND")], &mut conn_ctx, &server_ctx).await;
    // match resp {
    //     Resp::Array(Some(cmds)) => assert_eq!(cmds.len(), 180, "Expected 180 commands, got {}", cmds.len()),
    //     _ => panic!("Expected Array from COMMAND, got {:?}", resp),
    // }
}
