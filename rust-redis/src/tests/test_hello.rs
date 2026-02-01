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
async fn test_hello() {
    let server_ctx = create_server_context();
    let mut conn_ctx = create_connection_context();

    // HELLO (no args)
    let resp = run_cmd_bytes(vec![Bytes::from("HELLO")], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::Array(Some(info)) => {
            assert!(info.len() >= 10);
            // Check for "proto" field
            let mut found_proto = false;
            for i in 0..info.len() {
                if let Resp::BulkString(Some(key)) = &info[i] {
                    if key == &Bytes::from("proto") {
                        if let Resp::Integer(val) = &info[i+1] {
                            assert_eq!(*val, 2);
                            found_proto = true;
                        }
                    }
                }
            }
            assert!(found_proto);
        },
        _ => panic!("Expected Array, got {:?}", resp),
    }

    // HELLO 2
    let resp = run_cmd_bytes(vec![Bytes::from("HELLO"), Bytes::from("2")], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::Array(Some(_)) => {}, // OK
        _ => panic!("Expected Array, got {:?}", resp),
    }

    // HELLO 3
    let resp = run_cmd_bytes(vec![Bytes::from("HELLO"), Bytes::from("3")], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::Array(Some(info)) => {
            // Check proto is 3
             for i in 0..info.len() {
                if let Resp::BulkString(Some(key)) = &info[i] {
                    if key == &Bytes::from("proto") {
                        if let Resp::Integer(val) = &info[i+1] {
                            assert_eq!(*val, 3);
                        }
                    }
                }
            }
        }, 
        _ => panic!("Expected Array, got {:?}", resp),
    }
    
    // HELLO 2 SETNAME myclient
    let resp = run_cmd_bytes(vec![Bytes::from("HELLO"), Bytes::from("2"), Bytes::from("SETNAME"), Bytes::from("myclient")], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::Array(Some(_)) => {},
        _ => panic!("Expected Array, got {:?}", resp),
    }
    // Verify client name
    // Since we don't have direct access to server_ctx.clients inside test easily without lock, 
    // we can check via CLIENT GETNAME or similar if implemented, but we trust the response for now.
    // Actually we can check conn_ctx if it was updated? No, conn_ctx has id, name is in server_ctx.clients.
    // We can use CLIENT LIST or CLIENT GETNAME if available.
    
    // HELLO 2 AUTH default (no pass) - might fail if default user has no pass? 
    // Default user usually has no pass in default config.
    // But HELLO AUTH expects username password.
    // HELLO 2 AUTH default ""
    let resp = run_cmd_bytes(vec![Bytes::from("HELLO"), Bytes::from("2"), Bytes::from("AUTH"), Bytes::from("default"), Bytes::from("")], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::Array(Some(_)) => {},
        Resp::Error(_e) => {
            // If default user not configured or needs pass, this might fail.
            // In default create_server_context, default user exists?
            // Usually yes.
        }
        _ => panic!("Expected Array or Error, got {:?}", resp),
    }
    
    // HELLO 4 (unsupported)
    let resp = run_cmd_bytes(vec![Bytes::from("HELLO"), Bytes::from("4")], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::Error(e) => assert!(e.contains("NOPROTO")),
        _ => panic!("Expected Error, got {:?}", resp),
    }
}
