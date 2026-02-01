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
async fn test_touch() {
    let server_ctx = create_server_context();
    let mut conn_ctx = create_connection_context();

    // 1. TOUCH non-existent keys -> 0
    let resp = run_cmd_bytes(vec![Bytes::from("TOUCH"), Bytes::from("key1"), Bytes::from("key2")], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::Integer(n) => assert_eq!(n, 0, "Expected 0 touched keys, got {}", n),
        _ => panic!("Expected Integer from TOUCH, got {:?}", resp),
    }

    // 2. SET key1
    let _ = run_cmd_bytes(vec![Bytes::from("SET"), Bytes::from("key1"), Bytes::from("value1")], &mut conn_ctx, &server_ctx).await;

    // 3. TOUCH key1 key2 -> 1
    let resp = run_cmd_bytes(vec![Bytes::from("TOUCH"), Bytes::from("key1"), Bytes::from("key2")], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::Integer(n) => assert_eq!(n, 1, "Expected 1 touched key, got {}", n),
        _ => panic!("Expected Integer from TOUCH, got {:?}", resp),
    }

    // 4. SET key2
    let _ = run_cmd_bytes(vec![Bytes::from("SET"), Bytes::from("key2"), Bytes::from("value2")], &mut conn_ctx, &server_ctx).await;

    // 5. TOUCH key1 key2 -> 2
    let resp = run_cmd_bytes(vec![Bytes::from("TOUCH"), Bytes::from("key1"), Bytes::from("key2")], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::Integer(n) => assert_eq!(n, 2, "Expected 2 touched keys, got {}", n),
        _ => panic!("Expected Integer from TOUCH, got {:?}", resp),
    }

    // 6. Verify COMMAND output contains "touch"
    let resp = run_cmd_bytes(vec![Bytes::from("COMMAND")], &mut conn_ctx, &server_ctx).await;
    match resp {
        Resp::Array(Some(cmds)) => {
             let mut found = false;
             for cmd in cmds {
                 if let Resp::Array(Some(info)) = cmd {
                     if let Some(Resp::SimpleString(name)) = info.get(0) {
                         if name == "touch" {
                             found = true;
                             break;
                         }
                     }
                 }
             }
             assert!(found, "COMMAND output should contain 'touch'");
             // Also check total count if you want, but might be fragile if other tests run in parallel or if I miscount.
             // Previous count was 174. +1 for TOUCH = 175.
        }
        _ => panic!("Expected Array from COMMAND, got {:?}", resp),
    }
}
