use crate::resp::Resp;
use crate::tests::helper::{create_connection_context, create_server_context, run_cmd};
use bytes::Bytes;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn test_psync2_transition() {
    let ctx = create_server_context();
    let mut conn_ctx = create_connection_context();

    // 1. Set initial state (as if it was a slave of someone)
    {
        let mut run_id = ctx.repl.run_id.write().unwrap();
        *run_id = "1111111111111111111111111111111111111111".to_string();
    }
    ctx.repl.repl_offset.store(100, Ordering::Relaxed);
    {
        // Populate backlog
        let mut q = ctx.repl.repl_backlog.lock().await;
        // Add some dummy frames from offset 50 to 100
        for i in 50..=100 {
            q.push_back((i, Resp::SimpleString(Bytes::from("PING"))));
        }
    }

    // 2. Promote to Master (Shift ID)
    let res = run_cmd(vec!["REPLICAOF", "NO", "ONE"], &mut conn_ctx, &ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    // 3. Verify IDs
    let old_id = "1111111111111111111111111111111111111111".to_string();
    let new_id = ctx.repl.run_id.read().unwrap().clone();
    let replid2 = ctx.repl.replid2.read().unwrap().clone();
    let second_off = ctx.repl.second_repl_offset.load(Ordering::Relaxed);

    assert_ne!(new_id, old_id);
    assert_eq!(replid2, old_id);
    // repl_offset was 100, so second_off should be 101
    assert_eq!(second_off, 101);

    // 4. Simulate a Slave asking for old ID
    // Case A: Valid PSYNC2 request (offset inside backlog and <= second_off)
    // Offset 80 is in backlog (50..100) and <= 101.
    let res = run_cmd(vec!["PSYNC", &old_id, "80"], &mut conn_ctx, &ctx).await;

    match res {
        Resp::SimpleString(s) => {
            let s_str = String::from_utf8_lossy(&s);
            assert!(s_str.starts_with("CONTINUE"));
            assert!(s_str.contains(&new_id));
        }
        _ => panic!("Expected SimpleString CONTINUE, got {:?}", res),
    }

    // Case B: Invalid PSYNC2 request (offset > second_off)
    // Offset 102 is > 101. Should trigger FULLRESYNC.
    let res = run_cmd(vec!["PSYNC", &old_id, "102"], &mut conn_ctx, &ctx).await;
    match res {
        Resp::Multiple(resps) => {
            if let Resp::SimpleString(s) = &resps[0] {
                let s_str = String::from_utf8_lossy(&s);
                assert!(s_str.starts_with("FULLRESYNC"));
            } else {
                panic!(
                    "Expected SimpleString FULLRESYNC header, got {:?}",
                    resps[0]
                );
            }
        }
        _ => panic!("Expected Multiple (FULLRESYNC + RDB), got {:?}", res),
    }
}
