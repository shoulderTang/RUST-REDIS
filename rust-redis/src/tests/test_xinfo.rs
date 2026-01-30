use crate::resp::Resp;
use crate::tests::helper::run_cmd;

#[tokio::test]
async fn test_xinfo() {
    let server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();

    // Setup: XADD mystream 1-0 f1 v1, 2-0 f2 v2
    run_cmd(vec!["XADD", "mystream", "1-0", "f1", "v1"], &mut conn_ctx, &server_ctx).await;
    run_cmd(vec!["XADD", "mystream", "2-0", "f2", "v2"], &mut conn_ctx, &server_ctx).await;
    
    // Create group
    run_cmd(vec!["XGROUP", "CREATE", "mystream", "mygroup", "0-0"], &mut conn_ctx, &server_ctx).await;

    // 1. XINFO STREAM
    let res = run_cmd(vec!["XINFO", "STREAM", "mystream"], &mut conn_ctx, &server_ctx).await;
    if let Resp::Array(Some(arr)) = res {
        // Find length
        let mut length = 0;
        for i in (0..arr.len()).step_by(2) {
             if let Resp::SimpleString(s) = &arr[i] {
                 if s == "length" {
                     if let Resp::Integer(val) = arr[i+1] {
                         length = val;
                     }
                 }
             }
        }
        assert_eq!(length, 2);
    } else {
        panic!("Expected Array, got {:?}", res);
    }

    // 2. XINFO GROUPS
    let res = run_cmd(vec!["XINFO", "GROUPS", "mystream"], &mut conn_ctx, &server_ctx).await;
    if let Resp::Array(Some(arr)) = res {
        assert_eq!(arr.len(), 1);
        if let Resp::Array(Some(g_arr)) = &arr[0] {
             let mut name = String::new();
             for i in (0..g_arr.len()).step_by(2) {
                  if let Resp::SimpleString(s) = &g_arr[i] {
                      if s == "name" {
                          if let Resp::BulkString(Some(val)) = &g_arr[i+1] {
                              name = String::from_utf8_lossy(val).to_string();
                          }
                      }
                  }
             }
             assert_eq!(name, "mygroup");
        }
    } else {
        panic!("Expected Array");
    }

    // 3. XINFO CONSUMERS (empty)
    let res = run_cmd(vec!["XINFO", "CONSUMERS", "mystream", "mygroup"], &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::Array(Some(vec![])));
}
