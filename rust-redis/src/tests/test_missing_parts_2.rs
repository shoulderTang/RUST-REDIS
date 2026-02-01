#[cfg(test)]
mod tests {
    use crate::tests::helper::{create_server_context, create_connection_context, run_cmd};
    use crate::resp::Resp;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_client_tracking() {
        let server_ctx = create_server_context();
        let mut conn_ctx = create_connection_context();
        
        // Enable tracking
        let res = run_cmd(vec!["CLIENT", "TRACKING", "ON"], &mut conn_ctx, &server_ctx).await;
        assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));
        
        // Read a key to start tracking
        run_cmd(vec!["GET", "mykey"], &mut conn_ctx, &server_ctx).await;
        
        // Verify it's in tracking_clients
        assert!(server_ctx.tracking_clients.contains_key(&(0, b"mykey".to_vec())));
        
        // Modify the key from another connection
        let mut conn_ctx2 = create_connection_context();
        conn_ctx2.id = 1;
        run_cmd(vec!["SET", "mykey", "val"], &mut conn_ctx2, &server_ctx).await;
        
        // Verify it's removed from tracking_clients (after invalidation)
        assert!(!server_ctx.tracking_clients.contains_key(&(0, b"mykey".to_vec())));
    }

    #[tokio::test]
    async fn test_acl_ext() {
        let server_ctx = create_server_context();
        let mut conn_ctx = create_connection_context();
        
        // ACL DRYRUN
        let res = run_cmd(vec!["ACL", "DRYRUN", "default", "GET", "k1"], &mut conn_ctx, &server_ctx).await;
        assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));
        
        // ACL LOG
        let res = run_cmd(vec!["ACL", "LOG"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Array(Some(arr)) => assert_eq!(arr.len(), 0),
            _ => panic!("Expected Array, got {:?}", res),
        }
        
        // Trigger ACL failure to record log
        run_cmd(vec!["ACL", "SETUSER", "testuser", "off", "+get", "~*"], &mut conn_ctx, &server_ctx).await;
        let mut conn_ctx_test = create_connection_context();
        conn_ctx_test.current_username = "testuser".to_string();
        conn_ctx_test.authenticated = true;
        conn_ctx_test.id = 2;
        
        // This should fail and be logged
        run_cmd(vec!["SET", "k1", "v1"], &mut conn_ctx_test, &server_ctx).await;
        
        let res = run_cmd(vec!["ACL", "LOG"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Array(Some(arr)) => assert_eq!(arr.len(), 1),
            _ => panic!("Expected Array(1), got {:?}", res),
        }
    }

    #[tokio::test]
    async fn test_latency() {
        let server_ctx = create_server_context();
        let mut conn_ctx = create_connection_context();
        
        // Initial LATEST should be empty
        let res = run_cmd(vec!["LATENCY", "LATEST"], &mut conn_ctx, &server_ctx).await;
        assert_eq!(res, Resp::Array(Some(Vec::new())));
        
        // Record some latency
        crate::cmd::latency::record_latency(&server_ctx, "command", 10);
        
        let res = run_cmd(vec!["LATENCY", "LATEST"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Array(Some(arr)) => assert_eq!(arr.len(), 1),
            _ => panic!("Expected Array(1), got {:?}", res),
        }
    }
}
