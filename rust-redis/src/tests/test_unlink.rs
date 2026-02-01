#[cfg(test)]
mod tests {
    use crate::tests::helper::{create_server_context, create_connection_context, run_cmd};
    use crate::resp::Resp;

    #[tokio::test]
    async fn test_unlink_basic() {
        let server_ctx = create_server_context();
        let mut conn_ctx = create_connection_context();
        
        // Set some keys
        run_cmd(vec!["SET", "k1", "v1"], &mut conn_ctx, &server_ctx).await;
        run_cmd(vec!["SET", "k2", "v2"], &mut conn_ctx, &server_ctx).await;
        run_cmd(vec!["SET", "k3", "v3"], &mut conn_ctx, &server_ctx).await;

        // Unlink 2 keys
        let res = run_cmd(vec!["UNLINK", "k1", "k2", "nonexistent"], &mut conn_ctx, &server_ctx).await;
        assert_eq!(res, Resp::Integer(2));

        // Check if unlinked keys are gone
        assert_eq!(run_cmd(vec!["EXISTS", "k1"], &mut conn_ctx, &server_ctx).await, Resp::Integer(0));
        assert_eq!(run_cmd(vec!["EXISTS", "k2"], &mut conn_ctx, &server_ctx).await, Resp::Integer(0));
        assert_eq!(run_cmd(vec!["EXISTS", "k3"], &mut conn_ctx, &server_ctx).await, Resp::Integer(1));
    }

    #[tokio::test]
    async fn test_unlink_wrong_arity() {
        let server_ctx = create_server_context();
        let mut conn_ctx = create_connection_context();
        
        let res = run_cmd(vec!["UNLINK"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Error(e) => assert!(e.contains("wrong number of arguments")),
            _ => panic!("Expected error, got {:?}", res),
        }
    }
}
