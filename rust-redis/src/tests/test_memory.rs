#[cfg(test)]
mod tests {
    use crate::tests::helper::{create_server_context, create_connection_context, run_cmd};
    use crate::resp::Resp;

    #[tokio::test]
    async fn test_memory_usage() {
        let server_ctx = create_server_context();
        let mut conn_ctx = create_connection_context();
        
        run_cmd(vec!["SET", "key", "value"], &mut conn_ctx, &server_ctx).await;

        let res = run_cmd(vec!["MEMORY", "USAGE", "key"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Integer(size) => assert!(size > 0),
            _ => panic!("Expected Integer, got {:?}", res),
        }

        let res_none = run_cmd(vec!["MEMORY", "USAGE", "nonexistent"], &mut conn_ctx, &server_ctx).await;
        assert_eq!(res_none, Resp::BulkString(None));
    }

    #[tokio::test]
    async fn test_memory_stats() {
        let server_ctx = create_server_context();
        let mut conn_ctx = create_connection_context();
        
        let res = run_cmd(vec!["MEMORY", "STATS"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Array(Some(stats)) => {
                assert!(stats.len() > 0);
                // Check if it's a list of key-value pairs
                assert_eq!(stats.len() % 2, 0);
            },
            _ => panic!("Expected Array, got {:?}", res),
        }
    }

    #[tokio::test]
    async fn test_memory_help() {
        let server_ctx = create_server_context();
        let mut conn_ctx = create_connection_context();
        
        let res = run_cmd(vec!["MEMORY", "HELP"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Array(Some(help)) => assert!(help.len() > 0),
            _ => panic!("Expected Array, got {:?}", res),
        }
    }
}
