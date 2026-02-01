#[cfg(test)]
mod tests {
    use crate::tests::helper::{create_server_context, create_connection_context, run_cmd};
    use crate::resp::Resp;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_xadd_nomkstream() {
        let server_ctx = create_server_context();
        let mut conn_ctx = create_connection_context();
        
        // XADD with NOMKSTREAM on non-existent key
        let res = run_cmd(vec!["XADD", "mystream", "NOMKSTREAM", "*", "f1", "v1"], &mut conn_ctx, &server_ctx).await;
        assert_eq!(res, Resp::BulkString(None));
        
        // Create stream first
        run_cmd(vec!["XADD", "mystream", "*", "f1", "v1"], &mut conn_ctx, &server_ctx).await;
        
        // XADD with NOMKSTREAM on existent key
        let res = run_cmd(vec!["XADD", "mystream", "NOMKSTREAM", "*", "f2", "v2"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::BulkString(Some(_)) => {},
            _ => panic!("Expected BulkString ID, got {:?}", res),
        }
    }

    #[tokio::test]
    async fn test_xgroup_createconsumer() {
        let server_ctx = create_server_context();
        let mut conn_ctx = create_connection_context();
        
        run_cmd(vec!["XADD", "mystream", "*", "f1", "v1"], &mut conn_ctx, &server_ctx).await;
        run_cmd(vec!["XGROUP", "CREATE", "mystream", "mygroup", "$"], &mut conn_ctx, &server_ctx).await;
        
        // Create consumer
        let res = run_cmd(vec!["XGROUP", "CREATECONSUMER", "mystream", "mygroup", "myconsumer"], &mut conn_ctx, &server_ctx).await;
        assert_eq!(res, Resp::Integer(1));
        
        // Duplicate consumer
        let res = run_cmd(vec!["XGROUP", "CREATECONSUMER", "mystream", "mygroup", "myconsumer"], &mut conn_ctx, &server_ctx).await;
        assert_eq!(res, Resp::Integer(0));
        
        // Check info
        let res = run_cmd(vec!["XINFO", "CONSUMERS", "mystream", "mygroup"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Array(Some(consumers)) => {
                assert_eq!(consumers.len(), 1);
            },
            _ => panic!("Expected Array of consumers, got {:?}", res),
        }
    }

    #[tokio::test]
    async fn test_command_subcommands() {
        let server_ctx = create_server_context();
        let mut conn_ctx = create_connection_context();
        
        // COMMAND COUNT
        let res = run_cmd(vec!["COMMAND", "COUNT"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Integer(n) => assert!(n > 100),
            _ => panic!("Expected Integer count, got {:?}", res),
        }
        
        // COMMAND INFO
        let res = run_cmd(vec!["COMMAND", "INFO", "GET", "SET"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Array(Some(arr)) => {
                assert_eq!(arr.len(), 2);
            },
            _ => panic!("Expected Array(2), got {:?}", res),
        }
        
        // COMMAND GETKEYS
        let res = run_cmd(vec!["COMMAND", "GETKEYS", "MGET", "k1", "k2", "k3"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Array(Some(arr)) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], Resp::BulkString(Some(Bytes::from("k1"))));
                assert_eq!(arr[1], Resp::BulkString(Some(Bytes::from("k2"))));
                assert_eq!(arr[2], Resp::BulkString(Some(Bytes::from("k3"))));
            },
            _ => panic!("Expected Array of keys, got {:?}", res),
        }
    }
}
