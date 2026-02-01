#[cfg(test)]
mod tests {
    use crate::tests::helper::{create_server_context, create_connection_context, run_cmd};
    use crate::resp::Resp;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_copy() {
        let server_ctx = create_server_context();
        let mut conn_ctx = create_connection_context();
        
        run_cmd(vec!["SET", "k1", "v1"], &mut conn_ctx, &server_ctx).await;
        
        // Basic copy
        let res = run_cmd(vec!["COPY", "k1", "k2"], &mut conn_ctx, &server_ctx).await;
        assert_eq!(res, Resp::Integer(1));
        assert_eq!(run_cmd(vec!["GET", "k2"], &mut conn_ctx, &server_ctx).await, Resp::BulkString(Some(Bytes::from("v1"))));
        
        // Copy to another DB
        let res = run_cmd(vec!["COPY", "k1", "k3", "DB", "1"], &mut conn_ctx, &server_ctx).await;
        assert_eq!(res, Resp::Integer(1));
        
        // Check DB 1
        conn_ctx.db_index = 1;
        assert_eq!(run_cmd(vec!["GET", "k3"], &mut conn_ctx, &server_ctx).await, Resp::BulkString(Some(Bytes::from("v1"))));
    }

    #[tokio::test]
    async fn test_object() {
        let server_ctx = create_server_context();
        let mut conn_ctx = create_connection_context();
        
        run_cmd(vec!["SET", "k1", "v1"], &mut conn_ctx, &server_ctx).await;
        
        let res = run_cmd(vec!["OBJECT", "ENCODING", "k1"], &mut conn_ctx, &server_ctx).await;
        assert_eq!(res, Resp::BulkString(Some(Bytes::from("raw"))));
        
        let res = run_cmd(vec!["OBJECT", "IDLETIME", "k1"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Integer(_) => {},
            _ => panic!("Expected Integer, got {:?}", res),
        }
    }

    #[tokio::test]
    async fn test_smismember() {
        let server_ctx = create_server_context();
        let mut conn_ctx = create_connection_context();
        
        run_cmd(vec!["SADD", "myset", "m1", "m2"], &mut conn_ctx, &server_ctx).await;
        
        let res = run_cmd(vec!["SMISMEMBER", "myset", "m1", "m3", "m2"], &mut conn_ctx, &server_ctx).await;
        assert_eq!(res, Resp::Array(Some(vec![
            Resp::Integer(1),
            Resp::Integer(0),
            Resp::Integer(1),
        ])));
    }

    #[tokio::test]
    async fn test_zmscore() {
        let server_ctx = create_server_context();
        let mut conn_ctx = create_connection_context();
        
        run_cmd(vec!["ZADD", "myzset", "1", "m1", "2", "m2"], &mut conn_ctx, &server_ctx).await;
        
        let res = run_cmd(vec!["ZMSCORE", "myzset", "m1", "m3", "m2"], &mut conn_ctx, &server_ctx).await;
        assert_eq!(res, Resp::Array(Some(vec![
            Resp::BulkString(Some(Bytes::from("1"))),
            Resp::BulkString(None),
            Resp::BulkString(Some(Bytes::from("2"))),
        ])));
    }

    #[tokio::test]
    async fn test_server_cmds() {
        let server_ctx = create_server_context();
        let mut conn_ctx = create_connection_context();
        
        // TIME
        let res = run_cmd(vec!["TIME"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Array(Some(arr)) => assert_eq!(arr.len(), 2),
            _ => panic!("Expected Array(2), got {:?}", res),
        }
        
        // ROLE
        let res = run_cmd(vec!["ROLE"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Array(Some(arr)) => assert_eq!(arr[0], Resp::BulkString(Some(Bytes::from("master")))),
            _ => panic!("Expected master role, got {:?}", res),
        }
        
        // LASTSAVE
        let res = run_cmd(vec!["LASTSAVE"], &mut conn_ctx, &server_ctx).await;
        match res {
            Resp::Integer(_) => {},
            _ => panic!("Expected Integer, got {:?}", res),
        }
    }
}
