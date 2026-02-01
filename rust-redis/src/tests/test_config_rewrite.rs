use crate::cmd::{process_frame, ConnectionContext, ServerContext};
use crate::resp::Resp;
use crate::conf::Config;
use bytes::Bytes;
use std::sync::Arc;
use std::fs;

#[tokio::test]
async fn test_config_rewrite() {
    let mut server_ctx = crate::tests::helper::create_server_context();
    let mut conn_ctx = crate::tests::helper::create_connection_context();
    
    // We need a temp config file
    let temp_file = "temp_redis.conf";
    // Ensure it doesn't exist
    let _ = std::fs::remove_file(temp_file);
    
    // Create a dummy config with this file path
    let mut config = Config::default();
    config.config_file = Some(temp_file.to_string());
    config.port = 12345;
    config.bind = "127.0.0.1".to_string();
    server_ctx.config = Arc::new(config);

    // Run CONFIG REWRITE
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("CONFIG"))),
        Resp::BulkString(Some(Bytes::from("REWRITE"))),
    ]));
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    assert_eq!(res, Resp::SimpleString(Bytes::from("OK")));

    // Verify file exists and content
    assert!(std::path::Path::new(temp_file).exists());
    let content = fs::read_to_string(temp_file).unwrap();
    assert!(content.contains("port 12345"));
    assert!(content.contains("bind 127.0.0.1"));
    
    // Clean up
    let _ = std::fs::remove_file(temp_file);
}
