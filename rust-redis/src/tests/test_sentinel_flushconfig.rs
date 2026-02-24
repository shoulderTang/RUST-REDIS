#[path = "../sentinel.rs"]
mod sentinel;

use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn test_sentinel_flushconfig_writes_expected_lines() {
    let state = sentinel::SentinelState::new("127.0.0.1".to_string(), 26379);
    let _ = state.monitor_master("mymaster", "127.0.0.1", 6379, 2);
    let _ = state.set_master_options("mymaster", &[
        ("down-after-milliseconds".to_string(), "5000".to_string()),
        ("failover-timeout".to_string(), "60000".to_string()),
        ("parallel-syncs".to_string(), "3".to_string()),
        ("quorum".to_string(), "4".to_string()),
    ]);

    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    let path = std::env::temp_dir().join(format!("sentinel_flushconfig_{}.conf", ts));
    let path_str = path.to_string_lossy().to_string();
    let res = state.write_config_to_path(&path_str);
    assert!(res.is_ok());

    let content = std::fs::read_to_string(&path_str).unwrap();
    assert!(content.contains("sentinel monitor mymaster 127.0.0.1 6379 2"));
    assert!(content.contains("sentinel down-after-milliseconds mymaster 5000"));
    assert!(content.contains("sentinel failover-timeout mymaster 60000"));
    assert!(content.contains("sentinel parallel-syncs mymaster 3"));
    assert!(!content.is_empty());

    let _ = std::fs::remove_file(&path_str);
}
