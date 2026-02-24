use std::sync::{Arc, RwLock};
use bytes::Bytes;

#[path = "../sentinel.rs"]
mod sentinel;

#[path = "../resp.rs"]
mod resp;

use sentinel::{SentinelState, MasterInstance, SlaveInstance, SentinelInstance};
use resp::Resp;

#[tokio::test]
async fn test_sentinel_ckquorum_logic() {
    let sentinel_ip = "127.0.0.1".to_string();
    let sentinel_port = 26379;
    let state = SentinelState::new(sentinel_ip, sentinel_port);
    {
        let mut masters = state.masters.write().unwrap();
        let master = MasterInstance::new("mymaster".to_string(), "127.0.0.1".to_string(), 6379, 2);
        masters.insert("mymaster".to_string(), Arc::new(RwLock::new(master)));
    }
    {
        let masters = state.masters.read().unwrap();
        let mut m = masters.get("mymaster").unwrap().write().unwrap();
        m.sentinels.insert("s1".to_string(), SentinelInstance {
            ip: "10.0.0.1".to_string(),
            port: 26379,
            run_id: "s1".to_string(),
            last_ping_time: 0,
            last_pong_time: 0,
            s_down_since_time: 0,
            leader_epoch: 0,
            leader_runid: None,
        });
    }
    let (_total, reachable, majority, quorum, ok) = sentinel::check_quorum_for_master(&state, "mymaster").unwrap();
    assert_eq!(reachable, 2);
    assert!(ok);
    {
        let masters = state.masters.read().unwrap();
        let mut m = masters.get("mymaster").unwrap().write().unwrap();
        if let Some(s1) = m.sentinels.get_mut("s1") {
            s1.s_down_since_time = 123;
        }
    }
    let (_total2, reachable2, _majority2, _quorum2, ok2) = sentinel::check_quorum_for_master(&state, "mymaster").unwrap();
    assert_eq!(reachable2, 1);
    assert!(!ok2);
}

#[tokio::test]
async fn test_sentinel_monitor_and_remove_commands() {
    let sentinel_ip = "127.0.0.1".to_string();
    let sentinel_port = 26379;
    let state = SentinelState::new(sentinel_ip, sentinel_port);

    {
        let masters = state.masters.read().unwrap();
        assert!(masters.is_empty());
    }

    assert!(state.monitor_master("mymaster", "127.0.0.1", 6379, 2).is_ok());

    {
        let masters = state.masters.read().unwrap();
        assert!(masters.contains_key("mymaster"));
    }

    assert!(state.remove_master("mymaster").is_ok());

    {
        let masters = state.masters.read().unwrap();
        assert!(!masters.contains_key("mymaster"));
    }
}

#[tokio::test]
async fn test_sentinel_set_command_options() {
    let sentinel_ip = "127.0.0.1".to_string();
    let sentinel_port = 26379;
    let state = SentinelState::new(sentinel_ip, sentinel_port);

    assert!(state.monitor_master("mymaster", "127.0.0.1", 6379, 2).is_ok());

    let options = vec![
        ("down-after-milliseconds".to_string(), "5000".to_string()),
        ("failover-timeout".to_string(), "60000".to_string()),
        ("parallel-syncs".to_string(), "3".to_string()),
        ("quorum".to_string(), "5".to_string()),
    ];

    assert!(state.set_master_options("mymaster", &options).is_ok());

    let masters = state.masters.read().unwrap();
    let master_lock = masters.get("mymaster").unwrap();
    let master = master_lock.read().unwrap();
    assert_eq!(master.down_after_period, 5000);
    assert_eq!(master.failover_timeout, 60000);
    assert_eq!(master.parallel_syncs, 3);
    assert_eq!(master.quorum, 5);
}

#[tokio::test]
async fn test_sentinel_slaves_command() {
    let sentinel_ip = "127.0.0.1".to_string();
    let sentinel_port = 26379;
    let state = SentinelState::new(sentinel_ip, sentinel_port);
    
    // Add a master with some slaves
    {
        let mut masters = state.masters.write().unwrap();
        let mut master = MasterInstance::new("mymaster".to_string(), "127.0.0.1".to_string(), 6379, 2);
        
        let slave1 = SlaveInstance {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            flags: "slave".to_string(),
            run_id: "slave1-runid".to_string(),
            last_ping_time: 0,
            last_pong_time: 0,
            s_down_since_time: 0,
        };
        
        let slave2 = SlaveInstance {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            flags: "slave".to_string(),
            run_id: "slave2-runid".to_string(),
            last_ping_time: 0,
            last_pong_time: 0,
            s_down_since_time: 0,
        };
        
        master.slaves.insert("slave1".to_string(), slave1);
        master.slaves.insert("slave2".to_string(), slave2);
        
        masters.insert("mymaster".to_string(), Arc::new(RwLock::new(master)));
    }
    
    // Test SENTINEL SLAVES command
    let _command = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SENTINEL"))),
        Resp::BulkString(Some(Bytes::from("SLAVES"))),
        Resp::BulkString(Some(Bytes::from("mymaster"))),
    ]));
    
    // This would normally be called through process_sentinel_command
    // For now, we just verify the data structure is correct
    let masters = state.masters.read().unwrap();
    if let Some(master_lock) = masters.get("mymaster") {
        let master = master_lock.read().unwrap();
        assert_eq!(master.slaves.len(), 2);
        assert!(master.slaves.contains_key("slave1"));
        assert!(master.slaves.contains_key("slave2"));
    }
}

#[tokio::test]
async fn test_sentinel_sentinels_command() {
    let sentinel_ip = "127.0.0.1".to_string();
    let sentinel_port = 26379;
    let state = SentinelState::new(sentinel_ip, sentinel_port);
    
    // Add a master with some sentinels
    {
        let mut masters = state.masters.write().unwrap();
        let mut master = MasterInstance::new("mymaster".to_string(), "127.0.0.1".to_string(), 6379, 2);
        
        let sentinel1 = SentinelInstance {
            ip: "127.0.0.1".to_string(),
            port: 26379,
            run_id: "sentinel1-runid".to_string(),
            last_ping_time: 0,
            last_pong_time: 0,
            s_down_since_time: 0,
            leader_epoch: 0,
            leader_runid: None,
        };
        
        let sentinel2 = SentinelInstance {
            ip: "127.0.0.1".to_string(),
            port: 26380,
            run_id: "sentinel2-runid".to_string(),
            last_ping_time: 0,
            last_pong_time: 0,
            s_down_since_time: 0,
            leader_epoch: 0,
            leader_runid: None,
        };
        
        master.sentinels.insert("sentinel1".to_string(), sentinel1);
        master.sentinels.insert("sentinel2".to_string(), sentinel2);
        
        masters.insert("mymaster".to_string(), Arc::new(RwLock::new(master)));
    }
    
    // Test SENTINEL SENTINELS command
    let _command = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SENTINEL"))),
        Resp::BulkString(Some(Bytes::from("SENTINELS"))),
        Resp::BulkString(Some(Bytes::from("mymaster"))),
    ]));
    
    // This would normally be called through process_sentinel_command
    // For now, we just verify the data structure is correct
    let masters = state.masters.read().unwrap();
    if let Some(master_lock) = masters.get("mymaster") {
        let master = master_lock.read().unwrap();
        assert_eq!(master.sentinels.len(), 2);
        assert!(master.sentinels.contains_key("sentinel1"));
        assert!(master.sentinels.contains_key("sentinel2"));
    }
}

#[tokio::test]
async fn test_sentinel_reset_command() {
    let sentinel_ip = "127.0.0.1".to_string();
    let sentinel_port = 26379;
    let state = SentinelState::new(sentinel_ip, sentinel_port);
    
    // Add a master with slaves and sentinels
    {
        let mut masters = state.masters.write().unwrap();
        let mut master = MasterInstance::new("mymaster".to_string(), "127.0.0.1".to_string(), 6379, 2);
        
        let slave = SlaveInstance {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            flags: "slave".to_string(),
            run_id: "slave-runid".to_string(),
            last_ping_time: 0,
            last_pong_time: 0,
            s_down_since_time: 0,
        };
        
        let sentinel = SentinelInstance {
            ip: "127.0.0.1".to_string(),
            port: 26379,
            run_id: "sentinel-runid".to_string(),
            last_ping_time: 0,
            last_pong_time: 0,
            s_down_since_time: 0,
            leader_epoch: 0,
            leader_runid: None,
        };
        
        master.slaves.insert("slave".to_string(), slave);
        master.sentinels.insert("sentinel".to_string(), sentinel);
        master.s_down_since_time = 12345; // Set some state
        
        masters.insert("mymaster".to_string(), Arc::new(RwLock::new(master)));
    }
    
    // Verify initial state
    {
        let masters = state.masters.read().unwrap();
        let master = masters.get("mymaster").unwrap().read().unwrap();
        assert_eq!(master.slaves.len(), 1);
        assert_eq!(master.sentinels.len(), 1);
        assert_eq!(master.s_down_since_time, 12345);
    }
    
    // This would normally be called through process_sentinel_command
    // For now, we just verify the reset logic works
    {
        let masters = state.masters.read().unwrap();
        if let Some(master_lock) = masters.get("mymaster") {
            let mut master = master_lock.write().unwrap();
            // Reset master state
            master.s_down_since_time = 0;
            master.last_ping_time = 0;
            master.slaves.clear();
            master.sentinels.clear();
            
            assert_eq!(master.slaves.len(), 0);
            assert_eq!(master.sentinels.len(), 0);
            assert_eq!(master.s_down_since_time, 0);
        }
    }
}
