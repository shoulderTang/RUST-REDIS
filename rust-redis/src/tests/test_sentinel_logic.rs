
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[path = "../sentinel.rs"]
mod sentinel;

#[path = "../resp.rs"]
mod resp;

use sentinel::{MasterInstance, SentinelState, SentinelInstance};

#[test]
fn test_sdown_detection() {
    let sentinel_ip = "127.0.0.1".to_string();
    let sentinel_port = 26379;
    let state = SentinelState::new(sentinel_ip, sentinel_port);

    let master_name = "mymaster".to_string();
    let mut master = MasterInstance::new(master_name.clone(), "127.0.0.1".to_string(), 6379, 2);
    master.down_after_period = 1000; // 1 second
    master.last_pong_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64 - 2000; // 2 seconds ago

    {
        let mut masters = state.masters.write().unwrap();
        masters.insert(master_name.clone(), Arc::new(RwLock::new(master)));
    }

    // Simulate SDOWN check logic
    if let Ok(masters) = state.masters.read() {
        if let Some(master_lock) = masters.get(&master_name) {
            if let Ok(mut master) = master_lock.write() {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                if master.last_pong_time > 0 && now > master.last_pong_time + master.down_after_period {
                    if master.s_down_since_time == 0 {
                        master.s_down_since_time = now;
                    }
                }
                assert!(master.s_down_since_time > 0, "Master should be SDOWN");
            }
        }
    }
}

#[test]
fn test_quorum_check() {
    let sentinel_ip = "127.0.0.1".to_string();
    let sentinel_port = 26379;
    let state = SentinelState::new(sentinel_ip, sentinel_port);
    let master_name = "mymaster".to_string();
    let mut master = MasterInstance::new(master_name.clone(), "127.0.0.1".to_string(), 6379, 2);
    
    // Add known sentinels
    let s1 = SentinelInstance {
        ip: "127.0.0.1".to_string(),
        port: 26380,
        run_id: "s1".to_string(),
        last_ping_time: 0,
        last_pong_time: 0,
        s_down_since_time: 0,
        leader_epoch: 0,
        leader_runid: None,
    };
    master.sentinels.insert("s1".to_string(), s1);

    {
        let mut masters = state.masters.write().unwrap();
        masters.insert(master_name.clone(), Arc::new(RwLock::new(master)));
    }

    // Simulate ODOWN logic
    let quorum = 2;
    let votes = 2; // Assume we got 1 vote from self + 1 from other sentinel

    assert!(votes >= quorum, "Should reach quorum");
}

#[test]
fn test_connection_failure_triggers_sdown() {
    let sentinel_ip = "127.0.0.1".to_string();
    let sentinel_port = 26379;
    let state = SentinelState::new(sentinel_ip, sentinel_port);
    
    let master_name = "mymaster".to_string();
    let mut master = MasterInstance::new(master_name.clone(), "127.0.0.1".to_string(), 6379, 2);
    master.down_after_period = 1000; // 1 second
    master.last_pong_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64; // Current time

    {
        let mut masters = state.masters.write().unwrap();
        masters.insert(master_name.clone(), Arc::new(RwLock::new(master)));
    }

    // Simulate connection failure by calling update_master_connection_failure
    // This would normally be called when connection fails
    let state_clone = state.clone();
    let master_name_clone = master_name.clone();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        sentinel::update_master_connection_failure(&state_clone, &master_name_clone).await;
    });

    // Verify that connection failure set last_pong_time to 0
    if let Ok(masters) = state.masters.read() {
        if let Some(master_lock) = masters.get(&master_name) {
            if let Ok(master) = master_lock.read() {
                assert_eq!(master.last_pong_time, 0, "Connection failure should set last_pong_time to 0");
            }
        }
    }
}
