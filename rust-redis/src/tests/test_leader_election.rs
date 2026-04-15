#[path = "../sentinel.rs"]
mod sentinel;

use sentinel::{MasterInstance, SentinelInstance, SentinelState};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn test_leader_election_basic() {
    let sentinel_ip = "127.0.0.1".to_string();
    let sentinel_port = 26379;
    let state = SentinelState::new(sentinel_ip, sentinel_port);

    let master_name = "mymaster".to_string();
    let mut master = MasterInstance::new(master_name.clone(), "127.0.0.1".to_string(), 6379, 2);

    // Add some healthy sentinels
    let sentinel1 = SentinelInstance {
        ip: "127.0.0.1".to_string(),
        port: 26379,
        run_id: "sentinel1".to_string(),
        last_ping_time: 0,
        last_pong_time: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        s_down_since_time: 0,
        leader_epoch: 0,
        leader_runid: None,
    };

    let sentinel2 = SentinelInstance {
        ip: "127.0.0.1".to_string(),
        port: 26380,
        run_id: "sentinel2".to_string(),
        last_ping_time: 0,
        last_pong_time: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        s_down_since_time: 0,
        leader_epoch: 0,
        leader_runid: None,
    };

    master.sentinels.insert("sentinel1".to_string(), sentinel1);
    master.sentinels.insert("sentinel2".to_string(), sentinel2);

    {
        let mut masters = state.masters.write().unwrap();
        masters.insert(master_name.clone(), Arc::new(RwLock::new(master)));
    }

    // Test leader election
    let result = sentinel::elect_leader_for_master(&state, &master_name).await;
    assert!(result.is_ok());
    assert!(result.unwrap());

    // Verify we are now the leader
    let masters = state.masters.read().unwrap();
    if let Some(master_lock) = masters.get(&master_name) {
        let master = master_lock.read().unwrap();
        assert_eq!(master.current_leader_runid, Some(state.run_id.clone()));
        assert!(master.current_leader_epoch > 0);
    }
}

#[tokio::test]
async fn test_leader_election_insufficient_quorum() {
    let sentinel_ip = "127.0.0.1".to_string();
    let sentinel_port = 26379;
    let state = SentinelState::new(sentinel_ip, sentinel_port);

    let master_name = "mymaster".to_string();
    let mut master = MasterInstance::new(master_name.clone(), "127.0.0.1".to_string(), 6379, 3); // Higher quorum

    // Add only one healthy sentinel (not enough for quorum of 3)
    let sentinel1 = SentinelInstance {
        ip: "127.0.0.1".to_string(),
        port: 26379,
        run_id: "sentinel1".to_string(),
        last_ping_time: 0,
        last_pong_time: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        s_down_since_time: 0,
        leader_epoch: 0,
        leader_runid: None,
    };

    master.sentinels.insert("sentinel1".to_string(), sentinel1);

    {
        let mut masters = state.masters.write().unwrap();
        masters.insert(master_name.clone(), Arc::new(RwLock::new(master)));
    }

    // Test leader election - should fail due to insufficient quorum
    let result = sentinel::elect_leader_for_master(&state, &master_name).await;
    assert!(result.is_ok());
    assert!(!result.unwrap()); // Should return false (not elected)

    // Verify we are not the leader
    let masters = state.masters.read().unwrap();
    if let Some(master_lock) = masters.get(&master_name) {
        let master = master_lock.read().unwrap();
        assert_eq!(master.current_leader_runid, None);
        assert_eq!(master.current_leader_epoch, 0);
    }
}
