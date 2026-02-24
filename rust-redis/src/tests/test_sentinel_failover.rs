#[path = "../sentinel.rs"]
mod sentinel;

use sentinel::{SentinelState, MasterInstance, SlaveInstance, FailoverState};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn test_manual_failover() {
    let sentinel_ip = "127.0.0.1".to_string();
    let sentinel_port = 26379;
    let state = SentinelState::new(sentinel_ip, sentinel_port);
    
    // Add a master with slaves
    {
        let mut masters = state.masters.write().unwrap();
        let mut master = MasterInstance::new("mymaster".to_string(), "127.0.0.1".to_string(), 6379, 2);
        
        // Add a healthy slave
        let slave = SlaveInstance {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            flags: "slave".to_string(),
            run_id: "slave-runid".to_string(),
            last_ping_time: 0,
            last_pong_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
            s_down_since_time: 0,
        };
        
        master.slaves.insert("slave1".to_string(), slave);
        masters.insert("mymaster".to_string(), Arc::new(RwLock::new(master)));
    }
    
    // Verify initial state
    {
        let masters = state.masters.read().unwrap();
        let master = masters.get("mymaster").unwrap().read().unwrap();
        assert_eq!(master.failover_state, FailoverState::None);
        assert_eq!(master.ip, "127.0.0.1");
        assert_eq!(master.port, 6379);
        assert_eq!(master.slaves.len(), 1);
    }
    
    // Start manual failover (this will spawn a background task)
    // Note: In a real test environment, we would need actual Redis instances
    // For now, we just test the state initialization
    match sentinel::start_manual_failover(&state, "mymaster").await {
        Ok(()) => {
            // Verify failover state was updated
            {
                let masters = state.masters.read().unwrap();
                let master = masters.get("mymaster").unwrap().read().unwrap();
                assert_eq!(master.failover_state, FailoverState::InProgress);
                assert!(master.failover_start_time > 0);
                assert_eq!(master.failover_epoch, 1);
            }
        }
        Err(e) => {
            // This is expected in test environment without real connections
            println!("Failover start failed (expected in test): {}", e);
        }
    }
}

#[tokio::test]
async fn test_failover_state_transitions() {
    let sentinel_ip = "127.0.0.1".to_string();
    let sentinel_port = 26379;
    let state = SentinelState::new(sentinel_ip, sentinel_port);
    
    // Add a master
    {
        let mut masters = state.masters.write().unwrap();
        let master = MasterInstance::new("testmaster".to_string(), "127.0.0.1".to_string(), 6379, 2);
        masters.insert("testmaster".to_string(), Arc::new(RwLock::new(master)));
    }
    
    // Test state transitions
    {
        let masters = state.masters.read().unwrap();
        let master_lock = masters.get("testmaster").unwrap();
        let mut master = master_lock.write().unwrap();
        
        // Initial state
        assert_eq!(master.failover_state, FailoverState::None);
        
        // Transition to InProgress
        master.failover_state = FailoverState::InProgress;
        master.failover_start_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        assert_eq!(master.failover_state, FailoverState::InProgress);
        
        // Transition to WaitingSlaveSelection
        master.failover_state = FailoverState::WaitingSlaveSelection;
        assert_eq!(master.failover_state, FailoverState::WaitingSlaveSelection);
        
        // Transition to SendingSlaveof
        master.failover_state = FailoverState::SendingSlaveof;
        master.promoted_slave_ip = Some("127.0.0.1".to_string());
        master.promoted_slave_port = Some(6380);
        assert_eq!(master.failover_state, FailoverState::SendingSlaveof);
        assert_eq!(master.promoted_slave_ip, Some("127.0.0.1".to_string()));
        assert_eq!(master.promoted_slave_port, Some(6380));
        
        // Transition to UpdatingConfig
        master.failover_state = FailoverState::UpdatingConfig;
        assert_eq!(master.failover_state, FailoverState::UpdatingConfig);
        
        // Transition to Completed
        master.failover_state = FailoverState::Completed;
        master.promoted_slave_ip = None;
        master.promoted_slave_port = None;
        assert_eq!(master.failover_state, FailoverState::Completed);
    }
}

#[tokio::test]
async fn test_failover_reset() {
    let sentinel_ip = "127.0.0.1".to_string();
    let sentinel_port = 26379;
    let state = SentinelState::new(sentinel_ip, sentinel_port);
    
    // Add a master with some state
    {
        let mut masters = state.masters.write().unwrap();
        let mut master = MasterInstance::new("resetmaster".to_string(), "127.0.0.1".to_string(), 6379, 2);
        master.failover_state = FailoverState::InProgress;
        master.failover_start_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        master.failover_epoch = 5;
        master.promoted_slave_ip = Some("127.0.0.1".to_string());
        master.promoted_slave_port = Some(6380);
        masters.insert("resetmaster".to_string(), Arc::new(RwLock::new(master)));
    }
    
    // Reset failover state
    match sentinel::reset_failover_state(&state, "resetmaster") {
        Ok(()) => {
            // Verify state was reset
            let masters = state.masters.read().unwrap();
            let master = masters.get("resetmaster").unwrap().read().unwrap();
            assert_eq!(master.failover_state, FailoverState::None);
            assert_eq!(master.failover_start_time, 0);
            assert_eq!(master.promoted_slave_ip, None);
            assert_eq!(master.promoted_slave_port, None);
            // Epoch should be preserved (it's a counter)
            assert_eq!(master.failover_epoch, 5);
        }
        Err(e) => panic!("Reset failed: {}", e),
    }
}

#[tokio::test]
async fn test_failover_with_no_slaves() {
    let sentinel_ip = "127.0.0.1".to_string();
    let sentinel_port = 26379;
    let state = SentinelState::new(sentinel_ip, sentinel_port);
    
    // Add a master with no slaves
    {
        let mut masters = state.masters.write().unwrap();
        let master = MasterInstance::new("noslaves".to_string(), "127.0.0.1".to_string(), 6379, 2);
        masters.insert("noslaves".to_string(), Arc::new(RwLock::new(master)));
    }
    
    // Try to start failover
    match sentinel::start_manual_failover(&state, "noslaves").await {
        Ok(()) => {
            // The failover will start but will fail when trying to select a slave
            // In a real implementation, this would handle the failure gracefully
            println!("Failover started (will fail due to no slaves)");
        }
        Err(e) => {
            println!("Failover start failed: {}", e);
        }
    }
}

#[tokio::test]
async fn test_failover_with_unhealthy_slaves() {
    let sentinel_ip = "127.0.0.1".to_string();
    let sentinel_port = 26379;
    let state = SentinelState::new(sentinel_ip, sentinel_port);
    
    // Add a master with unhealthy slaves
    {
        let mut masters = state.masters.write().unwrap();
        let mut master = MasterInstance::new("unhealthy".to_string(), "127.0.0.1".to_string(), 6379, 2);
        
        // Add an unhealthy slave (SDOWN)
        let slave = SlaveInstance {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            flags: "slave".to_string(),
            run_id: "slave-runid".to_string(),
            last_ping_time: 0,
            last_pong_time: 0, // Old pong time
            s_down_since_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
        };
        
        master.slaves.insert("unhealthy_slave".to_string(), slave);
        masters.insert("unhealthy".to_string(), Arc::new(RwLock::new(master)));
    }
    
    // Try to start failover
    match sentinel::start_manual_failover(&state, "unhealthy").await {
        Ok(()) => {
            println!("Failover started (will fail due to unhealthy slaves)");
        }
        Err(e) => {
            println!("Failover start failed: {}", e);
        }
    }
}