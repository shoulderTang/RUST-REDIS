#[path = "../sentinel.rs"]
mod sentinel;

use std::sync::{Arc, RwLock};
use sentinel::{SentinelState, MasterInstance};

#[tokio::test]
async fn test_handle_sentinel_hello_message_adds_sentinel() {
    let state = SentinelState::new("127.0.0.1".to_string(), 26379);
    {
        let mut masters = state.masters.write().unwrap();
        let master = MasterInstance::new("mymaster".to_string(), "127.0.0.1".to_string(), 6379, 2);
        masters.insert("mymaster".to_string(), Arc::new(RwLock::new(master)));
    }
    let msg = "10.0.0.2,26379,run123,7,mymaster,127.0.0.1,6379,3";
    sentinel::handle_sentinel_hello_message(&state.masters, "mymaster", msg);
    let masters = state.masters.read().unwrap();
    let master = masters.get("mymaster").unwrap().read().unwrap();
    assert!(master.sentinels.contains_key("run123"));
    let s = master.sentinels.get("run123").unwrap();
    assert_eq!(s.ip, "10.0.0.2");
    assert_eq!(s.port, 26379);
    assert_eq!(s.leader_epoch, 7);
}

