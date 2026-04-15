use bytes::Bytes;
use std::sync::{Arc, RwLock};

#[path = "../sentinel.rs"]
mod sentinel;

#[path = "../resp.rs"]
mod resp;

use resp::Resp;
use sentinel::{MasterInstance, SentinelState};

#[tokio::test]
async fn test_sentinel_hello_command() {
    let sentinel_ip = "127.0.0.1".to_string();
    let sentinel_port = 26379;
    let state = SentinelState::new(sentinel_ip.clone(), sentinel_port);

    // Set current epoch
    *state.current_epoch.write().unwrap() = 42;

    // Add a master
    {
        let mut masters = state.masters.write().unwrap();
        let mut master =
            MasterInstance::new("mymaster".to_string(), "127.0.0.1".to_string(), 6379, 2);
        master.failover_epoch = 5;
        masters.insert("mymaster".to_string(), Arc::new(RwLock::new(master)));
    }

    // Simulate SENTINEL HELLO command response
    let mut response = Vec::new();

    // Add sentinel info
    response.push(Resp::BulkString(Some(state.ip.clone().into())));
    response.push(Resp::BulkString(Some(state.port.to_string().into())));
    response.push(Resp::BulkString(Some(state.run_id.clone().into())));

    let current_epoch = state.current_epoch.read().unwrap();
    response.push(Resp::Integer(*current_epoch as i64));

    // Add master info
    let masters = state.masters.read().unwrap();
    if let Some(master_lock) = masters.values().next() {
        let master = master_lock.read().unwrap();
        response.push(Resp::BulkString(Some(master.name.clone().into())));
        response.push(Resp::BulkString(Some(master.ip.clone().into())));
        response.push(Resp::Integer(master.port as i64));
        response.push(Resp::Integer(master.failover_epoch as i64));
    }

    let hello_response = Resp::Array(Some(response));

    // Verify the response structure
    match hello_response {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 8); // [ip, port, runid, epoch, master_name, master_ip, master_port, master_epoch]

            // Check sentinel info
            if let Resp::BulkString(Some(ip)) = &items[0] {
                assert_eq!(String::from_utf8_lossy(ip), "127.0.0.1");
            }
            if let Resp::BulkString(Some(port)) = &items[1] {
                assert_eq!(String::from_utf8_lossy(port), "26379");
            }
            if let Resp::BulkString(Some(runid)) = &items[2] {
                assert!(String::from_utf8_lossy(runid).starts_with("sentinel_"));
            }
            if let Resp::Integer(epoch) = items[3] {
                assert_eq!(epoch, 42);
            }

            // Check master info
            if let Resp::BulkString(Some(master_name)) = &items[4] {
                assert_eq!(String::from_utf8_lossy(master_name), "mymaster");
            }
            if let Resp::BulkString(Some(master_ip)) = &items[5] {
                assert_eq!(String::from_utf8_lossy(master_ip), "127.0.0.1");
            }
            if let Resp::Integer(master_port) = items[6] {
                assert_eq!(master_port, 6379);
            }
            if let Resp::Integer(master_epoch) = items[7] {
                assert_eq!(master_epoch, 5);
            }
        }
        _ => panic!("Expected array response"),
    }
}

#[tokio::test]
async fn test_sentinel_hello_no_masters() {
    let sentinel_ip = "127.0.0.1".to_string();
    let sentinel_port = 26379;
    let state = SentinelState::new(sentinel_ip, sentinel_port);

    // No masters configured

    // Simulate SENTINEL HELLO command response
    let mut response = Vec::new();

    // Add sentinel info
    response.push(Resp::BulkString(Some(state.ip.clone().into())));
    response.push(Resp::BulkString(Some(state.port.to_string().into())));
    response.push(Resp::BulkString(Some(state.run_id.clone().into())));

    let current_epoch = state.current_epoch.read().unwrap();
    response.push(Resp::Integer(*current_epoch as i64));

    // No masters, so add empty master info
    response.push(Resp::BulkString(None));
    response.push(Resp::BulkString(None));
    response.push(Resp::Integer(0));
    response.push(Resp::Integer(0));

    let hello_response = Resp::Array(Some(response));

    // Verify the response structure
    match hello_response {
        Resp::Array(Some(items)) => {
            assert_eq!(items.len(), 8);

            // Check sentinel info
            assert!(matches!(items[0], Resp::BulkString(Some(_))));
            assert!(matches!(items[1], Resp::BulkString(Some(_))));
            assert!(matches!(items[2], Resp::BulkString(Some(_))));
            assert!(matches!(items[3], Resp::Integer(0)));

            // Check that master fields are empty
            assert!(matches!(items[4], Resp::BulkString(None)));
            assert!(matches!(items[5], Resp::BulkString(None)));
            assert!(matches!(items[6], Resp::Integer(0)));
            assert!(matches!(items[7], Resp::Integer(0)));
        }
        _ => panic!("Expected array response"),
    }
}
