use crate::acl::Acl;
use crate::cmd::{process_frame, scripting};
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::{Arc, RwLock};

#[test]
fn test_acl_key_permissions() {
    let db = Arc::new(vec![Db::default()]);
    let mut db_index = 0;
    let cfg = Config::default();
    let acl = Arc::new(RwLock::new(Acl::new()));
    let mut current_username = "default".to_string();
    let mut authenticated = true;
    
    // Create user bob with access to user:*
    // ACL SETUSER bob on >secret +@all ~user:*
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ACL"))),
        Resp::BulkString(Some(Bytes::from("SETUSER"))),
        Resp::BulkString(Some(Bytes::from("bob"))),
        Resp::BulkString(Some(Bytes::from("on"))),
        Resp::BulkString(Some(Bytes::from(">secret"))),
        Resp::BulkString(Some(Bytes::from("+@all"))),
        Resp::BulkString(Some(Bytes::from("~user:*"))),
    ]));
    process_frame(
        req, &db, &mut db_index, &mut authenticated, &mut current_username, &acl, &None, &cfg, &scripting::create_script_manager()
    );
    
    // Auth as bob
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("AUTH"))),
        Resp::BulkString(Some(Bytes::from("bob"))),
        Resp::BulkString(Some(Bytes::from("secret"))),
    ]));
    let (res, _) = process_frame(
        req, &db, &mut db_index, &mut authenticated, &mut current_username, &acl, &None, &cfg, &scripting::create_script_manager()
    );
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected OK"),
    }
    assert_eq!(current_username, "bob");
    
    // SET user:1 val -> OK
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("user:1"))),
        Resp::BulkString(Some(Bytes::from("val"))),
    ]));
    let (res, _) = process_frame(
        req, &db, &mut db_index, &mut authenticated, &mut current_username, &acl, &None, &cfg, &scripting::create_script_manager()
    );
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("expected OK, got {:?}", res),
    }
    
    // SET admin:1 val -> NOPERM
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SET"))),
        Resp::BulkString(Some(Bytes::from("admin:1"))),
        Resp::BulkString(Some(Bytes::from("val"))),
    ]));
    let (res, _) = process_frame(
        req, &db, &mut db_index, &mut authenticated, &mut current_username, &acl, &None, &cfg, &scripting::create_script_manager()
    );
    match res {
        Resp::Error(e) => assert!(e.contains("NOPERM"), "Expected NOPERM, got {}", e),
        _ => panic!("expected Error"),
    }
}

#[test]
fn test_acl_persistence() {
    let db = Arc::new(vec![Db::default()]);
    let mut db_index = 0;
    
    // Use a temp file
    let temp_dir = std::env::temp_dir();
    let acl_path = temp_dir.join("test_users.acl");
    let acl_path_str = acl_path.to_str().unwrap().to_string();
    
    // Clean up before test
    if acl_path.exists() {
        let _ = std::fs::remove_file(&acl_path);
    }
    
    let mut cfg = Config::default();
    cfg.aclfile = Some(acl_path_str.clone());
    
    let acl = Arc::new(RwLock::new(Acl::new()));
    let mut current_username = "default".to_string();
    let mut authenticated = true;
    
    // Create user alice
    // ACL SETUSER alice on >pass123 +@all
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ACL"))),
        Resp::BulkString(Some(Bytes::from("SETUSER"))),
        Resp::BulkString(Some(Bytes::from("alice"))),
        Resp::BulkString(Some(Bytes::from("on"))),
        Resp::BulkString(Some(Bytes::from(">pass123"))),
        Resp::BulkString(Some(Bytes::from("+@all"))),
    ]));
    process_frame(
        req, &db, &mut db_index, &mut authenticated, &mut current_username, &acl, &None, &cfg, &scripting::create_script_manager()
    );
    
    // ACL SAVE
    let req_save = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ACL"))),
        Resp::BulkString(Some(Bytes::from("SAVE"))),
    ]));
    let (res, _) = process_frame(
        req_save, &db, &mut db_index, &mut authenticated, &mut current_username, &acl, &None, &cfg, &scripting::create_script_manager()
    );
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("ACL SAVE failed: {:?}", res),
    }
    
    // Verify file exists
    assert!(acl_path.exists());
    
    // Create a NEW ACL instance to test loading
    let new_acl = Arc::new(RwLock::new(Acl::new()));
    
    // ACL LOAD on new instance
    let req_load = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("ACL"))),
        Resp::BulkString(Some(Bytes::from("LOAD"))),
    ]));
    let (res, _) = process_frame(
        req_load, &db, &mut db_index, &mut authenticated, &mut current_username, &new_acl, &None, &cfg, &scripting::create_script_manager()
    );
    match res {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("ACL LOAD failed: {:?}", res),
    }
    
    // Check if alice exists in new_acl
    let acl_guard = new_acl.read().unwrap();
    let alice = acl_guard.get_user("alice");
    assert!(alice.is_some(), "User alice should exist after loading");
    let alice = alice.unwrap();
    assert!(alice.enabled, "Alice should be enabled");
    assert!(alice.check_password("pass123"), "Alice should have correct password");
    assert!(alice.all_commands, "Alice should have all commands");
    
    // Cleanup
    let _ = std::fs::remove_file(&acl_path);
}
