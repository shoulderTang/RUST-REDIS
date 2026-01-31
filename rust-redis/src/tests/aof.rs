use crate::aof::{Aof, AppendFsync};
use crate::cmd::scripting;
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_file() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/tmp/redis_aof_test_{}.aof", now)
}

#[tokio::test]
async fn test_aof_append_and_load() {
    let path = temp_file();

    // 1. Create AOF and append commands
    {
        let mut aof = Aof::new(&path, AppendFsync::Always)
            .await
            .expect("failed to create aof");

        // SET key1 value1
        let set_cmd = Resp::Array(Some(vec![
            Resp::BulkString(Some(Bytes::from("SET"))),
            Resp::BulkString(Some(Bytes::from("key1"))),
            Resp::BulkString(Some(Bytes::from("value1"))),
        ]));
        aof.append(&set_cmd).await.expect("failed to append set");

        // RPUSH list1 item1
        let rpush_cmd = Resp::Array(Some(vec![
            Resp::BulkString(Some(Bytes::from("RPUSH"))),
            Resp::BulkString(Some(Bytes::from("list1"))),
            Resp::BulkString(Some(Bytes::from("item1"))),
        ]));
        aof.append(&rpush_cmd)
            .await
            .expect("failed to append rpush");
    }

    // 2. Load AOF into a new DB
    let db_new = Arc::new(vec![RwLock::new(Db::default())]);
    let aof_loader = Aof::new(&path, AppendFsync::Always)
        .await
        .expect("failed to open aof for loading");
    //let script_manager = scripting::create_script_manager();
    let mut server_ctx = crate::tests::helper::create_server_context();
    Arc::make_mut(&mut server_ctx.config).appendfilename = path.to_string();
    server_ctx.databases = db_new.clone();
    aof_loader.load(&server_ctx)
        .await
        .expect("failed to load aof");

    // 3. Verify DB state
    // Check key1
    {
        let db = db_new[0].read().unwrap();
        let val = db.get(&Bytes::from("key1"));
        assert!(val.is_some(), "key1 not found");
        match &val.unwrap().value {
            crate::db::Value::String(s) => assert_eq!(s, &Bytes::from("value1")),
            _ => panic!("expected string for key1"),
        }
    }

    // Check list1
    {
        let db = db_new[0].read().unwrap();
        let list = db.get(&Bytes::from("list1"));
        assert!(list.is_some(), "list1 not found");
        match &list.unwrap().value {
            crate::db::Value::List(l) => {
                assert_eq!(l.len(), 1);
                assert_eq!(l[0], Bytes::from("item1"));
            }
            _ => panic!("expected list for list1"),
        }
    }

    // Cleanup
    tokio::fs::remove_file(&path)
        .await
        .expect("failed to remove temp file");
}
