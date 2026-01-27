#[cfg(test)]
mod tests {
    use crate::conf::Config;
    use crate::db::{Db, Value};
    use crate::rdb::{RdbEncoder, RdbLoader};
    use crate::stream::{Stream, StreamID};
    use bytes::Bytes;
    use std::fs;
    use std::path::Path;
    use std::str::FromStr;
    use std::sync::Arc;
    use dashmap::DashMap;

    use crate::aof::{Aof, AppendFsync};
    use crate::cmd::scripting::create_script_manager;
    use crate::cmd::{process_frame, ConnectionContext, ServerContext};
    use crate::resp::Resp;
    use tokio::io::AsyncWriteExt; // For writing to AOF manually if needed, but Aof struct has write methods

    #[tokio::test]
    async fn test_stream_aof_persistence() {
        let dir = "/tmp";
        let filename = format!("test_stream_{}.aof", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
        let path_str = Path::new(dir).join(&filename);
        let path = path_str.to_str().unwrap();

        // 1. Setup DB with Stream data
        let db = Arc::new(vec![Db::default()]);
        let key = Bytes::from("mystream_aof");
        
        let mut stream = Stream::new();
        let id1 = StreamID::new(1000, 1);
        let fields1 = vec![(Bytes::from("name"), Bytes::from("alice"))];
        stream.insert(id1, fields1.clone()).unwrap();
        
        let group_name = "mygroup".to_string();
        let group = crate::stream::ConsumerGroup::new(group_name.clone(), StreamID::new(0, 0));
        stream.groups.insert(group_name.clone(), group);

        db[0].insert(key.clone(), crate::db::Entry::new(Value::Stream(stream), None));

        // 2. Perform AOF Rewrite
        let mut aof = Aof::new(path, AppendFsync::No).await.unwrap();
        aof.rewrite(&db).await.unwrap();

        // 3. Load AOF into new DB
        let new_db = Arc::new(vec![Db::default()]);
        let config = Config::default();
        let script_manager = create_script_manager();
        
        // We need to use Aof::load. It's an instance method.
        // Re-open AOF to load
        let loader = Aof::new(path, AppendFsync::No).await.unwrap();
        loader.load(path, &new_db, &config, &script_manager).await.unwrap();

        // 4. Verify
        let entry = new_db[0].get(&key).unwrap();
        match &entry.value {
             Value::Stream(s) => {
                 assert_eq!(s.len(), 1);
                 assert_eq!(s.groups.len(), 1);
                 let retrieved = s.get(&id1).unwrap();
                 assert_eq!(retrieved.fields, fields1);
                 assert!(s.groups.contains_key(&group_name));
             }
             _ => panic!("Expected Stream"),
        }
        
        let _ = fs::remove_file(path_str);
    }

    #[tokio::test]
    async fn test_stream_aof_log_replay() {
        let dir = "/tmp";
        let filename = format!("test_stream_log_{}.aof", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
        let path_str = Path::new(dir).join(&filename);
        let path = path_str.to_str().unwrap();

        let db = Arc::new(vec![Db::default()]);
        let config = Arc::new(Config::default());
        let script_manager = create_script_manager();
        let acl = Arc::new(std::sync::RwLock::new(crate::acl::Acl::new()));

        let server_ctx = ServerContext {
            databases: db.clone(),
            acl: acl,
            aof: None,
            config: config.clone(),
            script_manager: script_manager,
            blocking_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
            blocking_zset_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
        };

        let mut conn_ctx = ConnectionContext {
            db_index: 0,
            authenticated: true,
            current_username: "default".to_string(),
            in_multi: false,
            multi_queue: Vec::new(),
        };
        
        // Helper to run command and write log
        macro_rules! run_and_log {
            ($args:expr, $conn:expr, $server:expr) => {
                {
                    let req = Resp::Array(Some(
                        $args.iter().map(|s| Resp::BulkString(Some(Bytes::from(s.to_string())))).collect()
                    ));
                    let (_res, log_cmd) = process_frame(req, $conn, $server).await;
                    log_cmd
                }
            }
        }

        // 1. XADD
        let log1 = run_and_log!(vec!["XADD", "mystream", "*", "name", "alice"], &mut conn_ctx, &server_ctx).unwrap();
        // Manually write log1 to file
        write_resp_to_file(path, &log1).await;

        // 2. XGROUP CREATE
        let log2 = run_and_log!(vec!["XGROUP", "CREATE", "mystream", "mygroup", "$", "MKSTREAM"], &mut conn_ctx, &server_ctx).unwrap();
        write_resp_to_file(path, &log2).await;

        // 3. XREADGROUP (read new)
        // Need to add more data first so XREADGROUP has something to read and log
        let log3 = run_and_log!(vec!["XADD", "mystream", "*", "name", "bob"], &mut conn_ctx, &server_ctx).unwrap();
        write_resp_to_file(path, &log3).await;

        let log4 = run_and_log!(vec!["XREADGROUP", "GROUP", "mygroup", "consumer1", "COUNT", "1", "STREAMS", "mystream", ">"], &mut conn_ctx, &server_ctx).unwrap();
        write_resp_to_file(path, &log4).await;
        
        // 4. XACK
        let stream_id_bob = {
            let entry = db[0].get(&Bytes::from("mystream")).unwrap();
            match &entry.value {
                Value::Stream(s) => s.last_id.to_string(),
                _ => panic!("Expected stream"),
            }
        };
        
        let log5 = run_and_log!(vec!["XACK", "mystream", "mygroup", &stream_id_bob], &mut conn_ctx, &server_ctx).unwrap();
        write_resp_to_file(path, &log5).await;

        // 5. XDEL (delete alice)
        let alice_id = {
            let entry = db[0].get(&Bytes::from("mystream")).unwrap();
            let stream_ids: Vec<String> = match &entry.value {
                Value::Stream(s) => s.range(&StreamID::new(0,0), &StreamID::new(u64::MAX, u64::MAX)).iter().map(|e| e.id.to_string()).collect(),
                _ => panic!(""),
            };
            stream_ids[0].clone()
        };
        
        let log6 = run_and_log!(vec!["XDEL", "mystream", &alice_id], &mut conn_ctx, &server_ctx).unwrap();
        write_resp_to_file(path, &log6).await;


        // 6. Load AOF into new DB
        let new_db = Arc::new(vec![Db::default()]);
        // We need to close the file/ensure it is flushed? write_resp_to_file opens and closes it each time.
        
        let loader = Aof::new(path, AppendFsync::No).await.unwrap();
        // create new script manager for loader
        let script_manager_loader = create_script_manager();
        loader.load(path, &new_db, &config, &script_manager_loader).await.unwrap();

        // 7. Verify
        let entry = new_db[0].get(&Bytes::from("mystream")).unwrap();
        match &entry.value {
             Value::Stream(s) => {
                 // We added 2 items, deleted 1. Should have 1.
                 assert_eq!(s.len(), 1); 
                 
                 // The remaining item should be bob
                 let bob_entry = s.get(&StreamID::from_str(&stream_id_bob).unwrap()).unwrap();
                 assert_eq!(bob_entry.fields[0].1, Bytes::from("bob"));
                 
                 // Verify Group
                 assert!(s.groups.contains_key("mygroup"));
                 let group = s.groups.get("mygroup").unwrap();
                 
                 // Verify PEL
                 // We read bob, then ACKed bob. PEL should be empty.
                 assert_eq!(group.pel.len(), 0);
                 
                 // Verify Consumer
                 let consumer = group.consumers.get("consumer1").unwrap();
                 assert_eq!(consumer.pending_ids.len(), 0);
             }
             _ => panic!("Expected Stream"),
        }
        
        let _ = fs::remove_file(path_str);
    }
    
    // Helper to write Resp to file (AOF format)
    async fn write_resp_to_file(path: &str, resp: &Resp) {
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .unwrap();
            
        use tokio::io::AsyncWriteExt;
        
        fn serialize(resp: &Resp, buf: &mut Vec<u8>) {
            match resp {
                Resp::Array(Some(arr)) => {
                    buf.extend_from_slice(format!("*{}\r\n", arr.len()).as_bytes());
                    for item in arr {
                        serialize(item, buf);
                    }
                }
                Resp::BulkString(Some(bytes)) => {
                    buf.extend_from_slice(format!("${}\r\n", bytes.len()).as_bytes());
                    buf.extend_from_slice(bytes);
                    buf.extend_from_slice(b"\r\n");
                }
                _ => panic!("Unsupported Resp type for test serialization"),
            }
        }
        
        let mut buf = Vec::new();
        serialize(resp, &mut buf);
        file.write_all(&buf).await.unwrap();
        file.sync_all().await.unwrap();
    }

    #[test]
    fn test_stream_rdb_persistence() {
        let dir = "/tmp";
        let dbfilename = format!("test_stream_{}.rdb", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
        let path = Path::new(dir).join(&dbfilename);
        let save_path = path.to_str().unwrap();

        // 1. Setup DB with Stream data
        let db = Arc::new(vec![Db::default()]);
        let key = Bytes::from("mystream");
        
        let mut stream = Stream::new();
        
        // Add entries
        let id1 = StreamID::new(1000, 1);
        let fields1 = vec![(Bytes::from("name"), Bytes::from("alice"))];
        stream.insert(id1, fields1.clone()).unwrap();
        
        let id2 = StreamID::new(1000, 2);
        let fields2 = vec![(Bytes::from("name"), Bytes::from("bob"))];
        stream.insert(id2, fields2.clone()).unwrap();

        // Add Consumer Group
        let group_name = "mygroup".to_string();
        let group = crate::stream::ConsumerGroup::new(group_name.clone(), StreamID::new(0, 0));
        stream.groups.insert(group_name.clone(), group);
        
        // Add Consumer and PEL (simulate XREADGROUP)
        // We manually insert into PEL for testing
        let consumer_name = "consumer1".to_string();
        // Access the group and modify it
        if let Some(group) = stream.groups.get_mut(&group_name) {
             // Create consumer
             let consumer = crate::stream::Consumer::new(consumer_name.clone());
             group.consumers.insert(consumer_name.clone(), consumer);
             
             // Add pending entry
             let pending = crate::stream::PendingEntry {
                 id: id1,
                 delivery_time: 123456789,
                 delivery_count: 1,
                 owner: consumer_name.clone(),
             };
             group.pel.insert(id1, pending);
             
             // Update consumer pending_ids
             if let Some(c) = group.consumers.get_mut(&consumer_name) {
                 c.pending_ids.insert(id1);
             }
        }

        db[0].insert(key.clone(), crate::db::Entry::new(Value::Stream(stream), None));

        // 2. Save RDB
        let mut encoder = RdbEncoder::new(save_path).unwrap();
        encoder.save(&db).unwrap();
        assert!(path.exists());

        // 3. Load RDB
        let mut loader = RdbLoader::new(save_path).unwrap();
        let loaded_db = Arc::new(vec![Db::default()]);
        loader.load(&loaded_db).unwrap();

        // 4. Verify data
        assert_eq!(loaded_db[0].len(), 1);
        let entry = loaded_db[0].get(&key).unwrap();
        match &entry.value {
            Value::Stream(s) => {
                assert_eq!(s.len(), 2);
                assert_eq!(s.groups.len(), 1);
                
                // Verify entries
                let retrieved_entry = s.get(&id1).unwrap();
                // Compare fields. Bytes vs Bytes.
                // fields1 is Vec<(Bytes, Bytes)>.
                // retrieved_entry.fields is Vec<(Bytes, Bytes)>.
                assert_eq!(retrieved_entry.fields, fields1);
                
                // Verify Group
                let group = s.groups.get(&group_name).unwrap();
                assert_eq!(group.consumers.len(), 1);
                assert_eq!(group.pel.len(), 1);
                
                // Verify PEL
                let pending = group.pel.get(&id1).unwrap();
                assert_eq!(pending.owner, consumer_name);
                assert_eq!(pending.delivery_time, 123456789);
                
                // Verify Consumer
                let consumer = group.consumers.get(&consumer_name).unwrap();
                assert!(consumer.pending_ids.contains(&id1));
            }
            _ => panic!("Expected Stream value"),
        }
        
        // Cleanup
        let _ = fs::remove_file(path);
    }
}
