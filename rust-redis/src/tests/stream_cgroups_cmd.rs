use crate::cmd::process_frame;
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;

#[tokio::test]
async fn test_xgroup_create_and_xreadgroup() {
    let db = Arc::new(vec![Arc::new(DashMap::new())]);
    let mut db_index = 0;
    let config = Config::default();
    let script_manager = crate::cmd::scripting::create_script_manager();

    // 1. Create a stream and add some entries
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XADD"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("100-1"))),
        Resp::BulkString(Some(Bytes::from("field1"))),
        Resp::BulkString(Some(Bytes::from("value1"))),
    ];
    process_frame(Resp::Array(Some(args)), &db, &mut db_index, &None, &config, &script_manager);

    let args = vec![
        Resp::BulkString(Some(Bytes::from("XADD"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("100-2"))),
        Resp::BulkString(Some(Bytes::from("field2"))),
        Resp::BulkString(Some(Bytes::from("value2"))),
    ];
    process_frame(Resp::Array(Some(args)), &db, &mut db_index, &None, &config, &script_manager);

    // 2. Create a consumer group
    // XGROUP CREATE mystream mygroup 0
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XGROUP"))),
        Resp::BulkString(Some(Bytes::from("CREATE"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("mygroup"))),
        Resp::BulkString(Some(Bytes::from("0-0"))),
    ];
    let (resp, _) = process_frame(Resp::Array(Some(args)), &db, &mut db_index, &None, &config, &script_manager);
    match resp {
        Resp::SimpleString(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("Expected OK"),
    }

    // 3. Read from the group using a consumer
    // XREADGROUP GROUP mygroup Alice COUNT 1 STREAMS mystream >
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XREADGROUP"))),
        Resp::BulkString(Some(Bytes::from("GROUP"))),
        Resp::BulkString(Some(Bytes::from("mygroup"))),
        Resp::BulkString(Some(Bytes::from("Alice"))),
        Resp::BulkString(Some(Bytes::from("COUNT"))),
        Resp::BulkString(Some(Bytes::from("1"))),
        Resp::BulkString(Some(Bytes::from("STREAMS"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from(">"))),
    ];
    let (resp, _) = process_frame(Resp::Array(Some(args)), &db, &mut db_index, &None, &config, &script_manager);

    // Expecting 100-1
    if let Resp::Array(Some(arr)) = resp {
        assert_eq!(arr.len(), 1);
        if let Resp::Array(Some(stream_res)) = &arr[0] {
             if let Resp::Array(Some(entries)) = &stream_res[1] {
                 assert_eq!(entries.len(), 1);
                 if let Resp::Array(Some(entry)) = &entries[0] {
                     if let Resp::BulkString(Some(id)) = &entry[0] {
                         assert_eq!(id, &Bytes::from("100-1"));
                     }
                 }
             }
        }
    } else {
        panic!("Expected array response");
    }

    // 4. Read again, expecting 100-2
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XREADGROUP"))),
        Resp::BulkString(Some(Bytes::from("GROUP"))),
        Resp::BulkString(Some(Bytes::from("mygroup"))),
        Resp::BulkString(Some(Bytes::from("Alice"))),
        Resp::BulkString(Some(Bytes::from("COUNT"))),
        Resp::BulkString(Some(Bytes::from("1"))),
        Resp::BulkString(Some(Bytes::from("STREAMS"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from(">"))),
    ];
    let (resp, _) = process_frame(Resp::Array(Some(args)), &db, &mut db_index, &None, &config, &script_manager);
    
    // Expecting 100-2
    if let Resp::Array(Some(arr)) = resp {
        assert_eq!(arr.len(), 1);
        if let Resp::Array(Some(stream_res)) = &arr[0] {
             if let Resp::Array(Some(entries)) = &stream_res[1] {
                 assert_eq!(entries.len(), 1);
                 if let Resp::Array(Some(entry)) = &entries[0] {
                     if let Resp::BulkString(Some(id)) = &entry[0] {
                         assert_eq!(id, &Bytes::from("100-2"));
                     }
                 }
             }
        }
    }

    // 5. Read pending entries (history) for Alice
    // XREADGROUP GROUP mygroup Alice STREAMS mystream 0
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XREADGROUP"))),
        Resp::BulkString(Some(Bytes::from("GROUP"))),
        Resp::BulkString(Some(Bytes::from("mygroup"))),
        Resp::BulkString(Some(Bytes::from("Alice"))),
        Resp::BulkString(Some(Bytes::from("STREAMS"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("0-0"))),
    ];
    let (resp, _) = process_frame(Resp::Array(Some(args)), &db, &mut db_index, &None, &config, &script_manager);

    // Expecting 2 entries (100-1 and 100-2) in PEL
    if let Resp::Array(Some(arr)) = resp {
         if let Resp::Array(Some(stream_res)) = &arr[0] {
             if let Resp::Array(Some(entries)) = &stream_res[1] {
                 assert_eq!(entries.len(), 2);
             }
        }
    }

    // 6. ACK one message
    // XACK mystream mygroup 100-1
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XACK"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("mygroup"))),
        Resp::BulkString(Some(Bytes::from("100-1"))),
    ];
    let (resp, _) = process_frame(Resp::Array(Some(args)), &db, &mut db_index, &None, &config, &script_manager);
    if let Resp::Integer(count) = resp {
        assert_eq!(count, 1);
    } else {
        panic!("Expected Integer response");
    }

    // 7. Read pending entries again, should have only 1 (100-2)
    let args = vec![
        Resp::BulkString(Some(Bytes::from("XREADGROUP"))),
        Resp::BulkString(Some(Bytes::from("GROUP"))),
        Resp::BulkString(Some(Bytes::from("mygroup"))),
        Resp::BulkString(Some(Bytes::from("Alice"))),
        Resp::BulkString(Some(Bytes::from("STREAMS"))),
        Resp::BulkString(Some(Bytes::from("mystream"))),
        Resp::BulkString(Some(Bytes::from("0-0"))),
    ];
    let (resp, _) = process_frame(Resp::Array(Some(args)), &db, &mut db_index, &None, &config, &script_manager);

    if let Resp::Array(Some(arr)) = resp {
         if let Resp::Array(Some(stream_res)) = &arr[0] {
             if let Resp::Array(Some(entries)) = &stream_res[1] {
                 assert_eq!(entries.len(), 1);
                 if let Resp::Array(Some(entry)) = &entries[0] {
                     if let Resp::BulkString(Some(id)) = &entry[0] {
                         assert_eq!(id, &Bytes::from("100-2"));
                     }
                 }
             }
        }
    }
}
