use crate::db::{Db, Entry, Value};
use crate::resp::Resp;
use crate::cmd::process_frame;
use crate::cmd::scripting;
use crate::conf::Config;
use bytes::Bytes;
use std::sync::Arc;

#[tokio::test]
async fn test_geo() {
    let db = Arc::new(vec![Db::default()]);
    let config = Arc::new(Config::default());
    let script_manager = scripting::create_script_manager();
    let acl = std::sync::Arc::new(std::sync::RwLock::new(crate::acl::Acl::new()));

    let mut conn_ctx = crate::cmd::ConnectionContext::new();
    let server_ctx = crate::cmd::ServerContext {
        databases: db.clone(),
        acl: acl.clone(),
        aof: None,
        config: config.clone(),
        script_manager: script_manager.clone(),
        blocking_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
    };

    // GEOADD Sicily 13.361389 38.115556 "Palermo" 15.087269 37.502669 "Catania"
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GEOADD"))),
        Resp::BulkString(Some(Bytes::from("Sicily"))),
        Resp::BulkString(Some(Bytes::from("13.361389"))),
        Resp::BulkString(Some(Bytes::from("38.115556"))),
        Resp::BulkString(Some(Bytes::from("Palermo"))),
        Resp::BulkString(Some(Bytes::from("15.087269"))),
        Resp::BulkString(Some(Bytes::from("37.502669"))),
        Resp::BulkString(Some(Bytes::from("Catania"))),
    ]));
    
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Integer(i) => assert_eq!(i, 2),
        _ => panic!("Expected Integer(2), got {:?}", res),
    }

    // GEODIST Sicily Palermo Catania
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GEODIST"))),
        Resp::BulkString(Some(Bytes::from("Sicily"))),
        Resp::BulkString(Some(Bytes::from("Palermo"))),
        Resp::BulkString(Some(Bytes::from("Catania"))),
    ]));
    
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => {
            let s = std::str::from_utf8(&b).unwrap();
            let dist: f64 = s.parse().unwrap();
            println!("Distance: {}", dist);
            // Expected around 166274.15 meters
            assert!((dist - 166274.0).abs() < 200.0);
        },
        _ => panic!("Expected BulkString, got {:?}", res),
    }

    // GEODIST Sicily Palermo Catania km
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GEODIST"))),
        Resp::BulkString(Some(Bytes::from("Sicily"))),
        Resp::BulkString(Some(Bytes::from("Palermo"))),
        Resp::BulkString(Some(Bytes::from("Catania"))),
        Resp::BulkString(Some(Bytes::from("km"))),
    ]));
    
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::BulkString(Some(b)) => {
            let s = std::str::from_utf8(&b).unwrap();
            let dist: f64 = s.parse().unwrap();
            println!("Distance km: {}", dist);
            assert!((dist - 166.274).abs() < 0.2);
        },
        _ => panic!("Expected BulkString, got {:?}", res),
    }

    // GEOHASH Sicily Palermo Catania
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GEOHASH"))),
        Resp::BulkString(Some(Bytes::from("Sicily"))),
        Resp::BulkString(Some(Bytes::from("Palermo"))),
        Resp::BulkString(Some(Bytes::from("Catania"))),
    ]));
    
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2);
            // Check Palermo hash
            if let Resp::BulkString(Some(b)) = &arr[0] {
                let s = std::str::from_utf8(&b).unwrap();
                println!("Palermo hash: {}", s);
                // Redis example: "sqc8b49rny0"
                assert!(s.starts_with("sqc8b"));
            } else {
                panic!("Expected BulkString for Palermo");
            }
            // Check Catania hash
            if let Resp::BulkString(Some(b)) = &arr[1] {
                let s = std::str::from_utf8(&b).unwrap();
                println!("Catania hash: {}", s);
                // Redis example: "sqdtr74h230"
                assert!(s.starts_with("sqdtr"));
            } else {
                panic!("Expected BulkString for Catania");
            }
        },
        _ => panic!("Expected Array, got {:?}", res),
    }

    // GEOPOS Sicily Palermo Catania
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GEOPOS"))),
        Resp::BulkString(Some(Bytes::from("Sicily"))),
        Resp::BulkString(Some(Bytes::from("Palermo"))),
        Resp::BulkString(Some(Bytes::from("Catania"))),
    ]));
    
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 2);
            // Check Palermo pos
            if let Resp::Array(Some(pos)) = &arr[0] {
                let lon: f64 = match &pos[0] {
                     Resp::BulkString(Some(b)) => std::str::from_utf8(b).unwrap().parse().unwrap(),
                     _ => panic!("Expected BulkString for lon"),
                };
                let lat: f64 = match &pos[1] {
                     Resp::BulkString(Some(b)) => std::str::from_utf8(b).unwrap().parse().unwrap(),
                     _ => panic!("Expected BulkString for lat"),
                };
                println!("Palermo pos: {}, {}", lon, lat);
                assert!((lon - 13.361389).abs() < 0.0001);
                assert!((lat - 38.115556).abs() < 0.0001);
            } else {
                panic!("Expected Array for Palermo pos");
            }
        },
        _ => panic!("Expected Array, got {:?}", res),
    }
}

#[tokio::test]
async fn test_georadius() {
    let db = Arc::new(vec![Db::default()]);
    let config = Arc::new(Config::default());
    let script_manager = scripting::create_script_manager();
    let acl = std::sync::Arc::new(std::sync::RwLock::new(crate::acl::Acl::new()));

    let mut conn_ctx = crate::cmd::ConnectionContext::new();
    let server_ctx = crate::cmd::ServerContext {
        databases: db.clone(),
        acl: acl.clone(),
        aof: None,
        config: config.clone(),
        script_manager: script_manager.clone(),
        blocking_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
    };

    // GEOADD Sicily 13.361389 38.115556 "Palermo" 15.087269 37.502669 "Catania"
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GEOADD"))),
        Resp::BulkString(Some(Bytes::from("Sicily"))),
        Resp::BulkString(Some(Bytes::from("13.361389"))),
        Resp::BulkString(Some(Bytes::from("38.115556"))),
        Resp::BulkString(Some(Bytes::from("Palermo"))),
        Resp::BulkString(Some(Bytes::from("15.087269"))),
        Resp::BulkString(Some(Bytes::from("37.502669"))),
        Resp::BulkString(Some(Bytes::from("Catania"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // GEORADIUS Sicily 15 37 200 km WITHDIST
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GEORADIUS"))),
        Resp::BulkString(Some(Bytes::from("Sicily"))),
        Resp::BulkString(Some(Bytes::from("15"))),
        Resp::BulkString(Some(Bytes::from("37"))),
        Resp::BulkString(Some(Bytes::from("200"))),
        Resp::BulkString(Some(Bytes::from("km"))),
        Resp::BulkString(Some(Bytes::from("WITHDIST"))),
    ]));
    
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            // Should match Catania and Palermo
            // Distance check:
            // 15,37 to Catania (15.087, 37.502) ~ 56km
            // 15,37 to Palermo (13.361, 38.115) ~ 190km
            
            // Should return 2 items
            assert_eq!(arr.len(), 2);
            
            // Since sorted by distance (default ASC), Catania first, then Palermo
            
            // Item 1: Catania
            if let Resp::Array(Some(item)) = &arr[0] {
                if let Resp::BulkString(Some(name)) = &item[0] {
                    assert_eq!(std::str::from_utf8(name).unwrap(), "Catania");
                } else { panic!("Expected name for item 1"); }
                
                if let Resp::BulkString(Some(dist)) = &item[1] {
                     let d: f64 = std::str::from_utf8(dist).unwrap().parse().unwrap();
                     println!("Dist to Catania: {}", d);
                     assert!(d < 100.0); 
                } else { panic!("Expected dist for item 1"); }
            } else { panic!("Expected array for item 1"); }

            // Item 2: Palermo
            if let Resp::Array(Some(item)) = &arr[1] {
                if let Resp::BulkString(Some(name)) = &item[0] {
                    assert_eq!(std::str::from_utf8(name).unwrap(), "Palermo");
                } else { panic!("Expected name for item 2"); }
                
                if let Resp::BulkString(Some(dist)) = &item[1] {
                     let d: f64 = std::str::from_utf8(dist).unwrap().parse().unwrap();
                     println!("Dist to Palermo: {}", d);
                     assert!(d > 100.0 && d < 200.0);
                } else { panic!("Expected dist for item 2"); }
            } else { panic!("Expected array for item 2"); }

        },
        _ => panic!("Expected Array, got {:?}", res),
    }

    // GEORADIUS Sicily 15 37 100 km (Should only match Catania)
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GEORADIUS"))),
        Resp::BulkString(Some(Bytes::from("Sicily"))),
        Resp::BulkString(Some(Bytes::from("15"))),
        Resp::BulkString(Some(Bytes::from("37"))),
        Resp::BulkString(Some(Bytes::from("100"))),
        Resp::BulkString(Some(Bytes::from("km"))),
    ]));
    
    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            assert_eq!(arr.len(), 1);
            if let Resp::BulkString(Some(name)) = &arr[0] {
                assert_eq!(std::str::from_utf8(name).unwrap(), "Catania");
            }
        },
        _ => panic!("Expected Array, got {:?}", res),
    }
}

#[tokio::test]
async fn test_georadiusbymember() {
    let db = Arc::new(vec![Db::default()]);
    let config = Arc::new(Config::default());
    let script_manager = scripting::create_script_manager();
    let acl = std::sync::Arc::new(std::sync::RwLock::new(crate::acl::Acl::new()));

    let mut conn_ctx = crate::cmd::ConnectionContext::new();
    let server_ctx = crate::cmd::ServerContext {
        databases: db.clone(),
        acl: acl.clone(),
        aof: None,
        config: config.clone(),
        script_manager: script_manager.clone(),
        blocking_waiters: std::sync::Arc::new(dashmap::DashMap::new()),
    };

    // GEOADD Sicily 13.583333 37.316667 "Agrigento"
    // GEOADD Sicily 13.361389 38.115556 "Palermo" 15.087269 37.502669 "Catania"
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GEOADD"))),
        Resp::BulkString(Some(Bytes::from("Sicily"))),
        Resp::BulkString(Some(Bytes::from("13.583333"))),
        Resp::BulkString(Some(Bytes::from("37.316667"))),
        Resp::BulkString(Some(Bytes::from("Agrigento"))),
        Resp::BulkString(Some(Bytes::from("13.361389"))),
        Resp::BulkString(Some(Bytes::from("38.115556"))),
        Resp::BulkString(Some(Bytes::from("Palermo"))),
        Resp::BulkString(Some(Bytes::from("15.087269"))),
        Resp::BulkString(Some(Bytes::from("37.502669"))),
        Resp::BulkString(Some(Bytes::from("Catania"))),
    ]));
    process_frame(req, &mut conn_ctx, &server_ctx).await;

    // GEORADIUSBYMEMBER Sicily Agrigento 100 km
    // Agrigento (13.58, 37.31)
    // Palermo (13.36, 38.11) ~ 90km
    // Catania (15.08, 37.50) ~ 130km
    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("GEORADIUSBYMEMBER"))),
        Resp::BulkString(Some(Bytes::from("Sicily"))),
        Resp::BulkString(Some(Bytes::from("Agrigento"))),
        Resp::BulkString(Some(Bytes::from("100"))),
        Resp::BulkString(Some(Bytes::from("km"))),
    ]));

    let (res, _) = process_frame(req, &mut conn_ctx, &server_ctx).await;
    match res {
        Resp::Array(Some(arr)) => {
            // Should match Agrigento (itself, dist 0) and Palermo (~90km)
            assert_eq!(arr.len(), 2);
            // Default sort is ASC, so Agrigento (0) then Palermo
            
            // Check items existence
            let names: Vec<String> = arr.iter().map(|item| {
                if let Resp::BulkString(Some(name)) = item {
                    std::str::from_utf8(name).unwrap().to_string()
                } else {
                    panic!("Expected BulkString");
                }
            }).collect();
            
            assert!(names.contains(&"Agrigento".to_string()));
            assert!(names.contains(&"Palermo".to_string()));
            assert!(!names.contains(&"Catania".to_string()));
        },
        _ => panic!("Expected Array, got {:?}", res),
    }
}
