use crate::db::{Db, Entry};
use crate::resp::{as_bytes, Resp};
use crate::rdb::{RdbEncoder, RdbLoader};
use std::io::Cursor;

const RDB_VERSION: u16 = 9;

pub fn dump(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 {
        return Resp::Error("ERR wrong number of arguments for 'dump' command".to_string());
    }
    
    let key = match as_bytes(&items[1]) {
        Some(k) => k,
        None => return Resp::Error("ERR invalid key".to_string()),
    };

    let entry = match db.get(key) {
        Some(e) => e,
        None => return Resp::BulkString(None),
    };

    let mut buf = Vec::new();
    {
        let mut encoder = RdbEncoder::new(&mut buf, false, true);
        if let Err(_) = encoder.dump_value(&entry.value) {
             return Resp::Error("ERR failed to dump value".to_string());
        }
        // Write RDB version (u16)
        let _ = encoder.write_u16_le(RDB_VERSION); 
        // Write CRC64 (u64)
        let crc = encoder.digest();
        let _ = encoder.write_u64_le(crc);
    }

    Resp::BulkString(Some(bytes::Bytes::from(buf)))
}

pub fn restore(items: &[Resp], db: &Db) -> Resp {
    // RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]
    if items.len() < 4 {
        return Resp::Error("ERR wrong number of arguments for 'restore' command".to_string());
    }

    let key = match as_bytes(&items[1]) {
        Some(k) => k.to_vec(),
        None => return Resp::Error("ERR invalid key".to_string()),
    };

    let ttl_ms = match as_bytes(&items[2]) {
        Some(b) => {
             let s = String::from_utf8_lossy(&b);
             match s.parse::<u64>() {
                 Ok(v) => v,
                 Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
             }
        },
        None => return Resp::Error("ERR invalid ttl".to_string()),
    };

    let serialized = match as_bytes(&items[3]) {
        Some(b) => b,
        None => return Resp::Error("ERR invalid serialized value".to_string()),
    };

    let mut replace = false;
    let mut absttl = false;
    let mut idletime: Option<u64> = None;
    let mut freq: Option<u32> = None;

    // Parse options
    let mut i = 4;
    while i < items.len() {
        if let Some(arg) = as_bytes(&items[i]) {
            let s = String::from_utf8_lossy(&arg).to_uppercase();
            match s.as_str() {
                "REPLACE" => replace = true,
                "ABSTTL" => absttl = true,
                "IDLETIME" => {
                    if i + 1 < items.len() {
                        if let Some(val) = as_bytes(&items[i+1]) {
                            if let Ok(v) = String::from_utf8_lossy(val).parse::<u64>() {
                                idletime = Some(v);
                            }
                        }
                        i += 1;
                    }
                }
                "FREQ" => {
                    if i + 1 < items.len() {
                        if let Some(val) = as_bytes(&items[i+1]) {
                            if let Ok(v) = String::from_utf8_lossy(val).parse::<u32>() {
                                freq = Some(v);
                            }
                        }
                        i += 1;
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }

    if db.contains_key(key.as_slice()) && !replace {
        return Resp::Error("BUSYKEY Target key name already exists.".to_string());
    }

    // Verify Checksum
    if serialized.len() < 10 { 
        return Resp::Error("ERR DUMP payload version or checksum are wrong".to_string());
    }

    let mut reader = Cursor::new(&serialized);
    let mut loader = RdbLoader::new(&mut reader);
    
    let value = match loader.restore_value() {
        Ok(v) => v,
        Err(_) => return Resp::Error("ERR Bad data format".to_string()),
    };

    // Read Version
    let version = match loader.read_u16_le() {
        Ok(v) => v,
        Err(_) => return Resp::Error("ERR DUMP payload version or checksum are wrong".to_string()),
    };

    if version != RDB_VERSION {
        // We could be lenient here, but for now strict check
        // Redis checks if version is supported.
        if version > RDB_VERSION {
             return Resp::Error("ERR DUMP payload version or checksum are wrong".to_string());
        }
    }
    
    // Calculate CRC digest BEFORE reading the stored CRC
    let actual_crc = loader.digest();
    
    // Read CRC
    let expected_crc = match loader.read_u64_le() {
        Ok(v) => v,
        Err(_) => return Resp::Error("ERR DUMP payload version or checksum are wrong".to_string()),
    };
    
    if actual_crc != expected_crc {
         return Resp::Error("ERR DUMP payload version or checksum are wrong".to_string());
    }

    // Calculate expire_at
    let expire_at = if ttl_ms > 0 {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
        if absttl {
            Some(ttl_ms)
        } else {
            Some(now + ttl_ms)
        }
    } else {
        None
    };

    let mut entry = Entry::new_with_expire(value, expire_at);
    if let Some(idle) = idletime {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        entry.lru = now.saturating_sub(idle);
    }
    if let Some(f) = freq {
        entry.lfu = f;
    }

    db.insert(bytes::Bytes::from(key), entry);

    Resp::SimpleString(bytes::Bytes::from_static(b"OK"))
}
