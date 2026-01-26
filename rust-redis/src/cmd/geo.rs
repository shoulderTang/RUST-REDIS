use crate::db::{Db, Entry, SortedSet, TotalOrderF64, Value};
use crate::resp::Resp;
use crate::geo::{geohash_encode, geohash_decode, geohash_to_base32, geodist as calc_dist};
use bytes::Bytes;
use std::cmp::Ordering;

pub fn geoadd(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 5 || (items.len() - 2) % 3 != 0 {
        return Resp::Error("ERR wrong number of arguments for 'geoadd' command".to_string());
    }

    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR key must be a string".to_string()),
    };

    // Use dashmap's entry API
    let mut entry = db
        .entry(key)
        .or_insert_with(|| Entry::new(Value::ZSet(SortedSet::new()), None));
    
    if entry.is_expired() {
        entry.value = Value::ZSet(SortedSet::new());
        entry.expires_at = None;
    }

    if let Value::ZSet(zset) = &mut entry.value {
        let mut count = 0;
        let mut i = 2;
        while i < items.len() {
            let lon_str = match &items[i] {
                Resp::BulkString(Some(b)) => std::str::from_utf8(b).ok(),
                Resp::SimpleString(s) => std::str::from_utf8(s).ok(),
                _ => return Resp::Error("ERR invalid longitude".to_string()),
            };
            let lat_str = match &items[i+1] {
                Resp::BulkString(Some(b)) => std::str::from_utf8(b).ok(),
                Resp::SimpleString(s) => std::str::from_utf8(s).ok(),
                _ => return Resp::Error("ERR invalid latitude".to_string()),
            };
            
            let lon: f64 = match lon_str.and_then(|s| s.parse().ok()) {
                Some(v) => v,
                None => return Resp::Error("ERR value is not a valid float".to_string()),
            };
            let lat: f64 = match lat_str.and_then(|s| s.parse().ok()) {
                Some(v) => v,
                None => return Resp::Error("ERR value is not a valid float".to_string()),
            };

            let member = match &items[i+2] {
                Resp::BulkString(Some(b)) => b.clone(),
                Resp::SimpleString(s) => s.clone(),
                _ => return Resp::Error("ERR member must be a string".to_string()),
            };

            // Redis standard: 26 steps for 52 bits
            let hash = geohash_encode(lat, lon, 26);
            let score = hash.bits as f64;

            if let Some(old_score) = zset.members.get(&member) {
                // If score changed, update
                // Note: we might want to update even if score is roughly same?
                // Redis updates if logic requires. Here simplified.
                if *old_score != score {
                    zset.scores.remove(&(TotalOrderF64(*old_score), member.clone()));
                    zset.members.insert(member.clone(), score);
                    zset.scores.insert((TotalOrderF64(score), member));
                    count += 1;
                }
            } else {
                zset.members.insert(member.clone(), score);
                zset.scores.insert((TotalOrderF64(score), member));
                count += 1;
            }

            i += 3;
        }
        Resp::Integer(count)
    } else {
        Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
    }
}

pub fn geodist(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 4 {
        return Resp::Error("ERR wrong number of arguments for 'geodist' command".to_string());
    }
    
    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        _ => return Resp::Error("ERR key must be a string".to_string()),
    };

    let member1 = match &items[2] {
        Resp::BulkString(Some(b)) => b.clone(),
        _ => return Resp::Error("ERR member must be a string".to_string()),
    };

    let member2 = match &items[3] {
        Resp::BulkString(Some(b)) => b.clone(),
        _ => return Resp::Error("ERR member must be a string".to_string()),
    };

    let unit_scale = if items.len() > 4 {
        match &items[4] {
            Resp::BulkString(Some(b)) => match std::str::from_utf8(b).unwrap_or("m") {
                "m" => 1.0,
                "km" => 1000.0,
                "mi" => 1609.34,
                "ft" => 0.3048,
                _ => return Resp::Error("ERR unsupported unit provided. please use m, km, ft, mi".to_string()),
            },
            _ => 1.0,
        }
    } else {
        1.0
    };

    if let Some(entry) = db.get(&key) {
        if let Value::ZSet(zset) = &entry.value {
            let score1 = zset.members.get(&member1);
            let score2 = zset.members.get(&member2);

            if let (Some(s1), Some(s2)) = (score1, score2) {
                 let hash1 = crate::geo::GeoHashBits { bits: *s1 as u64, step: 26 };
                 let (lat1, lon1) = geohash_decode(hash1);
                 
                 let hash2 = crate::geo::GeoHashBits { bits: *s2 as u64, step: 26 };
                 let (lat2, lon2) = geohash_decode(hash2);
                 
                 let dist = calc_dist(lat1, lon1, lat2, lon2);
                 let converted = dist / unit_scale;
                 
                 return Resp::BulkString(Some(Bytes::from(converted.to_string())));
            }
        }
    }
    
    Resp::BulkString(None)
}

pub fn geohash(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'geohash' command".to_string());
    }

    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        _ => return Resp::Error("ERR key must be a string".to_string()),
    };

    if let Some(entry) = db.get(&key) {
        if let Value::ZSet(zset) = &entry.value {
            let mut result = Vec::new();
            for i in 2..items.len() {
                let member = match &items[i] {
                    Resp::BulkString(Some(b)) => b.clone(),
                    _ => {
                        result.push(Resp::BulkString(None));
                        continue;
                    }
                };

                if let Some(score) = zset.members.get(&member) {
                     let hash = crate::geo::GeoHashBits { bits: *score as u64, step: 26 };
                     let (lat, lon) = geohash_decode(hash);
                     let geohash_str = geohash_to_base32(lat, lon);
                     result.push(Resp::BulkString(Some(Bytes::from(geohash_str))));
                } else {
                    result.push(Resp::BulkString(None));
                }
            }
            Resp::Array(Some(result))
        } else {
            Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        }
    } else {
        // Key not found
        let mut result = Vec::new();
        for _ in 2..items.len() {
            result.push(Resp::BulkString(None));
        }
        Resp::Array(Some(result))
    }
}

pub fn geopos(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'geopos' command".to_string());
    }

    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        _ => return Resp::Error("ERR key must be a string".to_string()),
    };

    if let Some(entry) = db.get(&key) {
        if let Value::ZSet(zset) = &entry.value {
            let mut result = Vec::new();
            for i in 2..items.len() {
                let member = match &items[i] {
                    Resp::BulkString(Some(b)) => b.clone(),
                    _ => {
                        result.push(Resp::Array(None));
                        continue;
                    }
                };

                if let Some(score) = zset.members.get(&member) {
                     let hash = crate::geo::GeoHashBits { bits: *score as u64, step: 26 };
                     let (lat, lon) = geohash_decode(hash);
                     
                     let pos = vec![
                         Resp::BulkString(Some(Bytes::from(lon.to_string()))),
                         Resp::BulkString(Some(Bytes::from(lat.to_string()))),
                     ];
                     result.push(Resp::Array(Some(pos)));
                } else {
                    result.push(Resp::Array(None));
                }
            }
            Resp::Array(Some(result))
        } else {
            Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        }
    } else {
        // Key not found
        let mut result = Vec::new();
        for _ in 2..items.len() {
            result.push(Resp::Array(None));
        }
        Resp::Array(Some(result))
    }
}

struct GeoRadiusOptions {
    with_coord: bool,
    with_dist: bool,
    with_hash: bool,
    count: Option<usize>,
    sort_asc: bool,
}

fn parse_georadius_options(items: &[Resp], start_index: usize) -> Result<GeoRadiusOptions, String> {
    let mut opts = GeoRadiusOptions {
        with_coord: false,
        with_dist: false,
        with_hash: false,
        count: None,
        sort_asc: true,
    };

    let mut i = start_index;
    while i < items.len() {
        if let Resp::BulkString(Some(b)) = &items[i] {
            let s = std::str::from_utf8(b).unwrap_or("").to_lowercase();
            match s.as_str() {
                "withcoord" => opts.with_coord = true,
                "withdist" => opts.with_dist = true,
                "withhash" => opts.with_hash = true,
                "count" => {
                    if i + 1 < items.len() {
                        if let Resp::BulkString(Some(cb)) = &items[i+1] {
                             if let Ok(c) = std::str::from_utf8(cb).unwrap_or("0").parse::<usize>() {
                                 opts.count = Some(c);
                                 i += 1;
                             } else {
                                 return Err("ERR value is not an integer or out of range".to_string());
                             }
                        } else {
                             return Err("ERR value is not an integer or out of range".to_string());
                        }
                    } else {
                        return Err("ERR syntax error".to_string());
                    }
                },
                "asc" => opts.sort_asc = true,
                "desc" => opts.sort_asc = false,
                "store" | "storedist" => return Err("ERR STORE option not supported".to_string()),
                _ => {}, 
            }
        }
        i += 1;
    }
    Ok(opts)
}

fn georadius_generic(
    db: &Db,
    key: &Bytes,
    lat_center: f64,
    lon_center: f64,
    radius_meters: f64,
    unit_scale: f64,
    opts: GeoRadiusOptions,
) -> Resp {
    if let Some(entry) = db.get(key) {
        if let Value::ZSet(zset) = &entry.value {
            struct GeoPoint {
                member: Bytes,
                dist: f64,
                score: f64,
                lat: f64,
                lon: f64,
            }

            let mut points = Vec::new();

            for (member, score) in &zset.members {
                let hash = crate::geo::GeoHashBits { bits: *score as u64, step: 26 };
                let (lat, lon) = geohash_decode(hash);
                let dist = calc_dist(lat, lon, lat_center, lon_center);

                if dist <= radius_meters {
                    points.push(GeoPoint {
                        member: member.clone(),
                        dist,
                        score: *score,
                        lat,
                        lon,
                    });
                }
            }

            if opts.sort_asc {
                points.sort_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap_or(Ordering::Equal));
            } else {
                points.sort_by(|a, b| b.dist.partial_cmp(&a.dist).unwrap_or(Ordering::Equal));
            }

            if let Some(c) = opts.count {
                if points.len() > c {
                    points.truncate(c);
                }
            }

            let mut result = Vec::new();
            for p in points {
                if !opts.with_coord && !opts.with_dist && !opts.with_hash {
                    result.push(Resp::BulkString(Some(p.member)));
                } else {
                    let mut item = Vec::new();
                    item.push(Resp::BulkString(Some(p.member)));
                    
                    if opts.with_dist {
                        let d = p.dist / unit_scale;
                        item.push(Resp::BulkString(Some(Bytes::from(d.to_string()))));
                    }
                    
                    if opts.with_hash {
                        item.push(Resp::Integer(p.score as i64));
                    }
                    
                    if opts.with_coord {
                         let pos = vec![
                             Resp::BulkString(Some(Bytes::from(p.lon.to_string()))),
                             Resp::BulkString(Some(Bytes::from(p.lat.to_string()))),
                         ];
                         item.push(Resp::Array(Some(pos)));
                    }
                    
                    result.push(Resp::Array(Some(item)));
                }
            }

            Resp::Array(Some(result))

        } else {
            Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        }
    } else {
        Resp::Array(Some(Vec::new()))
    }
}

pub fn georadius(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 6 {
        return Resp::Error("ERR wrong number of arguments for 'georadius' command".to_string());
    }

    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        _ => return Resp::Error("ERR key must be a string".to_string()),
    };

    let lon_center: f64 = match &items[2] {
        Resp::BulkString(Some(b)) => match std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => return Resp::Error("ERR value is not a valid float".to_string()),
        },
        _ => return Resp::Error("ERR value is not a valid float".to_string()),
    };

    let lat_center: f64 = match &items[3] {
        Resp::BulkString(Some(b)) => match std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => return Resp::Error("ERR value is not a valid float".to_string()),
        },
        _ => return Resp::Error("ERR value is not a valid float".to_string()),
    };

    let radius: f64 = match &items[4] {
        Resp::BulkString(Some(b)) => match std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => return Resp::Error("ERR value is not a valid float".to_string()),
        },
        _ => return Resp::Error("ERR value is not a valid float".to_string()),
    };

    let unit_scale = match &items[5] {
        Resp::BulkString(Some(b)) => match std::str::from_utf8(b).unwrap_or("m") {
            "m" => 1.0,
            "km" => 1000.0,
            "mi" => 1609.34,
            "ft" => 0.3048,
            _ => return Resp::Error("ERR unsupported unit provided. please use m, km, ft, mi".to_string()),
        },
        _ => return Resp::Error("ERR unsupported unit provided. please use m, km, ft, mi".to_string()),
    };

    let opts = match parse_georadius_options(items, 6) {
        Ok(o) => o,
        Err(e) => return Resp::Error(e),
    };

    let radius_meters = radius * unit_scale;

    georadius_generic(db, &key, lat_center, lon_center, radius_meters, unit_scale, opts)
}

pub fn georadiusbymember(items: &[Resp], db: &Db) -> Resp {
    // GEORADIUSBYMEMBER key member radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC] [STORE key] [STOREDIST key]
    if items.len() < 5 {
        return Resp::Error("ERR wrong number of arguments for 'georadiusbymember' command".to_string());
    }

    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        _ => return Resp::Error("ERR key must be a string".to_string()),
    };

    let member = match &items[2] {
        Resp::BulkString(Some(b)) => b.clone(),
        _ => return Resp::Error("ERR member must be a string".to_string()),
    };

    let radius: f64 = match &items[3] {
        Resp::BulkString(Some(b)) => match std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => return Resp::Error("ERR value is not a valid float".to_string()),
        },
        _ => return Resp::Error("ERR value is not a valid float".to_string()),
    };

    let unit_scale = match &items[4] {
        Resp::BulkString(Some(b)) => match std::str::from_utf8(b).unwrap_or("m") {
            "m" => 1.0,
            "km" => 1000.0,
            "mi" => 1609.34,
            "ft" => 0.3048,
            _ => return Resp::Error("ERR unsupported unit provided. please use m, km, ft, mi".to_string()),
        },
        _ => return Resp::Error("ERR unsupported unit provided. please use m, km, ft, mi".to_string()),
    };

    let opts = match parse_georadius_options(items, 5) {
        Ok(o) => o,
        Err(e) => return Resp::Error(e),
    };

    // Find member coordinates
    let (lat_center, lon_center) = if let Some(entry) = db.get(&key) {
        if let Value::ZSet(zset) = &entry.value {
            if let Some(score) = zset.members.get(&member) {
                 let hash = crate::geo::GeoHashBits { bits: *score as u64, step: 26 };
                 geohash_decode(hash)
            } else {
                return Resp::Error("ERR could not decode requested zset member".to_string());
            }
        } else {
            return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
        }
    } else {
        return Resp::Error("ERR could not decode requested zset member".to_string());
    };

    let radius_meters = radius * unit_scale;

    georadius_generic(db, &key, lat_center, lon_center, radius_meters, unit_scale, opts)
}
