use crate::db::{Db, Entry, SortedSet, TotalOrderF64, Value};
use crate::resp::Resp;
use crate::geo::{geohash_encode, geohash_decode, geohash_to_base32, geodist as calc_dist, is_in_box};
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
        let mut result = Vec::new();
        for _ in 2..items.len() {
            result.push(Resp::Array(None));
        }
        Resp::Array(Some(result))
    }
}

enum GeoShape {
    Circle { radius_m: f64 },
    Box { width_m: f64, height_m: f64 },
}

#[derive(Clone)]
struct GeoSearchOptions {
    with_coord: bool,
    with_dist: bool,
    with_hash: bool,
    count: Option<usize>,
    any: bool,
    sort_asc: Option<bool>,
    store: Option<Bytes>,
    store_dist: bool,
}

fn get_unit_scale(unit: &str) -> Result<f64, String> {
    match unit {
        "m" => Ok(1.0),
        "km" => Ok(1000.0),
        "mi" => Ok(1609.34),
        "ft" => Ok(0.3048),
        _ => Err("ERR unsupported unit provided. please use m, km, ft, mi".to_string()),
    }
}

fn geosearch_generic(
    db: &Db,
    key: &Bytes,
    lat_center: f64,
    lon_center: f64,
    shape: GeoShape,
    unit_scale: f64,
    opts: GeoSearchOptions,
) -> Resp {
    let entry = match db.get(key) {
        Some(e) => e,
        None => return if opts.store.is_some() { Resp::Integer(0) } else { Resp::Array(Some(Vec::new())) },
    };

    let zset = match &entry.value {
        Value::ZSet(zset) => zset,
        _ => return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    };

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

        let in_shape = match shape {
            GeoShape::Circle { radius_m } => dist <= radius_m,
            GeoShape::Box { width_m, height_m } => is_in_box(lat, lon, lat_center, lon_center, width_m, height_m),
        };

        if in_shape {
            points.push(GeoPoint {
                member: member.clone(),
                dist,
                score: *score,
                lat,
                lon,
            });
        }
    }

    if let Some(asc) = opts.sort_asc {
        if asc {
            points.sort_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap_or(Ordering::Equal));
        } else {
            points.sort_by(|a, b| b.dist.partial_cmp(&a.dist).unwrap_or(Ordering::Equal));
        }
    }

    if let Some(c) = opts.count {
        if points.len() > c {
            points.truncate(c);
        }
    }

    if let Some(dest_key) = opts.store {
        let mut dest_zset = SortedSet::new();
        let count = points.len() as i64;
        for p in points {
            let score = if opts.store_dist { p.dist / unit_scale } else { p.score };
            dest_zset.members.insert(p.member.clone(), score);
            dest_zset.scores.insert((TotalOrderF64(score), p.member));
        }
        db.insert(dest_key, Entry::new(Value::ZSet(dest_zset), None));
        Resp::Integer(count)
    } else {
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
        Resp::BulkString(Some(b)) => std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()).unwrap_or(0.0),
        _ => 0.0,
    };

    let lat_center: f64 = match &items[3] {
        Resp::BulkString(Some(b)) => std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()).unwrap_or(0.0),
        _ => 0.0,
    };

    let radius: f64 = match &items[4] {
        Resp::BulkString(Some(b)) => std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()).unwrap_or(0.0),
        _ => 0.0,
    };

    let unit = match &items[5] {
        Resp::BulkString(Some(b)) => std::str::from_utf8(b).unwrap_or("m").to_lowercase(),
        _ => "m".to_string(),
    };

    let unit_scale = match get_unit_scale(&unit) {
        Ok(s) => s,
        Err(e) => return Resp::Error(e),
    };

    let mut opts = GeoSearchOptions {
        with_coord: false,
        with_dist: false,
        with_hash: false,
        count: None,
        any: false,
        sort_asc: Some(true),
        store: None,
        store_dist: false,
    };

    let mut i = 6;
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
                             opts.count = std::str::from_utf8(cb).ok().and_then(|s| s.parse().ok());
                             i += 1;
                        }
                    }
                },
                "asc" => opts.sort_asc = Some(true),
                "desc" => opts.sort_asc = Some(false),
                "store" => {
                    if i + 1 < items.len() {
                        if let Resp::BulkString(Some(sb)) = &items[i+1] {
                            opts.store = Some(sb.clone());
                            i += 1;
                        }
                    }
                },
                "storedist" => {
                    if i + 1 < items.len() {
                        if let Resp::BulkString(Some(sb)) = &items[i+1] {
                            opts.store = Some(sb.clone());
                            opts.store_dist = true;
                            i += 1;
                        }
                    }
                },
                _ => {},
            }
        }
        i += 1;
    }

    geosearch_generic(db, &key, lat_center, lon_center, GeoShape::Circle { radius_m: radius * unit_scale }, unit_scale, opts)
}

pub fn georadiusbymember(items: &[Resp], db: &Db) -> Resp {
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
        Resp::BulkString(Some(b)) => std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()).unwrap_or(0.0),
        _ => 0.0,
    };

    let unit = match &items[4] {
        Resp::BulkString(Some(b)) => std::str::from_utf8(b).unwrap_or("m").to_lowercase(),
        _ => "m".to_string(),
    };

    let unit_scale = match get_unit_scale(&unit) {
        Ok(s) => s,
        Err(e) => return Resp::Error(e),
    };

    // Find member coordinates
    let (lat_center, lon_center) = match db.get(&key) {
        Some(entry) => {
            if let Value::ZSet(zset) = &entry.value {
                match zset.members.get(&member) {
                    Some(score) => {
                        let hash = crate::geo::GeoHashBits { bits: *score as u64, step: 26 };
                        geohash_decode(hash)
                    },
                    None => return Resp::Error("ERR could not decode requested zset member".to_string()),
                }
            } else {
                return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
            }
        },
        None => return Resp::Error("ERR could not decode requested zset member".to_string()),
    };

    let mut opts = GeoSearchOptions {
        with_coord: false,
        with_dist: false,
        with_hash: false,
        count: None,
        any: false,
        sort_asc: Some(true),
        store: None,
        store_dist: false,
    };

    let mut i = 5;
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
                             opts.count = std::str::from_utf8(cb).ok().and_then(|s| s.parse().ok());
                             i += 1;
                        }
                    }
                },
                "asc" => opts.sort_asc = Some(true),
                "desc" => opts.sort_asc = Some(false),
                "store" => {
                    if i + 1 < items.len() {
                        if let Resp::BulkString(Some(sb)) = &items[i+1] {
                            opts.store = Some(sb.clone());
                            i += 1;
                        }
                    }
                },
                "storedist" => {
                    if i + 1 < items.len() {
                        if let Resp::BulkString(Some(sb)) = &items[i+1] {
                            opts.store = Some(sb.clone());
                            opts.store_dist = true;
                            i += 1;
                        }
                    }
                },
                _ => {},
            }
        }
        i += 1;
    }

    geosearch_generic(db, &key, lat_center, lon_center, GeoShape::Circle { radius_m: radius * unit_scale }, unit_scale, opts)
}

pub fn geosearch(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'geosearch' command".to_string());
    }

    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        _ => return Resp::Error("ERR key must be a string".to_string()),
    };

    let mut lat_center = 0.0;
    let mut lon_center = 0.0;
    let mut shape = None;
    let mut unit_scale = 1.0;
    
    let mut opts = GeoSearchOptions {
        with_coord: false,
        with_dist: false,
        with_hash: false,
        count: None,
        any: false,
        sort_asc: None,
        store: None,
        store_dist: false,
    };

    let mut i = 2;
    while i < items.len() {
        if let Resp::BulkString(Some(b)) = &items[i] {
            let s = std::str::from_utf8(b).unwrap_or("").to_lowercase();
            match s.as_str() {
                "frommember" => {
                    if i + 1 < items.len() {
                        if let Resp::BulkString(Some(mb)) = &items[i+1] {
                            match db.get(&key) {
                                Some(entry) => {
                                    if let Value::ZSet(zset) = &entry.value {
                                        match zset.members.get(mb) {
                                            Some(score) => {
                                                let hash = crate::geo::GeoHashBits { bits: *score as u64, step: 26 };
                                                let (lat, lon) = geohash_decode(hash);
                                                lat_center = lat;
                                                lon_center = lon;
                                            },
                                            None => return Resp::Error("ERR could not decode requested zset member".to_string()),
                                        }
                                    } else {
                                        return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
                                    }
                                },
                                None => return Resp::Error("ERR could not decode requested zset member".to_string()),
                            }
                            i += 1;
                        }
                    }
                },
                "fromlonlat" => {
                    if i + 2 < items.len() {
                        lon_center = std::str::from_utf8(match &items[i+1] { Resp::BulkString(Some(b)) => b, _ => b"" }).unwrap_or("0").parse().unwrap_or(0.0);
                        lat_center = std::str::from_utf8(match &items[i+2] { Resp::BulkString(Some(b)) => b, _ => b"" }).unwrap_or("0").parse().unwrap_or(0.0);
                        i += 2;
                    }
                },
                "byradius" => {
                    if i + 2 < items.len() {
                        let radius: f64 = std::str::from_utf8(match &items[i+1] { Resp::BulkString(Some(b)) => b, _ => b"" }).unwrap_or("0").parse().unwrap_or(0.0);
                        let unit = std::str::from_utf8(match &items[i+2] { Resp::BulkString(Some(b)) => b, _ => b"" }).unwrap_or("m").to_lowercase();
                        unit_scale = match get_unit_scale(&unit) { Ok(s) => s, Err(e) => return Resp::Error(e) };
                        shape = Some(GeoShape::Circle { radius_m: radius * unit_scale });
                        i += 2;
                    }
                },
                "bybox" => {
                    if i + 3 < items.len() {
                        let width: f64 = std::str::from_utf8(match &items[i+1] { Resp::BulkString(Some(b)) => b, _ => b"" }).unwrap_or("0").parse().unwrap_or(0.0);
                        let height: f64 = std::str::from_utf8(match &items[i+2] { Resp::BulkString(Some(b)) => b, _ => b"" }).unwrap_or("0").parse().unwrap_or(0.0);
                        let unit = std::str::from_utf8(match &items[i+3] { Resp::BulkString(Some(b)) => b, _ => b"" }).unwrap_or("m").to_lowercase();
                        unit_scale = match get_unit_scale(&unit) { Ok(s) => s, Err(e) => return Resp::Error(e) };
                        shape = Some(GeoShape::Box { width_m: width * unit_scale, height_m: height * unit_scale });
                        i += 3;
                    }
                },
                "withcoord" => opts.with_coord = true,
                "withdist" => opts.with_dist = true,
                "withhash" => opts.with_hash = true,
                "count" => {
                    if i + 1 < items.len() {
                        if let Resp::BulkString(Some(cb)) = &items[i+1] {
                             opts.count = std::str::from_utf8(cb).ok().and_then(|s| s.parse().ok());
                             i += 1;
                        }
                        if i + 1 < items.len() {
                            if let Resp::BulkString(Some(ab)) = &items[i+1] {
                                if std::str::from_utf8(ab).unwrap_or("").to_lowercase() == "any" {
                                    opts.any = true;
                                    i += 1;
                                }
                            }
                        }
                    }
                },
                "asc" => opts.sort_asc = Some(true),
                "desc" => opts.sort_asc = Some(false),
                _ => {},
            }
        }
        i += 1;
    }

    if let Some(s) = shape {
        geosearch_generic(db, &key, lat_center, lon_center, s, unit_scale, opts)
    } else {
        Resp::Error("ERR exactly one of BYRADIUS or BYBOX can be specified".to_string())
    }
}

pub fn geosearchstore(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'geosearchstore' command".to_string());
    }

    let dest = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        _ => return Resp::Error("ERR destination must be a string".to_string()),
    };

    let key = match &items[2] {
        Resp::BulkString(Some(b)) => b.clone(),
        _ => return Resp::Error("ERR source must be a string".to_string()),
    };

    let mut lat_center = 0.0;
    let mut lon_center = 0.0;
    let mut shape = None;
    let mut unit_scale = 1.0;
    
    let mut opts = GeoSearchOptions {
        with_coord: false,
        with_dist: false,
        with_hash: false,
        count: None,
        any: false,
        sort_asc: None,
        store: Some(dest),
        store_dist: false,
    };

    let mut i = 3;
    while i < items.len() {
        if let Resp::BulkString(Some(b)) = &items[i] {
            let s = std::str::from_utf8(b).unwrap_or("").to_lowercase();
            match s.as_str() {
                "frommember" => {
                    if i + 1 < items.len() {
                        if let Resp::BulkString(Some(mb)) = &items[i+1] {
                            match db.get(&key) {
                                Some(entry) => {
                                    if let Value::ZSet(zset) = &entry.value {
                                        match zset.members.get(mb) {
                                            Some(score) => {
                                                let hash = crate::geo::GeoHashBits { bits: *score as u64, step: 26 };
                                                let (lat, lon) = geohash_decode(hash);
                                                lat_center = lat;
                                                lon_center = lon;
                                            },
                                            None => return Resp::Error("ERR could not decode requested zset member".to_string()),
                                        }
                                    } else {
                                        return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
                                    }
                                },
                                None => return Resp::Error("ERR could not decode requested zset member".to_string()),
                            }
                            i += 1;
                        }
                    }
                },
                "fromlonlat" => {
                    if i + 2 < items.len() {
                        lon_center = std::str::from_utf8(match &items[i+1] { Resp::BulkString(Some(b)) => b, _ => b"" }).unwrap_or("0").parse().unwrap_or(0.0);
                        lat_center = std::str::from_utf8(match &items[i+2] { Resp::BulkString(Some(b)) => b, _ => b"" }).unwrap_or("0").parse().unwrap_or(0.0);
                        i += 2;
                    }
                },
                "byradius" => {
                    if i + 2 < items.len() {
                        let radius: f64 = std::str::from_utf8(match &items[i+1] { Resp::BulkString(Some(b)) => b, _ => b"" }).unwrap_or("0").parse().unwrap_or(0.0);
                        let unit = std::str::from_utf8(match &items[i+2] { Resp::BulkString(Some(b)) => b, _ => b"" }).unwrap_or("m").to_lowercase();
                        unit_scale = match get_unit_scale(&unit) { Ok(s) => s, Err(e) => return Resp::Error(e) };
                        shape = Some(GeoShape::Circle { radius_m: radius * unit_scale });
                        i += 2;
                    }
                },
                "bybox" => {
                    if i + 3 < items.len() {
                        let width: f64 = std::str::from_utf8(match &items[i+1] { Resp::BulkString(Some(b)) => b, _ => b"" }).unwrap_or("0").parse().unwrap_or(0.0);
                        let height: f64 = std::str::from_utf8(match &items[i+2] { Resp::BulkString(Some(b)) => b, _ => b"" }).unwrap_or("0").parse().unwrap_or(0.0);
                        let unit = std::str::from_utf8(match &items[i+3] { Resp::BulkString(Some(b)) => b, _ => b"" }).unwrap_or("m").to_lowercase();
                        unit_scale = match get_unit_scale(&unit) { Ok(s) => s, Err(e) => return Resp::Error(e) };
                        shape = Some(GeoShape::Box { width_m: width * unit_scale, height_m: height * unit_scale });
                        i += 3;
                    }
                },
                "count" => {
                    if i + 1 < items.len() {
                        if let Resp::BulkString(Some(cb)) = &items[i+1] {
                             opts.count = std::str::from_utf8(cb).ok().and_then(|s| s.parse().ok());
                             i += 1;
                        }
                        if i + 1 < items.len() {
                            if let Resp::BulkString(Some(ab)) = &items[i+1] {
                                if std::str::from_utf8(ab).unwrap_or("").to_lowercase() == "any" {
                                    opts.any = true;
                                    i += 1;
                                }
                            }
                        }
                    }
                },
                "asc" => opts.sort_asc = Some(true),
                "desc" => opts.sort_asc = Some(false),
                "storedist" => opts.store_dist = true,
                _ => {},
            }
        }
        i += 1;
    }

    if let Some(s) = shape {
        geosearch_generic(db, &key, lat_center, lon_center, s, unit_scale, opts)
    } else {
        Resp::Error("ERR exactly one of BYRADIUS or BYBOX can be specified".to_string())
    }
}
