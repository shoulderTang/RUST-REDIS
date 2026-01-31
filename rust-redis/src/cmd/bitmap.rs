use crate::db::{Db, Entry, Value};
use crate::resp::{Resp, as_bytes};
use bytes::Bytes;

pub fn setbit(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 4 {
        return Resp::Error("ERR wrong number of arguments for 'SETBIT'".to_string());
    }

    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let offset = match as_bytes(&items[2]) {
        Some(b) => match String::from_utf8_lossy(&b).parse::<u64>() {
            Ok(v) => v,
            Err(_) => return Resp::Error("ERR bit offset is not an integer or out of range".to_string()),
        },
        None => return Resp::Error("ERR bit offset is not an integer or out of range".to_string()),
    };

    // Redis max offset is 2^32 - 1 (512MB)
    if offset >= 4294967296 {
        return Resp::Error("ERR bit offset is not an integer or out of range".to_string());
    }

    let value = match as_bytes(&items[3]) {
        Some(b) => match String::from_utf8_lossy(&b).parse::<u8>() {
            Ok(v) if v == 0 || v == 1 => v,
            _ => return Resp::Error("ERR bit is not an integer or out of range".to_string()),
        },
        None => return Resp::Error("ERR bit is not an integer or out of range".to_string()),
    };

    let byte_offset = (offset / 8) as usize;
    let bit_in_byte = (7 - (offset % 8)) as u8;

    if let Some(mut entry) = db.get_mut(&key) {
        if entry.is_expired() {
            drop(entry);
            // Treat as new
            let mut data = vec![0u8; byte_offset + 1];
            if value == 1 {
                data[byte_offset] |= 1 << bit_in_byte;
            }
            db.insert(key, Entry::new(Value::String(Bytes::from(data)), None));
            return Resp::Integer(0);
        }

        match &mut entry.value {
            Value::String(s) => {
                let mut vec = s.to_vec();
                if byte_offset >= vec.len() {
                    vec.resize(byte_offset + 1, 0);
                }

                let old_byte = vec[byte_offset];
                let old_bit = (old_byte >> bit_in_byte) & 1;

                if value == 1 {
                    vec[byte_offset] |= 1 << bit_in_byte;
                } else {
                    vec[byte_offset] &= !(1 << bit_in_byte);
                }

                *s = Bytes::from(vec);
                Resp::Integer(old_bit as i64)
            }
            _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        // Create new
        let mut data = vec![0u8; byte_offset + 1];
        if value == 1 {
            data[byte_offset] |= 1 << bit_in_byte;
        }
        db.insert(key, Entry::new(Value::String(Bytes::from(data)), None));
        Resp::Integer(0)
    }
}

pub fn getbit(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'GETBIT'".to_string());
    }

    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let offset = match as_bytes(&items[2]) {
        Some(b) => match String::from_utf8_lossy(&b).parse::<u64>() {
            Ok(v) => v,
            Err(_) => return Resp::Error("ERR bit offset is not an integer or out of range".to_string()),
        },
        None => return Resp::Error("ERR bit offset is not an integer or out of range".to_string()),
    };

    if let Some(entry) = db.get(&key) {
        if entry.is_expired() {
            return Resp::Integer(0);
        }

        match &entry.value {
            Value::String(s) => {
                let byte_offset = (offset / 8) as usize;
                let bit_in_byte = (7 - (offset % 8)) as u8;

                if byte_offset >= s.len() {
                    return Resp::Integer(0);
                }

                let byte = s.as_ref()[byte_offset];
                let bit = (byte >> bit_in_byte) & 1;
                Resp::Integer(bit as i64)
            }
            _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        Resp::Integer(0)
    }
}

pub fn bitcount(items: &[Resp], db: &Db) -> Resp {
    if items.len() != 2 && items.len() != 4 && items.len() != 5 {
        return Resp::Error("ERR wrong number of arguments for 'bitcount' command".to_string());
    }

    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    if let Some(entry) = db.get(&key) {
        if entry.is_expired() {
            return Resp::Integer(0);
        }

        match &entry.value {
            Value::String(s) => {
                let data = s.as_ref();
                let len = data.len() as i64;
                if len == 0 {
                    return Resp::Integer(0);
                }

                let mut start = 0;
                let mut end = len - 1;
                let mut is_bit = false;

                if items.len() >= 4 {
                    start = match as_bytes(&items[2]) {
                        Some(b) => String::from_utf8_lossy(b).parse::<i64>().unwrap_or(0),
                        None => 0,
                    };
                    end = match as_bytes(&items[3]) {
                        Some(b) => String::from_utf8_lossy(b).parse::<i64>().unwrap_or(len - 1),
                        None => len - 1,
                    };

                    if items.len() == 5 {
                        if let Some(arg) = as_bytes(&items[4]) {
                            let arg_str = String::from_utf8_lossy(arg).to_uppercase();
                            if arg_str == "BIT" {
                                is_bit = true;
                            } else if arg_str != "BYTE" {
                                return Resp::Error("ERR syntax error".to_string());
                            }
                        }
                    }
                }

                let (s_idx, e_idx) = if is_bit {
                    // Bit-based range
                    let bit_len = len * 8;
                    if start < 0 { start += bit_len; }
                    if end < 0 { end += bit_len; }
                    if start < 0 { start = 0; }
                    if end < 0 { end = -1; }
                    if start >= bit_len { return Resp::Integer(0); }
                    if end >= bit_len { end = bit_len - 1; }
                    if start > end { return Resp::Integer(0); }
                    
                    // This is a simplified bit-based count for now
                    // Redis standard says if it's BIT, start/end are bit offsets
                    let mut count = 0;
                    for bit_idx in start..=end {
                        let byte_idx = (bit_idx / 8) as usize;
                        let bit_in_byte = (7 - (bit_idx % 8)) as u8;
                        if (data[byte_idx] >> bit_in_byte) & 1 == 1 {
                            count += 1;
                        }
                    }
                    return Resp::Integer(count);
                } else {
                    // Byte-based range
                    if start < 0 { start += len; }
                    if end < 0 { end += len; }
                    if start < 0 { start = 0; }
                    if end < 0 { end = -1; }
                    if start >= len { return Resp::Integer(0); }
                    if end >= len { end = len - 1; }
                    if start > end { return Resp::Integer(0); }
                    (start as usize, end as usize)
                };

                let mut count = 0;
                for byte in &data[s_idx..=e_idx] {
                    count += byte.count_ones() as i64;
                }
                Resp::Integer(count)
            }
            _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        Resp::Integer(0)
    }
}

pub fn bitpos(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 || items.len() > 6 {
        return Resp::Error("ERR wrong number of arguments for 'bitpos' command".to_string());
    }

    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    let bit = match as_bytes(&items[2]) {
        Some(b) => match String::from_utf8_lossy(b).parse::<u8>() {
            Ok(v) if v == 0 || v == 1 => v,
            _ => return Resp::Error("ERR The bit argument must be 1 or 0.".to_string()),
        },
        None => return Resp::Error("ERR The bit argument must be 1 or 0.".to_string()),
    };

    if let Some(entry) = db.get(&key) {
        if entry.is_expired() {
            return if bit == 0 { Resp::Integer(0) } else { Resp::Integer(-1) };
        }

        match &entry.value {
            Value::String(s) => {
                let data = s.as_ref();
                let len = data.len() as i64;
                if len == 0 {
                    return if bit == 0 { Resp::Integer(0) } else { Resp::Integer(-1) };
                }

                let mut start = 0;
                let mut end = len - 1;
                let mut is_bit = false;
                let mut has_end = false;

                if items.len() >= 4 {
                    start = match as_bytes(&items[3]) {
                        Some(b) => String::from_utf8_lossy(b).parse::<i64>().unwrap_or(0),
                        None => 0,
                    };
                    if items.len() >= 5 {
                        end = match as_bytes(&items[4]) {
                            Some(b) => { has_end = true; String::from_utf8_lossy(b).parse::<i64>().unwrap_or(len - 1) },
                            None => len - 1,
                        };
                    }
                    if items.len() == 6 {
                        if let Some(arg) = as_bytes(&items[5]) {
                            let arg_str = String::from_utf8_lossy(arg).to_uppercase();
                            if arg_str == "BIT" {
                                is_bit = true;
                            } else if arg_str != "BYTE" {
                                return Resp::Error("ERR syntax error".to_string());
                            }
                        }
                    }
                }

                if is_bit {
                    let bit_len = len * 8;
                    if start < 0 { start += bit_len; }
                    if end < 0 { end += bit_len; }
                    if start < 0 { start = 0; }
                    if start >= bit_len { 
                        return if bit == 0 { Resp::Integer(start) } else { Resp::Integer(-1) };
                    }
                    if end >= bit_len { end = bit_len - 1; }
                    
                    for bit_idx in start..=end {
                        let byte_idx = (bit_idx / 8) as usize;
                        let bit_in_byte = (7 - (bit_idx % 8)) as u8;
                        if ((data[byte_idx] >> bit_in_byte) & 1) == bit {
                            return Resp::Integer(bit_idx);
                        }
                    }
                } else {
                    if start < 0 { start += len; }
                    if end < 0 { end += len; }
                    if start < 0 { start = 0; }
                    if start >= len {
                        return if bit == 0 { Resp::Integer(start * 8) } else { Resp::Integer(-1) };
                    }
                    if end >= len { end = len - 1; }
                    
                    for byte_idx in (start as usize)..=(end as usize) {
                        let byte = data[byte_idx];
                        if bit == 1 {
                            if byte != 0 {
                                for i in 0..8 {
                                    if (byte >> (7 - i)) & 1 == 1 {
                                        return Resp::Integer((byte_idx as i64 * 8) + i as i64);
                                    }
                                }
                            }
                        } else {
                            if byte != 0xFF {
                                for i in 0..8 {
                                    if (byte >> (7 - i)) & 1 == 0 {
                                        return Resp::Integer((byte_idx as i64 * 8) + i as i64);
                                    }
                                }
                            }
                        }
                    }
                }

                // If not found and searching for 0, it might be beyond the string
                if bit == 0 && !has_end {
                    return Resp::Integer(len * 8);
                }
                Resp::Integer(-1)
            }
            _ => Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    } else {
        if bit == 0 { Resp::Integer(0) } else { Resp::Integer(-1) }
    }
}

pub fn bitop(items: &[Resp], db: &Db) -> (Resp, Option<Resp>) {
    if items.len() < 4 {
        return (Resp::Error("ERR wrong number of arguments for 'bitop' command".to_string()), None);
    }

    let op = match as_bytes(&items[1]) {
        Some(b) => String::from_utf8_lossy(b).to_uppercase(),
        None => return (Resp::Error("ERR syntax error".to_string()), None),
    };

    let dest_key = match &items[2] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return (Resp::Error("ERR invalid destkey".to_string()), None),
    };

    let mut src_data = Vec::new();
    let mut max_len = 0;

    for i in 3..items.len() {
        let key = match &items[i] {
            Resp::BulkString(Some(b)) => b.clone(),
            Resp::SimpleString(s) => s.clone(),
            _ => continue,
        };
        if let Some(entry) = db.get(&key) {
            if !entry.is_expired() {
                if let Value::String(s) = &entry.value {
                    let d = s.as_ref().to_vec();
                    if d.len() > max_len { max_len = d.len(); }
                    src_data.push(d);
                    continue;
                } else {
                    return (Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()), None);
                }
            }
        }
        src_data.push(Vec::new());
    }

    if op == "NOT" && src_data.len() != 1 {
        return (Resp::Error("ERR BITOP NOT must be called with a single source key.".to_string()), None);
    }

    let mut res = vec![0u8; max_len];
    match op.as_str() {
        "AND" => {
            res.fill(0xFF);
            for data in src_data {
                for i in 0..max_len {
                    let val = if i < data.len() { data[i] } else { 0 };
                    res[i] &= val;
                }
            }
        }
        "OR" => {
            for data in src_data {
                for i in 0..data.len() {
                    res[i] |= data[i];
                }
            }
        }
        "XOR" => {
            for data in src_data {
                for i in 0..data.len() {
                    res[i] ^= data[i];
                }
            }
        }
        "NOT" => {
            let data = &src_data[0];
            for i in 0..max_len {
                res[i] = !data[i];
            }
        }
        _ => return (Resp::Error("ERR syntax error".to_string()), None),
    }

    db.insert(dest_key, Entry::new(Value::String(Bytes::from(res)), None));
    
    let mut log_args = Vec::new();
    for item in items { log_args.push(item.clone()); }
    (Resp::Integer(max_len as i64), Some(Resp::Array(Some(log_args))))
}

pub fn bitfield(items: &[Resp], db: &Db) -> (Resp, Option<Resp>) {
    if items.len() < 2 {
        return (Resp::Error("ERR wrong number of arguments for 'bitfield' command".to_string()), None);
    }

    let key = match &items[1] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(s) => s.clone(),
        _ => return (Resp::Error("ERR invalid key".to_string()), None),
    };

    let mut results = Vec::new();
    let mut overflow = "WRAP"; // WRAP, SAT, FAIL
    let mut needs_log = false;

    // We need to work with a mutable copy or direct DB access
    // For simplicity, let's get the data first
    let mut data = if let Some(mut entry) = db.get_mut(&key) {
        if entry.is_expired() {
            Vec::new()
        } else {
            match &mut entry.value {
                Value::String(s) => s.to_vec(),
                _ => return (Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()), None),
            }
        }
    } else {
        Vec::new()
    };

    let mut i = 2;
    while i < items.len() {
        let sub = match as_bytes(&items[i]) {
            Some(b) => String::from_utf8_lossy(b).to_uppercase(),
            None => break,
        };

        match sub.as_str() {
            "OVERFLOW" => {
                if i + 1 >= items.len() { break; }
                overflow = match as_bytes(&items[i+1]) {
                    Some(b) => {
                        let s = String::from_utf8_lossy(b).to_uppercase();
                        if s == "WRAP" || s == "SAT" || s == "FAIL" {
                            // We'll store it in a local variable that survives the loop
                            // but for now let's just use a &str
                            if s == "WRAP" { "WRAP" } else if s == "SAT" { "SAT" } else { "FAIL" }
                        } else {
                            return (Resp::Error("ERR syntax error".to_string()), None);
                        }
                    }
                    None => "WRAP",
                };
                i += 2;
            }
            "GET" | "SET" | "INCRBY" => {
                if i + 2 >= items.len() { break; }
                let type_str = match as_bytes(&items[i+1]) {
                    Some(b) => String::from_utf8_lossy(b).to_string(),
                    None => break,
                };
                let offset_str = match as_bytes(&items[i+2]) {
                    Some(b) => String::from_utf8_lossy(b).to_string(),
                    None => break,
                };

                let (is_signed, bits) = if type_str.starts_with('i') {
                    (true, type_str[1..].parse::<u32>().unwrap_or(0))
                } else if type_str.starts_with('u') {
                    (false, type_str[1..].parse::<u32>().unwrap_or(0))
                } else {
                    return (Resp::Error("ERR invalid bitfield type".to_string()), None);
                };

                if bits == 0 || bits > 64 || (is_signed && bits > 64) || (!is_signed && bits > 63) {
                     return (Resp::Error("ERR invalid bitfield type".to_string()), None);
                }

                let bit_offset = if offset_str.starts_with('#') {
                    offset_str[1..].parse::<u64>().unwrap_or(0) * bits as u64
                } else {
                    offset_str.parse::<u64>().unwrap_or(0)
                };

                if sub == "GET" {
                    results.push(Resp::Integer(get_bits(&data, bit_offset, bits, is_signed)));
                    i += 3;
                } else if sub == "SET" {
                    if i + 3 >= items.len() { break; }
                    let val = match as_bytes(&items[i+3]) {
                        Some(b) => String::from_utf8_lossy(b).parse::<i64>().unwrap_or(0),
                        None => 0,
                    };
                    let old = get_bits(&data, bit_offset, bits, is_signed);
                    set_bits(&mut data, bit_offset, bits, val, is_signed);
                    results.push(Resp::Integer(old));
                    needs_log = true;
                    i += 4;
                } else if sub == "INCRBY" {
                    if i + 3 >= items.len() { break; }
                    let incr = match as_bytes(&items[i+3]) {
                        Some(b) => String::from_utf8_lossy(b).parse::<i64>().unwrap_or(0),
                        None => 0,
                    };
                    let old = get_bits(&data, bit_offset, bits, is_signed);
                    let (new_val, ok) = incr_bits(old, incr, bits, is_signed, overflow);
                    if ok {
                        set_bits(&mut data, bit_offset, bits, new_val, is_signed);
                        results.push(Resp::Integer(new_val));
                    } else {
                        results.push(Resp::BulkString(None));
                    }
                    needs_log = true;
                    i += 4;
                }
            }
            _ => return (Resp::Error("ERR syntax error".to_string()), None),
        }
    }

    if needs_log {
        db.insert(key, Entry::new(Value::String(Bytes::from(data)), None));
        let mut log_args = Vec::new();
        for item in items { log_args.push(item.clone()); }
        (Resp::Array(Some(results)), Some(Resp::Array(Some(log_args))))
    } else {
        (Resp::Array(Some(results)), None)
    }
}

fn get_bits(data: &[u8], offset: u64, bits: u32, is_signed: bool) -> i64 {
    let mut val = 0u64;
    for i in 0..bits {
        let bit_idx = offset + i as u64;
        let byte_idx = (bit_idx / 8) as usize;
        if byte_idx >= data.len() { break; }
        let bit_in_byte = (7 - (bit_idx % 8)) as u8;
        if (data[byte_idx] >> bit_in_byte) & 1 == 1 {
            val |= 1 << (bits - 1 - i);
        }
    }

    if is_signed {
        let sign_bit = 1 << (bits - 1);
        if val & sign_bit != 0 {
            // Negative
            if bits == 64 {
                val as i64
            } else {
                let mask = (1 << bits) - 1;
                let val_signed = (val as i64) | !mask;
                val_signed
            }
        } else {
            val as i64
        }
    } else {
        val as i64
    }
}

fn set_bits(data: &mut Vec<u8>, offset: u64, bits: u32, val: i64, _is_signed: bool) {
    let uval = val as u64;
    let max_bit = offset + bits as u64;
    let max_byte = (max_bit + 7) / 8;
    if data.len() < max_byte as usize {
        data.resize(max_byte as usize, 0);
    }

    for i in 0..bits {
        let bit_idx = offset + i as u64;
        let byte_idx = (bit_idx / 8) as usize;
        let bit_in_byte = (7 - (bit_idx % 8)) as u8;
        let bit_val = (uval >> (bits - 1 - i)) & 1;
        if bit_val == 1 {
            data[byte_idx] |= 1 << bit_in_byte;
        } else {
            data[byte_idx] &= !(1 << bit_in_byte);
        }
    }
}

fn incr_bits(old: i64, incr: i64, bits: u32, is_signed: bool, overflow: &str) -> (i64, bool) {
    if is_signed {
        let min = if bits == 64 { i64::MIN } else { -(1 << (bits - 1)) };
        let max = if bits == 64 { i64::MAX } else { (1 << (bits - 1)) - 1 };
        
        let (new_val, over) = old.overflowing_add(incr);
        let mut final_val = new_val;
        
        // Manual range check for sub-64 bit types
        if bits < 64 {
            if final_val > max || (over && incr > 0) {
                match overflow {
                    "WRAP" => {
                        let range = 1i128 << bits;
                        let mut v = final_val as i128;
                        while v > max as i128 { v -= range; }
                        while v < min as i128 { v += range; }
                        final_val = v as i64;
                    }
                    "SAT" => final_val = max,
                    "FAIL" => return (0, false),
                    _ => {}
                }
            } else if final_val < min || (over && incr < 0) {
                match overflow {
                    "WRAP" => {
                        let range = 1i128 << bits;
                        let mut v = final_val as i128;
                        while v > max as i128 { v -= range; }
                        while v < min as i128 { v += range; }
                        final_val = v as i64;
                    }
                    "SAT" => final_val = min,
                    "FAIL" => return (0, false),
                    _ => {}
                }
            }
        } else if over {
             match overflow {
                "WRAP" => {}, // already wrapped by overflowing_add
                "SAT" => final_val = if incr > 0 { max } else { min },
                "FAIL" => return (0, false),
                _ => {}
            }
        }
        (final_val, true)
    } else {
        let max = (1u64 << bits) - 1;
        let uold = old as u64;
        let uincr = incr as u64;
        
        let (new_val, over) = uold.overflowing_add(uincr);
        let mut final_val = new_val;

        if bits < 64 {
             if final_val > max || over {
                match overflow {
                    "WRAP" => final_val &= max,
                    "SAT" => final_val = max,
                    "FAIL" => return (0, false),
                    _ => {}
                }
            }
        } else if over {
            match overflow {
                "WRAP" => {},
                "SAT" => final_val = max,
                "FAIL" => return (0, false),
                _ => {}
            }
        }
        (final_val as i64, true)
    }
}
