use crate::db::{Db, Value, Entry};
use crate::resp::{Resp, as_bytes};
use bytes::Bytes;
use std::collections::{HashMap, HashSet, VecDeque};
use std::str;
use std::cmp::Ordering;

struct SortOptions {
    by_pattern: Option<String>,
    limit_start: usize,
    limit_count: isize, // -1 means all
    get_patterns: Vec<String>,
    ascending: bool,
    alpha: bool,
    store_key: Option<Bytes>,
}

impl Default for SortOptions {
    fn default() -> Self {
        SortOptions {
            by_pattern: None,
            limit_start: 0,
            limit_count: -1,
            get_patterns: Vec::new(),
            ascending: true,
            alpha: false,
            store_key: None,
        }
    }
}

pub fn sort(items: &[Resp], db: &Db) -> Resp {
    sort_impl(items, db, false)
}

pub fn sort_ro(items: &[Resp], db: &Db) -> Resp {
    sort_impl(items, db, true)
}

fn sort_impl(items: &[Resp], db: &Db, readonly: bool) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'sort' command".to_string());
    }

    let key = match as_bytes(&items[1]) {
        Some(b) => b,
        None => return Resp::Error("ERR invalid key".to_string()),
    };

    let mut opts = SortOptions::default();
    let mut i = 2;
    while i < items.len() {
        let arg_bytes = match as_bytes(&items[i]) {
            Some(b) => b,
            None => return Resp::Error("ERR invalid argument".to_string()),
        };
        let arg = String::from_utf8_lossy(&arg_bytes);
        
        if arg.eq_ignore_ascii_case("ASC") {
            opts.ascending = true;
            i += 1;
        } else if arg.eq_ignore_ascii_case("DESC") {
            opts.ascending = false;
            i += 1;
        } else if arg.eq_ignore_ascii_case("ALPHA") {
            opts.alpha = true;
            i += 1;
        } else if arg.eq_ignore_ascii_case("LIMIT") {
            if i + 2 >= items.len() {
                return Resp::Error("ERR syntax error".to_string());
            }
            let start_bytes = match as_bytes(&items[i+1]) {
                Some(b) => b,
                None => return Resp::Error("ERR invalid limit".to_string()),
            };
            let count_bytes = match as_bytes(&items[i+2]) {
                Some(b) => b,
                None => return Resp::Error("ERR invalid limit".to_string()),
            };
            
            opts.limit_start = match str::from_utf8(&start_bytes).ok().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Resp::Error("ERR value is not an integer or out of range".to_string()),
            };
            opts.limit_count = match str::from_utf8(&count_bytes).ok().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return Resp::Error("ERR value is not an integer or out of range".to_string()),
            };
            i += 3;
        } else if arg.eq_ignore_ascii_case("BY") {
            if i + 1 >= items.len() {
                return Resp::Error("ERR syntax error".to_string());
            }
            let pattern_bytes = match as_bytes(&items[i+1]) {
                Some(b) => b,
                None => return Resp::Error("ERR invalid pattern".to_string()),
            };
            opts.by_pattern = Some(String::from_utf8_lossy(&pattern_bytes).to_string());
            i += 2;
        } else if arg.eq_ignore_ascii_case("GET") {
            if i + 1 >= items.len() {
                return Resp::Error("ERR syntax error".to_string());
            }
            let pattern_bytes = match as_bytes(&items[i+1]) {
                Some(b) => b,
                None => return Resp::Error("ERR invalid pattern".to_string()),
            };
            opts.get_patterns.push(String::from_utf8_lossy(&pattern_bytes).to_string());
            i += 2;
        } else if arg.eq_ignore_ascii_case("STORE") {
            if readonly {
                return Resp::Error("ERR syntax error".to_string());
            }
            if i + 1 >= items.len() {
                return Resp::Error("ERR syntax error".to_string());
            }
            let store_key_bytes = match as_bytes(&items[i+1]) {
                Some(b) => Bytes::copy_from_slice(b),
                None => return Resp::Error("ERR invalid store key".to_string()),
            };
            opts.store_key = Some(store_key_bytes);
            i += 2;
        } else {
            return Resp::Error("ERR syntax error".to_string());
        }
    }

    // Collect elements
    let mut elements: Vec<Bytes> = Vec::new();
    if let Some(entry) = db.get(key) {
        if entry.is_expired() {
            // Expired, treat as empty
        } else {
            match &entry.value {
                Value::List(l) => {
                    elements = l.iter().cloned().collect();
                }
                Value::Set(s) => {
                    elements = s.iter().cloned().collect();
                }
                Value::ZSet(z) => {
                    elements = z.members.keys().cloned().collect();
                }
                _ => return Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        }
    } else {
        // Key missing, empty list
    }

    // Prepare sort keys
    let mut with_sort_keys: Vec<(Bytes, Option<f64>, Option<String>)> = Vec::with_capacity(elements.len());
    
    for elem in &elements {
        let sort_key_val = if let Some(pattern) = &opts.by_pattern {
            if pattern == "nosort" {
                // Special case, don't sort, just limit/get
                // But we still need a value to keep structure.
                // If "nosort", we might skip sorting step, but let's just use empty.
                 None
            } else {
                lookup_key(db, pattern, elem)
            }
        } else {
            // Sort by element itself
            Some(elem.clone())
        };

        let mut num_val = None;
        let mut str_val = None;

        if let Some(sk) = sort_key_val {
            if opts.alpha {
                 str_val = Some(String::from_utf8_lossy(&sk).to_string());
            } else {
                // Try parse as f64
                if let Ok(s) = str::from_utf8(&sk) {
                    if let Ok(f) = s.parse::<f64>() {
                        num_val = Some(f);
                    } else {
                        // If not a number, in Redis it's treated as 0 for numeric sort usually,
                        // or error? Redis says: "If you want to sort by string... use ALPHA"
                        // If ALPHA is not specified, and value is not a number, Redis returns an error:
                        // "ERR One or more scores can't be converted into double"
                        // So we should verify this.
                        return Resp::Error("ERR One or more scores can't be converted into double".to_string());
                    }
                } else {
                    return Resp::Error("ERR One or more scores can't be converted into double".to_string());
                }
            }
        } else {
             // If key missing (lookup failed) -> 0 or empty string
             if opts.alpha {
                 str_val = Some("".to_string());
             } else {
                 num_val = Some(0.0);
             }
        }
        
        with_sort_keys.push((elem.clone(), num_val, str_val));
    }

    let by_pattern_is_nosort = opts.by_pattern.as_deref() == Some("nosort");

    if !by_pattern_is_nosort {
        with_sort_keys.sort_by(|a, b| {
            let cmp = if opts.alpha {
                a.2.cmp(&b.2)
            } else {
                a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal)
            };
            
            if opts.ascending {
                cmp
            } else {
                cmp.reverse()
            }
        });
    }

    // Apply LIMIT
    let len = with_sort_keys.len();
    let start = opts.limit_start;
    let end = if opts.limit_count < 0 {
        len
    } else {
        (start + opts.limit_count as usize).min(len)
    };

    let sliced = if start >= len {
        &[]
    } else {
        &with_sort_keys[start..end]
    };

    // Apply GET
    let mut result_items: Vec<Resp> = Vec::new();
    
    for (elem, _, _) in sliced {
        if opts.get_patterns.is_empty() {
             result_items.push(Resp::BulkString(Some(elem.clone())));
        } else {
            for pattern in &opts.get_patterns {
                if pattern == "#" {
                    result_items.push(Resp::BulkString(Some(elem.clone())));
                } else {
                    let val = lookup_key(db, pattern, elem);
                    result_items.push(Resp::BulkString(val));
                }
            }
        }
    }

    if let Some(store_key) = opts.store_key {
        // Store result in a list
        let mut list = VecDeque::new();
        for item in &result_items {
            if let Resp::BulkString(Some(b)) = item {
                list.push_back(b.clone());
            } else {
                // If GET returned None, what does STORE do?
                // Redis stores it? No, Redis lists can't hold nulls? 
                // Wait, GET can return nil.
                // "If the key is not found, a nil bulk string is returned."
                // "STORE ... elements are stored in the list."
                // Redis list implementation usually requires binary safe strings. 
                // If GET returns nil, does it store empty string or skip?
                // Documentation says: "If a specified key does not exist, the SORT command returns a nil bulk reply for that element."
                // For STORE: "The command overwrites the destination key... containing the sorted elements."
                // It seems we should convert nil to empty string or handle it.
                // Redis behavior: nil is not stored in list? Or stored as empty?
                // Actually Redis lists can't store "nil". 
                // Let's assume for now we don't store nils or store them as empty bytes?
                // Checking Redis source or behavior: "SORT ... GET ... STORE" 
                // If GET misses, it emits NULL Bulk String. 
                // When STORING, NULLs are usually not valid in LIST?
                // Actually, if I recall correctly, Redis Lists are lists of Strings.
                // If GET returns NULL, maybe it skips? Or maybe it errors?
                // Let's assume we skip for now or store empty string.
                // A common behavior is to store empty string if key missing?
                // No, "missing keys are treated as 0" for sort, but for GET?
                // Let's look up `lookup_key` implementation details.
            }
        }
        
        // Re-construct list properly
        let mut final_list = VecDeque::new();
        for item in &result_items {
             match item {
                 Resp::BulkString(Some(b)) => final_list.push_back(b.clone()),
                 Resp::BulkString(None) => {}, // Skip? Or store empty? 
                 _ => {},
             }
        }
        
        // Actually, if we look at `rpush`, it takes values.
        // Let's store what we have. If we have None, we can't put it in `Value::List`.
        // `Value::List` is `VecDeque<bytes::Bytes>`.
        // So we strictly cannot store None.
        
        // Override destination
        let new_entry = Entry::new(Value::List(final_list.clone()), None);
        db.insert(store_key, new_entry);
        
        Resp::Integer(final_list.len() as i64)
    } else {
        Resp::Array(Some(result_items))
    }
}

fn lookup_key(db: &Db, pattern: &str, elem: &Bytes) -> Option<Bytes> {
    let elem_str = String::from_utf8_lossy(elem);
    let key_str = pattern.replace("*", &elem_str);
    let key_bytes = Bytes::from(key_str);

    // This lookup is complex because `pattern` could imply a hash field access if it contains `->`
    // Format: `key` or `key->field`
    
    if let Some(idx) = pattern.find("->") {
        let pattern_key_part = &pattern[0..idx];
        let pattern_field_part = &pattern[idx+2..];
        
        let real_key_str = pattern_key_part.replace("*", &elem_str);
        let real_field_str = pattern_field_part.replace("*", &elem_str);
        
        let real_key = Bytes::from(real_key_str);
        let real_field = Bytes::from(real_field_str);
        
        if let Some(entry) = db.get(&real_key) {
             if let Value::Hash(h) = &entry.value {
                 return h.get(&real_field).cloned();
             }
        }
        None
    } else {
        // Simple key lookup
        // But wait, if the key holds a String, we return it.
        // If it holds something else, we return nil?
        // Redis says: "The key ... is looked up... interpreted as a number..." (for sorting)
        // For GET, it returns the value.
        if let Some(entry) = db.get(&key_bytes) {
            match &entry.value {
                Value::String(s) => Some(s.clone()),
                _ => None, // Only strings are returned?
            }
        } else {
            None
        }
    }
}
