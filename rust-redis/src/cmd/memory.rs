use crate::cmd::{ServerContext, as_bytes};
use crate::db::{Db, Value};
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::atomic::Ordering;
use memory_stats::memory_stats;

pub async fn memory(items: &[Resp], db: &Db, ctx: &ServerContext) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'MEMORY' command".to_string());
    }

    let subcommand = match &items[1] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_uppercase(),
        Resp::SimpleString(s) => String::from_utf8_lossy(s).to_uppercase(),
        _ => return Resp::Error("ERR syntax error".to_string()),
    };

    match subcommand.as_str() {
        "USAGE" => memory_usage(items, db).await,
        "STATS" => memory_stats_cmd(ctx).await,
        "HELP" => memory_help().await,
        _ => Resp::Error(format!("ERR unknown subcommand '{}'. Try USAGE, STATS, HELP.", subcommand)),
    }
}

async fn memory_usage(items: &[Resp], db: &Db) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'MEMORY USAGE' command".to_string());
    }

    let key = match &items[2] {
        Resp::BulkString(Some(b)) => b,
        Resp::SimpleString(s) => s,
        _ => return Resp::Error("ERR invalid key".to_string()),
    };

    // Optional SAMPLES argument
    let mut _samples = 5;
    if items.len() >= 5 {
        if let Some(arg) = as_bytes(&items[3]) {
            if arg.eq_ignore_ascii_case(b"SAMPLES") {
                if let Some(s_bytes) = as_bytes(&items[4]) {
                    if let Ok(s_str) = std::str::from_utf8(s_bytes) {
                        if let Ok(s_val) = s_str.parse::<usize>() {
                            _samples = s_val;
                        }
                    }
                }
            }
        }
    }

    if let Some(entry) = db.get(key) {
        if entry.is_expired() {
            return Resp::BulkString(None);
        }
        let size = estimate_value_size(&entry.value);
        // Include key size and some overhead
        let total_size = key.len() + size + 64; // 64 bytes overhead for Entry struct and DashMap node
        Resp::Integer(total_size as i64)
    } else {
        Resp::BulkString(None)
    }
}

fn estimate_value_size(val: &Value) -> usize {
    match val {
        Value::String(b) => b.len(),
        Value::List(l) => {
            l.iter().map(|b| b.len() + 16).sum::<usize>() + 32 // 16 bytes overhead per element, 32 for VecDeque
        }
        Value::Hash(h) => {
            h.iter().map(|(k, v)| k.len() + v.len() + 32).sum::<usize>() + 64 // 32 bytes overhead per entry
        }
        Value::Set(s) => {
            s.iter().map(|b| b.len() + 24).sum::<usize>() + 64 // 24 bytes overhead per entry
        }
        Value::ZSet(zs) => {
            let members_size = zs.members.iter().map(|(k, _)| k.len() + 40).sum::<usize>();
            let scores_size = zs.scores.len() * 48; // Estimate for BTreeSet node
            members_size + scores_size + 128
        }
        Value::Stream(s) => {
            // Very rough estimation for Stream based on length
            let count = s.len();
            let mut size = 256; // Base overhead
            size += count * 128; // Estimate 128 bytes per entry (ID + some fields)
            size
        }
        Value::HyperLogLog(_) => 12 * 1024, // HLL is typically 12KB in Redis
    }
}

async fn memory_stats_cmd(ctx: &ServerContext) -> Resp {
    let mut stats = Vec::new();

    let mut add_stat = |k: &str, v: Resp| {
        stats.push(Resp::BulkString(Some(Bytes::from(k.to_string()))));
        stats.push(v);
    };

    if let Some(usage) = memory_stats() {
        add_stat("peak.allocated", Resp::Integer(ctx.mem_peak_rss.load(Ordering::Relaxed) as i64));
        add_stat("total.allocated", Resp::Integer(usage.physical_mem as i64));
        add_stat("startup.allocated", Resp::Integer(0)); // We don't track this yet
        add_stat("replication.backlog", Resp::Integer(0));
        add_stat("clients.slaves", Resp::Integer(0));
        add_stat("clients.normal", Resp::Integer(ctx.client_count.load(Ordering::Relaxed) as i64));
        add_stat("aof.buffer", Resp::Integer(0));
        
        let mut db_total_keys = 0;
        for db_lock in ctx.databases.iter() {
            db_total_keys += db_lock.read().unwrap().len();
        }
        add_stat("keys.count", Resp::Integer(db_total_keys as i64));
        
        let dataset_bytes = usage.physical_mem as i64; // Simplified
        add_stat("dataset.bytes", Resp::Integer(dataset_bytes));
        
        if ctx.maxmemory.load(Ordering::Relaxed) > 0 {
            let maxmemory = ctx.maxmemory.load(Ordering::Relaxed) as i64;
            add_stat("dataset.percentage", Resp::BulkString(Some(Bytes::from(format!("{:.2}", (dataset_bytes as f64 / maxmemory as f64) * 100.0)))));
        }
    } else {
        return Resp::Error("ERR could not retrieve memory stats".to_string());
    }

    Resp::Array(Some(stats))
}

async fn memory_help() -> Resp {
    let help = vec![
        "MEMORY DOCTOR                        - Outputs memory problems report",
        "MEMORY STATS                         - Outputs memory usage details",
        "MEMORY PURGE                         - Ask the allocator to release memory",
        "MEMORY USAGE <key> [SAMPLES <count>] - Estimate memory usage of key",
        "MEMORY MALLOC-STATS                  - Outputs allocator internal stats",
        "MEMORY HELP                          - This help text",
    ];
    let mut res = Vec::new();
    for line in help {
        res.push(Resp::SimpleString(Bytes::from(line)));
    }
    Resp::Array(Some(res))
}
