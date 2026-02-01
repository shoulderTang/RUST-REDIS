use crate::cmd::ServerContext;
use crate::conf::EvictionPolicy;
use crate::db::Entry;
use memory_stats::memory_stats;
use rand::Rng;
use std::sync::atomic::Ordering;
use tracing::{info, warn};

pub fn perform_eviction(ctx: &ServerContext) -> Result<(), String> {
    let maxmemory = ctx.maxmemory.load(Ordering::Relaxed);
    if maxmemory == 0 {
        return Ok(());
    }

    let policy = *ctx.maxmemory_policy.read().unwrap();
    if policy == EvictionPolicy::NoEviction {
        // We still check if we are over limit, but we don't evict.
        // Actually, noeviction means we return error on write commands if over limit.
        // We'll handle this in the command dispatcher.
        return Ok(());
    }

    while is_over_maxmemory(maxmemory) {
        if !evict_one_key(ctx, policy) {
            warn!("Eviction failed to find a candidate key, but still over maxmemory");
            break;
        }
    }

    Ok(())
}

pub fn is_over_maxmemory(maxmemory: u64) -> bool {
    if maxmemory == 0 {
        return false;
    }
    if let Some(usage) = memory_stats() {
        usage.physical_mem as u64 > maxmemory
    } else {
        false
    }
}

fn evict_one_key(ctx: &ServerContext, policy: EvictionPolicy) -> bool {
    let samples = ctx.maxmemory_samples.load(Ordering::Relaxed);
    let mut best_key: Option<(usize, bytes::Bytes)> = None;
    let mut best_score: f64 = -1.0;

    let mut rng = rand::rng();

    // Sample across all databases
    for _ in 0..samples {
        let db_idx = rng.random_range(0..ctx.databases.len());
        let db = &ctx.databases[db_idx];
        
        let db_read = db.read().unwrap();
        if db_read.is_empty() {
            continue;
        }

        // DashMap doesn't support efficient random access, so we use its iterator
        // and skip a random number of elements.
        let skip = rng.random_range(0..db_read.len());
        if let Some(entry_ref) = db_read.iter().skip(skip).next() {
            let key = entry_ref.key().clone();
            let entry = entry_ref.value();

            let score = match policy {
                EvictionPolicy::AllKeysLru => entry.lru as f64,
                EvictionPolicy::VolatileLru => {
                    if entry.expires_at.is_some() {
                        entry.lru as f64
                    } else {
                        -1.0
                    }
                }
                EvictionPolicy::AllKeysLfu => entry.lfu as f64,
                EvictionPolicy::VolatileLfu => {
                    if entry.expires_at.is_some() {
                        entry.lfu as f64
                    } else {
                        -1.0
                    }
                }
                EvictionPolicy::VolatileTtl => {
                    if let Some(exp) = entry.expires_at {
                        exp as f64
                    } else {
                        -1.0
                    }
                }
                EvictionPolicy::AllKeysRandom | EvictionPolicy::VolatileRandom => {
                    if policy == EvictionPolicy::VolatileRandom && entry.expires_at.is_none() {
                        -1.0
                    } else {
                        0.0 // Randomly pick the first valid one
                    }
                }
                EvictionPolicy::NoEviction => -1.0,
            };

            if score >= 0.0 {
                if best_key.is_none() || compare_scores(policy, score, best_score) {
                    best_key = Some((db_idx, key));
                    best_score = score;
                }
            }
        }
    }

    if let Some((db_idx, key)) = best_key {
        let db = &ctx.databases[db_idx];
        let db_read = db.read().unwrap();
        if db_read.remove(&key).is_some() {
            info!("Evicted key {} from DB {}", String::from_utf8_lossy(&key), db_idx);
            return true;
        }
    }

    false
}

fn compare_scores(policy: EvictionPolicy, new_score: f64, old_score: f64) -> bool {
    match policy {
        EvictionPolicy::AllKeysLru | EvictionPolicy::VolatileLru => {
            // Smaller LRU (older access) is better
            new_score < old_score
        }
        EvictionPolicy::AllKeysLfu | EvictionPolicy::VolatileLfu => {
            // Smaller LFU (less frequency) is better
            new_score < old_score
        }
        EvictionPolicy::VolatileTtl => {
            // Smaller TTL (sooner expiration) is better
            new_score < old_score
        }
        EvictionPolicy::AllKeysRandom | EvictionPolicy::VolatileRandom => {
            // Any is fine
            true
        }
        _ => false,
    }
}
