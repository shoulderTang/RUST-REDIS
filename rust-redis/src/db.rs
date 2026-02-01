use dashmap::DashMap;
use std::time::{SystemTime, UNIX_EPOCH};

// RehashMap struct and implementation are removed for simplicity
// and replaced by DashMap as the default implementation.

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use crate::stream::Stream;
use crate::hll::HyperLogLog;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TotalOrderF64(pub f64);

impl Eq for TotalOrderF64 {}

impl PartialOrd for TotalOrderF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TotalOrderF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SortedSet {
    pub members: HashMap<bytes::Bytes, f64>,
    pub scores: BTreeSet<(TotalOrderF64, bytes::Bytes)>,
}

impl SortedSet {
    pub fn new() -> Self {
        SortedSet {
            members: HashMap::new(),
            scores: BTreeSet::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    String(bytes::Bytes),
    List(VecDeque<bytes::Bytes>),
    Hash(HashMap<bytes::Bytes, bytes::Bytes>),
    Set(HashSet<bytes::Bytes>),
    ZSet(SortedSet),
    Stream(Stream),
    HyperLogLog(HyperLogLog),
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub value: Value,
    pub expires_at: Option<u64>, // absolute timestamp in milliseconds
    pub lru: u64,               // last access time in seconds (simplified Redis LRU)
    pub lfu: u32,               // access counter (simplified Redis LFU)
}

impl Entry {
    pub fn new(value: Value, ttl_ms: Option<u64>) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let now_ms = now.as_millis() as u64;
        
        let expires_at = ttl_ms.map(|ms| now_ms + ms);
        
        Self { 
            value, 
            expires_at,
            lru: now.as_secs(),
            lfu: 1,
        }
    }

    pub fn new_with_expire(value: Value, expires_at: Option<u64>) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        
        Self { 
            value, 
            expires_at,
            lru: now.as_secs(),
            lfu: 1,
        }
    }

    pub fn touch(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        self.lru = now;
        self.lfu = self.lfu.saturating_add(1);
    }

    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis() as u64;
            now >= expires_at
        } else {
            false
        }
    }
}

pub type Db = Arc<DashMap<bytes::Bytes, Entry>>;
