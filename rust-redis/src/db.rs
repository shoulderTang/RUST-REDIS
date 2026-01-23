use dashmap::DashMap;
use std::time::{SystemTime, UNIX_EPOCH};

// RehashMap struct and implementation are removed for simplicity
// and replaced by DashMap as the default implementation.

use std::collections::{VecDeque, HashMap, HashSet, BTreeSet};
use std::cmp::Ordering;
use std::sync::Arc;

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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub enum Value {
    String(bytes::Bytes),
    List(VecDeque<bytes::Bytes>),
    Hash(HashMap<bytes::Bytes, bytes::Bytes>),
    Set(HashSet<bytes::Bytes>),
    ZSet(SortedSet),
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub value: Value,
    pub expires_at: Option<u64>, // absolute timestamp in milliseconds
}

impl Entry {
    pub fn new(value: Value, ttl_ms: Option<u64>) -> Self {
        let expires_at = ttl_ms.map(|ms| {
            let start = SystemTime::now();
            let since_the_epoch = start
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards");
            since_the_epoch.as_millis() as u64 + ms
        });
        Self { value, expires_at }
    }

    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
             let start = SystemTime::now();
             let since_the_epoch = start
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards");
             let now = since_the_epoch.as_millis() as u64;
             now >= expires_at
        } else {
            false
        }
    }
}

pub type Db = Arc<DashMap<bytes::Bytes, Entry>>;
