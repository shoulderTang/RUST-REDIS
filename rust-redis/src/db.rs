use dashmap::DashMap;
use std::time::{SystemTime, UNIX_EPOCH};

// RehashMap struct and implementation are removed for simplicity
// and replaced by DashMap as the default implementation.

#[derive(Clone, Debug)]
pub enum Value {
    String(bytes::Bytes),
    // List(Vec<bytes::Bytes>),
    // Hash(DashMap<bytes::Bytes, bytes::Bytes>),
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

pub type Db = DashMap<bytes::Bytes, Entry>;
