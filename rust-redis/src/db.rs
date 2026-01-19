use std::hash::{Hash, Hasher, BuildHasher};
use std::mem;
use dashmap::DashMap;

// RehashMap struct and implementation are removed for simplicity
// and replaced by DashMap as the default implementation.

#[derive(Clone, Debug)]
pub enum Value {
    String(bytes::Bytes),
    // List(Vec<bytes::Bytes>),
    // Hash(DashMap<bytes::Bytes, bytes::Bytes>),
}

pub type Db = DashMap<bytes::Bytes, Value>;
