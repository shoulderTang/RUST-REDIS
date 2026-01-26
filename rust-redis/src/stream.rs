use crate::rax::Rax;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;
use std::num::ParseIntError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamID {
    pub ms: u64,
    pub seq: u64,
}

impl StreamID {
    pub fn new(ms: u64, seq: u64) -> Self {
        StreamID { ms, seq }
    }

    pub fn to_be_bytes(&self) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&self.ms.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.seq.to_be_bytes());
        bytes
    }
}

impl FromStr for StreamID {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() != 2 {
            return Err("Invalid Stream ID format".to_string());
        }
        
        let ms = parts[0].parse::<u64>().map_err(|e| e.to_string())?;
        let seq = parts[1].parse::<u64>().map_err(|e| e.to_string())?;
        
        Ok(StreamID { ms, seq })
    }
}

impl fmt::Display for StreamID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamEntry {
    pub id: StreamID,
    pub fields: Vec<(Bytes, Bytes)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PendingEntry {
    pub id: StreamID,
    pub delivery_time: u128,
    pub delivery_count: u64,
    pub owner: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Consumer {
    pub name: String,
    pub seen_time: u128,
    pub pending_ids: HashSet<StreamID>,
}

impl Consumer {
    pub fn new(name: String) -> Self {
        Consumer {
            name,
            seen_time: 0,
            pending_ids: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConsumerGroup {
    pub name: String,
    pub last_id: StreamID,
    pub consumers: HashMap<String, Consumer>,
    pub pel: HashMap<StreamID, PendingEntry>,
}

impl ConsumerGroup {
    pub fn new(name: String, last_id: StreamID) -> Self {
        ConsumerGroup {
            name,
            last_id,
            consumers: HashMap::new(),
            pel: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Stream {
    pub rax: Rax<StreamEntry>,
    pub last_id: StreamID,
    pub groups: HashMap<String, ConsumerGroup>,
}

impl Default for Stream {
    fn default() -> Self {
        Self::new()
    }
}

impl Stream {
    pub fn new() -> Self {
        Stream {
            rax: Rax::new(),
            last_id: StreamID::new(0, 0),
            groups: HashMap::new(),
        }
    }

    pub fn insert(&mut self, id: StreamID, fields: Vec<(Bytes, Bytes)>) -> Result<StreamID, &'static str> {
        if id <= self.last_id {
            if id.ms == 0 && id.seq == 0 {
                return Err("ERR The ID specified in XADD must be greater than 0-0");
            }
            return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item");
        }

        let entry = StreamEntry {
            id,
            fields,
        };

        self.rax.insert(&id.to_be_bytes(), entry);
        self.last_id = id;
        Ok(id)
    }

    pub fn len(&self) -> usize {
        self.rax.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rax.is_empty()
    }
    
    pub fn get(&self, id: &StreamID) -> Option<&StreamEntry> {
        self.rax.get(&id.to_be_bytes())
    }

    pub fn remove(&mut self, id: &StreamID) -> Option<StreamEntry> {
        self.rax.remove(&id.to_be_bytes())
    }

    pub fn range(&self, start: &StreamID, end: &StreamID) -> Vec<StreamEntry> {
        let start_bytes = start.to_be_bytes();
        let end_bytes = end.to_be_bytes();
        
        let entries = self.rax.range(&start_bytes, &end_bytes);
        entries.into_iter().map(|(_, entry)| entry).collect()
    }

    pub fn rev_range(&self, start: &StreamID, end: &StreamID) -> Vec<StreamEntry> {
        let start_bytes = start.to_be_bytes();
        let end_bytes = end.to_be_bytes();
        
        let entries = self.rax.rev_range(&start_bytes, &end_bytes);
        entries.into_iter().map(|(_, entry)| entry).collect()
    }
}
