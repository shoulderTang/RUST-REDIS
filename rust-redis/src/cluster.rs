#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

pub const CLUSTER_SLOTS: usize = 16384;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeRole {
    Master,
    Replica,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotRange {
    pub start: u16,
    pub end: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SlotState {
    Stable,
    Migrating { to: NodeId },
    Importing { from: NodeId },
}

#[derive(Clone, Debug)]
pub struct ClusterNode {
    pub id: NodeId,
    pub ip: String,
    pub port: u16,
    pub role: NodeRole,
    pub slots: Vec<SlotRange>,
    pub epoch: u64,
    pub master_id: Option<NodeId>,
}

#[derive(Clone, Debug)]
pub struct ClusterState {
    pub nodes: HashMap<NodeId, ClusterNode>,
    pub slots: Vec<Option<NodeId>>,
    pub slot_state: Vec<SlotState>,
    pub current_epoch: u64,
    pub myself: NodeId,
    pub last_ok_ms: HashMap<NodeId, u64>,
}

impl ClusterState {
    pub fn to_config_text(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!("currentEpoch {}", self.current_epoch));
        lines.push(format!("myself {}", self.myself.0));
        for l in self.nodes_overview() {
            lines.push(l);
        }
        lines.join("\n") + "\n"
    }

    pub fn load_config_text(&mut self, text: &str, my_ip: &str, my_port: u16) -> Result<(), String> {
        let mut current_epoch = None;
        let mut myself_id: Option<NodeId> = None;
        let mut nodes: Vec<ClusterNode> = Vec::new();
        for raw in text.lines() {
            let line = raw.trim();
            if line.is_empty() {
                continue;
            }
            let low = line.to_lowercase();
            if low.starts_with("currentepoch ") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Ok(n) = parts[1].parse::<u64>() {
                        current_epoch = Some(n);
                    }
                }
                continue;
            }
            if low.starts_with("myself ") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    myself_id = Some(NodeId(parts[1].to_string()));
                }
                continue;
            }
            nodes.push(Self::parse_nodes_overview_line(line)?);
        }

        self.nodes.clear();
        self.slots.clear();
        self.slots.resize(CLUSTER_SLOTS, None);
        self.slot_state.clear();
        self.slot_state.resize(CLUSTER_SLOTS, SlotState::Stable);
        self.last_ok_ms.clear();

        for n in nodes.iter() {
            self.nodes.insert(n.id.clone(), n.clone());
            self.last_ok_ms.insert(n.id.clone(), Self::now_ms());
        }

        if let Some(e) = current_epoch {
            self.current_epoch = e;
        } else {
            let mut e = 0u64;
            for n in nodes.iter() {
                e = e.max(n.epoch);
            }
            self.current_epoch = e;
        }

        if let Some(id) = myself_id {
            self.myself = id;
        } else {
            if let Some(n) = nodes.iter().find(|x| x.ip == my_ip && x.port == my_port) {
                self.myself = n.id.clone();
            }
        }

        for m in nodes.iter().filter(|x| x.role == NodeRole::Master) {
            for r in &m.slots {
                for s in r.start..=r.end {
                    if (s as usize) < CLUSTER_SLOTS {
                        self.slots[s as usize] = Some(m.id.clone());
                        self.slot_state[s as usize] = SlotState::Stable;
                    }
                }
            }
        }
        Ok(())
    }
    pub fn new(myself: NodeId, ip: String, port: u16) -> Self {
        let mut nodes = HashMap::new();
        let epoch = 0u64;
        let me = ClusterNode {
            id: myself.clone(),
            ip,
            port,
            role: NodeRole::Master,
            slots: Vec::new(),
            epoch,
            master_id: None,
        };
        nodes.insert(myself.clone(), me);
        let mut slots = Vec::with_capacity(CLUSTER_SLOTS);
        slots.resize(CLUSTER_SLOTS, None);
        let mut slot_state = Vec::with_capacity(CLUSTER_SLOTS);
        slot_state.resize(CLUSTER_SLOTS, SlotState::Stable);
        let mut last_ok_ms = HashMap::new();
        last_ok_ms.insert(myself.clone(), Self::now_ms());
        Self {
            nodes,
            slots,
            slot_state,
            current_epoch: 0,
            myself,
            last_ok_ms,
        }
    }

    pub fn add_node(&mut self, id: NodeId, ip: String, port: u16, role: NodeRole, master_id: Option<NodeId>) -> Result<(), String> {
        if self.nodes.contains_key(&id) {
            return Err("node exists".into());
        }
        let node = ClusterNode {
            id: id.clone(),
            ip,
            port,
            role,
            slots: Vec::new(),
            epoch: self.current_epoch,
            master_id,
        };
        self.nodes.insert(id, node);
        Ok(())
    }

    pub fn add_slots(&mut self, node_id: &NodeId, slots: &[u16]) -> Result<(), String> {
        let node = self.nodes.get_mut(node_id).ok_or_else(|| "no such node".to_string())?;
        for s in slots {
            if (*s as usize) >= CLUSTER_SLOTS {
                return Err("invalid slot".into());
            }
            if self.slots[*s as usize].is_some() {
                return Err("slot already assigned".into());
            }
        }
        for s in slots {
            self.slots[*s as usize] = Some(node_id.clone());
            self.slot_state[*s as usize] = SlotState::Stable;
        }
        Self::merge_into_ranges(&mut node.slots, slots);
        Ok(())
    }

    pub fn del_slots(&mut self, node_id: &NodeId, slots: &[u16]) -> Result<(), String> {
        let node = self.nodes.get_mut(node_id).ok_or_else(|| "no such node".to_string())?;
        for s in slots {
            if (*s as usize) >= CLUSTER_SLOTS {
                return Err("invalid slot".into());
            }
            if self.slots[*s as usize].as_ref() != Some(node_id) {
                return Err("slot not owned".into());
            }
        }
        for s in slots {
            self.slots[*s as usize] = None;
            self.slot_state[*s as usize] = SlotState::Stable;
        }
        node.slots = Self::ranges_minus(&node.slots, slots);
        Ok(())
    }

    pub fn set_slot_migrating(&mut self, slot: u16, to: NodeId) -> Result<(), String> {
        if slot as usize >= CLUSTER_SLOTS {
            return Err("invalid slot".into());
        }
        if !self.nodes.contains_key(&to) {
            return Err("no such target node".into());
        }
        if self.slots[slot as usize] != Some(self.myself.clone()) {
            return Err("slot not owned by myself".into());
        }
        self.slot_state[slot as usize] = SlotState::Migrating { to };
        Ok(())
    }

    pub fn set_slot_importing(&mut self, slot: u16, from: NodeId) -> Result<(), String> {
        if slot as usize >= CLUSTER_SLOTS {
            return Err("invalid slot".into());
        }
        if !self.nodes.contains_key(&from) {
            return Err("no such source node".into());
        }
        if self.slots[slot as usize].is_some() {
            return Err("slot already assigned".into());
        }
        self.slot_state[slot as usize] = SlotState::Importing { from };
        Ok(())
    }

    pub fn set_slot_stable(&mut self, slot: u16) -> Result<(), String> {
        if slot as usize >= CLUSTER_SLOTS {
            return Err("invalid slot".into());
        }
        self.slot_state[slot as usize] = SlotState::Stable;
        Ok(())
    }

    pub fn nodes_overview(&self) -> Vec<String> {
        let mut lines = Vec::new();
        for n in self.nodes.values() {
            let role = match n.role { NodeRole::Master => "master", NodeRole::Replica => "slave" };
            let mut parts = vec![
                n.id.0.clone(),
                format!("{}:{}", n.ip, n.port),
                role.into(),
                n.epoch.to_string(),
            ];
            if let Some(mid) = &n.master_id {
                parts.push(mid.0.clone());
            }
            let mut ranges = Vec::new();
            for r in &n.slots {
                if r.start == r.end {
                    ranges.push(format!("{}", r.start));
                } else {
                    ranges.push(format!("{}-{}", r.start, r.end));
                }
            }
            parts.push(ranges.join(","));
            lines.push(parts.join(" "));
        }
        lines
    }

    pub fn parse_nodes_overview_text(text: &str) -> Result<Vec<ClusterNode>, String> {
        let mut nodes = Vec::new();
        for line in text.lines() {
            let l = line.trim();
            if l.is_empty() {
                continue;
            }
            nodes.push(Self::parse_nodes_overview_line(l)?);
        }
        Ok(nodes)
    }

    pub fn merge_nodes_overview_text(&mut self, text: &str) -> Result<(), String> {
        let nodes = Self::parse_nodes_overview_text(text)?;
        self.merge_topology(nodes);
        Ok(())
    }

    pub fn merge_topology(&mut self, incoming: Vec<ClusterNode>) {
        let mut masters = Vec::new();
        for node in incoming {
            if node.id == self.myself {
                continue;
            }
            if let Some(alias_id) = self
                .nodes
                .values()
                .find(|n| n.ip == node.ip && n.port == node.port && n.id != node.id)
                .map(|n| n.id.clone())
            {
                self.replace_node_id(&alias_id, &node.id);
            }

            if let Some(existing) = self.nodes.get_mut(&node.id) {
                existing.ip = node.ip.clone();
                existing.port = node.port;
                existing.role = node.role.clone();
                existing.master_id = node.master_id.clone();
                existing.epoch = node.epoch;
                existing.slots = node.slots.clone();
            } else {
                self.nodes.insert(node.id.clone(), node.clone());
            }
            self.last_ok_ms.insert(node.id.clone(), Self::now_ms());

            if node.role == NodeRole::Master && !node.slots.is_empty() {
                masters.push(node);
            }
        }

        for m in masters {
            for r in &m.slots {
                for slot in r.start..=r.end {
                    if slot as usize >= CLUSTER_SLOTS {
                        continue;
                    }
                    let idx = slot as usize;
                    if let Some(prev) = self.slots[idx].clone() {
                        if prev != m.id {
                            let _ = self.del_slots(&prev, &[slot]);
                        }
                    }
                    self.slots[idx] = Some(m.id.clone());
                    self.slot_state[idx] = SlotState::Stable;
                }
            }
        }
    }

    pub fn key_slot(key: &str) -> u16 {
        let tag = Self::hash_tag(key);
        let crc = Self::crc16(tag.as_bytes());
        (crc % CLUSTER_SLOTS as u16) as u16
    }

    pub fn record_ok(&mut self, id: &NodeId) {
        self.last_ok_ms.insert(id.clone(), Self::now_ms());
    }

    pub fn scan_and_failover<F>(&mut self, timeout_ms: u64, mut is_alive: F)
    where
        F: FnMut(&ClusterNode) -> bool,
    {
        let now = Self::now_ms();
        // Collect masters to evaluate to avoid borrow issues
        let masters: Vec<NodeId> = self
            .nodes
            .values()
            .filter(|n| n.role == NodeRole::Master && n.id != self.myself)
            .map(|n| n.id.clone())
            .collect();
        for mid in masters {
            let alive = if let Some(m) = self.nodes.get(&mid) {
                is_alive(m)
            } else {
                false
            };
            if alive {
                self.record_ok(&mid);
                continue;
            }
            let last_ok = self.last_ok_ms.get(&mid).copied().unwrap_or(0);
            if now.saturating_sub(last_ok) < timeout_ms {
                continue;
            }
            // Find a replica
            let candidate = self
                .nodes
                .values()
                .filter(|n| n.role == NodeRole::Replica && n.master_id.as_ref() == Some(&mid))
                .map(|n| n.id.clone())
                .min_by(|a, b| a.0.cmp(&b.0));
            if let Some(rep_id) = candidate {
                let _ = self.promote_replica_to_master(&mid, &rep_id);
            }
        }
    }

    pub fn promote_replica_to_master(&mut self, old_master: &NodeId, replica: &NodeId) -> Result<(), String> {
        // Snapshot master's slots
        let master_slots = if let Some(m) = self.nodes.get(old_master) {
            m.slots.clone()
        } else {
            return Err("old master not found".to_string());
        };
        // Update replica role
        if let Some(r) = self.nodes.get_mut(replica) {
            r.role = NodeRole::Master;
            r.master_id = None;
        } else {
            return Err("replica not found".to_string());
        }
        // Move slots to replica
        let to_assign: Vec<u16> = master_slots
            .iter()
            .flat_map(|r| r.start..=r.end)
            .collect();
        if let Some(_) = self.nodes.get(old_master) {
            let _ = self.del_slots(old_master, &to_assign);
        }
        self.add_slots(replica, &to_assign)?;
        Ok(())
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn parse_nodes_overview_line(line: &str) -> Result<ClusterNode, String> {
        let tokens: Vec<&str> = line.split_whitespace().collect();
        if tokens.len() < 4 {
            return Err("invalid nodes line".to_string());
        }
        let id = NodeId(tokens[0].to_string());
        let (ip, port) = Self::parse_ip_port(tokens[1])?;
        let role = match tokens[2].to_lowercase().as_str() {
            "master" => NodeRole::Master,
            "slave" | "replica" => NodeRole::Replica,
            _ => return Err("invalid role".to_string()),
        };
        let epoch = tokens[3].parse::<u64>().map_err(|_| "invalid epoch".to_string())?;

        let (master_id, slots_str) = match role {
            NodeRole::Master => (None, tokens.get(4).copied().unwrap_or("")),
            NodeRole::Replica => {
                let mid = tokens.get(4).copied().unwrap_or("");
                let mid = if mid.is_empty() { None } else { Some(NodeId(mid.to_string())) };
                (mid, tokens.get(5).copied().unwrap_or(""))
            }
        };

        let slots = Self::parse_slot_ranges(slots_str)?;
        Ok(ClusterNode {
            id,
            ip,
            port,
            role,
            slots,
            epoch,
            master_id,
        })
    }

    fn parse_ip_port(addr: &str) -> Result<(String, u16), String> {
        let mut it = addr.splitn(2, ':');
        let ip = it.next().unwrap_or("").to_string();
        let port = it
            .next()
            .ok_or_else(|| "invalid addr".to_string())?
            .parse::<u16>()
            .map_err(|_| "invalid port".to_string())?;
        if ip.is_empty() {
            return Err("invalid ip".to_string());
        }
        Ok((ip, port))
    }

    fn parse_slot_ranges(s: &str) -> Result<Vec<SlotRange>, String> {
        if s.trim().is_empty() {
            return Ok(Vec::new());
        }
        let mut ranges = Vec::new();
        for part in s.split(',') {
            let p = part.trim();
            if p.is_empty() {
                continue;
            }
            if let Some((a, b)) = p.split_once('-') {
                let start = a.parse::<u16>().map_err(|_| "invalid slot range".to_string())?;
                let end = b.parse::<u16>().map_err(|_| "invalid slot range".to_string())?;
                ranges.push(SlotRange { start, end });
            } else {
                let x = p.parse::<u16>().map_err(|_| "invalid slot".to_string())?;
                ranges.push(SlotRange { start: x, end: x });
            }
        }
        ranges.sort_by_key(|r| (r.start, r.end));
        Ok(ranges)
    }

    fn replace_node_id(&mut self, from: &NodeId, to: &NodeId) {
        if from == to {
            return;
        }

        if let Some(mut n) = self.nodes.remove(from) {
            n.id = to.clone();
            if let Some(existing) = self.nodes.get_mut(to) {
                existing.ip = n.ip;
                existing.port = n.port;
                existing.role = n.role;
                existing.master_id = n.master_id;
                existing.epoch = existing.epoch.max(n.epoch);
                if !n.slots.is_empty() {
                    existing.slots = n.slots;
                }
            } else {
                self.nodes.insert(to.clone(), n);
            }
        }

        for slot in &mut self.slots {
            if slot.as_ref() == Some(from) {
                *slot = Some(to.clone());
            }
        }

        for n in self.nodes.values_mut() {
            if n.master_id.as_ref() == Some(from) {
                n.master_id = Some(to.clone());
            }
        }

        for st in &mut self.slot_state {
            match st {
                SlotState::Migrating { to: dest } if dest == from => {
                    *dest = to.clone();
                }
                SlotState::Importing { from: src } if src == from => {
                    *src = to.clone();
                }
                _ => {}
            }
        }
    }

    fn hash_tag(key: &str) -> String {
        let b = key.as_bytes();
        let mut i = 0usize;
        while i < b.len() {
            if b[i] == b'{' {
                let mut j = i + 1;
                while j < b.len() {
                    if b[j] == b'}' {
                        if j > i + 1 {
                            return String::from_utf8_lossy(&b[i + 1..j]).to_string();
                        }
                        break;
                    }
                    j += 1;
                }
            }
            i += 1;
        }
        key.to_string()
    }

    fn crc16(data: &[u8]) -> u16 {
        let mut crc: u16 = 0;
        for &b in data {
            crc ^= (b as u16) << 8;
            for _ in 0..8 {
                if (crc & 0x8000) != 0 {
                    crc = (crc << 1) ^ 0x1021;
                } else {
                    crc <<= 1;
                }
            }
        }
        crc
    }

    fn merge_into_ranges(ranges: &mut Vec<SlotRange>, slots: &[u16]) {
        let mut s = slots.to_vec();
        s.sort_unstable();
        let mut i = 0usize;
        while i < s.len() {
            let mut j = i;
            while j + 1 < s.len() && s[j + 1] == s[j] + 1 {
                j += 1;
            }
            ranges.push(SlotRange { start: s[i], end: s[j] });
            i = j + 1;
        }
        ranges.sort_by_key(|r| (r.start, r.end));
    }

    fn ranges_minus(ranges: &[SlotRange], slots: &[u16]) -> Vec<SlotRange> {
        let mut set = HashSet::new();
        for r in ranges {
            for x in r.start..=r.end {
                set.insert(x);
            }
        }
        for s in slots {
            set.remove(s);
        }
        let mut v: Vec<u16> = set.into_iter().collect();
        v.sort_unstable();
        let mut res = Vec::new();
        let mut i = 0usize;
        while i < v.len() {
            let mut j = i;
            while j + 1 < v.len() && v[j + 1] == v[j] + 1 {
                j += 1;
            }
            res.push(SlotRange { start: v[i], end: v[j] });
            i = j + 1;
        }
        res
    }
}
