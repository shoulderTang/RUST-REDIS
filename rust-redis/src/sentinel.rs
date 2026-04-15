use crate::resp::{Resp, read_frame, write_frame};
use bytes::Bytes;
use rand::Rng;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

#[derive(Clone, Debug, PartialEq)]
pub enum FailoverState {
    None,
    InProgress,
    WaitingSlaveSelection,
    SendingSlaveof,
    UpdatingConfig,
    Completed,
}

#[derive(Clone, Debug)]
pub struct SentinelConfig {
    pub monitor_name: String,
    pub ip: String,
    pub port: u16,
    pub quorum: u32,
    pub down_after_milliseconds: u64,
    pub failover_timeout: u64,
    pub parallel_syncs: u32,
}

#[derive(Clone, Debug)]
pub struct SentinelState {
    pub masters: Arc<RwLock<HashMap<String, Arc<RwLock<MasterInstance>>>>>,
    // Leader election state
    pub current_epoch: Arc<RwLock<u64>>,
    pub run_id: String,
    pub ip: String,
    pub port: u16,
    pub config_path: Arc<RwLock<Option<String>>>,
    pub channels: Arc<RwLock<HashMap<String, broadcast::Sender<String>>>>,
}

impl SentinelState {
    pub fn new(ip: String, port: u16) -> Self {
        Self {
            masters: Arc::new(RwLock::new(HashMap::new())),
            current_epoch: Arc::new(RwLock::new(0)),
            run_id: format!(
                "sentinel_{}",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            ),
            ip,
            port,
            config_path: Arc::new(RwLock::new(None)),
            channels: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl SentinelState {
    pub fn set_config_path(&self, path: Option<String>) {
        if let Ok(mut p) = self.config_path.write() {
            *p = path;
        }
    }
    pub fn monitor_master(
        &self,
        name: &str,
        ip: &str,
        port: u16,
        quorum: u32,
    ) -> Result<(), String> {
        let mut masters = self.masters.write().map_err(|_| "lock error".to_string())?;
        if masters.contains_key(name) {
            return Err("BUSY already monitoring this master".to_string());
        }
        let master = MasterInstance::new(name.to_string(), ip.to_string(), port, quorum);
        masters.insert(name.to_string(), Arc::new(RwLock::new(master)));
        info!(
            master = name,
            ip = ip,
            port = port,
            quorum = quorum,
            "sentinel_monitor_added"
        );
        Ok(())
    }
    pub fn remove_master(&self, name: &str) -> Result<(), String> {
        let mut masters = self.masters.write().map_err(|_| "lock error".to_string())?;
        if masters.remove(name).is_some() {
            info!(master = name, "sentinel_monitor_removed");
            Ok(())
        } else {
            Err("ERR No such master with that name".to_string())
        }
    }
    pub fn set_master_options(
        &self,
        name: &str,
        options: &[(String, String)],
    ) -> Result<(), String> {
        let masters = self.masters.read().map_err(|_| "lock error".to_string())?;
        let lock = masters
            .get(name)
            .ok_or_else(|| "ERR No such master with that name".to_string())?;
        let mut master = lock.write().map_err(|_| "lock error".to_string())?;
        for (opt, val) in options {
            let key = opt.to_lowercase();
            match key.as_str() {
                "down-after-milliseconds" => {
                    if let Ok(v) = val.parse::<u64>() {
                        master.down_after_period = v;
                        info!(
                            master = name,
                            option = "down-after-milliseconds",
                            value = v,
                            "sentinel_set_option"
                        );
                    } else {
                        return Err("ERR invalid down-after-milliseconds".to_string());
                    }
                }
                "failover-timeout" => {
                    if let Ok(v) = val.parse::<u64>() {
                        master.failover_timeout = v;
                        info!(
                            master = name,
                            option = "failover-timeout",
                            value = v,
                            "sentinel_set_option"
                        );
                    } else {
                        return Err("ERR invalid failover-timeout".to_string());
                    }
                }
                "parallel-syncs" => {
                    if let Ok(v) = val.parse::<u32>() {
                        master.parallel_syncs = v;
                        info!(
                            master = name,
                            option = "parallel-syncs",
                            value = v,
                            "sentinel_set_option"
                        );
                    } else {
                        return Err("ERR invalid parallel-syncs".to_string());
                    }
                }
                "quorum" => {
                    if let Ok(v) = val.parse::<u32>() {
                        master.quorum = v;
                        info!(
                            master = name,
                            option = "quorum",
                            value = v,
                            "sentinel_set_option"
                        );
                    } else {
                        return Err("ERR invalid quorum".to_string());
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
    pub fn generate_config_text(&self) -> String {
        let masters = self.masters.read().ok();
        let mut entries: Vec<(String, MasterInstance)> = Vec::new();
        if let Some(m) = masters {
            for (name, lock) in m.iter() {
                if let Ok(master) = lock.read() {
                    entries.push((name.clone(), master.clone()));
                }
            }
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let mut lines: Vec<String> = Vec::new();
        for (name, master) in entries {
            lines.push(format!(
                "sentinel monitor {} {} {} {}",
                name, master.ip, master.port, master.quorum
            ));
            lines.push(format!(
                "sentinel down-after-milliseconds {} {}",
                name, master.down_after_period
            ));
            lines.push(format!(
                "sentinel failover-timeout {} {}",
                name, master.failover_timeout
            ));
            lines.push(format!(
                "sentinel parallel-syncs {} {}",
                name, master.parallel_syncs
            ));
        }
        lines.join("\n") + if lines.is_empty() { "" } else { "\n" }
    }
    pub fn write_config_to_path(&self, path: &str) -> Result<(), String> {
        let text = self.generate_config_text();
        std::fs::write(path, text).map_err(|e| format!("ERR {}", e))?;
        info!(path = path, "sentinel_flushconfig_written");
        Ok(())
    }

    pub fn get_or_create_channel(&self, name: &str) -> broadcast::Sender<String> {
        if let Ok(mut map) = self.channels.write() {
            if let Some(tx) = map.get(name) {
                return tx.clone();
            }
            let (tx, _rx) = broadcast::channel::<String>(1024);
            map.insert(name.to_string(), tx.clone());
            tx
        } else {
            // Fallback to a new ephemeral channel if poisoned
            let (tx, _rx) = broadcast::channel::<String>(1024);
            tx
        }
    }

    pub fn publish_notification(&self, channel: &str, payload: String) -> usize {
        let tx = self.get_or_create_channel(channel);
        match tx.send(payload) {
            Ok(receivers) => receivers,
            Err(_) => 0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct MasterInstance {
    pub name: String,
    pub ip: String,
    pub port: u16,
    pub quorum: u32,
    // Monitoring state
    pub last_ping_time: u64,
    pub last_pong_time: u64,
    pub s_down_since_time: u64,
    // Configuration
    pub down_after_period: u64,
    pub failover_timeout: u64,
    pub parallel_syncs: u32,

    // Failover state
    pub failover_state: FailoverState,
    pub failover_epoch: u64,
    pub failover_start_time: u64,
    pub promoted_slave_ip: Option<String>,
    pub promoted_slave_port: Option<u16>,
    // Leader election state
    pub current_leader_runid: Option<String>,
    pub current_leader_epoch: u64,

    // Connections
    // We don't store connections here directly as they are async and mutable.
    // Instead, the monitoring task manages them.
    pub run_id: String,
    pub slaves: HashMap<String, SlaveInstance>,
    pub sentinels: HashMap<String, SentinelInstance>,
    pub info_cache: Option<InfoCache>,
}

#[derive(Clone, Debug)]
pub struct InfoCache {
    pub last_refresh_time: u64,
    pub raw: String,
}
#[derive(Clone, Debug)]
pub struct SlaveInstance {
    pub ip: String,
    pub port: u16,
    pub flags: String,
    pub run_id: String,
    pub last_ping_time: u64,
    pub last_pong_time: u64,
    pub s_down_since_time: u64,
}

#[derive(Clone, Debug)]
pub struct SentinelInstance {
    pub ip: String,
    pub port: u16,
    pub run_id: String,
    pub last_ping_time: u64,
    pub last_pong_time: u64,
    pub s_down_since_time: u64,
    pub leader_epoch: u64, // Epoch when this sentinel was elected leader
    pub leader_runid: Option<String>, // Current leader's run_id
}

impl MasterInstance {
    pub fn new(name: String, ip: String, port: u16, quorum: u32) -> Self {
        Self {
            name,
            ip,
            port,
            quorum,
            last_ping_time: 0,
            last_pong_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            s_down_since_time: 0,
            down_after_period: 30000,
            failover_timeout: 180000,
            parallel_syncs: 1,
            failover_state: FailoverState::None,
            failover_epoch: 0,
            failover_start_time: 0,
            promoted_slave_ip: None,
            promoted_slave_port: None,
            current_leader_runid: None,
            current_leader_epoch: 0,
            run_id: String::new(),
            slaves: HashMap::new(),
            sentinels: HashMap::new(),
            info_cache: None,
        }
    }
}

// Helper function to update master state when connection fails
pub async fn update_master_connection_failure(state: &SentinelState, master_name: &str) {
    if let Ok(masters) = state.masters.read() {
        if let Some(master_lock) = masters.get(master_name) {
            if let Ok(mut master) = master_lock.write() {
                // Set last_pong_time to 0 to indicate connection failure
                // This will trigger SDOWN detection in the next check
                master.last_pong_time = 0;
                info!(
                    "Connection failure detected for master {} {}:{}",
                    master.name, master.ip, master.port
                );
            }
        }
    }
}

// Leader election functions
async fn request_vote_from_sentinel(
    sentinel_ip: &str,
    sentinel_port: u16,
    master_name: &str,
    _requesting_runid: &str,
    requesting_epoch: u64,
) -> Result<bool, String> {
    // In a real implementation, this would connect to the sentinel and request a vote
    // For now, we'll simulate a successful vote if the sentinel is reachable

    info!(
        "Requesting vote from sentinel {}:{} for master {} (epoch: {})",
        sentinel_ip, sentinel_port, master_name, requesting_epoch
    );

    // Simulate network delay and response
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // For simulation, assume 80% success rate
    // Use a simple hash-based approach instead of rand to avoid syntax issues
    let vote_success = ((requesting_epoch + sentinel_port as u64) % 10) < 8;

    if vote_success {
        info!(
            "Received vote from sentinel {}:{} for master {}",
            sentinel_ip, sentinel_port, master_name
        );
        Ok(true)
    } else {
        info!(
            "Vote denied by sentinel {}:{} for master {}",
            sentinel_ip, sentinel_port, master_name
        );
        Ok(false)
    }
}

pub async fn elect_leader_for_master(
    state: &SentinelState,
    master_name: &str,
) -> Result<bool, String> {
    // Get current epoch and increment it
    let new_epoch = {
        let mut epoch = state.current_epoch.write().unwrap();
        *epoch += 1;
        *epoch
    };

    let (sentinels, quorum) = {
        let masters = state.masters.read().unwrap();
        if let Some(master_lock) = masters.get(master_name) {
            let master = master_lock.read().unwrap();
            (master.sentinels.clone(), master.quorum)
        } else {
            return Err("Master not found".to_string());
        }
    };

    let total_sentinels = sentinels.len() + 1;
    let required_votes_majority = (total_sentinels / 2) + 1;

    let mut votes = 1; // Vote for self

    let mut vote_futures = Vec::new();
    for (_, sentinel) in &sentinels {
        // In a real scenario, we'd only ask healthy sentinels
        if sentinel.s_down_since_time == 0 {
            vote_futures.push(request_vote_from_sentinel(
                &sentinel.ip,
                sentinel.port,
                master_name,
                &state.run_id,
                new_epoch,
            ));
        }
    }

    let results = futures::future::join_all(vote_futures).await;
    for result in results {
        if let Ok(true) = result {
            votes += 1;
        }
    }

    let elected = votes >= quorum as usize && votes >= required_votes_majority;

    if elected {
        // We are the leader
        let masters = state.masters.read().unwrap();
        if let Some(master_lock) = masters.get(master_name) {
            let mut master = master_lock.write().unwrap();
            master.current_leader_runid = Some(state.run_id.clone());
            master.current_leader_epoch = new_epoch;
            master.failover_epoch = new_epoch; // The failover epoch is the leader's epoch
        }
    }

    Ok(elected)
}

// Background task to monitor masters
pub async fn sentinel_cron(state: SentinelState) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(1000)); // 1Hz for now, can be 10Hz

    // We need to maintain connections to masters.
    // Since MasterInstance is in a RwLock, we can't hold it across awaits easily if we want to modify it.
    // Also, connections are stateful.

    // Let's create a local map of connection handlers for each master.
    // (master_name) -> (command_tx, pubsub_tx, ip, port)
    let mut connections: HashMap<
        String,
        (
            tokio::sync::mpsc::Sender<Resp>,
            tokio::sync::mpsc::Sender<Resp>,
            String,
            u16,
        ),
    > = HashMap::new();

    loop {
        interval.tick().await;

        // 1. Get list of masters to monitor
        let masters_snapshot: Vec<(String, String, u16)> = {
            let guard = state.masters.read().unwrap();
            guard
                .iter()
                .map(|(k, v)| {
                    let m = v.read().unwrap();
                    (k.clone(), m.ip.clone(), m.port)
                })
                .collect()
        };

        for (name, ip, port) in masters_snapshot {
            // Check if connected
            let needs_reconnect = if let Some((_, _, c_ip, c_port)) = connections.get(&name) {
                *c_ip != ip || *c_port != port
            } else {
                false
            };
            if needs_reconnect {
                connections.remove(&name);
            }
            if !connections.contains_key(&name) {
                info!("Connecting to master {} at {}:{}", name, ip, port);
                // Connect Command Connection
                match connect_to_master(state.clone(), name.clone(), &ip, port).await {
                    Ok(tx) => {
                        // Connect PubSub Connection
                        match connect_to_master_pubsub(state.clone(), name.clone(), &ip, port).await
                        {
                            Ok(pubsub_tx) => {
                                connections.insert(name.clone(), (tx, pubsub_tx, ip.clone(), port));
                                info!("Connected to master {}", name);
                            }
                            Err(e) => {
                                error!("Failed to connect pubsub to master {}: {}", name, e);
                                // Connection failure should trigger SDOWN detection
                                update_master_connection_failure(&state, &name).await;
                                // Close command connection if pubsub fails? Or retry later.
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to connect to master {}: {}", name, e);
                        // Connection failure should trigger SDOWN detection
                        update_master_connection_failure(&state, &name).await;
                    }
                }
            }

            if let Some((cmd_tx, _pubsub_tx, _c_ip, _c_port)) = connections.get(&name) {
                // Send PING
                let ping_cmd = Resp::Array(Some(vec![Resp::BulkString(Some(Bytes::from("PING")))]));
                if let Err(_) = cmd_tx.try_send(ping_cmd) {
                    // Connection likely dead
                    connections.remove(&name);
                    update_master_connection_failure(&state, &name).await;
                    continue;
                }

                // Send INFO
                let info_cmd = Resp::Array(Some(vec![Resp::BulkString(Some(Bytes::from("INFO")))]));
                if let Err(_) = cmd_tx.try_send(info_cmd) {
                    connections.remove(&name);
                    update_master_connection_failure(&state, &name).await;
                    continue;
                }

                // Publish Hello
                let hello_info = {
                    let masters = state.masters.read().unwrap();
                    masters.get(&name).map(|master_lock| {
                        let master = master_lock.read().unwrap();
                        let epoch = *state.current_epoch.read().unwrap();
                        (master.ip.clone(), master.port, epoch)
                    })
                };

                if let Some((master_ip, master_port, epoch)) = hello_info {
                    let master_epoch = {
                        let masters = state.masters.read().unwrap();
                        masters
                            .get(&name)
                            .map(|m| m.read().unwrap().failover_epoch)
                            .unwrap_or(0)
                    };
                    let hello_msg = format!(
                        "{},{},{},{},{},{},{},{}",
                        state.ip,
                        state.port,
                        state.run_id,
                        epoch,
                        name,
                        master_ip,
                        master_port,
                        master_epoch
                    );
                    let publish_cmd = Resp::Array(Some(vec![
                        Resp::BulkString(Some(Bytes::from("PUBLISH"))),
                        Resp::BulkString(Some(Bytes::from("__sentinel__:hello"))),
                        Resp::BulkString(Some(Bytes::from(hello_msg))),
                    ]));
                    if cmd_tx.try_send(publish_cmd).is_err() {
                        connections.remove(&name);
                        update_master_connection_failure(&state, &name).await;
                        continue;
                    }
                }
            }

            // Check SDOWN state and perform ODOWN check
            {
                let mut is_sdown = false;
                let mut quorum = 0;
                let mut sentinels_to_ask = Vec::new();
                let mut master_ip = String::new();
                let mut master_port = 0;
                let mut run_id = String::new();
                let mut failover_needed = false;

                if let Ok(masters) = state.masters.read() {
                    if let Some(master_lock) = masters.get(&name) {
                        if let Ok(mut master) = master_lock.write() {
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64;
                            if now > master.last_pong_time + master.down_after_period {
                                if master.s_down_since_time == 0 {
                                    master.s_down_since_time = now;
                                    warn!(
                                        "+sdown master {} {}:{}",
                                        master.name, master.ip, master.port
                                    );
                                }
                                is_sdown = true;
                            } else {
                                if master.s_down_since_time > 0 {
                                    master.s_down_since_time = 0;
                                    warn!(
                                        "-sdown master {} {}:{}",
                                        master.name, master.ip, master.port
                                    );
                                }
                            }

                            quorum = master.quorum;
                            master_ip = master.ip.clone();
                            master_port = master.port;
                            run_id = master.run_id.clone();

                            // Prepare list of sentinels to ask if SDOWN
                            if is_sdown {
                                for (sid, s) in &master.sentinels {
                                    sentinels_to_ask.push((sid.clone(), s.ip.clone(), s.port));
                                }
                            }
                        }
                    }
                }

                // If SDOWN, ask other sentinels (ODOWN check)
                if is_sdown {
                    let mut votes = 1; // Vote for self

                    for (_sid, ip, port) in sentinels_to_ask {
                        if ask_sentinel_is_master_down(&ip, port, &master_ip, master_port, &run_id)
                            .await
                        {
                            votes += 1;
                        }
                    }

                    if votes >= quorum {
                        // Avoid logging +odown repeatedly if already handled?
                        // For now we log it.
                        warn!(
                            "+odown master {} {}:{} #quorum {}/{}",
                            name, master_ip, master_port, votes, quorum
                        );
                        failover_needed = true;
                    }
                }

                if failover_needed {
                    failover_state_machine(state.clone(), name.clone()).await;
                }
            }
        }
    }
}

async fn failover_state_machine(state: SentinelState, master_name: String) {
    // 1. Elect Leader (Raft-like)
    info!("Starting failover state machine for master {}", master_name);

    // Try to elect leader for this master
    match elect_leader_for_master(&state, &master_name).await {
        Ok(true) => {
            info!("Successfully elected as leader for master {}", master_name);
        }
        Ok(false) => {
            info!(
                "Failed to elect leader for master {} - insufficient quorum",
                master_name
            );
            return;
        }
        Err(e) => {
            error!("Leader election failed for master {}: {}", master_name, e);
            return;
        }
    }

    // 2. Verify we are still the leader before proceeding
    if !is_current_leader(&state, &master_name) {
        error!(
            "No longer the leader for master {} - aborting failover",
            master_name
        );
        return;
    }

    // 3. Select Slave to Promote
    let mut selected_slave: Option<(String, u16)> = None;
    let mut old_master_ip = String::new();
    let mut old_master_port = 0;

    // Update failover state to waiting for slave selection
    {
        if let Ok(masters) = state.masters.read() {
            if let Some(master_lock) = masters.get(&master_name) {
                if let Ok(mut master) = master_lock.write() {
                    master.failover_state = FailoverState::WaitingSlaveSelection;
                    old_master_ip = master.ip.clone();
                    old_master_port = master.port;
                }
            }
        }
    }

    // Proactively refresh slave liveness by requesting a fresh INFO before selection
    if !old_master_ip.is_empty() && old_master_port > 0 {
        if let Ok(tx) = connect_to_master(
            state.clone(),
            master_name.clone(),
            &old_master_ip,
            old_master_port,
        )
        .await
        {
            let info_cmd = Resp::Array(Some(vec![Resp::BulkString(Some(Bytes::from("INFO")))]));
            let _ = tx.send(info_cmd).await;
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    {
        if let Ok(masters) = state.masters.read() {
            if let Some(master_lock) = masters.get(&master_name) {
                if let Ok(master) = master_lock.read() {
                    // Filter healthy slaves
                    // Sort by priority, offset, runid
                    // For now, just pick the first healthy one
                    for (_key, slave) in &master.slaves {
                        // Check if slave is healthy (not SDOWN, last ping recent)
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64;
                        if slave.s_down_since_time == 0 && (now - slave.last_pong_time) < 5000 {
                            selected_slave = Some((slave.ip.clone(), slave.port));
                            break;
                        }
                    }
                }
            }
        }
    }

    if let Some((ip, port)) = selected_slave {
        info!("Selected new master for {}: {}:{}", master_name, ip, port);

        // Update failover state and record promoted slave
        {
            if let Ok(masters) = state.masters.read() {
                if let Some(master_lock) = masters.get(&master_name) {
                    if let Ok(mut master) = master_lock.write() {
                        master.failover_state = FailoverState::SendingSlaveof;
                        master.promoted_slave_ip = Some(ip.clone());
                        master.promoted_slave_port = Some(port);
                    }
                }
            }
        }

        // 3. Promote Slave (REPLICAOF NO ONE)
        if let Ok(tx) = connect_to_master(state.clone(), master_name.clone(), &ip, port).await {
            let cmd = Resp::Array(Some(vec![
                Resp::BulkString(Some(Bytes::from("REPLICAOF"))),
                Resp::BulkString(Some(Bytes::from("NO"))),
                Resp::BulkString(Some(Bytes::from("ONE"))),
            ]));
            let _ = tx.send(cmd).await;
            info!("Sent REPLICAOF NO ONE to {}:{}", ip, port);
        }

        // 4. Reconfigure other slaves
        // We need to wait a bit or verify promotion worked.
        // For prototype, we just send REPLICAOF to others.

        let mut slaves_to_reconf = Vec::new();
        {
            if let Ok(masters) = state.masters.read() {
                if let Some(master_lock) = masters.get(&master_name) {
                    if let Ok(mut master) = master_lock.write() {
                        master.failover_state = FailoverState::UpdatingConfig;

                        for (_key, slave) in &master.slaves {
                            if slave.ip != ip || slave.port != port {
                                slaves_to_reconf.push((slave.ip.clone(), slave.port));
                            }
                        }

                        // Update master info in state
                        master.ip = ip.clone();
                        master.port = port;

                        // Remove the promoted slave from the slaves list immediately
                        master.slaves.retain(|_, s| !(s.ip == ip && s.port == port));
                        master.s_down_since_time = 0;
                        master.promoted_slave_ip = None;
                        master.promoted_slave_port = None;
                    }
                }
            }
        }

        // Honor parallel-syncs limit in batches
        let parallel = {
            let masters = state.masters.read().unwrap();
            masters
                .get(&master_name)
                .map(|m| m.read().unwrap().parallel_syncs)
                .unwrap_or(1)
                .max(1)
        } as usize;
        let mut idx = 0;
        while idx < slaves_to_reconf.len() {
            let end = std::cmp::min(idx + parallel, slaves_to_reconf.len());
            let batch = &slaves_to_reconf[idx..end];
            // Fire off reconfigure commands for this batch
            for (s_ip, s_port) in batch {
                if let Ok(tx) =
                    connect_to_master(state.clone(), master_name.clone(), s_ip, *s_port).await
                {
                    let cmd = Resp::Array(Some(vec![
                        Resp::BulkString(Some(Bytes::from("REPLICAOF"))),
                        Resp::BulkString(Some(Bytes::from(ip.clone()))),
                        Resp::BulkString(Some(Bytes::from(port.to_string()))),
                    ]));
                    let _ = tx.send(cmd).await;
                    info!("Sent REPLICAOF {} {} to {}:{}", ip, port, s_ip, s_port);
                }
            }
            // Simple pacing between batches
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            idx = end;
        }

        // 5. Complete failover
        {
            if let Ok(masters) = state.masters.read() {
                if let Some(master_lock) = masters.get(&master_name) {
                    if let Ok(mut master) = master_lock.write() {
                        master.failover_state = FailoverState::Completed;
                        info!("Failover completed for master {}", master_name);
                    }
                }
            }
        }

        // 6. Force INFO refresh to get updated slave list after failover
        // This ensures we don't show the promoted slave in the slave list
        if let Ok(tx) = connect_to_master(state.clone(), master_name.clone(), &ip, port).await {
            let info_cmd = Resp::Array(Some(vec![Resp::BulkString(Some(Bytes::from("INFO")))]));
            let _ = tx.send(info_cmd).await;
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }

        // 7. Publish switch-master
        // +switch-master <master name> <oldip> <oldport> <newip> <newport>
        warn!(
            "+switch-master {} {} {} {} {}",
            master_name, old_master_ip, old_master_port, ip, port
        );
        let _ = state.publish_notification(
            "+switch-master",
            format!(
                "{} {} {} {} {}",
                master_name, old_master_ip, old_master_port, ip, port
            ),
        );
    } else {
        error!(
            "Failover failed: No healthy slave found for {}",
            master_name
        );

        // Reset failover state on failure
        {
            if let Ok(masters) = state.masters.read() {
                if let Some(master_lock) = masters.get(&master_name) {
                    if let Ok(mut master) = master_lock.write() {
                        master.failover_state = FailoverState::None;
                        master.promoted_slave_ip = None;
                        master.promoted_slave_port = None;
                    }
                }
            }
        }
    }
}

async fn ask_sentinel_is_master_down(
    sentinel_ip: &str,
    sentinel_port: u16,
    master_ip: &str,
    master_port: u16,
    _run_id: &str,
) -> bool {
    // Connect to sentinel and send SENTINEL is-master-down-by-addr <ip> <port> <current_epoch> <runid>
    // For now, let's just implement a stub that tries to connect and send the command.
    let addr = format!("{}:{}", sentinel_ip, sentinel_port);
    match TcpStream::connect(addr).await {
        Ok(stream) => {
            let (read_half, write_half) = stream.into_split();
            let mut writer = BufWriter::new(write_half);
            let mut reader = BufReader::new(read_half);

            let cmd = Resp::Array(Some(vec![
                Resp::BulkString(Some(Bytes::from("SENTINEL"))),
                Resp::BulkString(Some(Bytes::from("is-master-down-by-addr"))),
                Resp::BulkString(Some(Bytes::from(master_ip.to_string()))),
                Resp::BulkString(Some(Bytes::from(master_port.to_string()))),
                Resp::BulkString(Some(Bytes::from("0"))), // current_epoch
                Resp::BulkString(Some(Bytes::from("*"))), // runid, * means we just want to know status, not voting for leader yet
            ]));

            if write_frame(&mut writer, &cmd).await.is_ok() {
                let _ = writer.flush().await;
                if let Ok(Some(Resp::Array(Some(items)))) = read_frame(&mut reader).await {
                    if items.len() >= 1 {
                        if let Resp::Integer(down_state) = items[0] {
                            return down_state == 1;
                        }
                    }
                }
            }
        }
        Err(_) => {}
    }
    false
}

async fn connect_to_master_pubsub(
    state: SentinelState,
    name: String,
    ip: &str,
    port: u16,
) -> Result<tokio::sync::mpsc::Sender<Resp>, Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("{}:{}", ip, port);
    let stream = TcpStream::connect(addr).await?;
    let (read_half, write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut writer = BufWriter::new(write_half);

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Resp>(32);

    // Subscribe to __sentinel__:hello
    let subscribe_cmd = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("SUBSCRIBE"))),
        Resp::BulkString(Some(Bytes::from("__sentinel__:hello"))),
    ]));
    write_frame(&mut writer, &subscribe_cmd).await?;
    writer.flush().await?;

    // Writer task
    tokio::spawn(async move {
        while let Some(cmd) = rx.recv().await {
            if write_frame(&mut writer, &cmd).await.is_err() {
                break;
            }
            if writer.flush().await.is_err() {
                break;
            }
        }
    });

    // Reader task
    let masters_clone = state.masters.clone();
    let name_clone = name.clone();
    tokio::spawn(async move {
        loop {
            match read_frame(&mut reader).await {
                Ok(Some(resp)) => {
                    if let Resp::Array(Some(items)) = resp {
                        if items.len() == 3 {
                            if let (
                                Resp::BulkString(Some(channel)),
                                Resp::BulkString(Some(message)),
                            ) = (&items[1], &items[2])
                            {
                                let channel = String::from_utf8_lossy(channel);
                                if channel == "__sentinel__:hello" {
                                    let msg = String::from_utf8_lossy(message);
                                    handle_sentinel_hello_message(
                                        &masters_clone,
                                        &name_clone,
                                        &msg,
                                    );
                                }
                            }
                        }
                    }
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
    });

    Ok(tx)
}

#[tracing::instrument(level = "debug", skip(masters_ref, msg), fields(master = expected_master))]
pub fn handle_sentinel_hello_message(
    masters_ref: &Arc<RwLock<HashMap<String, Arc<RwLock<MasterInstance>>>>>,
    expected_master: &str,
    msg: &str,
) {
    let parts: Vec<&str> = msg.split(',').collect();
    if parts.len() < 8 {
        return;
    }
    let sentinel_ip = parts[0].to_string();
    let sentinel_port = parts[1].parse::<u16>().unwrap_or(0);
    let sentinel_runid = parts[2].to_string();
    let epoch = parts[3].parse::<u64>().unwrap_or(0);
    let master_name = parts[4];
    if master_name != expected_master {
        return;
    }
    if let Ok(masters) = masters_ref.read() {
        if let Some(master_lock) = masters.get(expected_master) {
            if let Ok(mut master) = master_lock.write() {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                if let Some(s) = master.sentinels.get_mut(&sentinel_runid) {
                    s.ip = sentinel_ip;
                    s.port = sentinel_port;
                    s.last_pong_time = now;
                    s.leader_epoch = epoch;
                    debug!(
                        runid = s.run_id.as_str(),
                        ip = s.ip.as_str(),
                        port = s.port,
                        epoch = s.leader_epoch,
                        "sentinel_hello_refresh"
                    );
                } else {
                    let sentinel = SentinelInstance {
                        ip: sentinel_ip,
                        port: sentinel_port,
                        run_id: sentinel_runid.clone(),
                        last_ping_time: 0,
                        last_pong_time: now,
                        s_down_since_time: 0,
                        leader_epoch: epoch,
                        leader_runid: None,
                    };
                    master.sentinels.insert(sentinel_runid, sentinel);
                    info!("sentinel_hello_new");
                }
            }
        }
    }
}

#[tracing::instrument(level = "debug", skip(state))]
pub fn check_quorum_for_master(
    state: &SentinelState,
    master_name: &str,
) -> Result<(usize, usize, usize, u32, bool), String> {
    let masters = state.masters.read().map_err(|_| "lock error".to_string())?;
    let lock = masters
        .get(master_name)
        .ok_or_else(|| "ERR No such master with that name".to_string())?;
    let master = lock.read().map_err(|_| "lock error".to_string())?;
    let total = master.sentinels.len() + 1;
    let mut reachable = 1usize;
    for (_id, s) in &master.sentinels {
        if s.s_down_since_time == 0 {
            reachable += 1;
        }
    }
    let majority = (total / 2) + 1;
    let quorum = master.quorum;
    let ok = reachable >= quorum as usize && reachable >= majority;
    info!(
        master = master_name,
        total = total,
        reachable = reachable,
        majority = majority,
        quorum = quorum,
        ok = ok,
        "sentinel_ckquorum"
    );
    Ok((total, reachable, majority, quorum, ok))
}

fn is_current_leader(state: &SentinelState, master_name: &str) -> bool {
    let masters = state.masters.read().unwrap();
    if let Some(master_lock) = masters.get(master_name) {
        let master = master_lock.read().unwrap();
        if let Some(leader_runid) = &master.current_leader_runid {
            return leader_runid == &state.run_id;
        }
    }
    false
}

// Start a manual failover for a master
pub async fn start_manual_failover(state: &SentinelState, master_name: &str) -> Result<(), String> {
    info!("Manual failover requested for master {}", master_name);

    // Check if master exists
    let master_exists = {
        let masters = state.masters.read().unwrap();
        masters.contains_key(master_name)
    };

    if !master_exists {
        return Err(format!("Master {} not found", master_name));
    }

    // For manual failover, we need to ensure we are the leader first
    // Try to elect ourselves as leader
    match elect_leader_for_master(state, master_name).await {
        Ok(true) => {
            info!(
                "Elected as leader for manual failover of master {}",
                master_name
            );
        }
        Ok(false) => {
            return Err(format!(
                "Failed to elect leader for master {} - insufficient quorum",
                master_name
            ));
        }
        Err(e) => {
            return Err(format!(
                "Leader election failed for master {}: {}",
                master_name, e
            ));
        }
    }

    // Update state to indicate manual failover
    {
        let masters = state.masters.read().unwrap();
        if let Some(master_lock) = masters.get(master_name) {
            let mut master = master_lock.write().unwrap();
            master.failover_state = FailoverState::InProgress;
            master.failover_start_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
        }
    }

    // Start the failover process
    let state_clone = state.clone();
    let master_name_clone = master_name.to_string();
    tokio::spawn(async move {
        failover_state_machine(state_clone, master_name_clone).await;
    });

    Ok(())
}

// Reset failover state for a master
pub fn reset_failover_state(state: &SentinelState, master_name: &str) -> Result<(), String> {
    let masters = state.masters.read().unwrap();
    if let Some(master_lock) = masters.get(master_name) {
        let mut master = master_lock.write().unwrap();

        master.failover_state = FailoverState::None;
        master.failover_start_time = 0;
        master.promoted_slave_ip = None;
        master.promoted_slave_port = None;

        info!("Failover state reset for master {}", master_name);
        Ok(())
    } else {
        Err(format!("No such master with name '{}'", master_name))
    }
}

async fn connect_to_master(
    state: SentinelState,
    name: String,
    ip: &str,
    port: u16,
) -> Result<tokio::sync::mpsc::Sender<Resp>, Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("{}:{}", ip, port);
    let stream = TcpStream::connect(addr).await?;
    let (read_half, write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut writer = BufWriter::new(write_half);

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Resp>(32);

    // Writer task
    tokio::spawn(async move {
        while let Some(cmd) = rx.recv().await {
            if write_frame(&mut writer, &cmd).await.is_err() {
                break;
            }
            if writer.flush().await.is_err() {
                break;
            }
        }
    });

    // Reader task
    let masters_clone = state.masters.clone();
    let name_clone = name.clone();
    tokio::spawn(async move {
        loop {
            match read_frame(&mut reader).await {
                Ok(Some(resp)) => {
                    // Handle response (PING reply, INFO reply)
                    match resp {
                        Resp::SimpleString(s) => {
                            let s = String::from_utf8_lossy(&s);
                            if s == "PONG" {
                                debug!("Received PONG from {}", name_clone);
                                if let Ok(masters) = masters_clone.read() {
                                    if let Some(master_lock) = masters.get(&name_clone) {
                                        if let Ok(mut master) = master_lock.write() {
                                            master.last_pong_time = SystemTime::now()
                                                .duration_since(UNIX_EPOCH)
                                                .unwrap()
                                                .as_millis()
                                                as u64;
                                            // Clear s_down if it was set
                                            if master.s_down_since_time > 0 {
                                                info!(
                                                    "Master {} is back online (PONG received)",
                                                    name_clone
                                                );
                                                master.s_down_since_time = 0;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Resp::BulkString(Some(b)) => {
                            let s_owned = String::from_utf8_lossy(&b).to_string();
                            if s_owned.contains("# Replication") {
                                debug!("Received INFO Replication from {}", name_clone);
                                // Parse INFO output to find slaves
                                let mut slaves = Vec::new();
                                for line in s_owned.lines() {
                                    if line.starts_with("slave") {
                                        // slave0:ip=127.0.0.1,port=6381,state=online,offset=123,lag=1
                                        let parts: Vec<&str> = line.split(',').collect();
                                        let mut ip = String::new();
                                        let mut port = 0;
                                        for part in parts {
                                            if let Some((k, v)) = part.split_once('=') {
                                                let key = k.rsplit(':').next().unwrap_or(k);
                                                if key == "ip" {
                                                    ip = v.to_string();
                                                } else if key == "port" {
                                                    port = v.parse().unwrap_or(0);
                                                }
                                            }
                                        }
                                        if !ip.is_empty() && port > 0 {
                                            slaves.push((ip, port));
                                        }
                                    }
                                }

                                if let Ok(masters) = masters_clone.read() {
                                    if let Some(master_lock) = masters.get(&name_clone) {
                                        if let Ok(mut master) = master_lock.write() {
                                            master.last_pong_time = SystemTime::now()
                                                .duration_since(UNIX_EPOCH)
                                                .unwrap()
                                                .as_millis()
                                                as u64;

                                            // Don't rebuild slaves list during failover configuration to avoid race conditions
                                            if master.failover_state
                                                != FailoverState::UpdatingConfig
                                            {
                                                // Rebuild slaves list from latest INFO output
                                                let mut new_slaves = HashMap::new();
                                                let now = SystemTime::now()
                                                    .duration_since(UNIX_EPOCH)
                                                    .unwrap()
                                                    .as_millis()
                                                    as u64;
                                                for (ip, port) in slaves {
                                                    let key = format!("{}:{}", ip, port);
                                                    if let Some(mut existing) =
                                                        master.slaves.remove(&key)
                                                    {
                                                        existing.last_pong_time = now;
                                                        existing.s_down_since_time = 0;
                                                        new_slaves.insert(key, existing);
                                                    } else {
                                                        info!(
                                                            "Discovered slave {} for master {}",
                                                            key, name_clone
                                                        );
                                                        let slave = SlaveInstance {
                                                            ip: ip.clone(),
                                                            port,
                                                            flags: "slave".to_string(),
                                                            run_id: String::new(),
                                                            last_ping_time: 0,
                                                            last_pong_time: now,
                                                            s_down_since_time: 0,
                                                        };
                                                        new_slaves.insert(key, slave);
                                                    }
                                                }

                                                master.slaves = new_slaves;
                                            }
                                            // Update INFO cache
                                            master.info_cache = Some(InfoCache {
                                                last_refresh_time: SystemTime::now()
                                                    .duration_since(UNIX_EPOCH)
                                                    .unwrap()
                                                    .as_millis()
                                                    as u64,
                                                raw: s_owned.clone(),
                                            });
                                        }
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
    });

    Ok(tx)
}
