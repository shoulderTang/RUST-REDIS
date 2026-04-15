use crate::cluster::{NodeId, NodeRole, SlotState};
use crate::cmd::{ConnectionContext, ServerContext};
use crate::resp::{Resp, as_bytes};
use bytes::Bytes;

fn parse_addr(s: &str) -> Option<(String, u16)> {
    let mut it = s.splitn(2, ':');
    let ip = it.next()?.to_string();
    let port = it.next()?.parse::<u16>().ok()?;
    Some((ip, port))
}

pub fn cluster(
    items: &[Resp],
    _conn_ctx: &mut ConnectionContext,
    server_ctx: &ServerContext,
) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'cluster' command".to_string());
    }
    let sub = match as_bytes(&items[1]) {
        Some(b) => String::from_utf8_lossy(&b).to_uppercase(),
        None => return Resp::Error("ERR invalid subcommand".to_string()),
    };
    match sub.as_str() {
        "NODES" => {
            let lines = {
                let st = server_ctx.cluster.read().unwrap();
                st.nodes_overview_redis()
            };
            let mut s = lines.join("\n");
            s.push('\n');
            Resp::BulkString(Some(Bytes::from(s)))
        }
        "MYID" => {
            let id = {
                let st = server_ctx.cluster.read().unwrap();
                st.myself.0.clone()
            };
            Resp::BulkString(Some(Bytes::from(id)))
        }
        "KEYSLOT" => {
            if items.len() != 3 {
                return Resp::Error(
                    "ERR wrong number of arguments for 'cluster keyslot'".to_string(),
                );
            }
            let key = match as_bytes(&items[2]) {
                Some(b) => String::from_utf8_lossy(&b).to_string(),
                None => return Resp::Error("ERR invalid key".to_string()),
            };
            let slot = crate::cluster::ClusterState::key_slot(&key) as i64;
            Resp::Integer(slot)
        }
        "COUNTKEYSINSLOT" => {
            if items.len() != 3 {
                return Resp::Error(
                    "ERR wrong number of arguments for 'cluster countkeysinslot'".to_string(),
                );
            }
            let slot = match as_bytes(&items[2]) {
                Some(b) => match String::from_utf8_lossy(&b).parse::<u16>() {
                    Ok(n) => n,
                    Err(_) => return Resp::Error("ERR invalid slot".to_string()),
                },
                None => return Resp::Error("ERR invalid slot".to_string()),
            };
            let db = {
                let db_lock = server_ctx.databases[0].read().unwrap();
                db_lock.clone()
            };
            let mut cnt = 0i64;
            for e in db.iter() {
                let k = e.key();
                let key_str = String::from_utf8_lossy(&k[..]).to_string();
                let s = crate::cluster::ClusterState::key_slot(&key_str);
                if s == slot {
                    cnt += 1;
                }
            }
            Resp::Integer(cnt)
        }
        "GETKEYSINSLOT" => {
            if items.len() != 4 {
                return Resp::Error(
                    "ERR wrong number of arguments for 'cluster getkeysinslot'".to_string(),
                );
            }
            let slot = match as_bytes(&items[2]) {
                Some(b) => match String::from_utf8_lossy(&b).parse::<u16>() {
                    Ok(n) => n,
                    Err(_) => return Resp::Error("ERR invalid slot".to_string()),
                },
                None => return Resp::Error("ERR invalid slot".to_string()),
            };
            let count = match as_bytes(&items[3]) {
                Some(b) => match String::from_utf8_lossy(&b).parse::<usize>() {
                    Ok(n) => n,
                    Err(_) => return Resp::Error("ERR invalid count".to_string()),
                },
                None => return Resp::Error("ERR invalid count".to_string()),
            };
            let db = {
                let db_lock = server_ctx.databases[0].read().unwrap();
                db_lock.clone()
            };
            let mut res = Vec::new();
            for e in db.iter() {
                if res.len() >= count {
                    break;
                }
                let k = e.key();
                let key_str = String::from_utf8_lossy(&k[..]).to_string();
                let s = crate::cluster::ClusterState::key_slot(&key_str);
                if s == slot {
                    res.push(Resp::BulkString(Some(Bytes::from(k.clone()))));
                }
            }
            Resp::Array(Some(res))
        }
        "MEET" => {
            if items.len() < 4 {
                return Resp::Error("ERR wrong number of arguments for 'cluster meet'".to_string());
            }
            let ip = match as_bytes(&items[2]) {
                Some(b) => String::from_utf8_lossy(&b).to_string(),
                None => return Resp::Error("ERR invalid ip".to_string()),
            };
            let port = match as_bytes(&items[3]) {
                Some(b) => match String::from_utf8_lossy(&b).parse::<u16>() {
                    Ok(p) => p,
                    Err(_) => return Resp::Error("ERR invalid port".to_string()),
                },
                None => return Resp::Error("ERR invalid port".to_string()),
            };
            {
                let mut st = server_ctx.cluster.write().unwrap();
                // Only add placeholder if no node with same address exists
                let exists = st.nodes.values().any(|n| n.ip == ip && n.port == port);
                if !exists {
                    let id = NodeId(format!("{}:{}", ip, port));
                    let _ = st.add_node(id, ip.clone(), port, NodeRole::Master, None);
                }
            }
            let ctx_clone = server_ctx.clone();
            let ip_clone = ip.clone();
            tokio::spawn(async move {
                // Try to quickly replace placeholder with real run_id via MYID
                if let Ok(Some(myid)) =
                    crate::cmd::fetch_cluster_myid(&ctx_clone, &ip_clone, port).await
                {
                    if let Ok(mut st) = ctx_clone.cluster.write() {
                        let node = crate::cluster::ClusterNode {
                            id: NodeId(myid),
                            ip: ip_clone.clone(),
                            port,
                            role: crate::cluster::NodeRole::Master,
                            slots: vec![],
                            epoch: 0,
                            master_id: None,
                        };
                        st.merge_topology(vec![node]);
                    }
                }
                if let Ok(Some(text)) =
                    crate::cmd::fetch_cluster_nodes_text(&ctx_clone, &ip_clone, port).await
                {
                    if let Ok(parsed) =
                        crate::cluster::ClusterState::parse_nodes_overview_text(&text)
                    {
                        if let Ok(mut st) = ctx_clone.cluster.write() {
                            st.merge_topology(parsed);
                        }
                    }
                }
                let my_ip = ctx_clone.config.bind.clone();
                let my_port = ctx_clone.config.port;
                let _ = crate::cmd::send_cluster_meet(&ctx_clone, &ip_clone, port, &my_ip, my_port)
                    .await;
            });
            Resp::SimpleString(Bytes::from_static(b"OK"))
        }
        "ADDSLOTS" => {
            if items.len() < 3 {
                return Resp::Error(
                    "ERR wrong number of arguments for 'cluster addslots'".to_string(),
                );
            }
            let mut slots = Vec::new();
            for it in items.iter().skip(2) {
                if let Some(b) = as_bytes(it) {
                    if let Ok(s) = std::str::from_utf8(&b) {
                        if let Ok(n) = s.parse::<u16>() {
                            slots.push(n);
                            continue;
                        }
                    }
                }
                return Resp::Error("ERR invalid slot".to_string());
            }
            {
                let mut st = server_ctx.cluster.write().unwrap();
                let me = st.myself.clone();
                if let Err(e) = st.add_slots(&me, &slots) {
                    return Resp::Error(format!("ERR {}", e));
                }
            }
            Resp::SimpleString(Bytes::from_static(b"OK"))
        }
        "ADDSLOTSRANGE" => {
            if items.len() < 4 || (items.len() - 2) % 2 != 0 {
                return Resp::Error(
                    "ERR wrong number of arguments for 'cluster addslotsrange'".to_string(),
                );
            }
            let mut slots = Vec::new();
            let mut it = items.iter().skip(2);
            while let Some(start_b) = it.next().and_then(as_bytes) {
                let end_b = match it.next().and_then(as_bytes) {
                    Some(b) => b,
                    None => return Resp::Error("ERR invalid slot range".to_string()),
                };
                let start = match std::str::from_utf8(&start_b)
                    .ok()
                    .and_then(|s| s.parse::<u16>().ok())
                {
                    Some(v) => v,
                    None => return Resp::Error("ERR invalid slot".to_string()),
                };
                let end = match std::str::from_utf8(&end_b)
                    .ok()
                    .and_then(|s| s.parse::<u16>().ok())
                {
                    Some(v) => v,
                    None => return Resp::Error("ERR invalid slot".to_string()),
                };
                if start > end {
                    return Resp::Error("ERR invalid slot range".to_string());
                }
                for x in start..=end {
                    slots.push(x);
                }
            }
            {
                let mut st = server_ctx.cluster.write().unwrap();
                let me = st.myself.clone();
                if let Err(e) = st.add_slots(&me, &slots) {
                    return Resp::Error(format!("ERR {}", e));
                }
            }
            Resp::SimpleString(Bytes::from_static(b"OK"))
        }
        "DELSLOTS" => {
            if items.len() < 3 {
                return Resp::Error(
                    "ERR wrong number of arguments for 'cluster delslots'".to_string(),
                );
            }
            let mut slots = Vec::new();
            for it in items.iter().skip(2) {
                if let Some(b) = as_bytes(it) {
                    if let Ok(s) = std::str::from_utf8(&b) {
                        if let Ok(n) = s.parse::<u16>() {
                            slots.push(n);
                            continue;
                        }
                    }
                }
                return Resp::Error("ERR invalid slot".to_string());
            }
            {
                let mut st = server_ctx.cluster.write().unwrap();
                let me = st.myself.clone();
                if let Err(e) = st.del_slots(&me, &slots) {
                    return Resp::Error(format!("ERR {}", e));
                }
            }
            Resp::SimpleString(Bytes::from_static(b"OK"))
        }
        "DELSLOTSRANGE" => {
            if items.len() < 4 || (items.len() - 2) % 2 != 0 {
                return Resp::Error(
                    "ERR wrong number of arguments for 'cluster delslotsrange'".to_string(),
                );
            }
            let mut slots = Vec::new();
            let mut it = items.iter().skip(2);
            while let Some(start_b) = it.next().and_then(as_bytes) {
                let end_b = match it.next().and_then(as_bytes) {
                    Some(b) => b,
                    None => return Resp::Error("ERR invalid slot range".to_string()),
                };
                let start = match std::str::from_utf8(&start_b)
                    .ok()
                    .and_then(|s| s.parse::<u16>().ok())
                {
                    Some(v) => v,
                    None => return Resp::Error("ERR invalid slot".to_string()),
                };
                let end = match std::str::from_utf8(&end_b)
                    .ok()
                    .and_then(|s| s.parse::<u16>().ok())
                {
                    Some(v) => v,
                    None => return Resp::Error("ERR invalid slot".to_string()),
                };
                if start > end {
                    return Resp::Error("ERR invalid slot range".to_string());
                }
                for x in start..=end {
                    slots.push(x);
                }
            }
            {
                let mut st = server_ctx.cluster.write().unwrap();
                let me = st.myself.clone();
                if let Err(e) = st.del_slots(&me, &slots) {
                    return Resp::Error(format!("ERR {}", e));
                }
            }
            Resp::SimpleString(Bytes::from_static(b"OK"))
        }
        "SETSLOT" => {
            if items.len() < 4 {
                return Resp::Error(
                    "ERR wrong number of arguments for 'cluster setslot'".to_string(),
                );
            }
            let slot = match as_bytes(&items[2]) {
                Some(b) => match String::from_utf8_lossy(&b).parse::<u16>() {
                    Ok(n) => n,
                    Err(_) => return Resp::Error("ERR invalid slot".to_string()),
                },
                None => return Resp::Error("ERR invalid slot".to_string()),
            };
            let mode = match as_bytes(&items[3]) {
                Some(b) => String::from_utf8_lossy(&b).to_uppercase(),
                None => return Resp::Error("ERR invalid subcommand".to_string()),
            };
            match mode.as_str() {
                "STABLE" => {
                    let mut st = server_ctx.cluster.write().unwrap();
                    if (slot as usize) >= st.slot_state.len() {
                        return Resp::Error("ERR invalid slot".to_string());
                    }
                    st.slot_state[slot as usize] = SlotState::Stable;
                    Resp::SimpleString(Bytes::from_static(b"OK"))
                }
                "MIGRATING" => {
                    if items.len() < 5 {
                        return Resp::Error(
                            "ERR wrong number of arguments for 'cluster setslot migrating'"
                                .to_string(),
                        );
                    }
                    let to_id = match as_bytes(&items[4]) {
                        Some(b) => NodeId(String::from_utf8_lossy(&b).to_string()),
                        None => return Resp::Error("ERR invalid node id".to_string()),
                    };
                    let mut st = server_ctx.cluster.write().unwrap();
                    if (slot as usize) >= st.slot_state.len() {
                        return Resp::Error("ERR invalid slot".to_string());
                    }
                    st.slot_state[slot as usize] = SlotState::Migrating { to: to_id };
                    Resp::SimpleString(Bytes::from_static(b"OK"))
                }
                "IMPORTING" => {
                    if items.len() < 5 {
                        return Resp::Error(
                            "ERR wrong number of arguments for 'cluster setslot importing'"
                                .to_string(),
                        );
                    }
                    let from_id = match as_bytes(&items[4]) {
                        Some(b) => NodeId(String::from_utf8_lossy(&b).to_string()),
                        None => return Resp::Error("ERR invalid node id".to_string()),
                    };
                    let mut st = server_ctx.cluster.write().unwrap();
                    if (slot as usize) >= st.slot_state.len() {
                        return Resp::Error("ERR invalid slot".to_string());
                    }
                    st.slot_state[slot as usize] = SlotState::Importing { from: from_id };
                    Resp::SimpleString(Bytes::from_static(b"OK"))
                }
                "NODE" => {
                    if items.len() < 5 {
                        return Resp::Error(
                            "ERR wrong number of arguments for 'cluster setslot node'".to_string(),
                        );
                    }
                    let node_id = match as_bytes(&items[4]) {
                        Some(b) => NodeId(String::from_utf8_lossy(&b).to_string()),
                        None => return Resp::Error("ERR invalid node id".to_string()),
                    };
                    let mut st = server_ctx.cluster.write().unwrap();
                    if !st.nodes.contains_key(&node_id) {
                        return Resp::Error("ERR Unknown node".to_string());
                    }
                    if (slot as usize) >= st.slots.len() {
                        return Resp::Error("ERR invalid slot".to_string());
                    }
                    if let Some(prev) = st.slots[slot as usize].clone() {
                        let _ = st.del_slots(&prev, &[slot]);
                    }
                    if let Err(e) = st.add_slots(&node_id, &[slot]) {
                        return Resp::Error(format!("ERR {}", e));
                    }
                    Resp::SimpleString(Bytes::from_static(b"OK"))
                }
                _ => Resp::Error("ERR unknown setslot subcommand".to_string()),
            }
        }
        "FORGET" => {
            if items.len() != 3 {
                return Resp::Error(
                    "ERR wrong number of arguments for 'cluster forget'".to_string(),
                );
            }
            let node_id = match as_bytes(&items[2]) {
                Some(b) => NodeId(String::from_utf8_lossy(&b).to_string()),
                None => return Resp::Error("ERR invalid node id".to_string()),
            };
            let mut st = server_ctx.cluster.write().unwrap();
            if st.myself == node_id {
                return Resp::Error("ERR Cannot forget myself".to_string());
            }
            if let Some(n) = st.nodes.get(&node_id) {
                if !n.slots.is_empty() {
                    return Resp::Error("ERR Node has assigned slots".to_string());
                }
            } else {
                return Resp::SimpleString(Bytes::from_static(b"OK"));
            }
            st.nodes.remove(&node_id);
            Resp::SimpleString(Bytes::from_static(b"OK"))
        }
        "RESET" => {
            let mut st = server_ctx.cluster.write().unwrap();
            let my = st.myself.clone();
            let mut me = st.nodes.get(&my).cloned().unwrap();
            me.role = NodeRole::Master;
            me.master_id = None;
            me.slots.clear();
            st.nodes.clear();
            st.nodes.insert(my.clone(), me);
            for i in 0..st.slots.len() {
                st.slots[i] = None;
                st.slot_state[i] = SlotState::Stable;
            }
            st.current_epoch = 0;
            Resp::SimpleString(Bytes::from_static(b"OK"))
        }
        "REPLICATE" => {
            if items.len() != 3 {
                return Resp::Error(
                    "ERR wrong number of arguments for 'cluster replicate'".to_string(),
                );
            }
            let req_master_id = match as_bytes(&items[2]) {
                Some(b) => NodeId(String::from_utf8_lossy(&b).to_string()),
                None => return Resp::Error("ERR invalid node id".to_string()),
            };
            let mut st = server_ctx.cluster.write().unwrap();
            // Resolve master id:
            // 1) If exact id exists, use it.
            // 2) Otherwise, if the provided id looks like ip:port, try to find node by address.
            // 3) If still not found, return Unknown master.
            let mut master_id = req_master_id.clone();
            if !st.nodes.contains_key(&master_id) {
                if let Some((ip, port)) = parse_addr(&req_master_id.0) {
                    if let Some((nid, _)) =
                        st.nodes.iter().find(|(_, n)| n.ip == ip && n.port == port)
                    {
                        master_id = nid.clone();
                    }
                }
            }
            if !st.nodes.contains_key(&master_id) {
                return Resp::Error("ERR Unknown master".to_string());
            }
            let my = st.myself.clone();
            // Collect slots to remove without holding a mutable borrow
            let slots_to_remove: Vec<u16> = if let Some(me) = st.nodes.get(&my) {
                me.slots.iter().flat_map(|r| r.start..=r.end).collect()
            } else {
                Vec::new()
            };
            if !slots_to_remove.is_empty() {
                let _ = st.del_slots(&my, &slots_to_remove);
            }
            if let Some(me) = st.nodes.get_mut(&my) {
                me.role = NodeRole::Replica;
                me.master_id = Some(master_id);
            }
            Resp::SimpleString(Bytes::from_static(b"OK"))
        }
        "REPLICAS" | "SLAVES" => {
            if items.len() != 3 {
                return Resp::Error(
                    "ERR wrong number of arguments for 'cluster replicas'".to_string(),
                );
            }
            let master_id = match as_bytes(&items[2]) {
                Some(b) => NodeId(String::from_utf8_lossy(&b).to_string()),
                None => return Resp::Error("ERR invalid node id".to_string()),
            };
            let st = server_ctx.cluster.read().unwrap();
            let mut lines = Vec::new();
            for n in st.nodes.values() {
                if n.role == NodeRole::Replica && n.master_id.as_ref() == Some(&master_id) {
                    lines.push(format!("{} {}:{} replica", n.id.0, n.ip, n.port));
                }
            }
            let mut s = lines.join("\n");
            s.push('\n');
            Resp::BulkString(Some(Bytes::from(s)))
        }
        "SLOTS" => {
            let st = server_ctx.cluster.read().unwrap();
            let mut res = Vec::new();
            for n in st.nodes.values() {
                if n.role == NodeRole::Master {
                    for r in &n.slots {
                        let mut entry = vec![
                            Resp::Integer(r.start as i64),
                            Resp::Integer(r.end as i64),
                            Resp::Array(Some(vec![
                                Resp::BulkString(Some(Bytes::from(n.ip.clone()))),
                                Resp::Integer(n.port as i64),
                                Resp::BulkString(Some(Bytes::from(n.id.0.clone()))),
                            ])),
                        ];
                        // 追加副本节点数组
                        let replicas: Vec<Resp> = st
                            .nodes
                            .values()
                            .filter(|x| {
                                x.role == NodeRole::Replica && x.master_id.as_ref() == Some(&n.id)
                            })
                            .map(|x| {
                                Resp::Array(Some(vec![
                                    Resp::BulkString(Some(Bytes::from(x.ip.clone()))),
                                    Resp::Integer(x.port as i64),
                                    Resp::BulkString(Some(Bytes::from(x.id.0.clone()))),
                                ]))
                            })
                            .collect();
                        entry.push(Resp::Array(Some(replicas)));
                        res.push(Resp::Array(Some(entry)));
                    }
                }
            }
            Resp::Array(Some(res))
        }
        "HELP" => {
            let help = vec![
                "ADDSLOTS",
                "BUMPEPOCH",
                "COUNTKEYSINSLOT",
                "DELSLOTS",
                "FLUSHSLOTS",
                "FORGET",
                "GETKEYSINSLOT",
                "INFO",
                "KEYSLOT",
                "LINKS",
                "MEET",
                "MYSHARDID",
                "MYID",
                "REPLICAS",
                "REPLICATE",
                "RESET",
                "SAVECONFIG",
                "SET-CONFIG-EPOCH",
                "SETSLOT",
                "SLOTS",
            ];
            let arr = help
                .into_iter()
                .map(|s| Resp::SimpleString(Bytes::from(s.to_string())))
                .collect();
            Resp::Array(Some(arr))
        }
        "INFO" => {
            let st = server_ctx.cluster.read().unwrap();
            let info = st.info_string();
            Resp::BulkString(Some(Bytes::from(info)))
        }
        "FLUSHSLOTS" => {
            let mut st = server_ctx.cluster.write().unwrap();
            let me = st.myself.clone();
            if let Some(node) = st.nodes.get_mut(&me) {
                let slots_to_remove: Vec<u16> =
                    node.slots.iter().flat_map(|r| r.start..=r.end).collect();
                if !slots_to_remove.is_empty() {
                    let _ = st.del_slots(&me, &slots_to_remove);
                }
            }
            Resp::SimpleString(Bytes::from_static(b"OK"))
        }
        "BUMPEPOCH" => {
            let mut st = server_ctx.cluster.write().unwrap();
            st.current_epoch += 1;
            Resp::SimpleString(Bytes::from_static(b"OK"))
        }
        "SAVECONFIG" => {
            let (text, path) = {
                let st = server_ctx.cluster.read().unwrap();
                let content = st.to_config_text();
                let p = std::path::Path::new(&server_ctx.config.dir)
                    .join(&server_ctx.config.cluster_config_file);
                (content, p)
            };
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            match std::fs::write(&path, text.as_bytes()) {
                Ok(_) => Resp::SimpleString(Bytes::from_static(b"OK")),
                Err(e) => Resp::Error(format!("ERR {}", e)),
            }
        }
        "SET-CONFIG-EPOCH" => {
            if items.len() != 3 {
                return Resp::Error(
                    "ERR wrong number of arguments for 'cluster set-config-epoch'".to_string(),
                );
            }
            let epoch = match as_bytes(&items[2]) {
                Some(b) => match String::from_utf8_lossy(&b).parse::<u64>() {
                    Ok(n) => n,
                    Err(_) => return Resp::Error("ERR invalid epoch".to_string()),
                },
                None => return Resp::Error("ERR invalid epoch".to_string()),
            };
            let mut st = server_ctx.cluster.write().unwrap();
            if st.current_epoch != 0 {
                return Resp::Error("ERR Node already has a config epoch".to_string());
            }
            st.current_epoch = epoch;
            Resp::SimpleString(Bytes::from_static(b"OK"))
        }
        "LINKS" => Resp::Array(Some(vec![])),
        "MYSHARDID" => {
            let st = server_ctx.cluster.read().unwrap();
            Resp::BulkString(Some(Bytes::from(st.myself.0.clone())))
        }
        _ => Resp::Error("ERR unknown subcommand".to_string()),
    }
}
