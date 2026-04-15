use crate::cmd::{ConnectionContext, ServerContext, WaitContext};
use crate::rdb::{RdbEncoder, RdbLoader};
use crate::resp::{Resp, read_frame, write_frame};
use bytes::Bytes;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::time::{self, Duration};
use tracing::{error, info};

use rand::Rng;

pub fn replicaof(items: &[Resp], ctx: &ServerContext) -> Resp {
    if items.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'replicaof' command".to_string());
    }
    let host = match &items[1] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
        Resp::SimpleString(b) => String::from_utf8_lossy(b).to_string(),
        _ => return Resp::Error("ERR invalid host".to_string()),
    };
    let port_s = match &items[2] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
        Resp::SimpleString(b) => String::from_utf8_lossy(b).to_string(),
        _ => return Resp::Error("ERR invalid port".to_string()),
    };

    if host.eq_ignore_ascii_case("NO") && port_s.eq_ignore_ascii_case("ONE") {
        if let Ok(mut role) = ctx.replication_role.write() {
            *role = crate::cmd::ReplicationRole::Master;
        }
        if let Ok(mut mh) = ctx.master_host.write() {
            *mh = None;
        }
        if let Ok(mut mp) = ctx.master_port.write() {
            *mp = None;
        }

        // Shift replication ID
        {
            let mut run_id_guard = ctx.run_id.write().unwrap();
            let mut replid2_guard = ctx.replid2.write().unwrap();

            *replid2_guard = run_id_guard.clone();
            ctx.second_repl_offset.store(
                ctx.repl_offset.load(std::sync::atomic::Ordering::Relaxed) as i64 + 1,
                std::sync::atomic::Ordering::Relaxed,
            );

            // Generate new run_id
            let mut rng = rand::rng();
            *run_id_guard = (0..40)
                .map(|_| rng.sample(rand::distr::Alphanumeric) as char)
                .collect();
        }

        return Resp::SimpleString(Bytes::from_static(b"OK"));
    }

    let port: u16 = match port_s.parse() {
        Ok(p) => p,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    if let Ok(mut role) = ctx.replication_role.write() {
        *role = crate::cmd::ReplicationRole::Slave;
    }
    if let Ok(mut mh) = ctx.master_host.write() {
        *mh = Some(host.clone());
    }
    if let Ok(mut mp) = ctx.master_port.write() {
        *mp = Some(port);
    }

    let ctx_cloned = ctx.clone();
    tokio::spawn(async move {
        if let Err(e) = replication_worker(&ctx_cloned, &host, port).await {
            error!("Replication worker exited with error: {}", e);
        }
        ctx_cloned
            .master_link_established
            .store(false, std::sync::atomic::Ordering::Relaxed);
        info!("Replication worker stopped, master link status set to down");
    });

    Resp::SimpleString(Bytes::from_static(b"OK"))
}

async fn replication_worker(
    ctx: &ServerContext,
    host: &str,
    port: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("{}:{}", host, port);
    let stream = TcpStream::connect(&addr).await?;
    let (read_half, write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut writer = BufWriter::new(write_half);

    let req = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from_static(b"PSYNC"))),
        Resp::BulkString(Some(Bytes::from("?".to_string()))),
        Resp::BulkString(Some(Bytes::from("-1".to_string()))),
    ]));
    write_frame(&mut writer, &req).await?;
    writer.flush().await?;

    let last_off = Arc::new(AtomicU64::new(0));
    let first_resp = read_frame(&mut reader).await?.ok_or("EOF during PSYNC")?;

    match first_resp {
        Resp::SimpleString(s) => {
            let s_str = String::from_utf8_lossy(&s);
            let parts: Vec<&str> = s_str.split_whitespace().collect();
            if parts.is_empty() {
                return Err("empty PSYNC response".into());
            }

            match parts[0] {
                "FULLRESYNC" => {
                    if parts.len() >= 3 {
                        if let Ok(off) = parts[2].parse::<u64>() {
                            last_off.store(off, std::sync::atomic::Ordering::Relaxed);
                        }
                    }

                    // Read RDB
                    let rdb_resp = read_frame(&mut reader)
                        .await?
                        .ok_or("EOF waiting for RDB")?;
                    let rdb_data = match rdb_resp {
                        Resp::BulkString(Some(b)) => b,
                        _ => return Err("invalid RDB payload".into()),
                    };

                    for db_lock in ctx.databases.iter() {
                        db_lock.write().unwrap().clear();
                    }
                    let mut loader = RdbLoader::new(Cursor::new(rdb_data.as_ref()));
                    loader.load(&ctx.databases)?;
                }
                "CONTINUE" => {
                    // CONTINUE [replid]
                    // Do nothing, just proceed to command loop
                }
                _ => return Err(format!("unknown PSYNC response: {}", parts[0]).into()),
            }
        }
        Resp::Error(e) => return Err(format!("PSYNC error: {}", e).into()),
        _ => return Err("invalid PSYNC response format".into()),
    }

    // Heartbeat task: periodically send PING and REPLCONF ACK <offset>
    ctx.master_link_established
        .store(true, std::sync::atomic::Ordering::Relaxed);
    info!("Master link established with {}:{}", host, port);

    let heartbeat_last_off = last_off.clone();
    let hb_interval_secs = ctx
        .repl_ping_replica_period
        .load(std::sync::atomic::Ordering::Relaxed);
    let interval_duration = if hb_interval_secs == 0 {
        Duration::from_secs(1)
    } else {
        Duration::from_secs(hb_interval_secs)
    };
    let mut hb_writer = writer;

    let listening_port = ctx.config.port;
    let lp_cmd = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from_static(b"REPLCONF"))),
        Resp::BulkString(Some(Bytes::from_static(b"listening-port"))),
        Resp::BulkString(Some(Bytes::from(listening_port.to_string()))),
    ]));
    write_frame(&mut hb_writer, &lp_cmd).await?;
    hb_writer.flush().await?;

    // Create a channel to manually send frames from the main loop
    let (tx_writer, mut rx_writer) = tokio::sync::mpsc::channel::<Resp>(100);

    tokio::spawn(async move {
        let mut ticker = time::interval(interval_duration);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let pong = Resp::Array(Some(vec![Resp::BulkString(Some(Bytes::from_static(b"PING")))]));
                    let ack = Resp::Array(Some(vec![
                        Resp::BulkString(Some(Bytes::from_static(b"REPLCONF"))),
                        Resp::BulkString(Some(Bytes::from_static(b"ACK"))),
                        Resp::BulkString(Some(Bytes::from(heartbeat_last_off.load(std::sync::atomic::Ordering::Relaxed).to_string()))),
                    ]));
                    if write_frame(&mut hb_writer, &pong).await.is_err() { break; }
                    if hb_writer.flush().await.is_err() { break; }
                    if write_frame(&mut hb_writer, &ack).await.is_err() { break; }
                    if hb_writer.flush().await.is_err() { break; }
                },
                msg = rx_writer.recv() => {
                    if let Some(resp) = msg {
                        if write_frame(&mut hb_writer, &resp).await.is_err() { break; }
                        if hb_writer.flush().await.is_err() { break; }
                    } else {
                        break;
                    }
                }
            }
        }
    });

    let mut conn_ctx = ConnectionContext::new(0, None, None, None);
    conn_ctx.authenticated = true;
    conn_ctx.is_master = true;

    let timeout_duration =
        Duration::from_secs(ctx.repl_timeout.load(std::sync::atomic::Ordering::Relaxed));

    loop {
        // Read frame with timeout
        let read_future = read_frame(&mut reader);
        let frame_opt = match tokio::time::timeout(timeout_duration, read_future).await {
            Ok(res) => res?,
            Err(_) => return Err("timeout reading from master".into()),
        };

        match frame_opt {
            Some(frame) => {
                if matches!(frame, Resp::Array(_)) {
                    // Check for REPLCONF GETACK
                    if let Resp::Array(Some(ref items)) = frame {
                        if items.len() >= 3 {
                            if let (
                                Some(Resp::BulkString(Some(cmd))),
                                Some(Resp::BulkString(Some(subcmd))),
                            ) = (items.get(0), items.get(1))
                            {
                                let cmd_s = String::from_utf8_lossy(cmd);
                                let sub_s = String::from_utf8_lossy(subcmd);
                                if cmd_s.eq_ignore_ascii_case("REPLCONF")
                                    && sub_s.eq_ignore_ascii_case("GETACK")
                                {
                                    // Send ACK immediately
                                    let ack = Resp::Array(Some(vec![
                                        Resp::BulkString(Some(Bytes::from_static(b"REPLCONF"))),
                                        Resp::BulkString(Some(Bytes::from_static(b"ACK"))),
                                        Resp::BulkString(Some(Bytes::from(
                                            last_off
                                                .load(std::sync::atomic::Ordering::Relaxed)
                                                .to_string(),
                                        ))),
                                    ]));
                                    let _ = tx_writer.send(ack).await;
                                }
                            }
                        }
                    }

                    // Check if the frame is a REPLCONF command, which should not be propagated or AOF'd
                    let is_replconf = if let Resp::Array(Some(ref items)) = frame {
                        if let Some(Resp::BulkString(Some(cmd))) = items.get(0) {
                            String::from_utf8_lossy(cmd).eq_ignore_ascii_case("REPLCONF")
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    // Clone frame for AOF if needed
                    let frame_for_aof = if ctx.aof.is_some() && !is_replconf {
                        Some(frame.clone())
                    } else {
                        None
                    };

                    // Clone frame for propagation and backlog
                    let frame_for_prop = if !is_replconf {
                        Some(frame.clone())
                    } else {
                        None
                    };

                    let _ = crate::cmd::process_frame(frame, &mut conn_ctx, ctx).await;

                    // Append to AOF if enabled
                    if let Some(frame_to_append) = frame_for_aof {
                        if let Some(aof) = &ctx.aof {
                            aof.append(&frame_to_append).await;
                        }
                    }

                    // Propagate to sub-replicas (Cascading replication)
                    if let Some(prop_frame) = frame_for_prop {
                        // Send the frame to all replicas.
                        for replica in ctx.replicas.iter() {
                            let _ = replica.value().try_send(prop_frame.clone());
                        }

                        // Also write to backlog
                        if let Ok(mut backlog) = ctx.repl_backlog.lock() {
                            // Limit backlog size
                            while backlog.len() > 10000 {
                                // Simple limit
                                backlog.pop_front();
                            }
                            let current = last_off.load(std::sync::atomic::Ordering::Relaxed);
                            backlog.push_back((current, prop_frame));
                        }
                    }

                    last_off.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // Also update global offset for INFO command correctness
                    ctx.repl_offset.store(
                        last_off.load(std::sync::atomic::Ordering::Relaxed),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
            }
            None => break,
        }
    }

    Ok(())
}

pub fn replconf(items: &[Resp], conn_ctx: &mut ConnectionContext, ctx: &ServerContext) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'replconf' command".to_string());
    }
    let sub = match &items[1] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
        Resp::SimpleString(s) => String::from_utf8_lossy(s).to_string(),
        _ => String::new(),
    };
    if sub.eq_ignore_ascii_case("listening-port") {
        if items.len() < 3 {
            return Resp::Error(
                "ERR wrong number of arguments for 'replconf listening-port'".to_string(),
            );
        }
        let (port, is_explicit_zero) = match &items[2] {
            Resp::BulkString(Some(b)) => {
                let port_str = String::from_utf8_lossy(b);
                let is_zero = port_str == "0";
                (port_str.parse::<u16>().unwrap_or(0), is_zero)
            }
            Resp::SimpleString(s) => {
                let port_str = String::from_utf8_lossy(s);
                let is_zero = port_str == "0";
                (port_str.parse::<u16>().unwrap_or(0), is_zero)
            }
            Resp::Integer(i) => {
                let port_int = *i as i64;
                let is_zero = port_int == 0;
                if port_int > 0 && port_int <= u16::MAX as i64 {
                    (port_int as u16, is_zero)
                } else {
                    (0, is_zero)
                }
            }
            _ => (0, false),
        };
        if port > 0 || (port == 0 && !is_explicit_zero) {
            ctx.replica_listening_port.insert(conn_ctx.id, port);
        } else {
            ctx.replica_listening_port.remove(&conn_ctx.id);
        }
        return Resp::SimpleString(Bytes::from_static(b"OK"));
    }
    if sub.eq_ignore_ascii_case("ACK") {
        if items.len() < 3 {
            return Resp::Error("ERR wrong number of arguments for 'replconf ack'".to_string());
        }
        let off = match &items[2] {
            Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).parse::<u64>().unwrap_or(0),
            Resp::SimpleString(s) => String::from_utf8_lossy(s).parse::<u64>().unwrap_or(0),
            Resp::Integer(i) => *i as u64,
            _ => 0,
        };
        ctx.replica_ack.insert(conn_ctx.id, off);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u64;
        ctx.replica_ack_time.insert(conn_ctx.id, now);

        // Check waiters
        if let Ok(mut waiters) = ctx.repl_waiters.lock() {
            let mut i = 0;
            while i < waiters.len() {
                let target_offset = waiters[i].target_offset;
                let num_replicas = waiters[i].num_replicas;
                let curr_ack_count = ctx
                    .replica_ack
                    .iter()
                    .filter(|r| *r.value() >= target_offset)
                    .count();

                if curr_ack_count >= num_replicas {
                    if let Some(w) = waiters.remove(i) {
                        let _ = w.tx.send(curr_ack_count);
                    }
                } else if waiters[i].tx.is_closed() {
                    waiters.remove(i);
                } else {
                    i += 1;
                }
            }
        }

        return Resp::SimpleString(Bytes::from_static(b"OK"));
    }
    Resp::SimpleString(Bytes::from_static(b"OK"))
}

pub async fn psync(items: &[Resp], conn_ctx: &mut ConnectionContext, ctx: &ServerContext) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'psync' command".to_string());
    }
    let req_runid = match &items[1] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
        Resp::SimpleString(b) => String::from_utf8_lossy(b).to_string(),
        _ => "?".to_string(),
    };
    let req_off: i64 = match &items[2] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).parse().unwrap_or(-1),
        Resp::SimpleString(b) => String::from_utf8_lossy(b).parse().unwrap_or(-1),
        Resp::Integer(v) => *v,
        _ => -1,
    };

    if let Some(sender) = &conn_ctx.msg_sender {
        ctx.replicas.insert(conn_ctx.id, sender.clone());
        conn_ctx.is_replica = true;
        if let Ok(mut state) = conn_ctx.replication_state.lock() {
            *state = crate::cmd::ReplicationState::TransferringRdb;
        }
        ctx.rdb_sync_client_id
            .store(conn_ctx.id, std::sync::atomic::Ordering::Relaxed);
    }

    let runid = ctx.run_id.read().unwrap().clone();
    let replid2 = ctx.replid2.read().unwrap().clone();
    let current_off = ctx.repl_offset.load(std::sync::atomic::Ordering::Relaxed) as i64;
    let second_off = ctx
        .second_repl_offset
        .load(std::sync::atomic::Ordering::Relaxed);

    let can_try_partial = if req_runid == runid {
        true
    } else if req_runid == replid2 {
        req_off <= second_off
    } else {
        false
    };

    if can_try_partial && req_off >= 0 {
        let mut to_send: Vec<Resp> = Vec::new();
        let earliest_off = {
            if let Ok(q) = ctx.repl_backlog.lock() {
                q.front().map(|(o, _)| *o as i64).unwrap_or(current_off)
            } else {
                current_off
            }
        };
        if req_off >= earliest_off {
            if let Ok(q) = ctx.repl_backlog.lock() {
                for (off, frame) in q.iter() {
                    if (*off as i64) > req_off {
                        to_send.push(frame.clone());
                    }
                }
            }
            if let Some(sender) = &conn_ctx.msg_sender {
                for f in to_send.into_iter() {
                    let _ = sender.try_send(f);
                }
            }
            return Resp::SimpleString(Bytes::from(format!("CONTINUE {}", runid)));
        }
    }

    // Delay start if configured (repl-diskless-sync + delay)
    if ctx
        .repl_diskless_sync
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        let delay = ctx
            .repl_diskless_sync_delay
            .load(std::sync::atomic::Ordering::Relaxed);
        if delay > 0 {
            tokio::time::sleep(tokio::time::Duration::from_secs(delay)).await;
        }
    }

    let header = Resp::SimpleString(Bytes::from(format!("FULLRESYNC {} {}", runid, current_off)));
    let compression = ctx
        .rdbcompression
        .load(std::sync::atomic::Ordering::Relaxed);
    let checksum = ctx.rdbchecksum.load(std::sync::atomic::Ordering::Relaxed);

    // Generate RDB snapshot in a blocking thread to avoid blocking the async runtime.
    // The entire snapshot is buffered in memory so the length prefix can be sent atomically.
    let databases_clone = ctx.databases.clone();
    let rdb_data = tokio::task::spawn_blocking(move || {
        let mut buf: Vec<u8> = Vec::new();
        let mut enc = RdbEncoder::new(&mut buf, compression, checksum);
        let _ = enc.save(&databases_clone);
        buf
    })
    .await
    .unwrap_or_default();

    Resp::Multiple(vec![header, Resp::BulkString(Some(Bytes::from(rdb_data)))])
}

pub async fn wait(items: &[Resp], _conn_ctx: &mut ConnectionContext, ctx: &ServerContext) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'wait' command".to_string());
    }

    let num_replicas: usize = match &items[1] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).parse().unwrap_or(0),
        Resp::SimpleString(s) => String::from_utf8_lossy(s).parse().unwrap_or(0),
        Resp::Integer(i) => *i as usize,
        _ => return Resp::Error("ERR invalid numreplicas".to_string()),
    };

    let timeout: u64 = match &items[2] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).parse().unwrap_or(0),
        Resp::SimpleString(s) => String::from_utf8_lossy(s).parse().unwrap_or(0),
        Resp::Integer(i) => *i as u64,
        _ => return Resp::Error("ERR invalid timeout".to_string()),
    };

    let current_offset = ctx.repl_offset.load(std::sync::atomic::Ordering::Relaxed) as u64;

    // Check currently acknowledged replicas
    let ack_count = ctx
        .replica_ack
        .iter()
        .filter(|r| *r.value() >= current_offset)
        .count();

    if ack_count >= num_replicas {
        return Resp::Integer(ack_count as i64);
    }

    // Request ACK from all replicas immediately
    let getack_cmd = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("REPLCONF"))),
        Resp::BulkString(Some(Bytes::from("GETACK"))),
        Resp::BulkString(Some(Bytes::from("*"))),
    ]));

    for replica in ctx.replicas.iter() {
        let _ = replica.value().try_send(getack_cmd.clone());
    }

    // Create waiter
    let (tx, rx) = tokio::sync::oneshot::channel();
    let waiter = WaitContext {
        target_offset: current_offset,
        num_replicas,
        tx,
    };

    {
        if let Ok(mut waiters) = ctx.repl_waiters.lock() {
            waiters.push_back(waiter);
        }
    }

    let timeout_fut = async {
        if timeout > 0 {
            time::sleep(Duration::from_millis(timeout)).await;
        } else {
            std::future::pending::<()>().await;
        }
    };

    tokio::select! {
        res = rx => {
            match res {
                Ok(count) => Resp::Integer(count as i64),
                Err(_) => Resp::Integer(ack_count as i64),
            }
        },
        _ = timeout_fut => {
            let count = ctx.replica_ack.iter().filter(|r| *r.value() >= current_offset).count();
             Resp::Integer(count as i64)
        }
    }
}
