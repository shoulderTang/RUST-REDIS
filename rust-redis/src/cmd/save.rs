use crate::rdb;
use crate::resp::Resp;
use bytes::Bytes;
use tracing::{error, info};

use crate::cmd::ServerContext;
use std::sync::atomic::Ordering;

pub fn save(_items: &[Resp], ctx: &ServerContext) -> Resp {
    // Snapshot dirty before the blocking save so we don't discard concurrent writes.
    let dirty_before = ctx.persist.dirty.load(Ordering::Relaxed);
    match rdb::rdb_save(&ctx.databases, &ctx.config) {
        Ok(_) => {
            ctx.persist.last_bgsave_ok.store(true, Ordering::Relaxed);
            ctx.persist.dirty.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(dirty_before))
            }).ok();
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            ctx.persist.last_save_time.store(now, Ordering::Relaxed);
            Resp::SimpleString(Bytes::from("OK"))
        }
        Err(e) => Resp::Error(format!("ERR {}", e)),
    }
}

pub fn bgsave(_items: &[Resp], ctx: &ServerContext) -> Resp {
    if ctx.persist.rdb_child_pid.load(Ordering::Relaxed) != -1 {
        return Resp::Error("ERR background save already in progress".to_string());
    }

    let databases_clone = ctx.databases.clone();
    let config_clone = ctx.config.clone();
    let last_bgsave_ok = ctx.persist.last_bgsave_ok.clone();
    let rdb_child_pid = ctx.persist.rdb_child_pid.clone();
    let last_save_time = ctx.persist.last_save_time.clone();
    let dirty = ctx.persist.dirty.clone();

    // Snapshot the dirty counter before the save starts.  On success we only
    // subtract this value so that writes arriving *during* the save are not lost.
    let dirty_before = ctx.persist.dirty.load(Ordering::Relaxed);

    // Use 1 as in-progress sentinel (replaces child PID; no fork involved)
    ctx.persist.rdb_child_pid.store(1, Ordering::Relaxed);

    std::thread::spawn(move || {
        match rdb::rdb_save(&databases_clone, &config_clone) {
            Ok(_) => {
                last_bgsave_ok.store(true, Ordering::Relaxed);
                // Subtract only what was dirty at save-start time; preserve any
                // additional writes that occurred while the save was running.
                dirty.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_sub(dirty_before))
                }).ok();
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64;
                last_save_time.store(now, Ordering::Relaxed);
                info!("Background saving terminated with success");
            }
            Err(e) => {
                last_bgsave_ok.store(false, Ordering::Relaxed);
                error!("Background saving failed: {}", e);
            }
        }
        rdb_child_pid.store(-1, Ordering::Relaxed);
    });

    info!("Background saving started");
    Resp::SimpleString(Bytes::from("Background saving started"))
}

pub fn lastsave(_items: &[Resp], ctx: &ServerContext) -> Resp {
    let last_save = ctx.persist.last_save_time.load(Ordering::Relaxed);
    Resp::Integer(last_save)
}
