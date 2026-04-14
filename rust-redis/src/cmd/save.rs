use crate::rdb;
use crate::resp::Resp;
use bytes::Bytes;
use tracing::{error, info};

use crate::cmd::ServerContext;
use std::sync::atomic::Ordering;

pub fn save(_items: &[Resp], ctx: &ServerContext) -> Resp {
    match rdb::rdb_save(&ctx.databases, &ctx.config) {
        Ok(_) => {
            ctx.last_bgsave_ok.store(true, Ordering::Relaxed);
            ctx.dirty.store(0, Ordering::Relaxed);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            ctx.last_save_time.store(now, Ordering::Relaxed);
            Resp::SimpleString(Bytes::from("OK"))
        }
        Err(e) => Resp::Error(format!("ERR {}", e)),
    }
}

pub fn bgsave(_items: &[Resp], ctx: &ServerContext) -> Resp {
    if ctx.rdb_child_pid.load(Ordering::Relaxed) != -1 {
        return Resp::Error("ERR background save already in progress".to_string());
    }

    let databases_clone = ctx.databases.clone();
    let config_clone = ctx.config.clone();
    let last_bgsave_ok = ctx.last_bgsave_ok.clone();
    let rdb_child_pid = ctx.rdb_child_pid.clone();
    let last_save_time = ctx.last_save_time.clone();
    let dirty = ctx.dirty.clone();

    // Use 1 as in-progress sentinel (replaces child PID; no fork involved)
    ctx.rdb_child_pid.store(1, Ordering::Relaxed);

    std::thread::spawn(move || {
        match rdb::rdb_save(&databases_clone, &config_clone) {
            Ok(_) => {
                last_bgsave_ok.store(true, Ordering::Relaxed);
                dirty.store(0, Ordering::Relaxed);
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
    let last_save = ctx.last_save_time.load(Ordering::Relaxed);
    Resp::Integer(last_save)
}
