use crate::conf::Config;
use crate::db::Db;
use crate::rdb;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::Arc;
use tracing::{error, info};

use std::sync::RwLock;

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
    let databases_clone = ctx.databases.clone();
    let config_clone = ctx.config.clone();
    let last_bgsave_ok = ctx.last_bgsave_ok.clone();
    let dirty = ctx.dirty.clone();
    let last_save_time = ctx.last_save_time.clone();

    std::thread::spawn(move || {
        if let Err(e) = rdb::rdb_save(&databases_clone, &config_clone) {
            error!("Background saving failed: {}", e);
            last_bgsave_ok.store(false, Ordering::Relaxed);
        } else {
            info!("Background saving terminated with success");
            last_bgsave_ok.store(true, Ordering::Relaxed);
            dirty.store(0, Ordering::Relaxed);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            last_save_time.store(now, Ordering::Relaxed);
        }
    });

    Resp::SimpleString(Bytes::from("Background saving started"))
}

pub fn lastsave(_items: &[Resp], ctx: &ServerContext) -> Resp {
    let last_save = ctx.last_save_time.load(Ordering::Relaxed);
    Resp::Integer(last_save)
}
