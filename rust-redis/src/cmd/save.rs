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

use libc::{self, pid_t, c_int};

pub fn bgsave(_items: &[Resp], ctx: &ServerContext) -> Resp {
    let databases_clone = ctx.databases.clone();
    let config_clone = ctx.config.clone();
    let last_bgsave_ok = ctx.last_bgsave_ok.clone();
    
    // Check if child process already exists
    if ctx.rdb_child_pid.load(Ordering::Relaxed) != -1 {
        return Resp::Error("ERR background save already in progress".to_string());
    }

    unsafe {
        let pid = libc::fork();
        match pid {
            -1 => {
                error!("Background save fork failed");
                last_bgsave_ok.store(false, Ordering::Relaxed);
                Resp::Error("ERR background save fork failed".to_string())
            }
            0 => {
                // Child process
                // Close listening sockets and other resources if necessary (not strictly required for simple RDB save)
                // Perform synchronous RDB save
                let res = rdb::rdb_save(&databases_clone, &config_clone);
                let exit_code = if res.is_ok() { 0 } else { 1 };
                libc::_exit(exit_code);
            }
            child_pid => {
                // Parent process
                info!("Background saving started by pid {}", child_pid);
                ctx.rdb_child_pid.store(child_pid, Ordering::Relaxed);
                Resp::SimpleString(Bytes::from("Background saving started"))
            }
        }
    }
}

pub fn lastsave(_items: &[Resp], ctx: &ServerContext) -> Resp {
    let last_save = ctx.last_save_time.load(Ordering::Relaxed);
    Resp::Integer(last_save)
}
