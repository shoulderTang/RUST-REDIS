use crate::conf::Config;
use crate::db::Db;
use crate::rdb;
use crate::resp::Resp;
use bytes::Bytes;
use std::sync::Arc;
use tracing::{error, info};

pub fn save(_items: &[Resp], databases: &Arc<Vec<Db>>, config: &Config) -> Resp {
    match rdb::rdb_save(databases, config) {
        Ok(_) => Resp::SimpleString(Bytes::from("OK")),
        Err(e) => Resp::Error(format!("ERR {}", e)),
    }
}

pub fn bgsave(_items: &[Resp], databases: &Arc<Vec<Db>>, config: &Config) -> Resp {
    let databases_clone = databases.clone();
    let config_clone = config.clone();

    std::thread::spawn(move || {
        if let Err(e) = rdb::rdb_save(&databases_clone, &config_clone) {
            error!("Background saving failed: {}", e);
        } else {
            info!("Background saving terminated with success");
        }
    });

    Resp::SimpleString(Bytes::from("Background saving started"))
}
