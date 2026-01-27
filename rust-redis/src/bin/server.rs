#![allow(unexpected_cfgs)]
#![allow(unused_imports)]
#![allow(dead_code)]
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::{error, info, warn};
#[path = "../aof.rs"]
mod aof;
#[path = "../cmd/mod.rs"]
mod cmd;
#[path = "../conf.rs"]
mod conf;
#[path = "../db.rs"]
mod db;
#[path = "../rdb.rs"]
mod rdb;
#[path = "../rax.rs"]
mod rax;
#[path = "../hll.rs"]
mod hll;
#[path = "../stream.rs"]
mod stream;
#[path = "../resp.rs"]
mod resp;
#[path = "../geo.rs"]
mod geo;
#[path = "../acl.rs"]
pub mod acl;

#[cfg(test)]
#[path = "../tests/mod.rs"]
mod tests;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cfg_path = std::env::args().nth(1);
    let cfg = match conf::load_config(cfg_path.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to load config {:?}, using default: {}", cfg_path, e);
            conf::Config::default()
        }
    };

    if let Some(path) = &cfg.logfile {
        let file_appender = tracing_appender::rolling::never(".", path);
        let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(non_blocking)
            .init();
        // The guard must be held for the lifetime of the application
        // We move it into a long-lived async block or just keep it in main scope,
        // but main is async. _guard drop will flush logs.
        // However, we enter a loop at the end of main, so _guard will be dropped only when main returns.
        run_server(cfg, Some(_guard)).await;
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .init();
        run_server(cfg, None).await;
    }
}

async fn run_server(
    cfg: conf::Config,
    _guard: Option<tracing_appender::non_blocking::WorkerGuard>,
) {
    let addr = cfg.address();
    info!("starting server, listen on {}", addr);
    if let Some(path) = &cfg.logfile {
        info!("logging to file: {}", path);
    }

    let listener = TcpListener::bind(&addr).await.unwrap();
    
    // Initialize multiple databases
    let mut dbs = Vec::with_capacity(cfg.databases as usize);
    for _ in 0..cfg.databases {
        dbs.push(db::Db::default());
    }
    let databases = Arc::new(dbs);

    if !cfg.appendonly {
        if let Err(e) = rdb::rdb_load(&databases, &cfg) {
            warn!("Failed to load RDB: {}", e);
        }
    }

    // Create script cache
    let script_manager = cmd::scripting::create_script_manager();
    
    // Initialize ACL
    let mut acl_store = acl::Acl::new();
    
    // Load from ACL file if configured
    if let Some(acl_file) = &cfg.aclfile {
        // If the file doesn't exist, we just start with default ACL.
        // If it exists, we try to load it.
        if std::path::Path::new(acl_file).exists() {
            if let Err(e) = acl_store.load_from_file(acl_file) {
                warn!("Failed to load ACL file {}: {}", acl_file, e);
            } else {
                info!("Loaded ACL from file: {}", acl_file);
            }
        }
    }

    // Apply requirepass to default user if set (compatibility)
    if let Some(pass) = &cfg.requirepass {
        if let Some(default_user_arc) = acl_store.users.get("default") {
             let mut default_user = (**default_user_arc).clone();
             // Add the password
             default_user.passwords.insert(pass.clone());
             acl_store.set_user(default_user);
        }
    }

    let acl = Arc::new(std::sync::RwLock::new(acl_store));

    let aof = if cfg.appendonly {
        info!("AOF enabled, file: {}", cfg.appendfilename);
        let aof = aof::Aof::new(&cfg.appendfilename, cfg.appendfsync)
            .await
            .expect("failed to open AOF file");
        aof.load(&cfg.appendfilename, &databases, &cfg, &script_manager)
            .await
            .expect("failed to load AOF");
        Some(Arc::new(Mutex::new(aof)))
    } else {
        None
    };

    // Create script cache if not created (e.g. AOF disabled), or share the one used for loading
    // Since we can't easily extract it from the if/else block without defining it outside, let's define it outside.

    let cfg_arc = Arc::new(cfg);

    // Background task to clean up expired keys
    let databases_for_cleanup = databases.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(100));
        loop {
            interval.tick().await;
            for db in databases_for_cleanup.iter() {
                db.retain(|_, v| !v.is_expired());
            }
        }
    });

    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        info!("accepted connection from {}", addr);
        let databases_cloned = databases.clone();
        let aof_cloned = aof.clone();
        let cfg_cloned = cfg_arc.clone();
        let script_manager_cloned = script_manager.clone();
        let acl_cloned = acl.clone();

        tokio::spawn(async move {
            let (read_half, write_half) = socket.into_split();
            let mut reader = BufReader::new(read_half);
            let mut writer = BufWriter::new(write_half);
            let mut db_index = 0;
            let mut authenticated = false;
            let mut current_username = "default".to_string();

            loop {
                let frame = match resp::read_frame(&mut reader).await {
                    Ok(Some(f)) => f,
                    Ok(None) => return,
                    Err(_) => return,
                };
                let (response, cmd_to_log) = cmd::process_frame(
                    frame,
                    &databases_cloned,
                    &mut db_index,
                    &mut authenticated,
                    &mut current_username,
                    &acl_cloned,
                    &aof_cloned,
                    &cfg_cloned,
                    &script_manager_cloned,
                );

                if resp::write_frame(&mut writer, &response).await.is_err() {
                    return;
                }

                if let Some(cmd) = cmd_to_log {
                    if let Some(aof) = &aof_cloned {
                        if let Err(e) = aof.lock().await.append(&cmd).await {
                            error!("failed to append to AOF: {}", e);
                        }
                    }
                }

                // Smart flush: only flush if the buffer is empty, meaning we might wait for IO.
                // If the buffer is not empty, we have more pipelined requests to process immediately.
                if reader.buffer().is_empty() {
                    if writer.flush().await.is_err() {
                        return;
                    }
                }
            }
        });
    }
}
