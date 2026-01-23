#![allow(unexpected_cfgs)]
#![allow(unused_imports)]
#![allow(dead_code)]
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::{info, warn, error};
#[path = "../resp.rs"]
mod resp;
#[path = "../db.rs"]
mod db;
#[path = "../cmd/mod.rs"]
mod cmd;
#[path = "../conf.rs"]
mod conf;
#[path = "../aof.rs"]
mod aof;

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

async fn run_server(cfg: conf::Config, _guard: Option<tracing_appender::non_blocking::WorkerGuard>) {
    let addr = cfg.address();
    info!("starting server, listen on {}", addr);
    if let Some(path) = &cfg.logfile {
        info!("logging to file: {}", path);
    }
    
    let listener = TcpListener::bind(&addr).await.unwrap();
    //let db = Arc::new(db::Db::default());
    let db = db::Db::default();
    //let db = Arc::new(db);

    let aof = if cfg.appendonly {
        info!("AOF enabled, file: {}", cfg.appendfilename);
        let aof = aof::Aof::new(&cfg.appendfilename).await.expect("failed to open AOF file");
        aof.load(&cfg.appendfilename, &db).await.expect("failed to load AOF");
        Some(Arc::new(Mutex::new(aof)))
    } else {
        None
    };

    // Background task to clean up expired keys
    let db_for_cleanup = db.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(100));
        loop {
            interval.tick().await;
            db_for_cleanup.retain(|_, v| !v.is_expired());
        }
    });

    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        info!("accepted connection from {}", addr);
        let db_cloned = db.clone();
        let aof_cloned = aof.clone();

        tokio::spawn(async move {
            let (read_half, write_half) = socket.into_split();
            let mut reader = BufReader::new(read_half);
            let mut writer = BufWriter::new(write_half);

            loop {
                let frame = match resp::read_frame(&mut reader).await {
                    Ok(Some(f)) => f,
                    Ok(None) => return,
                    Err(_) => return,
                };
                let (response, cmd_to_log) = cmd::process_frame(frame, &db_cloned);

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
