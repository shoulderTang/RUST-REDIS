#![allow(unexpected_cfgs)]
#![allow(unused_imports)]
#![allow(dead_code)]
use std::future::Future;
use std::io::{self, ErrorKind};
use std::pin::Pin;
use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::mem;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{info, warn, error};
#[path = "../resp.rs"]
mod resp;
#[path = "../db.rs"]
mod db;
#[path = "../cmd.rs"]
mod cmd;
#[path = "../conf.rs"]
mod conf;

struct CommandMessage {
    frame: resp::Resp,
    resp_tx: mpsc::UnboundedSender<resp::Resp>,
}

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
    let (tx, mut rx) = mpsc::unbounded_channel::<CommandMessage>();

    tokio::spawn(async move {
        let mut db_state: db::Db = Default::default();
        while let Some(msg) = rx.recv().await {
            let CommandMessage { frame, resp_tx } = msg;
            let response = cmd::process_frame(frame, &mut db_state);
            let _ = resp_tx.send(response);
        }
    });

    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        info!("accepted connection from {}", addr);
        let tx_cloned = tx.clone();

        tokio::spawn(async move {
            let (read_half, write_half) = socket.into_split();
            let mut reader = BufReader::new(read_half);
            let mut writer = BufWriter::new(write_half);
            let (resp_tx, mut resp_rx) = mpsc::unbounded_channel::<resp::Resp>();

            loop {
                let frame = match resp::read_frame(&mut reader).await {
                    Ok(Some(f)) => f,
                    Ok(None) => return,
                    Err(_) => return,
                };
                if tx_cloned
                    .send(CommandMessage { frame, resp_tx: resp_tx.clone() })
                    .is_err()
                {
                    return;
                }
                let response = match resp_rx.recv().await {
                    Some(r) => r,
                    None => return,
                };
                if resp::write_frame(&mut writer, &response).await.is_err() {
                    return;
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
