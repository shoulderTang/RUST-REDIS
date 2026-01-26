use crate::cmd::process_frame;
use crate::cmd::scripting::ScriptManager;
use crate::conf::Config;
use crate::db::{Db, Value};
use crate::resp::{Resp, read_frame};
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::{File, OpenOptions};
use tokio::io::{self, AsyncWriteExt, BufWriter};
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AppendFsync {
    Always,
    EverySec,
    No,
}

pub struct Aof {
    writer: BufWriter<File>,
    path: String,
    policy: AppendFsync,
    sync_task: Option<JoinHandle<()>>,
}

impl Aof {
    pub async fn new(path: &str, policy: AppendFsync) -> io::Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(path)
            .await?;

        let sync_task = if policy == AppendFsync::EverySec {
            let file_clone = file.try_clone().await?;
            Some(tokio::spawn(async move {
                let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
                loop {
                    interval.tick().await;
                    if let Err(e) = file_clone.sync_data().await {
                        // Log error but don't panic?
                        eprintln!("AOF background sync failed: {}", e);
                    }
                }
            }))
        } else {
            None
        };

        Ok(Aof {
            writer: BufWriter::new(file),
            path: path.to_string(),
            policy,
            sync_task,
        })
    }

    pub async fn append(&mut self, frame: &Resp) -> io::Result<()> {
        write_resp(&mut self.writer, frame).await?;
        self.writer.flush().await?;

        if self.policy == AppendFsync::Always {
            self.writer.get_mut().sync_all().await?;
        }

        Ok(())
    }

    pub async fn load(
        &self,
        path: &str,
        db: &Db,
        cfg: &Config,
        script_manager: &Arc<ScriptManager>,
    ) -> io::Result<()> {
        // Check if file exists first
        match tokio::fs::metadata(path).await {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        }

        let file = tokio::fs::File::open(path).await?;
        let mut reader = tokio::io::BufReader::new(file);

        loop {
            match read_frame(&mut reader).await {
                Ok(Some(frame)) => {
                    // process_frame requires Aof option now, but during load we pass None
                    // to avoid recursive or circular dependency issues and because we don't want to log loaded commands
                    let _ = process_frame(frame, db, &None, cfg, script_manager);
                }
                Ok(None) => break,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    pub async fn rewrite(&mut self, db: &Db) -> io::Result<()> {
        let temp_path = format!("{}.tmp", self.path);
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)
            .await?;
        let mut writer = BufWriter::new(file);

        // Iterate over DB and write reconstruction commands
        for entry in db.iter() {
            let key = entry.key();
            let val = entry.value();

            // Check if expired
            if val.is_expired() {
                continue;
            }

            let cmd = match &val.value {
                Value::String(v) => Some(Resp::Array(Some(vec![
                    Resp::BulkString(Some(Bytes::from("SET"))),
                    Resp::BulkString(Some(key.clone())),
                    Resp::BulkString(Some(v.clone())),
                ]))),
                Value::List(l) => {
                    let mut args = Vec::with_capacity(2 + l.len());
                    args.push(Resp::BulkString(Some(Bytes::from("RPUSH"))));
                    args.push(Resp::BulkString(Some(key.clone())));
                    for item in l {
                        args.push(Resp::BulkString(Some(item.clone())));
                    }
                    Some(Resp::Array(Some(args)))
                }
                Value::Hash(h) => {
                    let mut args = Vec::with_capacity(2 + h.len() * 2);
                    args.push(Resp::BulkString(Some(Bytes::from("HMSET"))));
                    args.push(Resp::BulkString(Some(key.clone())));
                    for (f, v) in h {
                        args.push(Resp::BulkString(Some(f.clone())));
                        args.push(Resp::BulkString(Some(v.clone())));
                    }
                    Some(Resp::Array(Some(args)))
                }
                Value::Set(s) => {
                    let mut args = Vec::with_capacity(2 + s.len());
                    args.push(Resp::BulkString(Some(Bytes::from("SADD"))));
                    args.push(Resp::BulkString(Some(key.clone())));
                    for m in s {
                        args.push(Resp::BulkString(Some(m.clone())));
                    }
                    Some(Resp::Array(Some(args)))
                }
                Value::ZSet(z) => {
                    let mut args = Vec::with_capacity(2 + z.members.len() * 2);
                    args.push(Resp::BulkString(Some(Bytes::from("ZADD"))));
                    args.push(Resp::BulkString(Some(key.clone())));
                    for (m, s) in &z.members {
                        args.push(Resp::BulkString(Some(Bytes::from(s.to_string()))));
                        args.push(Resp::BulkString(Some(m.clone())));
                    }
                    Some(Resp::Array(Some(args)))
                }
                Value::Stream(s) => {
                    // 1. Reconstruct entries
                    let start = crate::stream::StreamID::new(0, 0);
                    let end = crate::stream::StreamID::new(u64::MAX, u64::MAX);
                    let entries = s.range(&start, &end);

                    for entry in entries {
                        let mut args = Vec::with_capacity(3 + entry.fields.len() * 2);
                        args.push(Resp::BulkString(Some(Bytes::from("XADD"))));
                        args.push(Resp::BulkString(Some(key.clone())));
                        args.push(Resp::BulkString(Some(Bytes::from(entry.id.to_string()))));
                        for (f, v) in entry.fields {
                            args.push(Resp::BulkString(Some(f)));
                            args.push(Resp::BulkString(Some(v)));
                        }
                        let cmd = Resp::Array(Some(args));
                        write_resp(&mut writer, &cmd).await?;
                    }

                    // 2. Reconstruct groups
                    for (name, group) in &s.groups {
                        let mut args = Vec::with_capacity(6);
                        args.push(Resp::BulkString(Some(Bytes::from("XGROUP"))));
                        args.push(Resp::BulkString(Some(Bytes::from("CREATE"))));
                        args.push(Resp::BulkString(Some(key.clone())));
                        args.push(Resp::BulkString(Some(Bytes::from(name.clone()))));
                        args.push(Resp::BulkString(Some(Bytes::from(group.last_id.to_string()))));
                        args.push(Resp::BulkString(Some(Bytes::from("MKSTREAM"))));
                        
                        let cmd = Resp::Array(Some(args));
                        write_resp(&mut writer, &cmd).await?;
                    }
                    None
                }
                Value::HyperLogLog(hll) => Some(Resp::Array(Some(vec![
                    Resp::BulkString(Some(Bytes::from("SET"))),
                    Resp::BulkString(Some(key.clone())),
                    Resp::BulkString(Some(Bytes::copy_from_slice(&hll.registers))),
                ]))),
            };

            if let Some(c) = cmd {
                write_resp(&mut writer, &c).await?;
            }

            // Handle expiration
            if let Some(expires_at) = val.expires_at {
                let pexpireat_cmd = Resp::Array(Some(vec![
                    Resp::BulkString(Some(Bytes::from("PEXPIREAT"))),
                    Resp::BulkString(Some(key.clone())),
                    Resp::BulkString(Some(Bytes::from(expires_at.to_string()))),
                ]));
                write_resp(&mut writer, &pexpireat_cmd).await?;
            }
        }

        writer.flush().await?;
        writer.get_mut().sync_all().await?; // Ensure data is safe before rename
        drop(writer); // Close file

        // Rename temp to real
        tokio::fs::rename(&temp_path, &self.path).await?;

        // Stop old sync task if exists
        if let Some(task) = self.sync_task.take() {
            task.abort();
        }

        // Reopen writer
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;

        // Restart sync task if needed
        if self.policy == AppendFsync::EverySec {
            let file_clone = file.try_clone().await?;
            self.sync_task = Some(tokio::spawn(async move {
                let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
                loop {
                    interval.tick().await;
                    if let Err(e) = file_clone.sync_data().await {
                        eprintln!("AOF background sync failed: {}", e);
                    }
                }
            }));
        }

        self.writer = BufWriter::new(file);

        Ok(())
    }
}

fn write_resp<'a, W>(
    writer: &'a mut W,
    resp: &'a Resp,
) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>
where
    W: AsyncWriteExt + Unpin + Send,
{
    Box::pin(async move {
        match resp {
            Resp::SimpleString(s) => {
                writer.write_all(b"+").await?;
                writer.write_all(s).await?;
                writer.write_all(b"\r\n").await?;
            }
            Resp::Error(s) => {
                writer.write_all(b"-").await?;
                writer.write_all(s.as_bytes()).await?;
                writer.write_all(b"\r\n").await?;
            }
            Resp::Integer(i) => {
                writer.write_all(b":").await?;
                writer.write_all(i.to_string().as_bytes()).await?;
                writer.write_all(b"\r\n").await?;
            }
            Resp::BulkString(Some(b)) => {
                writer.write_all(b"$").await?;
                writer.write_all(b.len().to_string().as_bytes()).await?;
                writer.write_all(b"\r\n").await?;
                writer.write_all(b).await?;
                writer.write_all(b"\r\n").await?;
            }
            Resp::BulkString(None) => {
                writer.write_all(b"$-1\r\n").await?;
            }
            Resp::Array(Some(items)) => {
                writer.write_all(b"*").await?;
                writer.write_all(items.len().to_string().as_bytes()).await?;
                writer.write_all(b"\r\n").await?;
                for item in items {
                    write_resp(writer, item).await?;
                }
            }
            Resp::Array(None) => {
                writer.write_all(b"*-1\r\n").await?;
            }
        }
        Ok(())
    })
}
