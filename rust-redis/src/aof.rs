use tokio::fs::{File, OpenOptions};
use tokio::io::{self, AsyncWriteExt, BufWriter};
use crate::resp::{Resp, read_frame};
use crate::db::Db;
use crate::cmd::process_frame;
use std::pin::Pin;
use std::future::Future;

pub struct Aof {
    writer: BufWriter<File>,
}

impl Aof {
    pub async fn new(path: &str) -> io::Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(path)
            .await?;
        Ok(Aof {
            writer: BufWriter::new(file),
        })
    }

    pub async fn append(&mut self, frame: &Resp) -> io::Result<()> {
        write_resp(&mut self.writer, frame).await?;
        self.writer.flush().await?;
        Ok(())
    }

    pub async fn load(&self, path: &str, db: &Db) -> io::Result<()> {
        // Check if file exists first
        match tokio::fs::metadata(path).await {
            Ok(_) => {},
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        }

        let file = tokio::fs::File::open(path).await?;
        let mut reader = tokio::io::BufReader::new(file);

        loop {
            match read_frame(&mut reader).await {
                Ok(Some(frame)) => {
                    // process_frame might return a tuple or just Resp depending on our changes.
                    // Since we haven't changed process_frame yet, this code assumes it returns Resp.
                    // If we change it, we will need to update this line.
                    // For now, I'll assume I will update process_frame to return (Resp, Option<Resp>)
                    // So I will handle it here defensively or update it later.
                    // Actually, I can just ignore the return value.
                    let _ = process_frame(frame, db);
                }
                Ok(None) => break,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

fn write_resp<'a, W>(writer: &'a mut W, resp: &'a Resp) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resp::Resp;
    use bytes::Bytes;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_file() -> String {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        format!("/tmp/redis_aof_test_{}.aof", now)
    }

    #[tokio::test]
    async fn test_aof_append_and_load() {
        let path = temp_file();
        
        // 1. Create AOF and append commands
        {
            let mut aof = Aof::new(&path).await.expect("failed to create aof");
            
            // SET key1 value1
            let set_cmd = Resp::Array(Some(vec![
                Resp::BulkString(Some(Bytes::from("SET"))),
                Resp::BulkString(Some(Bytes::from("key1"))),
                Resp::BulkString(Some(Bytes::from("value1"))),
            ]));
            aof.append(&set_cmd).await.expect("failed to append set");

            // RPUSH list1 item1
            let rpush_cmd = Resp::Array(Some(vec![
                Resp::BulkString(Some(Bytes::from("RPUSH"))),
                Resp::BulkString(Some(Bytes::from("list1"))),
                Resp::BulkString(Some(Bytes::from("item1"))),
            ]));
            aof.append(&rpush_cmd).await.expect("failed to append rpush");
        }

        // 2. Load AOF into a new DB
        let db_new = Db::default();
        let aof_loader = Aof::new(&path).await.expect("failed to open aof for loading");
        aof_loader.load(&path, &db_new).await.expect("failed to load aof");

        // 3. Verify DB state
        // Check key1
        let val = db_new.get(&Bytes::from("key1"));
        assert!(val.is_some(), "key1 not found");
        match &val.unwrap().value {
            crate::db::Value::String(s) => assert_eq!(s, &Bytes::from("value1")),
            _ => panic!("expected string for key1"),
        }

        // Check list1
        let list = db_new.get(&Bytes::from("list1"));
        assert!(list.is_some(), "list1 not found");
        match &list.unwrap().value {
            crate::db::Value::List(l) => {
                assert_eq!(l.len(), 1);
                assert_eq!(l[0], Bytes::from("item1"));
            },
            _ => panic!("expected list for list1"),
        }

        // Cleanup
        tokio::fs::remove_file(&path).await.expect("failed to remove temp file");
    }
}
