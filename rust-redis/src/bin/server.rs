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
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use std::sync::Arc;
#[path = "../resp.rs"]
mod resp;
#[path = "../db.rs"]
mod db;
#[path = "../cmd.rs"]
mod cmd;

enum Resp {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<Resp>>),
}

#[cfg(feature = "rehash")]
pub struct RehashMap<K, V> {
    old: Vec<Vec<(K, V)>>,
    new: Option<Vec<Vec<(K, V)>>>,
    rehash_idx: usize,
    len: usize,
}

#[cfg(feature = "rehash")]
impl<K, V> RehashMap<K, V>
where
    K: Hash + Eq,
{
    pub fn new() -> Self {
        let cap = 64;
        let mut old = Vec::with_capacity(cap);
        old.resize_with(cap, Vec::new);
        Self {
            old,
            new: None,
            rehash_idx: 0,
            len: 0,
        }
    }
}

#[cfg(feature = "rehash")]
impl<K, V> RehashMap<K, V>
where
    K: Hash + Eq,
{
    fn index_for(&self, k: &K, cap: usize) -> usize {
        let mut h = RandomState::new().build_hasher();
        k.hash(&mut h);
        (h.finish() as usize) % cap
    }

    fn effective_capacity(&self) -> usize {
        match &self.new {
            Some(n) => n.len(),
            None => self.old.len(),
        }
    }

    fn need_grow(&self) -> bool {
        let cap = self.effective_capacity();
        self.len * 4 >= cap * 3
    }

    fn start_rehash_if_needed(&mut self) {
        if self.new.is_none() && self.need_grow() {
            let new_cap = self.old.len() * 2;
            let mut n = Vec::with_capacity(new_cap);
            n.resize_with(new_cap, Vec::new);
            self.new = Some(n);
            self.rehash_idx = 0;
        }
    }

    fn rehash_step(&mut self) {
        if self.new.is_some() {
            let old_len = self.old.len();
            let mut finish = false;
            if let Some(new) = self.new.as_mut() {
                if self.rehash_idx < old_len {
                    let mut bucket = mem::take(&mut self.old[self.rehash_idx]);
                    let new_len = new.len();
                    for (k, v) in bucket.drain(..) {
                        let idx = self.index_for(&k, new_len);
                        new[idx].push((k, v));
                    }
                    self.rehash_idx += 1;
                }
                if self.rehash_idx >= old_len {
                    finish = true;
                }
            }
            if finish {
                let n = self.new.take().unwrap();
                self.old = n;
                self.rehash_idx = 0;
            }
        }
    }

    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        self.start_rehash_if_needed();
        self.rehash_step();
        if self.new.is_some() {
            let mut replaced = None;
            let mut added = false;
            {
                let new = self.new.as_mut().unwrap();
                let cap = new.len();
                let idx = self.index_for(&k, cap);
                for i in 0..new[idx].len() {
                    if new[idx][i].0 == k {
                        replaced = Some(std::mem::replace(&mut new[idx][i].1, v));
                        break;
                    }
                }
                if replaced.is_none() {
                    new[idx].push((k, v));
                    added = true;
                }
            }
            if added {
                self.len += 1;
            }
            replaced
        } else {
            let mut replaced = None;
            let mut added = false;
            {
                let cap = self.old.len();
                let idx = self.index_for(&k, cap);
                for i in 0..self.old[idx].len() {
                    if self.old[idx][i].0 == k {
                        replaced = Some(std::mem::replace(&mut self.old[idx][i].1, v));
                        break;
                    }
                }
                if replaced.is_none() {
                    self.old[idx].push((k, v));
                    added = true;
                }
            }
            if added {
                self.len += 1;
            }
            replaced
        }
    }

    pub fn get(&mut self, k: &K) -> Option<&V> {
        self.rehash_step();
        if self.new.is_some() {
            {
                let new = self.new.as_mut().unwrap();
                let cap_new = new.len();
                let idx = self.index_for(k, cap_new);
                if let Some(pos) = new[idx].iter().position(|(kk, _)| kk == k) {
                    return Some(&new[idx][pos].1);
                }
            }
            let cap_old = self.old.len();
            let idx_old = self.index_for(k, cap_old);
            if let Some(pos) = self.old[idx_old].iter().position(|(kk, _)| kk == k) {
                return Some(&self.old[idx_old][pos].1);
            }
            None
        } else {
            let cap = self.old.len();
            let idx = self.index_for(k, cap);
            if let Some(pos) = self.old[idx].iter().position(|(kk, _)| kk == k) {
                return Some(&self.old[idx][pos].1);
            }
            None
        }
    }

    pub fn remove(&mut self, k: &K) -> Option<V> {
        self.rehash_step();
        if self.new.is_some() {
            {
                let new = self.new.as_mut().unwrap();
                let cap_new = new.len();
                let idx = self.index_for(k, cap_new);
                if let Some(pos) = new[idx].iter().position(|(kk, _)| kk == k) {
                    self.len -= 1;
                    let (_, v) = new[idx].swap_remove(pos);
                    return Some(v);
                }
            }
            let cap_old = self.old.len();
            let idx_old = self.index_for(k, cap_old);
            if let Some(pos) = self.old[idx_old].iter().position(|(kk, _)| kk == k) {
                self.len -= 1;
                let (_, v) = self.old[idx_old].swap_remove(pos);
                return Some(v);
            }
            None
        } else {
            let cap = self.old.len();
            let idx = self.index_for(k, cap);
            if let Some(pos) = self.old[idx].iter().position(|(kk, _)| kk == k) {
                self.len -= 1;
                let (_, v) = self.old[idx].swap_remove(pos);
                return Some(v);
            }
            None
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_rehashing(&self) -> bool {
        self.new.is_some()
    }
}

async fn read_line<R>(reader: &mut R) -> io::Result<Option<String>>
where
    R: AsyncBufReadExt + Unpin,
{
    let mut line = String::new();
    let n = reader.read_line(&mut line).await?;
    if n == 0 {
        return Ok(None);
    }
    if line.ends_with("\r\n") {
        line.truncate(line.len() - 2);
    } else if line.ends_with('\n') {
        line.pop();
    }
    Ok(Some(line))
}

async fn read_integer_line<R>(reader: &mut R) -> io::Result<Option<i64>>
where
    R: AsyncBufReadExt + Unpin,
{
    let line = match read_line(reader).await? {
        Some(l) => l,
        None => return Ok(None),
    };
    let value = line.parse::<i64>().map_err(|_| io::Error::new(ErrorKind::InvalidData, "invalid integer"))?;
    Ok(Some(value))
}

async fn read_bulk_string<R>(reader: &mut R) -> io::Result<Option<Resp>>
where
    R: AsyncBufReadExt + AsyncReadExt + Unpin,
{
    let len = match read_integer_line(reader).await? {
        Some(l) => l,
        None => return Ok(None),
    };
    if len == -1 {
        return Ok(Some(Resp::BulkString(None)));
    }
    if len < 0 {
        return Err(io::Error::new(ErrorKind::InvalidData, "negative bulk string length"));
    }
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf).await?;
    let mut crlf = [0u8; 2];
    reader.read_exact(&mut crlf).await?;
    if &crlf != b"\r\n" {
        return Err(io::Error::new(ErrorKind::InvalidData, "invalid bulk string terminator"));
    }
    Ok(Some(Resp::BulkString(Some(buf))))
}

async fn read_array(reader: &mut BufReader<OwnedReadHalf>) -> io::Result<Option<Resp>> {
    let len = match read_integer_line(reader).await? {
        Some(l) => l,
        None => return Ok(None),
    };
    if len == -1 {
        return Ok(Some(Resp::Array(None)));
    }
    if len < 0 {
        return Err(io::Error::new(ErrorKind::InvalidData, "negative array length"));
    }
    let mut items = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let frame = match read_frame(reader).await? {
            Some(f) => f,
            None => return Ok(None),
        };
        items.push(frame);
    }
    Ok(Some(Resp::Array(Some(items))))
}

fn read_frame<'a>(reader: &'a mut BufReader<OwnedReadHalf>) -> Pin<Box<dyn Future<Output = io::Result<Option<Resp>>> + Send + 'a>> {
    Box::pin(async move {
        let mut prefix = [0u8; 1];
        let n = match reader.read_exact(&mut prefix).await {
            Ok(_) => 1,
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => 0,
            Err(e) => return Err(e),
        };
        if n == 0 {
            return Ok(None);
        }
        match prefix[0] {
            b'+' => {
                let line = match read_line(reader).await? {
                    Some(l) => l,
                    None => return Ok(None),
                };
                Ok(Some(Resp::SimpleString(line)))
            }
            b'-' => {
                let line = match read_line(reader).await? {
                    Some(l) => l,
                    None => return Ok(None),
                };
                Ok(Some(Resp::Error(line)))
            }
            b':' => {
                let value = match read_integer_line(reader).await? {
                    Some(v) => v,
                    None => return Ok(None),
                };
                Ok(Some(Resp::Integer(value)))
            }
            b'$' => read_bulk_string(reader).await,
            b'*' => read_array(reader).await,
            _ => Err(io::Error::new(ErrorKind::InvalidData, "unknown RESP type")),
        }
    })
}

fn write_frame<'a>(writer: &'a mut BufWriter<OwnedWriteHalf>, frame: &'a Resp) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
    Box::pin(async move {
        match frame {
            Resp::SimpleString(s) => {
                writer.write_all(b"+").await?;
                writer.write_all(s.as_bytes()).await?;
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
            Resp::BulkString(None) => {
                writer.write_all(b"$-1\r\n").await?;
            }
            Resp::BulkString(Some(data)) => {
                writer.write_all(b"$").await?;
                writer.write_all(data.len().to_string().as_bytes()).await?;
                writer.write_all(b"\r\n").await?;
                writer.write_all(data).await?;
                writer.write_all(b"\r\n").await?;
            }
            Resp::Array(None) => {
                writer.write_all(b"*-1\r\n").await?;
            }
            Resp::Array(Some(items)) => {
                writer.write_all(b"*").await?;
                writer.write_all(items.len().to_string().as_bytes()).await?;
                writer.write_all(b"\r\n").await?;
                for item in items {
                    write_frame(writer, item).await?;
                }
            }
        }
        Ok(())
    })
}

fn as_bytes(r: &Resp) -> Option<&[u8]> {
    match r {
        Resp::BulkString(Some(b)) => Some(b.as_slice()),
        Resp::BulkString(None) => None,
        Resp::SimpleString(s) => Some(s.as_bytes()),
        _ => None,
    }
}

async fn process_frame(
    frame: Resp,
    db: &Arc<Mutex<HashMap<String, Vec<u8>>>>,
) -> Resp {
    match frame {
        Resp::Array(Some(items)) => {
            if items.is_empty() {
                return Resp::Error("ERR empty command".to_string());
            }
            let cmd_raw = match as_bytes(&items[0]) {
                Some(b) => b,
                None => return Resp::Error("ERR invalid command".to_string()),
            };
            let cmd = String::from_utf8_lossy(cmd_raw).to_uppercase();
            match cmd.as_str() {
                "PING" => {
                    if items.len() == 1 {
                        Resp::SimpleString("PONG".to_string())
                    } else if items.len() == 2 {
                        match as_bytes(&items[1]) {
                            Some(b) => Resp::BulkString(Some(b.to_vec())),
                            None => Resp::BulkString(None),
                        }
                    } else {
                        Resp::Error("ERR wrong number of arguments for 'PING'".to_string())
                    }
                }
                "SET" => {
                    if items.len() != 3 {
                        return Resp::Error("ERR wrong number of arguments for 'SET'".to_string());
                    }
                    let key_bytes = match as_bytes(&items[1]) {
                        Some(b) => b,
                        None => return Resp::Error("ERR invalid key".to_string()),
                    };
                    let val_bytes = match as_bytes(&items[2]) {
                        Some(b) => b,
                        None => return Resp::BulkString(None),
                    };
                    let key = match std::str::from_utf8(key_bytes) {
                        Ok(s) => s.to_string(),
                        Err(_) => return Resp::Error("ERR key must be UTF-8".to_string()),
                    };
                    let mut guard = db.lock().await;
                    guard.insert(key, val_bytes.to_vec());
                    Resp::SimpleString("OK".to_string())
                }
                "GET" => {
                    if items.len() != 2 {
                        return Resp::Error("ERR wrong number of arguments for 'GET'".to_string());
                    }
                    let key_bytes = match as_bytes(&items[1]) {
                        Some(b) => b,
                        None => return Resp::Error("ERR invalid key".to_string()),
                    };
                    let key = match std::str::from_utf8(key_bytes) {
                        Ok(s) => s.to_string(),
                        Err(_) => return Resp::Error("ERR key must be UTF-8".to_string()),
                    };
                    let mut guard = db.lock().await;
                    match guard.get(&key) {
                        Some(v) => Resp::BulkString(Some(v.clone())),
                        None => Resp::BulkString(None),
                    }
                }
                _ => Resp::Error("ERR unknown command".to_string()),
            }
        }
        _ => Resp::Error("ERR protocol error: expected array".to_string()),
    }
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6380").await.unwrap();
    let db: Arc<Mutex<db::Db>> = Arc::new(Mutex::new(Default::default()));

    loop {
        let (socket, _addr) = listener.accept().await.unwrap();
        let db_cloned = Arc::clone(&db);

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
                let response = cmd::process_frame(frame, &db_cloned).await;
                if resp::write_frame(&mut writer, &response).await.is_err() {
                    return;
                }
                if writer.flush().await.is_err() {
                    return;
                }
            }
        });
    }
}
