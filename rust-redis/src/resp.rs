use std::future::Future;
use std::io::{self, ErrorKind};
use std::pin::Pin;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use bytes::{Bytes, BytesMut};

#[derive(Clone, Debug)]
pub enum Resp {
    SimpleString(Bytes),
    Error(String),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Option<Vec<Resp>>),
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
    let mut buf = BytesMut::with_capacity(len as usize);
    buf.resize(len as usize, 0);
    reader.read_exact(&mut buf[..]).await?;
    let mut crlf = [0u8; 2];
    reader.read_exact(&mut crlf).await?;
    if &crlf != b"\r\n" {
        return Err(io::Error::new(ErrorKind::InvalidData, "invalid bulk string terminator"));
    }
    Ok(Some(Resp::BulkString(Some(buf.freeze()))))
}

async fn read_array<R>(reader: &mut R) -> io::Result<Option<Resp>>
where
    R: AsyncBufReadExt + AsyncReadExt + Unpin + Send,
{
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

pub fn read_frame<'a, R>(reader: &'a mut R) -> Pin<Box<dyn Future<Output = io::Result<Option<Resp>>> + Send + 'a>>
where
    R: AsyncBufReadExt + AsyncReadExt + Unpin + Send,
{
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
                let bytes = Bytes::copy_from_slice(line.as_bytes());
                Ok(Some(Resp::SimpleString(bytes)))
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

pub fn write_frame<'a>(writer: &'a mut BufWriter<OwnedWriteHalf>, frame: &'a Resp) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
    Box::pin(async move {
        match frame {
            Resp::SimpleString(s) => {
                writer.write_all(b"+").await?;
                writer.write_all(s.as_ref()).await?;
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
                writer.write_all(data.as_ref()).await?;
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

pub fn as_bytes(r: &Resp) -> Option<&[u8]> {
    match r {
        Resp::BulkString(Some(b)) => Some(b.as_ref()),
        Resp::BulkString(None) => None,
        Resp::SimpleString(s) => Some(s.as_ref()),
        _ => None,
    }
}
