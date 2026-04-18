use bytes::{Bytes, BytesMut};
use std::future::Future;
use std::io::{self, ErrorKind};
use std::pin::Pin;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::tcp::OwnedWriteHalf;

/// Format a signed integer into a stack buffer without heap allocation.
/// Returns the ASCII decimal bytes slice.
fn fmt_int(n: i64, buf: &mut [u8; 20]) -> &[u8] {
    if n == 0 {
        buf[19] = b'0';
        return &buf[19..];
    }
    let negative = n < 0;
    let mut v = if negative {
        n.wrapping_neg() as u64
    } else {
        n as u64
    };
    let mut pos = 20usize;
    while v > 0 {
        pos -= 1;
        buf[pos] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    if negative {
        pos -= 1;
        buf[pos] = b'-';
    }
    &buf[pos..]
}

/// Format a usize into a stack buffer without heap allocation.
fn fmt_usize(n: usize, buf: &mut [u8; 20]) -> &[u8] {
    if n == 0 {
        buf[19] = b'0';
        return &buf[19..];
    }
    let mut v = n;
    let mut pos = 20usize;
    while v > 0 {
        pos -= 1;
        buf[pos] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    &buf[pos..]
}

#[derive(Clone, Debug, PartialEq)]
pub enum Resp {
    SimpleString(Bytes),
    /// Owned error string, for dynamically formatted messages.
    Error(String),
    /// Zero-allocation error for static string literals.
    #[allow(dead_code)]
    StaticError(&'static str),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Option<Vec<Resp>>),
    #[allow(dead_code)]
    Multiple(Vec<Resp>),
    #[allow(dead_code)]
    NoReply,
    #[allow(dead_code)]
    Control(String),
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

/// Parse a CRLF-terminated integer line without any heap allocation.
/// Returns `Ok(None)` on clean EOF before the first byte.
async fn read_integer_line<R>(reader: &mut R) -> io::Result<Option<i64>>
where
    R: AsyncBufReadExt + Unpin,
{
    let mut value: i64 = 0;
    let mut negative = false;
    let mut got_data = false;

    loop {
        let filled = reader.fill_buf().await?;
        if filled.is_empty() {
            if !got_data {
                return Ok(None);
            }
            return Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "connection reset while reading integer",
            ));
        }
        let mut consumed = 0usize;
        let mut done = false;
        for &b in filled {
            consumed += 1;
            got_data = true;
            match b {
                b'\r' => {} // will be followed by '\n'
                b'\n' => {
                    done = true;
                    break;
                }
                b'-' if !negative && value == 0 && consumed == 1 => {
                    negative = true;
                }
                b'0'..=b'9' => {
                    value = value
                        .wrapping_mul(10)
                        .wrapping_add((b - b'0') as i64);
                }
                _ => {
                    reader.consume(consumed);
                    return Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "invalid byte in integer line",
                    ));
                }
            }
        }
        reader.consume(consumed);
        if done {
            return Ok(Some(if negative { -value } else { value }));
        }
    }
}

async fn read_bulk_string<R>(reader: &mut R) -> io::Result<Option<Resp>>
where
    R: AsyncBufReadExt + AsyncReadExt + Unpin,
{
    let line = match read_line(reader).await? {
        Some(l) => l,
        None => return Ok(None),
    };

    if line.starts_with("EOF:") {
        let delimiter = &line[4..];
        if delimiter.len() != 40 {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "invalid EOF delimiter length",
            ));
        }
        let delimiter_bytes = delimiter.as_bytes();
        let d_len = delimiter_bytes.len();

        let mut buf = Vec::new();

        loop {
            let b = reader.read_u8().await?;
            buf.push(b);
            if buf.len() >= d_len {
                if &buf[buf.len() - d_len..] == delimiter_bytes {
                    buf.truncate(buf.len() - d_len);
                    break;
                }
            }
        }
        return Ok(Some(Resp::BulkString(Some(Bytes::from(buf)))));
    }

    let len = line
        .parse::<i64>()
        .map_err(|_| io::Error::new(ErrorKind::InvalidData, "invalid integer"))?;

    if len == -1 {
        return Ok(Some(Resp::BulkString(None)));
    }
    if len < 0 {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "negative bulk string length",
        ));
    }
    let mut buf = BytesMut::with_capacity(len as usize);
    buf.resize(len as usize, 0);
    reader.read_exact(&mut buf[..]).await?;
    let mut crlf = [0u8; 2];
    reader.read_exact(&mut crlf).await?;
    if &crlf != b"\r\n" {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "invalid bulk string terminator",
        ));
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
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "negative array length",
        ));
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

pub fn read_frame<'a, R>(
    reader: &'a mut R,
) -> Pin<Box<dyn Future<Output = io::Result<Option<Resp>>> + Send + 'a>>
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

pub fn write_frame<'a>(
    writer: &'a mut BufWriter<OwnedWriteHalf>,
    frame: &'a Resp,
) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
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
            Resp::StaticError(s) => {
                writer.write_all(b"-").await?;
                writer.write_all(s.as_bytes()).await?;
                writer.write_all(b"\r\n").await?;
            }
            Resp::Integer(i) => {
                let mut buf = [0u8; 20];
                writer.write_all(b":").await?;
                writer.write_all(fmt_int(*i, &mut buf)).await?;
                writer.write_all(b"\r\n").await?;
            }
            Resp::BulkString(None) => {
                writer.write_all(b"$-1\r\n").await?;
            }
            Resp::BulkString(Some(data)) => {
                let mut buf = [0u8; 20];
                writer.write_all(b"$").await?;
                writer.write_all(fmt_usize(data.len(), &mut buf)).await?;
                writer.write_all(b"\r\n").await?;
                writer.write_all(data.as_ref()).await?;
                writer.write_all(b"\r\n").await?;
            }
            Resp::Array(None) => {
                writer.write_all(b"*-1\r\n").await?;
            }
            Resp::Array(Some(items)) => {
                let mut buf = [0u8; 20];
                writer.write_all(b"*").await?;
                writer.write_all(fmt_usize(items.len(), &mut buf)).await?;
                writer.write_all(b"\r\n").await?;
                for item in items {
                    write_frame(writer, item).await?;
                }
            }
            Resp::Multiple(items) => {
                for item in items {
                    write_frame(writer, item).await?;
                }
            }
            Resp::NoReply | Resp::Control(_) => {}
        }
        Ok(())
    })
}

impl Resp {
    #[allow(dead_code)]
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut buf = [0u8; 20];
        match self {
            Resp::SimpleString(s) => {
                let mut v = Vec::with_capacity(3 + s.len());
                v.push(b'+');
                v.extend_from_slice(s.as_ref());
                v.extend_from_slice(b"\r\n");
                v
            }
            Resp::Error(s) => {
                let mut v = Vec::with_capacity(3 + s.len());
                v.push(b'-');
                v.extend_from_slice(s.as_bytes());
                v.extend_from_slice(b"\r\n");
                v
            }
            Resp::StaticError(s) => {
                let mut v = Vec::with_capacity(3 + s.len());
                v.push(b'-');
                v.extend_from_slice(s.as_bytes());
                v.extend_from_slice(b"\r\n");
                v
            }
            Resp::Integer(i) => {
                let digits = fmt_int(*i, &mut buf);
                let mut v = Vec::with_capacity(3 + digits.len());
                v.push(b':');
                v.extend_from_slice(digits);
                v.extend_from_slice(b"\r\n");
                v
            }
            Resp::BulkString(None) => b"$-1\r\n".to_vec(),
            Resp::BulkString(Some(data)) => {
                let len_bytes = fmt_usize(data.len(), &mut buf);
                let mut v = Vec::with_capacity(3 + len_bytes.len() + data.len() + 2);
                v.push(b'$');
                v.extend_from_slice(len_bytes);
                v.extend_from_slice(b"\r\n");
                v.extend_from_slice(data.as_ref());
                v.extend_from_slice(b"\r\n");
                v
            }
            Resp::Array(None) => b"*-1\r\n".to_vec(),
            Resp::Array(Some(items)) => {
                let len_bytes = fmt_usize(items.len(), &mut buf);
                let mut v = Vec::with_capacity(3 + len_bytes.len());
                v.push(b'*');
                v.extend_from_slice(len_bytes);
                v.extend_from_slice(b"\r\n");
                for item in items {
                    v.extend_from_slice(&item.as_bytes());
                }
                v
            }
            Resp::Multiple(items) => {
                let mut v = Vec::new();
                for item in items {
                    v.extend_from_slice(&item.as_bytes());
                }
                v
            }
            Resp::NoReply | Resp::Control(_) => Vec::new(),
        }
    }
}

#[allow(dead_code)]
pub fn as_bytes(r: &Resp) -> Option<&[u8]> {
    match r {
        Resp::BulkString(Some(b)) => Some(b.as_ref()),
        Resp::BulkString(None) => None,
        Resp::SimpleString(s) => Some(s.as_ref()),
        _ => None,
    }
}
