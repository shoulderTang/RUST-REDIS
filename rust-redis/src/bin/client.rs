#[path = "../resp.rs"]
mod resp;
use std::io;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use resp::{Resp, read_frame, write_frame};

fn to_bulk(s: &str) -> Resp {
    Resp::BulkString(Some(bytes::Bytes::copy_from_slice(s.as_bytes())))
}

fn tokens_to_resp(tokens: &[String]) -> Option<Resp> {
    if tokens.is_empty() {
        return None;
    }
    let mut items = Vec::with_capacity(tokens.len());
    for t in tokens {
        items.push(to_bulk(t));
    }
    Some(Resp::Array(Some(items)))
}

fn print_resp(r: &Resp) {
    match r {
        Resp::SimpleString(s) => {
            match std::str::from_utf8(s.as_ref()) {
                Ok(text) => println!("{}", text),
                Err(_) => {
                    let hex = s.as_ref().iter().map(|x| format!("{:02x}", x)).collect::<String>();
                    println!("0x{}", hex);
                }
            }
        }
        Resp::Error(s) => {
            println!("(error) {}", s);
        }
        Resp::Integer(i) => {
            println!("{}", i);
        }
        Resp::BulkString(None) => {
            println!("(nil)");
        }
        Resp::BulkString(Some(b)) => {
            match std::str::from_utf8(b.as_ref()) {
                Ok(s) => println!("{}", s),
                Err(_) => {
                    let hex = b.as_ref().iter().map(|x| format!("{:02x}", x)).collect::<String>();
                    println!("0x{}", hex);
                }
            }
        }
        Resp::Array(None) => {
            println!("(nil array)");
        }
        Resp::Array(Some(items)) => {
            println!("(array) {}", items.len());
            for (i, it) in items.iter().enumerate() {
                print!("{}) ", i + 1);
                print_resp(it);
            }
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:6380".to_string());
    let stream = TcpStream::connect(addr).await?;
    let (read_half, write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut writer = BufWriter::new(write_half);

    let stdin = tokio::io::stdin();
    let mut in_reader = BufReader::new(stdin);
    let mut line = String::new();

    loop {
        line.clear();
        print!("> ");
        let _ = io::Write::flush(&mut io::stdout());
        let n = in_reader.read_line(&mut line).await?;
        if n == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.eq_ignore_ascii_case("quit") || trimmed.eq_ignore_ascii_case("exit") {
            break;
        }
        let tokens: Vec<String> = trimmed.split_whitespace().map(|s| s.to_string()).collect();
        let req = match tokens_to_resp(&tokens) {
            Some(r) => r,
            None => continue,
        };
        if let Err(_) = write_frame(&mut writer, &req).await {
            println!("(error) write failed");
            break;
        }
        if let Err(_) = writer.flush().await {
            println!("(error) flush failed");
            break;
        }
        match read_frame(&mut reader).await {
            Ok(Some(resp)) => {
                print_resp(&resp);
            }
            Ok(None) => {
                println!("(error) connection closed");
                break;
            }
            Err(_) => {
                println!("(error) read failed");
                break;
            }
        }
    }
    Ok(())
}
