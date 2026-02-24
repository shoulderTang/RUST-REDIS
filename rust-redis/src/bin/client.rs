#[path = "../resp.rs"]
mod resp;
use resp::{read_frame, write_frame, Resp};
use std::collections::HashMap;
use std::io;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;

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
        Resp::SimpleString(s) => match std::str::from_utf8(s.as_ref()) {
            Ok(text) => println!("{}", text),
            Err(_) => {
                let hex = s.as_ref().iter().map(|x| format!("{:02x}", x)).collect::<String>();
                println!("0x{}", hex);
            }
        },
        Resp::Error(s) => {
            println!("(error) {}", s);
        }
        Resp::Integer(i) => {
            println!("{}", i);
        }
        Resp::BulkString(None) => {
            println!("(nil)");
        }
        Resp::BulkString(Some(b)) => match std::str::from_utf8(b.as_ref()) {
            Ok(s) => println!("{}", s),
            Err(_) => {
                let hex = b.as_ref().iter().map(|x| format!("{:02x}", x)).collect::<String>();
                println!("0x{}", hex);
            }
        },
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
        Resp::Multiple(items) => {
            for it in items {
                print_resp(it);
            }
        }
        Resp::NoReply | Resp::Control(_) => {}
    }
}

fn hash_tag(key: &str) -> String {
    if let Some(start) = key.find('{') {
        if let Some(end) = key[start + 1..].find('}') {
            let inner = &key[start + 1..start + 1 + end];
            if !inner.is_empty() {
                return inner.to_string();
            }
        }
    }
    key.to_string()
}

fn crc16(data: &[u8]) -> u16 {
    const POLY: u16 = 0x1021;
    let mut crc: u16 = 0xFFFF;
    for b in data {
        crc ^= (*b as u16) << 8;
        for _ in 0..8 {
            if (crc & 0x8000) != 0 {
                crc = (crc << 1) ^ POLY;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

fn key_slot(key: &str) -> u16 {
    let tag = hash_tag(key);
    (crc16(tag.as_bytes()) % 16384) as u16
}

fn extract_key_from_tokens(tokens: &[String]) -> Option<&str> {
    if tokens.len() >= 2 {
        Some(tokens[1].as_str())
    } else {
        None
    }
}

async fn send_and_recv(addr: &str, req: &Resp) -> io::Result<Resp> {
    let stream = TcpStream::connect(addr).await?;
    let (read_half, write_half) = stream.into_split();
    let mut writer = BufWriter::new(write_half);
    write_frame(&mut writer, req).await?;
    writer.flush().await?;
    let mut reader = BufReader::new(read_half);
    let resp = read_frame(&mut reader).await?;
    resp.ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "no response"))
}

fn update_slots_from_slots_resp(slots: &Resp, slot_map: &mut HashMap<u16, String>) {
    if let Resp::Array(Some(entries)) = slots {
        for entry in entries {
            if let Resp::Array(Some(parts)) = entry {
                if parts.len() >= 3 {
                    let start = match &parts[0] {
                        Resp::Integer(n) => *n as u16,
                        _ => continue,
                    };
                    let end = match &parts[1] {
                        Resp::Integer(n) => *n as u16,
                        _ => continue,
                    };
                    let master = match &parts[2] {
                        Resp::Array(Some(node)) if node.len() >= 2 => {
                            let ip = match &node[0] {
                                Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
                                _ => continue,
                            };
                            let port = match &node[1] {
                                Resp::Integer(p) => *p as u16,
                                _ => continue,
                            };
                            format!("{}:{}", ip, port)
                        }
                        _ => continue,
                    };
                    for s in start..=end {
                        slot_map.insert(s, master.clone());
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let default_addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:6380".to_string());
    let stdin = tokio::io::stdin();
    let mut in_reader = BufReader::new(stdin);
    let mut line = String::new();
    let mut slot_map: HashMap<u16, String> = HashMap::new();

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

        let target_addr = if let Some(key) = extract_key_from_tokens(&tokens) {
            let s = key_slot(key);
            slot_map.get(&s).cloned().unwrap_or_else(|| default_addr.clone())
        } else {
            default_addr.clone()
        };

        let mut resp = match send_and_recv(&target_addr, &req).await {
            Ok(r) => r,
            Err(e) => {
                println!("(error) {}", e);
                continue;
            }
        };

        if let Resp::Error(err) = &resp {
            let upper = err.to_uppercase();
            if upper.starts_with("MOVED ") {
                let parts: Vec<&str> = err.split_whitespace().collect();
                if parts.len() >= 3 {
                    let slot: u16 = parts[1].parse().unwrap_or(0);
                    let new_addr = parts[2].to_string();
                    slot_map.insert(slot, new_addr.clone());
                    // Try refresh slots table from the hinted node
                    let slots_req = Resp::Array(Some(vec![to_bulk("CLUSTER"), to_bulk("SLOTS")]));
                    if let Ok(slots_resp) = send_and_recv(&new_addr, &slots_req).await {
                        update_slots_from_slots_resp(&slots_resp, &mut slot_map);
                    }
                    resp = match send_and_recv(&new_addr, &req).await {
                        Ok(r) => r,
                        Err(e) => Resp::Error(format!("route error: {}", e)),
                    };
                }
            } else if upper.starts_with("ASK ") {
                let parts: Vec<&str> = err.split_whitespace().collect();
                if parts.len() >= 3 {
                    let ask_addr = parts[2].to_string();
                    let asking = Resp::Array(Some(vec![to_bulk("ASKING")]));
                    let _ = send_and_recv(&ask_addr, &asking).await;
                    resp = match send_and_recv(&ask_addr, &req).await {
                        Ok(r) => r,
                        Err(e) => Resp::Error(format!("ask route error: {}", e)),
                    };
                }
            }
        }

        print_resp(&resp);
    }
    Ok(())
}
