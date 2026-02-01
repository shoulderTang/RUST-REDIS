use crate::resp::{Resp, as_bytes};
use crate::cmd::{ConnectionContext, ServerContext};
use bytes::Bytes;

pub fn hello(items: &[Resp], conn_ctx: &mut ConnectionContext, server_ctx: &ServerContext) -> Resp {
    let mut version = 2;

    if items.len() > 1 {
        let ver_str = match as_bytes(&items[1]) {
             Some(b) => String::from_utf8_lossy(b).to_string(),
             None => return Resp::Error("ERR syntax error".to_string()),
        };
        
        match ver_str.parse::<i64>() {
            Ok(v) if v == 2 || v == 3 => {
                 version = v;
            },
            Ok(_) => return Resp::Error("NOPROTO unsupported protocol version".to_string()),
            Err(_) => return Resp::Error("ERR syntax error".to_string()),
        }
        
        let mut i = 2;
        while i < items.len() {
            let arg = match as_bytes(&items[i]) {
                Some(b) => String::from_utf8_lossy(b).to_string().to_uppercase(),
                None => return Resp::Error("ERR syntax error".to_string()),
            };
            
            match arg.as_str() {
                "AUTH" => {
                    if i + 2 >= items.len() {
                        return Resp::Error("ERR syntax error".to_string());
                    }
                    let username = match as_bytes(&items[i+1]) {
                        Some(b) => String::from_utf8_lossy(b).to_string(),
                        None => return Resp::Error("ERR syntax error".to_string()),
                    };
                    let password = match as_bytes(&items[i+2]) {
                        Some(b) => String::from_utf8_lossy(b).to_string(),
                        None => return Resp::Error("ERR syntax error".to_string()),
                    };
                    
                    let acl_guard = server_ctx.acl.read().unwrap();
                    if let Some(_user) = acl_guard.authenticate(&username, &password) {
                         conn_ctx.authenticated = true;
                         conn_ctx.current_username = username;
                    } else {
                         return Resp::Error("WRONGPASS invalid username-password pair".to_string());
                    }
                    i += 3;
                },
                "SETNAME" => {
                    if i + 1 >= items.len() {
                        return Resp::Error("ERR syntax error".to_string());
                    }
                    let name = match as_bytes(&items[i+1]) {
                        Some(b) => String::from_utf8_lossy(b).to_string(),
                        None => return Resp::Error("ERR syntax error".to_string()),
                    };
                     if name.chars().any(char::is_whitespace) {
                         return Resp::Error("ERR Client names cannot contain spaces, newlines or special characters.".to_string());
                    }
                    
                    if let Some(mut ci) = server_ctx.clients.get_mut(&conn_ctx.id) {
                        ci.name = name;
                    }
                    i += 2;
                },
                _ => return Resp::Error("ERR syntax error".to_string()),
            }
        }
    }
    
    let mut info = Vec::new();
    info.push(Resp::BulkString(Some(Bytes::from("server"))));
    info.push(Resp::BulkString(Some(Bytes::from("redis"))));
    
    info.push(Resp::BulkString(Some(Bytes::from("version"))));
    info.push(Resp::BulkString(Some(Bytes::from("6.2.5"))));
    
    info.push(Resp::BulkString(Some(Bytes::from("proto"))));
    info.push(Resp::Integer(version));
    
    info.push(Resp::BulkString(Some(Bytes::from("id"))));
    info.push(Resp::Integer(conn_ctx.id as i64));
    
    info.push(Resp::BulkString(Some(Bytes::from("mode"))));
    info.push(Resp::BulkString(Some(Bytes::from("standalone"))));
    
    info.push(Resp::BulkString(Some(Bytes::from("role"))));
    info.push(Resp::BulkString(Some(Bytes::from("master"))));
    
    info.push(Resp::BulkString(Some(Bytes::from("modules"))));
    info.push(Resp::Array(Some(Vec::new())));
    
    Resp::Array(Some(info))
}
