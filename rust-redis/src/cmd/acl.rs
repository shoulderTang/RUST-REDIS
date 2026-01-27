use crate::resp::{Resp, as_bytes};
use crate::cmd::{ConnectionContext, ServerContext};
use bytes::Bytes;

pub fn auth(items: &[Resp], conn_ctx: &mut ConnectionContext, server_ctx: &ServerContext) -> Resp {
    if items.len() == 2 {
        match &items[1] {
            Resp::BulkString(Some(b)) => {
                let pass = String::from_utf8_lossy(b);
                // Try authenticate as default user
                let acl_guard = server_ctx.acl.read().unwrap();
                if let Some(_) = acl_guard.authenticate("default", &pass) {
                    conn_ctx.authenticated = true;
                    conn_ctx.current_username = "default".to_string();
                    Resp::SimpleString(bytes::Bytes::from_static(b"OK"))
                } else {
                    // Fallback to legacy requirepass check if not handled by ACL (though ACL should handle it)
                    if let Some(ref required) = server_ctx.config.requirepass {
                        if pass == *required {
                            conn_ctx.authenticated = true;
                            conn_ctx.current_username = "default".to_string();
                            Resp::SimpleString(bytes::Bytes::from_static(b"OK"))
                        } else {
                            Resp::Error("ERR invalid password".to_string())
                        }
                    } else {
                        Resp::Error("ERR invalid password".to_string())
                    }
                }
            }
            _ => Resp::Error("ERR invalid password".to_string()),
        }
    } else if items.len() == 3 {
         // AUTH username password
         let username = match as_bytes(&items[1]) {
             Some(b) => String::from_utf8_lossy(b).to_string(),
             None => return Resp::Error("ERR invalid username".to_string()),
         };
         let password = match as_bytes(&items[2]) {
             Some(b) => String::from_utf8_lossy(b).to_string(),
             None => return Resp::Error("ERR invalid password".to_string()),
         };
         
         let acl_guard = server_ctx.acl.read().unwrap();
         if let Some(_user) = acl_guard.authenticate(&username, &password) {
             conn_ctx.authenticated = true;
             conn_ctx.current_username = username;
             Resp::SimpleString(bytes::Bytes::from_static(b"OK"))
         } else {
             Resp::Error("WRONGPASS invalid username-password pair".to_string())
         }
    } else {
        Resp::Error("ERR wrong number of arguments for 'auth' command".to_string())
    }
}

pub fn acl(items: &[Resp], conn_ctx: &ConnectionContext, server_ctx: &ServerContext) -> Resp {
    if items.len() < 2 {
         Resp::Error("ERR wrong number of arguments for 'acl' command".to_string())
    } else {
         let subcmd = match as_bytes(&items[1]) {
             Some(b) => String::from_utf8_lossy(b).to_string().to_uppercase(),
             None => return Resp::Error("ERR invalid subcommand".to_string()),
         };
         match subcmd.as_str() {
             "WHOAMI" => Resp::BulkString(Some(bytes::Bytes::from(conn_ctx.current_username.clone()))),
             "USERS" => {
                 let acl_guard = server_ctx.acl.read().unwrap();
                let users: Vec<Resp> = acl_guard.users.keys().map(|k| Resp::BulkString(Some(bytes::Bytes::from(k.clone())))).collect();
                Resp::Array(Some(users))
             },
             "SETUSER" => {
                // ACL SETUSER <username> [rules...]
                if items.len() < 3 {
                     Resp::Error("ERR wrong number of arguments for 'acl setuser' command".to_string())
                } else {
                    let username = match as_bytes(&items[2]) {
                        Some(b) => String::from_utf8_lossy(b).to_string(),
                        None => return Resp::Error("ERR invalid username".to_string()),
                    };
                    
                    let mut acl_guard = server_ctx.acl.write().unwrap();
                    let mut user = if let Some(u) = acl_guard.get_user(&username) {
                        (*u).clone()
                    } else {
                        crate::acl::User::new(&username)
                    };
                    
                    let mut rules = Vec::new();
                    for item in items.iter().skip(3) {
                        if let Some(b) = as_bytes(item) {
                            rules.push(String::from_utf8_lossy(b).to_string());
                        }
                    }
                    user.parse_rules(&rules);
                    
                    acl_guard.set_user(user);
                    Resp::SimpleString(bytes::Bytes::from_static(b"OK"))
                }
            },
            "SAVE" => {
                if let Some(acl_file) = &server_ctx.config.aclfile {
                    let acl_guard = server_ctx.acl.read().unwrap();
                    if let Err(e) = acl_guard.save_to_file(acl_file) {
                        Resp::Error(format!("ERR saving ACL: {}", e))
                    } else {
                         Resp::SimpleString(bytes::Bytes::from_static(b"OK"))
                    }
                } else {
                     Resp::Error("ERR no aclfile configured".to_string())
                }
            },
            "LOAD" => {
                 if let Some(acl_file) = &server_ctx.config.aclfile {
                    let mut acl_guard = server_ctx.acl.write().unwrap();
                    if let Err(e) = acl_guard.load_from_file(acl_file) {
                         Resp::Error(format!("ERR loading ACL: {}", e))
                    } else {
                         Resp::SimpleString(bytes::Bytes::from_static(b"OK"))
                    }
                 } else {
                     Resp::Error("ERR no aclfile configured".to_string())
                 }
            },
            "LIST" => {
                let acl_guard = server_ctx.acl.read().unwrap();
                let users: Vec<Resp> = acl_guard.users.values().map(|u| Resp::BulkString(Some(bytes::Bytes::from(u.to_string())))).collect();
                Resp::Array(Some(users))
            },
            "DELUSER" => {
                 if items.len() != 3 {
                      Resp::Error("ERR wrong number of arguments for 'acl deluser' command".to_string())
                 } else {
                      let username = match as_bytes(&items[2]) {
                         Some(b) => String::from_utf8_lossy(b).to_string(),
                         None => return Resp::Error("ERR invalid username".to_string()),
                     };
                     let mut acl_guard = server_ctx.acl.write().unwrap();
                     if acl_guard.del_user(&username) {
                         Resp::Integer(1)
                     } else {
                         Resp::Integer(0)
                     }
                 }
             },
             _ => Resp::Error("ERR unknown or unsupported ACL subcommand".to_string()),
         }
    }
}
