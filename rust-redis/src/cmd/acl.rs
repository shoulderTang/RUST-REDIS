use crate::cmd::{AclLogEntry, ConnectionContext, ServerContext};
use crate::resp::{Resp, as_bytes};
use bytes::Bytes;
use std::collections::VecDeque;

pub fn auth(items: &[Resp], conn_ctx: &mut ConnectionContext, server_ctx: &ServerContext) -> Resp {
    if items.len() == 2 {
        match &items[1] {
            Resp::BulkString(Some(b)) => {
                let pass = String::from_utf8_lossy(b);
                // Try authenticate as default user
                let acl = server_ctx.acl.load();
                if let Some(_) = acl.authenticate("default", &pass) {
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

        let acl = server_ctx.acl.load();
        if let Some(_user) = acl.authenticate(&username, &password) {
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
            "WHOAMI" => {
                Resp::BulkString(Some(bytes::Bytes::from(conn_ctx.current_username.clone())))
            }
            "USERS" => {
                let acl = server_ctx.acl.load();
                let users: Vec<Resp> = acl
                    .users
                    .keys()
                    .map(|k| Resp::BulkString(Some(bytes::Bytes::from(k.clone()))))
                    .collect();
                Resp::Array(Some(users))
            }
            "SETUSER" => {
                // ACL SETUSER <username> [rules...]
                if items.len() < 3 {
                    Resp::Error(
                        "ERR wrong number of arguments for 'acl setuser' command".to_string(),
                    )
                } else {
                    let username = match as_bytes(&items[2]) {
                        Some(b) => String::from_utf8_lossy(b).to_string(),
                        None => return Resp::Error("ERR invalid username".to_string()),
                    };

                    let mut rules = Vec::new();
                    for item in items.iter().skip(3) {
                        if let Some(b) = as_bytes(item) {
                            rules.push(String::from_utf8_lossy(b).to_string());
                        }
                    }
                    server_ctx.acl.rcu(|old| {
                        let mut new_acl = (**old).clone();
                        let mut user = if let Some(u) = new_acl.get_user(&username) {
                            (*u).clone()
                        } else {
                            crate::acl::User::new(&username)
                        };
                        user.parse_rules(&rules);
                        new_acl.set_user(user);
                        std::sync::Arc::new(new_acl)
                    });
                    Resp::SimpleString(bytes::Bytes::from_static(b"OK"))
                }
            }
            "SAVE" => {
                if let Some(acl_file) = &server_ctx.config.aclfile {
                    let acl = server_ctx.acl.load();
                    if let Err(e) = acl.save_to_file(acl_file) {
                        Resp::Error(format!("ERR saving ACL: {}", e))
                    } else {
                        Resp::SimpleString(bytes::Bytes::from_static(b"OK"))
                    }
                } else {
                    Resp::Error("ERR no aclfile configured".to_string())
                }
            }
            "LOAD" => {
                if let Some(acl_file) = &server_ctx.config.aclfile {
                    let path = acl_file.clone();
                    let mut result = Ok(());
                    server_ctx.acl.rcu(|old| {
                        let mut new_acl = (**old).clone();
                        result = new_acl.load_from_file(&path);
                        std::sync::Arc::new(new_acl)
                    });
                    match result {
                        Err(e) => Resp::Error(format!("ERR loading ACL: {}", e)),
                        Ok(_) => Resp::SimpleString(bytes::Bytes::from_static(b"OK")),
                    }
                } else {
                    Resp::Error("ERR no aclfile configured".to_string())
                }
            }
            "LIST" => {
                let acl = server_ctx.acl.load();
                let users: Vec<Resp> = acl
                    .users
                    .values()
                    .map(|u| Resp::BulkString(Some(bytes::Bytes::from(u.to_string()))))
                    .collect();
                Resp::Array(Some(users))
            }
            "DELUSER" => {
                if items.len() != 3 {
                    Resp::Error(
                        "ERR wrong number of arguments for 'acl deluser' command".to_string(),
                    )
                } else {
                    let username = match as_bytes(&items[2]) {
                        Some(b) => String::from_utf8_lossy(b).to_string(),
                        None => return Resp::Error("ERR invalid username".to_string()),
                    };
                    let mut deleted = false;
                    server_ctx.acl.rcu(|old| {
                        let mut new_acl = (**old).clone();
                        deleted = new_acl.del_user(&username);
                        std::sync::Arc::new(new_acl)
                    });
                    if deleted { Resp::Integer(1) } else { Resp::Integer(0) }
                }
            }
            "LOG" => {
                if items.len() > 3 {
                    return Resp::Error(
                        "ERR wrong number of arguments for 'acl log' command".to_string(),
                    );
                }
                if items.len() == 3 {
                    let arg = match as_bytes(&items[2]) {
                        Some(b) => String::from_utf8_lossy(b).to_uppercase(),
                        None => return Resp::Error("ERR syntax error".to_string()),
                    };
                    if arg == "RESET" {
                        let mut log = server_ctx.clients_ctx.acl_log.write().unwrap();
                        log.clear();
                        return Resp::SimpleString(Bytes::from("OK"));
                    } else {
                        // ACL LOG <count>
                        if let Ok(count) = arg.parse::<usize>() {
                            let log = server_ctx.clients_ctx.acl_log.read().unwrap();
                            let mut results = Vec::new();
                            for entry in log.iter().take(count) {
                                results.push(format_acl_log_entry(entry));
                            }
                            return Resp::Array(Some(results));
                        } else {
                            return Resp::Error(
                                "ERR value is not an integer or out of range".to_string(),
                            );
                        }
                    }
                } else {
                    // ACL LOG (returns all)
                    let log = server_ctx.clients_ctx.acl_log.read().unwrap();
                    let mut results = Vec::new();
                    for entry in log.iter() {
                        results.push(format_acl_log_entry(entry));
                    }
                    return Resp::Array(Some(results));
                }
            }
            "DRYRUN" => {
                if items.len() < 4 {
                    return Resp::Error(
                        "ERR wrong number of arguments for 'acl dryrun' command".to_string(),
                    );
                }
                let username = match as_bytes(&items[2]) {
                    Some(b) => String::from_utf8_lossy(b).to_string(),
                    None => return Resp::Error("ERR invalid username".to_string()),
                };
                let cmd_to_test = match as_bytes(&items[3]) {
                    Some(b) => String::from_utf8_lossy(b).to_string(),
                    None => return Resp::Error("ERR invalid command".to_string()),
                };

                let acl = server_ctx.acl.load();
                if let Some(user) = acl.get_user(&username) {
                    if user.can_execute(&cmd_to_test) {
                        // Check keys if provided
                        let mut all_keys_allowed = true;
                        for i in 4..items.len() {
                            if let Some(key) = as_bytes(&items[i]) {
                                if !user.can_access_key(key) {
                                    all_keys_allowed = false;
                                    break;
                                }
                            }
                        }
                        if all_keys_allowed {
                            Resp::SimpleString(Bytes::from("OK"))
                        } else {
                            Resp::Error(format!(
                                "user {} has no permissions to access one of the keys used as arguments",
                                username
                            ))
                        }
                    } else {
                        Resp::Error(format!(
                            "user {} has no permissions to run the '{}' command",
                            username, cmd_to_test
                        ))
                    }
                } else {
                    Resp::Error(format!("user {} not found", username))
                }
            }
            _ => Resp::Error("ERR unknown or unsupported ACL subcommand".to_string()),
        }
    }
}

fn format_acl_log_entry(entry: &AclLogEntry) -> Resp {
    let mut map = Vec::new();
    map.push(Resp::BulkString(Some(Bytes::from("count"))));
    map.push(Resp::Integer(entry.count as i64));
    map.push(Resp::BulkString(Some(Bytes::from("reason"))));
    map.push(Resp::BulkString(Some(Bytes::from(entry.reason.clone()))));
    map.push(Resp::BulkString(Some(Bytes::from("context"))));
    map.push(Resp::BulkString(Some(Bytes::from(entry.context.clone()))));
    map.push(Resp::BulkString(Some(Bytes::from("object"))));
    map.push(Resp::BulkString(Some(Bytes::from(entry.object.clone()))));
    map.push(Resp::BulkString(Some(Bytes::from("username"))));
    map.push(Resp::BulkString(Some(Bytes::from(entry.username.clone()))));
    map.push(Resp::BulkString(Some(Bytes::from("age-seconds"))));
    map.push(Resp::Integer(entry.age as i64));
    map.push(Resp::BulkString(Some(Bytes::from("client-id"))));
    map.push(Resp::Integer(entry.client_id as i64));
    Resp::Array(Some(map))
}

pub fn record_acl_log(server_ctx: &ServerContext, entry: AclLogEntry) {
    let mut log = server_ctx.clients_ctx.acl_log.write().unwrap();
    log.push_front(entry);
    if log.len() > 128 {
        log.pop_back();
    }
}
