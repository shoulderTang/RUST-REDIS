use crate::cmd::{ConnectionContext, ServerContext};
use crate::resp::Resp;
use bytes::Bytes;

fn to_bytes<S: AsRef<str>>(s: S) -> Bytes {
    Bytes::from(s.as_ref().to_string())
}

pub fn client(
    items: &[Resp],
    conn_ctx: &mut ConnectionContext,
    server_ctx: &ServerContext,
) -> (Resp, Option<Resp>) {
    if items.len() < 2 {
        return (
            Resp::Error("ERR wrong number of arguments for 'client' command".to_string()),
            None,
        );
    }
    let sub = match &items[1] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_lowercase(),
        _ => return (Resp::Error("ERR unknown subcommand".to_string()), None),
    };
    match sub.as_str() {
        "list" => {
            let mut lines = Vec::new();
            for entry in server_ctx.clients_ctx.clients.iter() {
                let c = entry.value();
                let age = c.connect_time.elapsed().as_secs();
                let idle = c.last_activity.elapsed().as_secs();
                let mut fields = Vec::new();
                fields.push(format!("id={}", c.id));
                fields.push(format!("addr={}", c.addr));
                fields.push(format!("name={}", c.name));
                fields.push(format!("age={}", age));
                fields.push(format!("idle={}", idle));
                fields.push(format!("flags={}", c.flags));
                fields.push(format!("db={}", c.db));
                fields.push(format!("sub={}", c.sub));
                fields.push(format!("psub={}", c.psub));
                fields.push(format!("cmd={}", c.cmd));
                lines.push(fields.join(" "));
            }
            let text = lines.join("\n");
            (Resp::BulkString(Some(to_bytes(text))), None)
        }
        "setname" => {
            if items.len() < 3 {
                return (
                    Resp::Error("ERR wrong number of arguments for 'client setname'".to_string()),
                    None,
                );
            }
            let new_name = match &items[2] {
                Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
                _ => return (Resp::Error("ERR invalid client name".to_string()), None),
            };
            if new_name.contains(' ') {
                return (
                    Resp::Error("ERR Client names cannot contain spaces".to_string()),
                    None,
                );
            }
            if let Some(mut ci) = server_ctx.clients_ctx.clients.get_mut(&conn_ctx.id) {
                ci.name = new_name;
            }
            (Resp::SimpleString(Bytes::from("OK")), None)
        }
        "getname" => {
            let name = server_ctx
                .clients_ctx.clients
                .get(&conn_ctx.id)
                .map(|c| c.name.clone())
                .unwrap_or_default();
            if name.is_empty() {
                (Resp::BulkString(None), None)
            } else {
                (Resp::BulkString(Some(to_bytes(name))), None)
            }
        }
        "kill" => {
            if items.len() >= 3 {
                // CLIENT KILL ID <id> or CLIENT KILL ADDR <addr>
                if let Resp::BulkString(Some(flag)) = &items[2] {
                    let flag_s = String::from_utf8_lossy(flag).to_uppercase();
                    if flag_s == "ID" && items.len() >= 4 {
                        if let Resp::BulkString(Some(idb)) = &items[3] {
                            if let Ok(id) = String::from_utf8_lossy(idb).parse::<u64>() {
                                let killed = kill_client_by_id(server_ctx, id);
                                return (Resp::Integer(if killed { 1 } else { 0 }), None);
                            }
                        }
                        return (Resp::Integer(0), None);
                    } else if flag_s == "ADDR" && items.len() >= 4 {
                        if let Resp::BulkString(Some(addrb)) = &items[3] {
                            let addr = String::from_utf8_lossy(addrb).to_string();
                            let killed = kill_client_by_addr(server_ctx, &addr);
                            return (Resp::Integer(if killed { 1 } else { 0 }), None);
                        }
                        return (Resp::Integer(0), None);
                    }
                }
                // Legacy form CLIENT KILL <ip:port>
                if let Resp::BulkString(Some(addrb)) = &items[2] {
                    let addr = String::from_utf8_lossy(addrb).to_string();
                    let killed = kill_client_by_addr(server_ctx, &addr);
                    if killed {
                        return (Resp::SimpleString(Bytes::from("OK")), None);
                    } else {
                        return (Resp::Error("ERR no such client".to_string()), None);
                    }
                }
            }
            (
                Resp::Error("ERR wrong number of arguments for 'client kill'".to_string()),
                None,
            )
        }
        "pause" => (Resp::SimpleString(Bytes::from("OK")), None),
        "unpause" => (Resp::SimpleString(Bytes::from("OK")), None),
        "tracking" => {
            // CLIENT TRACKING ON|OFF
            if items.len() >= 3 {
                if let Resp::BulkString(Some(argb)) = &items[2] {
                    let arg = String::from_utf8_lossy(argb).to_uppercase();
                    if arg == "ON" {
                        conn_ctx.client_tracking = true;
                        conn_ctx.client_caching = true;
                        return (Resp::SimpleString(Bytes::from("OK")), None);
                    } else if arg == "OFF" {
                        conn_ctx.client_tracking = false;
                        conn_ctx.client_caching = false;
                        return (Resp::SimpleString(Bytes::from("OK")), None);
                    }
                }
            }
            (
                Resp::Error("ERR wrong number of arguments for 'client tracking'".to_string()),
                None,
            )
        }
        _ => (
            Resp::Error(format!(
                "ERR unknown subcommand '{}'. Try CLIENT HELP.",
                sub
            )),
            None,
        ),
    }
}

fn kill_client_by_id(server_ctx: &ServerContext, id: u64) -> bool {
    if let Some((_k, ci)) = server_ctx.clients_ctx.clients.remove(&id) {
        if let Some(tx) = ci.shutdown_tx {
            let _ = tx.send(true);
        }
        true
    } else {
        false
    }
}

fn kill_client_by_addr(server_ctx: &ServerContext, addr: &str) -> bool {
    let victim_id = server_ctx
        .clients_ctx.clients
        .iter()
        .find(|c| c.value().addr == addr)
        .map(|c| c.value().id);
    if let Some(id) = victim_id {
        kill_client_by_id(server_ctx, id)
    } else {
        false
    }
}
