use crate::cmd::{ConnectionContext, ServerContext};
use crate::resp::Resp;
use bytes::Bytes;
use dashmap::DashMap;
use glob::Pattern;

pub async fn subscribe(
    args: Vec<Resp>,
    conn_ctx: &mut ConnectionContext,
    server_ctx: &ServerContext,
) -> Resp {
    if args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'subscribe' command".to_string());
    }

    let len = args.len();
    let mut last_resp = Resp::Error("Internal error".to_string());

    // Skip command name
    for (i, arg) in args.into_iter().enumerate().skip(1) {
        let channel_name = match arg {
            Resp::BulkString(Some(ref b)) => String::from_utf8_lossy(b).to_string(),
            Resp::SimpleString(ref b) => String::from_utf8_lossy(b).to_string(),
            _ => continue, 
        };

        // Add to connection subscriptions
        if conn_ctx.subscriptions.insert(channel_name.clone()) {
            // New subscription, add to global map
            // Use entry to handle concurrent access safely
            let channel_map = server_ctx
                .pubsub_channels
                .entry(channel_name.clone())
                .or_insert_with(DashMap::new);
            
            if let Some(sender) = &conn_ctx.msg_sender {
                channel_map.insert(conn_ctx.id, sender.clone());
            }
        }

        let count = (conn_ctx.subscriptions.len() + conn_ctx.psubscriptions.len()) as i64;
        let resp = Resp::Array(Some(vec![
            Resp::BulkString(Some(Bytes::from("subscribe"))),
            Resp::BulkString(Some(Bytes::from(channel_name))),
            Resp::Integer(count),
        ]));

        if i < len - 1 {
            if let Some(sender) = &conn_ctx.msg_sender {
                let _ = sender.send(resp).await;
            }
        } else {
            last_resp = resp;
        }
    }

    last_resp
}

pub async fn unsubscribe(
    args: Vec<Resp>,
    conn_ctx: &mut ConnectionContext,
    server_ctx: &ServerContext,
) -> Resp {
    let channels_to_unsubscribe: Vec<String> = if args.len() <= 1 {
        conn_ctx.subscriptions.iter().cloned().collect()
    } else {
        args.iter()
            .skip(1)
            .filter_map(|arg| match arg {
                Resp::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                Resp::SimpleString(b) => Some(String::from_utf8_lossy(b).to_string()),
                _ => None,
            })
            .collect()
    };

    if channels_to_unsubscribe.is_empty() && args.len() <= 1 {
        // "UNSUBSCRIBE" with no args and no subscriptions
        return Resp::Array(Some(vec![
            Resp::BulkString(Some(Bytes::from("unsubscribe"))),
            Resp::BulkString(None),
            Resp::Integer((conn_ctx.subscriptions.len() + conn_ctx.psubscriptions.len()) as i64),
        ]));
    }

    let len = channels_to_unsubscribe.len();
    let mut last_resp = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("unsubscribe"))),
        Resp::BulkString(None),
        Resp::Integer(0),
    ]));

    for (i, channel_name) in channels_to_unsubscribe.into_iter().enumerate() {
        // Remove from connection subscriptions
        conn_ctx.subscriptions.remove(&channel_name);

        // Remove from global map
        if let Some(subscribers) = server_ctx.pubsub_channels.get(&channel_name) {
            subscribers.remove(&conn_ctx.id);
            // If empty, we could remove the channel from pubsub_channels, 
            // but that requires another lock or check. Leaving it is fine for now.
        }

        let count = (conn_ctx.subscriptions.len() + conn_ctx.psubscriptions.len()) as i64;
        let resp = Resp::Array(Some(vec![
            Resp::BulkString(Some(Bytes::from("unsubscribe"))),
            Resp::BulkString(Some(Bytes::from(channel_name))),
            Resp::Integer(count),
        ]));

        if i < len - 1 {
            if let Some(sender) = &conn_ctx.msg_sender {
                let _ = sender.send(resp).await;
            }
        } else {
            last_resp = resp;
        }
    }

    last_resp
}

pub async fn psubscribe(
    args: Vec<Resp>,
    conn_ctx: &mut ConnectionContext,
    server_ctx: &ServerContext,
) -> Resp {
    if args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'psubscribe' command".to_string());
    }

    let len = args.len();
    let mut last_resp = Resp::Error("Internal error".to_string());

    for (i, arg) in args.into_iter().enumerate().skip(1) {
        let pattern = match arg {
            Resp::BulkString(Some(ref b)) => String::from_utf8_lossy(b).to_string(),
            Resp::SimpleString(ref b) => String::from_utf8_lossy(b).to_string(),
            _ => continue,
        };

        if conn_ctx.psubscriptions.insert(pattern.clone()) {
             let pattern_map = server_ctx
                .pubsub_patterns
                .entry(pattern.clone())
                .or_insert_with(DashMap::new);
            
            if let Some(sender) = &conn_ctx.msg_sender {
                pattern_map.insert(conn_ctx.id, sender.clone());
            }
        }

        let count = (conn_ctx.subscriptions.len() + conn_ctx.psubscriptions.len()) as i64;
        let resp = Resp::Array(Some(vec![
            Resp::BulkString(Some(Bytes::from("psubscribe"))),
            Resp::BulkString(Some(Bytes::from(pattern))),
            Resp::Integer(count),
        ]));

        if i < len - 1 {
            if let Some(sender) = &conn_ctx.msg_sender {
                let _ = sender.send(resp).await;
            }
        } else {
            last_resp = resp;
        }
    }
    last_resp
}

pub async fn punsubscribe(
    args: Vec<Resp>,
    conn_ctx: &mut ConnectionContext,
    server_ctx: &ServerContext,
) -> Resp {
    let patterns_to_unsubscribe: Vec<String> = if args.len() <= 1 {
        conn_ctx.psubscriptions.iter().cloned().collect()
    } else {
        args.iter()
            .skip(1)
            .filter_map(|arg| match arg {
                Resp::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                Resp::SimpleString(b) => Some(String::from_utf8_lossy(b).to_string()),
                _ => None,
            })
            .collect()
    };

    if patterns_to_unsubscribe.is_empty() && args.len() <= 1 {
         return Resp::Array(Some(vec![
            Resp::BulkString(Some(Bytes::from("punsubscribe"))),
            Resp::BulkString(None),
            Resp::Integer((conn_ctx.subscriptions.len() + conn_ctx.psubscriptions.len()) as i64),
        ]));
    }

    let len = patterns_to_unsubscribe.len();
    let mut last_resp = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("punsubscribe"))),
        Resp::BulkString(None),
        Resp::Integer(0),
    ]));

    for (i, pattern) in patterns_to_unsubscribe.into_iter().enumerate() {
        conn_ctx.psubscriptions.remove(&pattern);

        if let Some(subscribers) = server_ctx.pubsub_patterns.get(&pattern) {
            subscribers.remove(&conn_ctx.id);
        }

        let count = (conn_ctx.subscriptions.len() + conn_ctx.psubscriptions.len()) as i64;
        let resp = Resp::Array(Some(vec![
            Resp::BulkString(Some(Bytes::from("punsubscribe"))),
            Resp::BulkString(Some(Bytes::from(pattern))),
            Resp::Integer(count),
        ]));

        if i < len - 1 {
             if let Some(sender) = &conn_ctx.msg_sender {
                let _ = sender.send(resp).await;
            }
        } else {
            last_resp = resp;
        }
    }
    last_resp
}

pub async fn publish(
    args: Vec<Resp>,
    _conn_ctx: &mut ConnectionContext,
    server_ctx: &ServerContext,
) -> Resp {
    if args.len() != 3 {
        return Resp::Error("ERR wrong number of arguments for 'publish' command".to_string());
    }

    let channel_name = match &args[1] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
        Resp::SimpleString(b) => String::from_utf8_lossy(b).to_string(),
        _ => return Resp::Error("Invalid channel name".to_string()),
    };

    let message_bytes = match &args[2] {
        Resp::BulkString(Some(b)) => b.clone(),
        Resp::SimpleString(b) => b.clone(),
        Resp::Integer(i) => Bytes::from(i.to_string()),
        _ => return Resp::Error("Invalid message".to_string()),
    };

    let mut senders = Vec::new();
    if let Some(subscribers) = server_ctx.pubsub_channels.get(&channel_name) {
        for sub in subscribers.iter() {
            senders.push(sub.value().clone());
        }
    }

    let mut count = 0;
    let msg_frame = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("message"))),
        Resp::BulkString(Some(Bytes::from(channel_name.clone()))),
        Resp::BulkString(Some(message_bytes.clone())),
    ]));

    for sender in senders {
        if sender.send(msg_frame.clone()).await.is_ok() {
            count += 1;
        }
    }

    // Pattern matching
    for item in server_ctx.pubsub_patterns.iter() {
        let pattern_str = item.key();
        if let Ok(pat) = Pattern::new(pattern_str) {
            if pat.matches(&channel_name) {
                let subscribers = item.value();
                 let msg_frame = Resp::Array(Some(vec![
                    Resp::BulkString(Some(Bytes::from("pmessage"))),
                    Resp::BulkString(Some(Bytes::from(pattern_str.clone()))),
                    Resp::BulkString(Some(Bytes::from(channel_name.clone()))),
                    Resp::BulkString(Some(message_bytes.clone())),
                ]));
                
                for sub in subscribers.iter() {
                    if sub.value().send(msg_frame.clone()).await.is_ok() {
                         count += 1;
                    }
                }
            }
        }
    }

    Resp::Integer(count)
}

pub async fn pubsub_command(
    args: Vec<Resp>,
    _conn_ctx: &mut ConnectionContext,
    server_ctx: &ServerContext,
) -> Resp {
    if args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'pubsub' command".to_string());
    }
    
    let subcmd = match &args[1] {
        Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string().to_uppercase(),
        Resp::SimpleString(b) => String::from_utf8_lossy(b).to_string().to_uppercase(),
        _ => return Resp::Error("ERR invalid subcommand".to_string()),
    };

    match subcmd.as_str() {
        "CHANNELS" => {
            let pattern = if args.len() > 2 {
                match &args[2] {
                    Resp::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                    Resp::SimpleString(b) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                }
            } else {
                None
            };
            
            let mut channels = Vec::new();
            for item in server_ctx.pubsub_channels.iter() {
                 let channel = item.key();
                 // Only list active channels (with at least one subscriber)
                 if item.value().is_empty() {
                     continue;
                 }

                 if let Some(p) = &pattern {
                     if let Ok(pat) = Pattern::new(p) {
                         if pat.matches(channel) {
                             channels.push(Resp::BulkString(Some(Bytes::from(channel.clone()))));
                         }
                     } else if p == channel {
                         // Fallback for invalid patterns: treat as literal match
                         channels.push(Resp::BulkString(Some(Bytes::from(channel.clone()))));
                     }
                 } else {
                     channels.push(Resp::BulkString(Some(Bytes::from(channel.clone()))));
                 }
            }
            Resp::Array(Some(channels))
        }
        "NUMSUB" => {
             let mut result = Vec::new();
             for arg in args.iter().skip(2) {
                 let channel = match arg {
                    Resp::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
                    Resp::SimpleString(b) => String::from_utf8_lossy(b).to_string(),
                    _ => continue,
                 };
                 
                 let count = if let Some(subs) = server_ctx.pubsub_channels.get(&channel) {
                     subs.len() as i64
                 } else {
                     0
                 };
                 
                 result.push(Resp::BulkString(Some(Bytes::from(channel))));
                 result.push(Resp::Integer(count));
             }
             Resp::Array(Some(result))
        }
        "NUMPAT" => {
            let count = server_ctx.pubsub_patterns.len() as i64;
            Resp::Integer(count)
        }
        _ => Resp::Error("ERR unknown subcommand".to_string()),
    }
}
