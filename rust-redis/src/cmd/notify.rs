use crate::cmd::{ServerContext, Command};
use crate::resp::Resp;
use bytes::Bytes;

pub const NOTIFY_KEYSPACE: u32 = 1 << 0;    /* K */
pub const NOTIFY_KEYEVENT: u32 = 1 << 1;    /* E */
pub const NOTIFY_GENERIC: u32 = 1 << 2;     /* g */
pub const NOTIFY_STRING: u32 = 1 << 3;      /* $ */
pub const NOTIFY_LIST: u32 = 1 << 4;        /* l */
pub const NOTIFY_SET: u32 = 1 << 5;         /* s */
pub const NOTIFY_HASH: u32 = 1 << 6;        /* h */
pub const NOTIFY_ZSET: u32 = 1 << 7;        /* z */
pub const NOTIFY_EXPIRED: u32 = 1 << 8;     /* x */
pub const NOTIFY_EVICTED: u32 = 1 << 9;     /* e */
pub const NOTIFY_STREAM: u32 = 1 << 10;    /* t */
pub const NOTIFY_KEY_MISS: u32 = 1 << 11;   /* m */
pub const NOTIFY_ALL: u32 = NOTIFY_GENERIC | NOTIFY_STRING | NOTIFY_LIST | NOTIFY_SET | NOTIFY_HASH | NOTIFY_ZSET | NOTIFY_EXPIRED | NOTIFY_EVICTED | NOTIFY_STREAM; /* A */

pub fn get_notify_flags_for_command(cmd: Command) -> u32 {
    match cmd {
        Command::Set | Command::SetNx | Command::SetEx | Command::PSetEx | Command::GetSet | Command::SetRange | Command::Append => NOTIFY_STRING,
        Command::Incr | Command::Decr | Command::IncrBy | Command::IncrByFloat | Command::DecrBy => NOTIFY_STRING,
        Command::Del | Command::Expire | Command::PExpire | Command::ExpireAt | Command::PExpireAt | Command::Persist | Command::Rename | Command::RenameNx | Command::Move | Command::Sort | Command::SortRo => NOTIFY_GENERIC,
        Command::Lpush | Command::Lpushx | Command::Rpush | Command::Rpushx | Command::Lpop | Command::Rpop | Command::Linsert | Command::Lrem | Command::Ltrim | Command::Lmove | Command::Blmove => NOTIFY_LIST,
        Command::Sadd | Command::Srem | Command::SMove | Command::SPop | Command::SInterStore | Command::SUnionStore | Command::SDiffStore => NOTIFY_SET,
        Command::Hset | Command::HsetNx | Command::HincrBy | Command::HincrByFloat | Command::Hdel => NOTIFY_HASH,
        Command::Zadd | Command::ZIncrBy | Command::Zrem | Command::Zpopmin | Command::Zpopmax | Command::Zunionstore | Command::Zinterstore | Command::Zdiffstore => NOTIFY_ZSET,
        Command::Xadd | Command::Xdel | Command::Xtrim | Command::Xgroup | Command::Xack | Command::Xclaim => NOTIFY_STREAM,
        _ => 0,
    }
}

pub fn parse_notify_flags(s: &str) -> u32 {
    let mut flags = 0;
    for c in s.chars() {
        match c {
            'K' => flags |= NOTIFY_KEYSPACE,
            'E' => flags |= NOTIFY_KEYEVENT,
            'g' => flags |= NOTIFY_GENERIC,
            '$' => flags |= NOTIFY_STRING,
            'l' => flags |= NOTIFY_LIST,
            's' => flags |= NOTIFY_SET,
            'h' => flags |= NOTIFY_HASH,
            'z' => flags |= NOTIFY_ZSET,
            't' => flags |= NOTIFY_STREAM,
            'x' => flags |= NOTIFY_EXPIRED,
            'e' => flags |= NOTIFY_EVICTED,
            'm' => flags |= NOTIFY_KEY_MISS,
            'A' => flags |= NOTIFY_ALL,
            _ => {}
        }
    }
    flags
}

pub fn flags_to_string(flags: u32) -> String {
    let mut s = String::new();
    if flags & NOTIFY_KEYSPACE != 0 { s.push('K'); }
    if flags & NOTIFY_KEYEVENT != 0 { s.push('E'); }
    if flags & NOTIFY_GENERIC != 0 { s.push('g'); }
    if flags & NOTIFY_STRING != 0 { s.push('$'); }
    if flags & NOTIFY_LIST != 0 { s.push('l'); }
    if flags & NOTIFY_SET != 0 { s.push('s'); }
    if flags & NOTIFY_HASH != 0 { s.push('h'); }
    if flags & NOTIFY_ZSET != 0 { s.push('z'); }
    if flags & NOTIFY_STREAM != 0 { s.push('t'); }
    if flags & NOTIFY_EXPIRED != 0 { s.push('x'); }
    if flags & NOTIFY_EVICTED != 0 { s.push('e'); }
    if flags & NOTIFY_KEY_MISS != 0 { s.push('m'); }
    s
}

pub async fn notify_keyspace_event(
    server_ctx: &ServerContext,
    flags: u32,
    event: &str,
    key: &[u8],
    db_index: usize,
) {
    let notify_flags = server_ctx.notify_keyspace_events.load(std::sync::atomic::Ordering::Relaxed);
    
    // If neither K nor E is set, or the event type is not enabled, return
    if (notify_flags & (NOTIFY_KEYSPACE | NOTIFY_KEYEVENT)) == 0 || (notify_flags & flags) == 0 {
        return;
    }

    let key_str = String::from_utf8_lossy(key);
    
    // __keyspace@<db>__:<key> event
    if (notify_flags & NOTIFY_KEYSPACE) != 0 {
        let channel = format!("__keyspace@{}__:{}", db_index, key_str);
        publish_event(server_ctx, &channel, event).await;
    }

    // __keyevent@<db>__:<event> key
    if (notify_flags & NOTIFY_KEYEVENT) != 0 {
        let channel = format!("__keyevent@{}__:{}", db_index, event);
        publish_event(server_ctx, &channel, &key_str).await;
    }
}

async fn publish_event(server_ctx: &ServerContext, channel: &str, message: &str) {
    let mut senders = Vec::new();
    if let Some(subscribers) = server_ctx.pubsub_channels.get(channel) {
        for sub in subscribers.iter() {
            senders.push(sub.value().clone());
        }
    }

    let msg_frame = Resp::Array(Some(vec![
        Resp::BulkString(Some(Bytes::from("message"))),
        Resp::BulkString(Some(Bytes::from(channel.to_string()))),
        Resp::BulkString(Some(Bytes::from(message.to_string()))),
    ]));

    for sender in senders {
        let _ = sender.send(msg_frame.clone()).await;
    }

    // Pattern matching
    for item in server_ctx.pubsub_patterns.iter() {
        let pattern_str = item.key();
        if let Ok(pat) = glob::Pattern::new(pattern_str) {
            if pat.matches(channel) {
                let subscribers = item.value();
                let msg_frame = Resp::Array(Some(vec![
                    Resp::BulkString(Some(Bytes::from("pmessage"))),
                    Resp::BulkString(Some(Bytes::from(pattern_str.clone()))),
                    Resp::BulkString(Some(Bytes::from(channel.to_string()))),
                    Resp::BulkString(Some(Bytes::from(message.to_string()))),
                ]));
                
                for sub in subscribers.iter() {
                    let _ = sub.value().send(msg_frame.clone()).await;
                }
            }
        }
    }
}
