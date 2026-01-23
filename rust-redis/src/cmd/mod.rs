use crate::db::Db;
use crate::resp::{as_bytes, Resp};
use crate::aof::Aof;
use crate::conf::Config;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::error;

mod string;
mod list;
mod hash;
mod set;
mod zset;
mod key;
mod command;
mod config;

#[cfg(test)]
#[path = "../cmd_test.rs"]
mod tests;

enum Command {
    Ping,
    Set,
    Mset,
    Del,
    Get,
    Mget,
    Incr,
    Decr,
    IncrBy,
    DecrBy,
    Append,
    StrLen,
    Expire,
    Ttl,
    Dbsize,
    Keys,
    Lpush,
    Rpush,
    Lpop,
    Rpop,
    Llen,
    Lrange,
    Hset,
    Hget,
    Hgetall,
    Hmset,
    Hmget,
    Hdel,
    Hlen,
    Sadd,
    Srem,
    Sismember,
    Smembers,
    Scard,
    Zadd,
    Zrem,
    Zscore,
    Zcard,
    Zrank,
    Zrange,
    Shutdown,
    Command,
    Config,
    BgRewriteAof,
    Unknown,
}

pub fn process_frame(
    frame: Resp,
    db: &Db,
    aof: &Option<Arc<Mutex<Aof>>>,
    cfg: &Config,
) -> (Resp, Option<Resp>) {
    let cmd_to_log = if let Resp::Array(Some(ref items)) = frame {
        if !items.is_empty() {
             if let Some(b) = as_bytes(&items[0]) {
                 if let Ok(s) = std::str::from_utf8(&b) {
                     if command::is_write_command(s) {
                         Some(frame.clone())
                     } else {
                         None
                     }
                 } else { None }
             } else { None }
        } else { None }
    } else { None };

    let res = match frame {
        Resp::Array(Some(items)) => {
            if items.is_empty() {
                return (Resp::Error("ERR empty command".to_string()), None);
            }
            let cmd_raw = match as_bytes(&items[0]) {
                Some(b) => b,
                None => return (Resp::Error("ERR invalid command".to_string()), None),
            };
            match command_name(cmd_raw) {
                Command::Ping => {
                    if items.len() == 1 {
                        Resp::SimpleString(bytes::Bytes::from_static(b"PONG"))
                    } else if items.len() == 2 {
                        match &items[1] {
                            Resp::BulkString(Some(b)) => Resp::BulkString(Some(b.clone())),
                            Resp::SimpleString(s) => Resp::BulkString(Some(s.clone())),
                            _ => Resp::BulkString(None),
                        }
                    } else {
                        Resp::Error("ERR wrong number of arguments for 'PING'".to_string())
                    }
                }
                Command::Set => string::set(&items, db),
                Command::Mset => string::mset(&items, db),
                Command::Del => key::del(&items, db),
                Command::Get => string::get(&items, db),
                Command::Mget => string::mget(&items, db),
                Command::Incr => string::incr(&items, db),
                Command::Decr => string::decr(&items, db),
                Command::IncrBy => string::incrby(&items, db),
                Command::DecrBy => string::decrby(&items, db),
                Command::Append => string::append(&items, db),
                Command::StrLen => string::strlen(&items, db),
                Command::Lpush => list::lpush(&items, db),
                Command::Rpush => list::rpush(&items, db),
                Command::Lpop => list::lpop(&items, db),
                Command::Rpop => list::rpop(&items, db),
                Command::Llen => list::llen(&items, db),
                Command::Lrange => list::lrange(&items, db),
                Command::Hset => hash::hset(&items, db),
                Command::Hget => hash::hget(&items, db),
                Command::Hgetall => hash::hgetall(&items, db),
                Command::Hmset => hash::hmset(&items, db),
                Command::Hmget => hash::hmget(&items, db),
                Command::Hdel => hash::hdel(&items, db),
                Command::Hlen => hash::hlen(&items, db),
                Command::Sadd => set::sadd(&items, db),
                Command::Srem => set::srem(&items, db),
                Command::Sismember => set::sismember(&items, db),
                Command::Smembers => set::smembers(&items, db),
                Command::Scard => set::scard(&items, db),
                Command::Zadd => zset::zadd(&items, db),
                Command::Zrem => zset::zrem(&items, db),
                Command::Zscore => zset::zscore(&items, db),
                Command::Zcard => zset::zcard(&items, db),
                Command::Zrank => zset::zrank(&items, db),
                Command::Zrange => zset::zrange(&items, db),
                Command::Expire => key::expire(&items, db),
                Command::Ttl => key::ttl(&items, db),
                Command::Dbsize => key::dbsize(&items, db),
                Command::Keys => key::keys(&items, db),
                Command::Shutdown => {
                    std::process::exit(0);
                }
                Command::Command => command::command(&items),
                Command::Config => config::config(&items, cfg),
                Command::BgRewriteAof => {
                    if let Some(aof) = aof {
                        let aof = aof.clone();
                        let db = db.clone();
                        tokio::spawn(async move {
                            if let Err(e) = aof.lock().await.rewrite(&db).await {
                                error!("Background AOF rewrite failed: {}", e);
                            }
                        });
                        Resp::SimpleString(bytes::Bytes::from_static(b"Background append only file rewriting started"))
                    } else {
                        Resp::Error("ERR AOF is not enabled".to_string())
                    }
                }
                Command::Unknown => Resp::Error("ERR unknown command".to_string()),
            }
        }
        _ => Resp::Error("ERR protocol error: expected array".to_string()),
    };
    (res, cmd_to_log)
}


fn command_name(raw: &[u8]) -> Command {
    if equals_ignore_ascii_case(raw, b"PING") {
        Command::Ping
    } else if equals_ignore_ascii_case(raw, b"SET") {
        Command::Set
    } else if equals_ignore_ascii_case(raw, b"MSET") {
        Command::Mset
    } else if equals_ignore_ascii_case(raw, b"DEL") {
        Command::Del
    } else if equals_ignore_ascii_case(raw, b"GET") {
        Command::Get
    } else if equals_ignore_ascii_case(raw, b"MGET") {
        Command::Mget
    } else if equals_ignore_ascii_case(raw, b"INCR") {
        Command::Incr
    } else if equals_ignore_ascii_case(raw, b"DECR") {
        Command::Decr
    } else if equals_ignore_ascii_case(raw, b"INCRBY") {
        Command::IncrBy
    } else if equals_ignore_ascii_case(raw, b"DECRBY") {
        Command::DecrBy
    } else if equals_ignore_ascii_case(raw, b"APPEND") {
        Command::Append
    } else if equals_ignore_ascii_case(raw, b"STRLEN") {
        Command::StrLen
    } else if equals_ignore_ascii_case(raw, b"EXPIRE") {
        Command::Expire
    } else if equals_ignore_ascii_case(raw, b"TTL") {
        Command::Ttl
    } else if equals_ignore_ascii_case(raw, b"DBSIZE") {
        Command::Dbsize
    } else if equals_ignore_ascii_case(raw, b"KEYS") {
        Command::Keys
    } else if equals_ignore_ascii_case(raw, b"LPUSH") {
        Command::Lpush
    } else if equals_ignore_ascii_case(raw, b"RPUSH") {
        Command::Rpush
    } else if equals_ignore_ascii_case(raw, b"LPOP") {
        Command::Lpop
    } else if equals_ignore_ascii_case(raw, b"RPOP") {
        Command::Rpop
    } else if equals_ignore_ascii_case(raw, b"LLEN") {
        Command::Llen
    } else if equals_ignore_ascii_case(raw, b"LRANGE") {
        Command::Lrange
    } else if equals_ignore_ascii_case(raw, b"HSET") {
        Command::Hset
    } else if equals_ignore_ascii_case(raw, b"HGET") {
        Command::Hget
    } else if equals_ignore_ascii_case(raw, b"HGETALL") {
        Command::Hgetall
    } else if equals_ignore_ascii_case(raw, b"HMSET") {
        Command::Hmset
    } else if equals_ignore_ascii_case(raw, b"HMGET") {
        Command::Hmget
    } else if equals_ignore_ascii_case(raw, b"HDEL") {
        Command::Hdel
    } else if equals_ignore_ascii_case(raw, b"HLEN") {
        Command::Hlen
    } else if equals_ignore_ascii_case(raw, b"SADD") {
        Command::Sadd
    } else if equals_ignore_ascii_case(raw, b"SREM") {
        Command::Srem
    } else if equals_ignore_ascii_case(raw, b"SISMEMBER") {
        Command::Sismember
    } else if equals_ignore_ascii_case(raw, b"SMEMBERS") {
        Command::Smembers
    } else if equals_ignore_ascii_case(raw, b"SCARD") {
        Command::Scard
    } else if equals_ignore_ascii_case(raw, b"ZADD") {
        Command::Zadd
    } else if equals_ignore_ascii_case(raw, b"ZREM") {
        Command::Zrem
    } else if equals_ignore_ascii_case(raw, b"ZSCORE") {
        Command::Zscore
    } else if equals_ignore_ascii_case(raw, b"ZCARD") {
        Command::Zcard
    } else if equals_ignore_ascii_case(raw, b"ZRANK") {
        Command::Zrank
    } else if equals_ignore_ascii_case(raw, b"ZRANGE") {
        Command::Zrange
    } else if equals_ignore_ascii_case(raw, b"SHUTDOWN") {
        Command::Shutdown
    } else if equals_ignore_ascii_case(raw, b"COMMAND") {
        Command::Command
    } else if equals_ignore_ascii_case(raw, b"CONFIG") {
        Command::Config
    } else if equals_ignore_ascii_case(raw, b"BGREWRITEAOF") {
        Command::BgRewriteAof
    } else {
        Command::Unknown
    }
}

fn equals_ignore_ascii_case(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    for i in 0..a.len() {
        let ca = a[i];
        let cb = b[i];
        if ca == cb {
            continue;
        }
        if ca.to_ascii_lowercase() != cb.to_ascii_lowercase() {
            return false;
        }
    }
    true
}
