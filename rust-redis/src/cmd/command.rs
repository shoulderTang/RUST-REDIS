use crate::resp::Resp;
use bytes::Bytes;

struct CommandInfo {
    name: &'static str,
    arity: i64,
    flags: &'static [&'static str],
    first_key: i64,
    last_key: i64,
    step: i64,
}

const COMMAND_TABLE: &[CommandInfo] = &[
    CommandInfo { name: "ping", arity: -1, flags: &["fast", "stale"], first_key: 0, last_key: 0, step: 0 },
    CommandInfo { name: "set", arity: 3, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "get", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "expire", arity: 3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "ttl", arity: 2, flags: &["readonly", "fast", "random"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "dbsize", arity: 1, flags: &["readonly", "fast"], first_key: 0, last_key: 0, step: 0 },
    
    // List
    CommandInfo { name: "lpush", arity: -3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "rpush", arity: -3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "lpop", arity: 2, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "rpop", arity: 2, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "llen", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "lrange", arity: 4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    
    // Hash
    CommandInfo { name: "hset", arity: 4, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "hget", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "hgetall", arity: 2, flags: &["readonly", "random"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "hmset", arity: -4, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "hmget", arity: -3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "hdel", arity: -3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "hlen", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    
    // Set
    CommandInfo { name: "sadd", arity: -3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "srem", arity: -3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "sismember", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "smembers", arity: 2, flags: &["readonly", "sort_for_script"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "scard", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    
    // ZSet
    CommandInfo { name: "zadd", arity: -4, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "zrem", arity: -3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "zscore", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "zcard", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "zrank", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandInfo { name: "zrange", arity: -4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },

    // System
    CommandInfo { name: "shutdown", arity: 1, flags: &["admin", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    CommandInfo { name: "command", arity: 1, flags: &["random", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    CommandInfo { name: "config", arity: -2, flags: &["admin", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
];

pub fn is_write_command(name: &str) -> bool {
    COMMAND_TABLE.iter().any(|c| c.name.eq_ignore_ascii_case(name) && c.flags.contains(&"write"))
}

pub fn command(_items: &[Resp]) -> Resp {
    let mut cmd_resps = Vec::with_capacity(COMMAND_TABLE.len());
    for info in COMMAND_TABLE {
        let mut flag_resps = Vec::with_capacity(info.flags.len());
        for f in info.flags {
            flag_resps.push(Resp::SimpleString(Bytes::from(*f)));
        }
        
        let cmd_info = vec![
            Resp::BulkString(Some(Bytes::from(info.name))),
            Resp::Integer(info.arity),
            Resp::Array(Some(flag_resps)),
            Resp::Integer(info.first_key),
            Resp::Integer(info.last_key),
            Resp::Integer(info.step),
        ];
        cmd_resps.push(Resp::Array(Some(cmd_info)));
    }
    
    Resp::Array(Some(cmd_resps))
}
