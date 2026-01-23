use crate::resp::Resp;
use bytes::Bytes;

pub fn command(_items: &[Resp]) -> Resp {
    // Format: name, arity, flags, first_key, last_key, step
    // Note: This is a simplified version of command metadata.
    let commands = vec![
        ("ping", -1, vec!["fast", "stale"], 0, 0, 0),
        ("set", 3, vec!["write", "denyoom"], 1, 1, 1),
        ("get", 2, vec!["readonly", "fast"], 1, 1, 1),
        ("expire", 3, vec!["write", "fast"], 1, 1, 1),
        ("ttl", 2, vec!["readonly", "fast", "random"], 1, 1, 1),
        ("dbsize", 1, vec!["readonly", "fast"], 0, 0, 0),
        
        // List
        ("lpush", -3, vec!["write", "denyoom", "fast"], 1, 1, 1),
        ("rpush", -3, vec!["write", "denyoom", "fast"], 1, 1, 1),
        ("lpop", 2, vec!["write", "fast"], 1, 1, 1),
        ("rpop", 2, vec!["write", "fast"], 1, 1, 1),
        ("llen", 2, vec!["readonly", "fast"], 1, 1, 1),
        ("lrange", 4, vec!["readonly"], 1, 1, 1),
        
        // Hash
        ("hset", 4, vec!["write", "denyoom", "fast"], 1, 1, 1),
        ("hget", 3, vec!["readonly", "fast"], 1, 1, 1),
        ("hgetall", 2, vec!["readonly", "random"], 1, 1, 1),
        ("hmset", -4, vec!["write", "denyoom", "fast"], 1, 1, 1),
        ("hmget", -3, vec!["readonly", "fast"], 1, 1, 1),
        ("hdel", -3, vec!["write", "fast"], 1, 1, 1),
        ("hlen", 2, vec!["readonly", "fast"], 1, 1, 1),
        
        // Set
        ("sadd", -3, vec!["write", "denyoom", "fast"], 1, 1, 1),
        ("srem", -3, vec!["write", "fast"], 1, 1, 1),
        ("sismember", 3, vec!["readonly", "fast"], 1, 1, 1),
        ("smembers", 2, vec!["readonly", "sort_for_script"], 1, 1, 1),
        ("scard", 2, vec!["readonly", "fast"], 1, 1, 1),
        
        // ZSet
        ("zadd", -4, vec!["write", "denyoom", "fast"], 1, 1, 1),
        ("zrem", -3, vec!["write", "fast"], 1, 1, 1),
        ("zscore", 3, vec!["readonly", "fast"], 1, 1, 1),
        ("zcard", 2, vec!["readonly", "fast"], 1, 1, 1),
        ("zrank", 3, vec!["readonly", "fast"], 1, 1, 1),
        ("zrange", -4, vec!["readonly"], 1, 1, 1),

        // System
        ("shutdown", 1, vec!["admin", "loading", "stale"], 0, 0, 0),
        ("command", 1, vec!["random", "loading", "stale"], 0, 0, 0),
        ("config", -2, vec!["admin", "loading", "stale"], 0, 0, 0),
    ];

    let mut cmd_resps = Vec::with_capacity(commands.len());
    for (name, arity, flags, first, last, step) in commands {
        let mut flag_resps = Vec::with_capacity(flags.len());
        for f in flags {
            flag_resps.push(Resp::SimpleString(Bytes::from(f)));
        }
        
        let info = vec![
            Resp::BulkString(Some(Bytes::from(name))),
            Resp::Integer(arity),
            Resp::Array(Some(flag_resps)),
            Resp::Integer(first),
            Resp::Integer(last),
            Resp::Integer(step),
        ];
        cmd_resps.push(Resp::Array(Some(info)));
    }
    
    Resp::Array(Some(cmd_resps))
}
