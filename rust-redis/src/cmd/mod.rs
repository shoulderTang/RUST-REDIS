use crate::aof::Aof;
use crate::cmd::scripting::ScriptManager;
use crate::conf::Config;
use crate::db::Db;
use crate::acl::Acl;
use crate::resp::{Resp, as_bytes};
use std::sync::{Arc, RwLock, OnceLock};
use std::sync::atomic::Ordering;
use std::collections::{HashMap, VecDeque, HashSet};
use tokio::sync::Mutex;
use dashmap::DashMap;
use tracing::error;

pub mod command;
pub mod config;
pub mod hash;
pub mod key;
pub mod list;
pub mod scripting;
pub mod save;
pub mod set;
pub mod stream;
pub mod string;
pub mod zset;
pub mod hll;
pub mod geo;
pub mod info;
pub mod acl;
pub mod pubsub;
pub mod client;
pub mod monitor;
pub mod slowlog;


#[derive(Debug, Clone)]
pub struct ConnectionContext {
    pub id: u64,
    pub db_index: usize,
    pub authenticated: bool,
    pub current_username: String,
    pub in_multi: bool,
    pub multi_queue: Vec<Vec<Resp>>,
    pub msg_sender: Option<tokio::sync::mpsc::Sender<Resp>>,
    pub subscriptions: HashSet<String>,
    pub psubscriptions: HashSet<String>,
    pub shutdown: Option<tokio::sync::watch::Receiver<bool>>,
    pub is_lua: bool,
}

impl ConnectionContext {
    pub fn new(id: u64, msg_sender: Option<tokio::sync::mpsc::Sender<Resp>>, shutdown: Option<tokio::sync::watch::Receiver<bool>>) -> Self {
        Self {
            id,
            db_index: 0,
            authenticated: false,
            current_username: "default".to_string(),
            in_multi: false,
            multi_queue: Vec::new(),
            msg_sender,
            subscriptions: HashSet::new(),
            psubscriptions: HashSet::new(),
            shutdown,
            is_lua: false,
        }
    }
}

#[derive(Clone)]
pub struct ClientInfo {
    pub id: u64,
    pub addr: String,
    pub name: String,
    pub db: usize,
    pub sub: usize,
    pub psub: usize,
    pub flags: String,
    pub cmd: String,
    pub connect_time: std::time::Instant,
    pub last_activity: std::time::Instant,
    pub shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
}

#[derive(Clone)]
pub struct ServerContext {
    pub databases: Arc<Vec<Db>>,
    pub acl: Arc<RwLock<Acl>>,
    pub aof: Option<Arc<Mutex<Aof>>>,
    pub config: Arc<Config>,
    pub script_manager: Arc<ScriptManager>,
    pub blocking_waiters: Arc<DashMap<(usize, Vec<u8>), VecDeque<tokio::sync::mpsc::Sender<(Vec<u8>, Vec<u8>)>>>>,
    pub blocking_zset_waiters: Arc<DashMap<(usize, Vec<u8>), VecDeque<(tokio::sync::mpsc::Sender<(Vec<u8>, Vec<u8>, f64)>, bool)>>>,
    pub pubsub_channels: Arc<DashMap<String, DashMap<u64, tokio::sync::mpsc::Sender<Resp>>>>,
    pub pubsub_patterns: Arc<DashMap<String, DashMap<u64, tokio::sync::mpsc::Sender<Resp>>>>,
    pub run_id: String,
    pub start_time: std::time::Instant,
    pub client_count: Arc<std::sync::atomic::AtomicU64>,
    pub blocked_client_count: Arc<std::sync::atomic::AtomicU64>,
    pub clients: Arc<DashMap<u64, ClientInfo>>,
    pub monitors: Arc<DashMap<u64, tokio::sync::mpsc::Sender<Resp>>>,
    pub slowlog: Arc<Mutex<VecDeque<SlowLogEntry>>>,
    pub slowlog_next_id: Arc<std::sync::atomic::AtomicU64>,
    pub slowlog_max_len: Arc<std::sync::atomic::AtomicUsize>,
    pub slowlog_threshold_us: Arc<std::sync::atomic::AtomicI64>,
    pub mem_peak_rss: Arc<std::sync::atomic::AtomicU64>,
    pub maxmemory: Arc<std::sync::atomic::AtomicU64>,
}

#[derive(Clone)]
pub struct SlowLogEntry {
    pub id: u64,
    pub timestamp: i64,
    pub microseconds: i64,
    pub args: Vec<bytes::Bytes>,
    pub client_addr: String,
    pub client_name: String,
}




#[derive(Debug, PartialEq, Copy, Clone)]
enum Command {
    Ping,
    Set,
    SetNx,
    SetEx,
    PSetEx,
    GetSet,
    GetDel,
    GetEx,
    GetRange,
    Mset,
    MsetNx,
    SetRange,
    Del,
    Get,
    Mget,
    Incr,
    Decr,
    IncrBy,
    IncrByFloat,
    DecrBy,
    Append,
    StrAlgo,
    StrLen,
    Lpush,
    Lpushx,
    Rpush,
    Rpushx,
    Lpop,
    Rpop,
    Blpop,
    Brpop,
    Blmove,
    Lmove,
    Llen,
    Lindex,
    Linsert,
    Lrem,
    Lpos,
    Ltrim,
    Lrange,
    Hset,
    HsetNx,
    HincrBy,
    HincrByFloat,
    Hget,
    Hgetall,
    Hmset,
    Hmget,
    Hdel,
    Hlen,
    Hkeys,
    Hvals,
    HstrLen,
    HRandField,
    HScan,
    Sadd,
    Srem,
    Sismember,
    Smembers,
    Scard,
    SPop,
    SRandMember,
    SScan,
    SMove,
    SInter,
    SInterStore,
    SUnion,
    SUnionStore,
    SDiff,
    SDiffStore,
    Zadd,
    ZIncrBy,
    Zrem,
    Zscore,
    Zcard,
    Zrank,
    ZRevRank,
    Zrange,
    ZRevRange,
    Zrangebyscore,
    Zrangebylex,
    Zcount,
    Zlexcount,
    Zpopmin,
    Bzpopmin,
    Zpopmax,
    Bzpopmax,
    ZScan,
    ZRandMember,
    Zunion,
    Zunionstore,
    Zinter,
    Zinterstore,
    Zdiff,
    Zdiffstore,
    Pfadd,
    Pfcount,
    Pfmerge,
    GeoAdd,
    GeoDist,
    GeoHash,
    GeoPos,
    GeoRadius,
    GeoRadiusByMember,
    Expire,
    PExpire,
    ExpireAt,
    PExpireAt,
    Ttl,
    PTtl,
    Exists,
    Type,
    Rename,
    RenameNx,
    Persist,
    FlushDb,
    FlushAll,
    Dbsize,
    Keys,
    Scan,
    Save,
    Bgsave,
    Shutdown,
    Command,
    Config,
    Info,
    BgRewriteAof,
    Multi,
    Exec,
    Discard,
    Eval,
    EvalSha,
    Script,
    Select,
    Auth,
    Acl,
    Xadd,
    Xlen,
    Xrange,
    Xrevrange,
    Xdel,
    Xread,
    Xgroup,
    Xreadgroup,
    Xack,
    Subscribe,
    Unsubscribe,
    Publish,
    Psubscribe,
    Punsubscribe,
    PubSub,
    Client,
    Monitor,
    Slowlog,
    Unknown,
}

fn get_command_keys(cmd: Command, items: &[Resp]) -> Vec<Vec<u8>> {
    let mut keys = Vec::new();
    match cmd {
        Command::Set | Command::SetNx | Command::SetEx | Command::PSetEx | Command::GetSet | Command::Get | Command::GetDel | Command::GetEx | Command::GetRange | Command::SetRange | Command::Incr | Command::Decr | Command::IncrBy | Command::IncrByFloat | Command::DecrBy |
        Command::Append | Command::StrLen | Command::Lpush | Command::Rpush | Command::Lpop | Command::Rpop | Command::Blpop | Command::Brpop |
        Command::Llen | Command::Lrange | Command::Linsert | Command::Lrem | Command::Lpos | Command::Ltrim | Command::Hset | Command::HsetNx | Command::HincrBy | Command::HincrByFloat | Command::Hget | Command::Hgetall | Command::Hmset | Command::Hdel | Command::Hlen | Command::Hkeys | Command::Hvals | Command::HstrLen | Command::HRandField | Command::HScan | Command::Sadd | Command::Srem | Command::Sismember |
        Command::Smembers | Command::Scard | Command::SPop | Command::SRandMember | Command::SScan | Command::Zadd | Command::ZIncrBy | Command::Zrem | Command::Zscore | Command::Zcard |
        Command::Zrank | Command::ZRevRank | Command::Zrange | Command::ZRevRange | Command::Zrangebyscore | Command::Zrangebylex | Command::Zcount | Command::Zlexcount | Command::Zpopmin | Command::Bzpopmin | Command::Zpopmax | Command::Bzpopmax | Command::ZScan | Command::ZRandMember | Command::Pfadd | Command::Pfcount | Command::GeoAdd | Command::GeoDist |
        Command::GeoHash | Command::GeoPos | Command::GeoRadius | Command::GeoRadiusByMember | Command::Expire | Command::PExpire | Command::ExpireAt | Command::PExpireAt |
        Command::Ttl | Command::PTtl | Command::Type | Command::Persist | Command::Xadd | Command::Xlen | Command::Xrange | Command::Xrevrange | Command::Xdel => {
             if items.len() > 1 {
                 if let Some(key) = as_bytes(&items[1]) {
                     keys.push(key.to_vec());
                 }
             }
        }
        Command::Rename | Command::RenameNx | Command::SMove => {
            if items.len() > 2 {
                if let Some(key) = as_bytes(&items[1]) {
                    keys.push(key.to_vec());
                }
                if let Some(key) = as_bytes(&items[2]) {
                    keys.push(key.to_vec());
                }
            }
        }
        Command::Mset | Command::MsetNx => {
             for i in (1..items.len()).step_by(2) {
                 if let Some(key) = as_bytes(&items[i]) {
                     keys.push(key.to_vec());
                 }
             }
        }
        Command::Exists => {
             for i in 1..items.len() {
                 if let Some(key) = as_bytes(&items[i]) {
                     keys.push(key.to_vec());
                 }
             }
        }
        Command::Mget | Command::Del | Command::Pfmerge | Command::SInter | Command::SInterStore | Command::SUnion | Command::SDiff | Command::SDiffStore => {
             for i in 1..items.len() {
                 if let Some(key) = as_bytes(&items[i]) {
                     keys.push(key.to_vec());
                 }
             }
        }
        Command::Eval | Command::EvalSha => {
             if items.len() > 2 {
                 if let Some(numkeys_bytes) = as_bytes(&items[2]) {
                     if let Ok(numkeys_str) = std::str::from_utf8(&numkeys_bytes) {
                         if let Ok(numkeys) = numkeys_str.parse::<usize>() {
                             for i in 0..numkeys {
                                 if 3 + i < items.len() {
                                     if let Some(key) = as_bytes(&items[3+i]) {
                                         keys.push(key.to_vec());
                                     }
                                 }
                             }
                         }
                     }
                 }
             }
        }
        Command::Blmove | Command::Lmove => {
            if items.len() > 2 {
                if let Some(key) = as_bytes(&items[1]) {
                    keys.push(key.to_vec());
                }
                if let Some(key) = as_bytes(&items[2]) {
                    keys.push(key.to_vec());
                }
            }
        }
        Command::Zunion => {
            if items.len() > 1 {
                if let Some(numkeys_bytes) = as_bytes(&items[1]) {
                    if let Ok(numkeys_str) = std::str::from_utf8(&numkeys_bytes) {
                        if let Ok(numkeys) = numkeys_str.parse::<usize>() {
                            for i in 0..numkeys {
                                if 2 + i < items.len() {
                                    if let Some(key) = as_bytes(&items[2+i]) {
                                        keys.push(key.to_vec());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Command::Zunionstore => {
            if items.len() > 2 {
                if let Some(dest) = as_bytes(&items[1]) {
                    keys.push(dest.to_vec());
                }
                if let Some(numkeys_bytes) = as_bytes(&items[2]) {
                    if let Ok(numkeys_str) = std::str::from_utf8(&numkeys_bytes) {
                        if let Ok(numkeys) = numkeys_str.parse::<usize>() {
                            for i in 0..numkeys {
                                if 3 + i < items.len() {
                                    if let Some(key) = as_bytes(&items[3+i]) {
                                        keys.push(key.to_vec());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Command::Zinter => {
            if items.len() > 1 {
                if let Some(numkeys_bytes) = as_bytes(&items[1]) {
                    if let Ok(numkeys_str) = std::str::from_utf8(&numkeys_bytes) {
                        if let Ok(numkeys) = numkeys_str.parse::<usize>() {
                            for i in 0..numkeys {
                                if 2 + i < items.len() {
                                    if let Some(key) = as_bytes(&items[2+i]) {
                                        keys.push(key.to_vec());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Command::Zinterstore => {
            if items.len() > 2 {
                if let Some(dest) = as_bytes(&items[1]) {
                    keys.push(dest.to_vec());
                }
                if let Some(numkeys_bytes) = as_bytes(&items[2]) {
                    if let Ok(numkeys_str) = std::str::from_utf8(&numkeys_bytes) {
                        if let Ok(numkeys) = numkeys_str.parse::<usize>() {
                            for i in 0..numkeys {
                                if 3 + i < items.len() {
                                    if let Some(key) = as_bytes(&items[3+i]) {
                                        keys.push(key.to_vec());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Command::Zdiff => {
            if items.len() > 1 {
                if let Some(numkeys_bytes) = as_bytes(&items[1]) {
                    if let Ok(numkeys_str) = std::str::from_utf8(&numkeys_bytes) {
                        if let Ok(numkeys) = numkeys_str.parse::<usize>() {
                            for i in 0..numkeys {
                                if 2 + i < items.len() {
                                    if let Some(key) = as_bytes(&items[2+i]) {
                                        keys.push(key.to_vec());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Command::Zdiffstore => {
            if items.len() > 2 {
                if let Some(dest) = as_bytes(&items[1]) {
                    keys.push(dest.to_vec());
                }
                if let Some(numkeys_bytes) = as_bytes(&items[2]) {
                    if let Ok(numkeys_str) = std::str::from_utf8(&numkeys_bytes) {
                        if let Ok(numkeys) = numkeys_str.parse::<usize>() {
                            for i in 0..numkeys {
                                if 3 + i < items.len() {
                                    if let Some(key) = as_bytes(&items[3+i]) {
                                        keys.push(key.to_vec());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Command::StrAlgo => {
             for i in 2..items.len() {
                 if let Some(arg) = as_bytes(&items[i]) {
                     if arg.eq_ignore_ascii_case(b"KEYS") {
                         if i + 2 < items.len() {
                             if let Some(key) = as_bytes(&items[i+1]) {
                                 keys.push(key.to_vec());
                             }
                             if let Some(key) = as_bytes(&items[i+2]) {
                                 keys.push(key.to_vec());
                             }
                         }
                         break;
                     }
                 }
             }
        }
        _ => {}
    }
    keys
}

pub async fn process_frame(
    frame: Resp,
    conn_ctx: &mut ConnectionContext,
    server_ctx: &ServerContext,
) -> (Resp, Option<Resp>) {
    let original_frame = frame.clone();
    //println!("loaded frame: {:?}", frame);
    let (res, custom_log, cmd_name_opt) = match frame {
        Resp::Array(Some(items)) => {
            if items.is_empty() {
                (Resp::Error("ERR empty command".to_string()), None, None)
            } else {
                let cmd_raw = match as_bytes(&items[0]) {
                    Some(b) => b,
                    None => return (Resp::Error("ERR invalid command".to_string()), None),
                };

                let cmd_name = command_name(cmd_raw);

                // Authentication Check
                if server_ctx.config.requirepass.is_some() && !conn_ctx.authenticated {
                     if let Command::Auth = cmd_name {
                         // allowed
                     } else {
                        return (Resp::Error("NOAUTH Authentication required.".to_string()), None);
                     }
                }

                // ACL Check
                if let Err(e) = check_access(cmd_name, cmd_raw, &items, conn_ctx, server_ctx) {
                    (e, None, Some(cmd_name))
                } else {
                    // Monitor broadcasting
                    if !server_ctx.monitors.is_empty() {
                        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
                        let timestamp = format!("{}.{:06}", now.as_secs(), now.subsec_micros());
                        
                        let client_addr = if conn_ctx.is_lua {
                            String::from("lua")
                        } else if let Some(ci) = server_ctx.clients.get(&conn_ctx.id) {
                            ci.addr.clone()
                        } else {
                            String::from("unknown")
                        };
                        
                        let mut cmd_str = format!("{} [{} {}]", timestamp, conn_ctx.db_index, client_addr);
                        
                        for item in items.iter() {
                            match item {
                                 Resp::BulkString(Some(b)) | Resp::SimpleString(b) => {
                                     let s = String::from_utf8_lossy(&b[..]);
                                     cmd_str.push_str(&format!(" \"{}\"", s));
                                 }
                                 Resp::Integer(i) => {
                                      cmd_str.push_str(&format!(" \"{}\"", i));
                                 }
                                 _ => {}
                            }
                        }
                        
                        for m in server_ctx.monitors.iter() {
                            let _ = m.value().try_send(Resp::SimpleString(bytes::Bytes::from(cmd_str.clone())));
                        }
                    }

                    let start = std::time::Instant::now();
                    let (res, log) = dispatch_command(cmd_name, &items, conn_ctx, server_ctx).await;
                    let elapsed_us = start.elapsed().as_micros() as i64;
                    if cmd_name != Command::Slowlog && elapsed_us >= server_ctx.slowlog_threshold_us.load(Ordering::Relaxed) {
                        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
                        let timestamp = now.as_secs() as i64;
                        let mut args = Vec::new();
                        for item in items.iter() {
                            match item {
                                Resp::BulkString(Some(b)) => args.push(b.clone()),
                                Resp::SimpleString(b) => args.push(b.clone()),
                                Resp::Integer(i) => args.push(bytes::Bytes::from(i.to_string())),
                                _ => {}
                            }
                        }
                        let (client_addr, client_name) = if let Some(ci) = server_ctx.clients.get(&conn_ctx.id) {
                            (ci.addr.clone(), ci.name.clone())
                        } else {
                            (String::from("unknown"), String::new())
                        };
                        let id = server_ctx.slowlog_next_id.fetch_add(1, Ordering::Relaxed);
                        let entry = SlowLogEntry {
                            id,
                            timestamp,
                            microseconds: elapsed_us,
                            args,
                            client_addr,
                            client_name,
                        };
                        let mut logq = server_ctx.slowlog.lock().await;
                        logq.push_front(entry);
                        let max_len = server_ctx.slowlog_max_len.load(Ordering::Relaxed);
                        while logq.len() > max_len {
                            logq.pop_back();
                        }
                    }
                    (res, log, Some(cmd_name))
                }
            }
        }
        _ => (Resp::Error("ERR protocol error: expected array".to_string()), None, None),
    };

    let cmd_to_log = if let Some(l) = custom_log {
        Some(l)
    } else if let (Resp::Array(Some(items)), Some(cmd_name)) = (&original_frame, cmd_name_opt) {
        if items.is_empty() {
            None
        } else if let Some(b) = as_bytes(&items[0]) {
            if let Ok(s) = std::str::from_utf8(&b) {
                if command::is_write_command(s) && !conn_ctx.in_multi {
                    match cmd_name {
                        Command::Multi | Command::Exec | Command::Discard => None,
                        Command::Blpop => {
                            match &res {
                                Resp::Array(Some(arr)) if arr.len() >= 2 => {
                                    let key_bytes = match &arr[0] {
                                        Resp::BulkString(Some(k)) => k.clone(),
                                        Resp::SimpleString(k) => k.clone(),
                                        _ => bytes::Bytes::new(),
                                    };
                                    if !key_bytes.is_empty() {
                                        Some(Resp::Array(Some(vec![
                                            Resp::BulkString(Some(bytes::Bytes::from_static(b"LPOP"))),
                                            Resp::BulkString(Some(key_bytes)),
                                        ])))
                                    } else {
                                        None
                                    }
                                }
                                _ => None,
                            }
                        }
                        Command::Brpop => {
                            match &res {
                                Resp::Array(Some(arr)) if arr.len() >= 2 => {
                                    let key_bytes = match &arr[0] {
                                        Resp::BulkString(Some(k)) => k.clone(),
                                        Resp::SimpleString(k) => k.clone(),
                                        _ => bytes::Bytes::new(),
                                    };
                                    if !key_bytes.is_empty() {
                                        Some(Resp::Array(Some(vec![
                                            Resp::BulkString(Some(bytes::Bytes::from_static(b"RPOP"))),
                                            Resp::BulkString(Some(key_bytes)),
                                        ])))
                                    } else {
                                        None
                                    }
                                }
                                _ => None,
                            }
                        }
                        Command::Blmove => {
                            // Rewrite to LMOVE with the same arguments
                            if let Resp::Array(Some(orig_items)) = &original_frame {
                                if !orig_items.is_empty() {
                                    let mut new_items = orig_items.clone();
                                    // Replace command name
                                    new_items[0] = Resp::BulkString(Some(bytes::Bytes::from_static(b"LMOVE")));
                                    Some(Resp::Array(Some(new_items)))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        }
                        Command::Bzpopmin => {
                            // Rewrite to ZPOPMIN key
                            match &res {
                                Resp::Array(Some(arr)) if arr.len() >= 2 => {
                                    let key_bytes = match &arr[0] {
                                        Resp::BulkString(Some(k)) => k.clone(),
                                        Resp::SimpleString(k) => k.clone(),
                                        _ => bytes::Bytes::new(),
                                    };
                                    if !key_bytes.is_empty() {
                                        Some(Resp::Array(Some(vec![
                                            Resp::BulkString(Some(bytes::Bytes::from_static(b"ZPOPMIN"))),
                                            Resp::BulkString(Some(key_bytes)),
                                        ])))
                                    } else {
                                        None
                                    }
                                }
                                _ => None,
                            }
                        }
                        Command::Bzpopmax => {
                            // Rewrite to ZPOPMAX key
                            match &res {
                                Resp::Array(Some(arr)) if arr.len() >= 2 => {
                                    let key_bytes = match &arr[0] {
                                        Resp::BulkString(Some(k)) => k.clone(),
                                        Resp::SimpleString(k) => k.clone(),
                                        _ => bytes::Bytes::new(),
                                    };
                                    if !key_bytes.is_empty() {
                                        Some(Resp::Array(Some(vec![
                                            Resp::BulkString(Some(bytes::Bytes::from_static(b"ZPOPMAX"))),
                                            Resp::BulkString(Some(key_bytes)),
                                        ])))
                                    } else {
                                        None
                                    }
                                }
                                _ => None,
                            }
                        }
                        _ => {
                            if command::is_blocking_command(s) {
                                None
                            } else {
                                Some(original_frame.clone())
                            }
                        }
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    (res, cmd_to_log)
}

fn check_access(
    cmd: Command,
    cmd_raw: &[u8],
    items: &[Resp],
    conn_ctx: &ConnectionContext,
    server_ctx: &ServerContext,
) -> Result<(), Resp> {
    let acl_guard = server_ctx.acl.read().unwrap();
    if let Some(user) = acl_guard.get_user(&conn_ctx.current_username) {
        let cmd_str = String::from_utf8_lossy(cmd_raw);
        if !user.can_execute(&cmd_str) {
             return Err(Resp::Error(format!("NOPERM this user has no permissions to run the '{}' command", cmd_str)));
        }
        
        if !user.all_keys {
            let keys = get_command_keys(cmd, items);
            for key in keys {
                 if !user.can_access_key(&key) {
                      return Err(Resp::Error(format!("NOPERM this user has no permissions to access the key '{}'", String::from_utf8_lossy(&key))));
                 }
            }
        }
        Ok(())
    } else {
         Err(Resp::Error("ERR User not found".to_string()))
    }
}

async fn dispatch_command(
    cmd: Command,
    items: &[Resp],
    conn_ctx: &mut ConnectionContext,
    server_ctx: &ServerContext,
) -> (Resp, Option<Resp>) {
    if conn_ctx.in_multi {
        match cmd {
            Command::Multi => {
                return (
                    Resp::Error("ERR MULTI calls can not be nested".to_string()),
                    None,
                );
            }
            Command::Exec | Command::Discard => {}
            _ => {
                conn_ctx.multi_queue.push(items.to_vec());
                return (
                    Resp::SimpleString(bytes::Bytes::from_static(b"QUEUED")),
                    None,
                );
            }
        }
    }

    if !conn_ctx.subscriptions.is_empty() {
        match cmd {
            Command::Subscribe | Command::Unsubscribe | Command::Ping => {},
            _ => {
                 return (Resp::Error("ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context".to_string()), None);
            }
        }
    }

    let db = &server_ctx.databases[conn_ctx.db_index];
    match cmd {
        Command::Multi => {
            if items.len() != 1 {
                return (
                    Resp::Error(
                        "ERR wrong number of arguments for 'multi' command".to_string(),
                    ),
                    None,
                );
            }
            conn_ctx.in_multi = true;
            conn_ctx.multi_queue.clear();
            (Resp::SimpleString(bytes::Bytes::from_static(b"OK")), None)
        }
        Command::Exec => {
            if !conn_ctx.in_multi {
                return (
                    Resp::Error("ERR EXEC without MULTI".to_string()),
                    None,
                );
            }

            conn_ctx.in_multi = false;
            let queued = std::mem::take(&mut conn_ctx.multi_queue);
            let mut results = Vec::with_capacity(queued.len());

            for q in queued {
                if q.is_empty() {
                    results.push(Resp::Error("ERR empty command".to_string()));
                    continue;
                }
                let cmd_raw = match as_bytes(&q[0]) {
                    Some(b) => b,
                    None => {
                        results
                            .push(Resp::Error("ERR invalid command".to_string()));
                        continue;
                    }
                };
                let inner_cmd = command_name(cmd_raw);
                if let Err(e) = check_access(inner_cmd, cmd_raw, &q, conn_ctx, server_ctx) {
                    results.push(e);
                    continue;
                }
                let (res, _) = Box::pin(dispatch_command(inner_cmd, &q, conn_ctx, server_ctx)).await;
                results.push(res);
            }

            (Resp::Array(Some(results)), None)
        }
        Command::Discard => {
            if !conn_ctx.in_multi {
                return (
                    Resp::Error("ERR DISCARD without MULTI".to_string()),
                    None,
                );
            }
            conn_ctx.in_multi = false;
            conn_ctx.multi_queue.clear();
            (Resp::SimpleString(bytes::Bytes::from_static(b"OK")), None)
        }
        Command::Auth => (acl::auth(items, conn_ctx, server_ctx), None),
        Command::Acl => (acl::acl(items, conn_ctx, server_ctx), None),
        Command::Ping => {
            if items.len() == 1 {
                (Resp::SimpleString(bytes::Bytes::from_static(b"PONG")), None)
            } else if items.len() == 2 {
                match &items[1] {
                    Resp::BulkString(Some(b)) => (Resp::BulkString(Some(b.clone())), None),
                    Resp::SimpleString(s) => (Resp::BulkString(Some(s.clone())), None),
                    _ => (Resp::BulkString(None), None),
                }
            } else {
                (Resp::Error("ERR wrong number of arguments for 'PING'".to_string()), None)
            }
        }
        Command::Set => (string::set(items, db), None),
        Command::SetNx => (string::setnx(items, db), None),
        Command::SetEx => (string::setex(items, db), None),
        Command::PSetEx => (string::psetex(items, db), None),
        Command::GetSet => (string::getset(items, db), None),
        Command::GetDel => (string::getdel(items, db), None),
        Command::GetEx => (string::getex(items, db), None),
        Command::GetRange => (string::getrange(items, db), None),
        Command::Mset => (string::mset(items, db), None),
        Command::MsetNx => (string::msetnx(items, db), None),
        Command::SetRange => (string::setrange(items, db), None),
        Command::Del => (key::del(items, db), None),
        Command::Get => (string::get(items, db), None),
        Command::Mget => (string::mget(items, db), None),
        Command::Incr => (string::incr(items, db), None),
        Command::Decr => (string::decr(items, db), None),
        Command::IncrBy => (string::incrby(items, db), None),
        Command::IncrByFloat => (string::incrbyfloat(items, db), None),
        Command::DecrBy => (string::decrby(items, db), None),
        Command::Append => (string::append(items, db), None),
        Command::StrLen => (string::strlen(items, db), None),
        Command::StrAlgo => (string::stralgo(items, db), None),
        Command::Lpush => (list::lpush(items, conn_ctx, server_ctx), None),
        Command::Lpushx => (list::lpushx(items, db), None),
        Command::Rpush => (list::rpush(items, conn_ctx, server_ctx), None),
        Command::Rpushx => (list::rpushx(items, db), None),
        Command::Lpop => (list::lpop(items, db), None),
        Command::Rpop => (list::rpop(items, db), None),
        Command::Blpop => (list::blpop(items, conn_ctx, server_ctx).await, None),
        Command::Brpop => (list::brpop(items, conn_ctx, server_ctx).await, None),
        Command::Blmove => (list::blmove(items, conn_ctx, server_ctx).await, None),
        Command::Lmove => (list::lmove(items, db), None),
        Command::Linsert => (list::linsert(items, db), None),
        Command::Lrem => (list::lrem(items, db), None),
        Command::Lpos => (list::lpos(items, db), None),
        Command::Ltrim => (list::ltrim(items, db), None),
        Command::Lindex => (list::lindex(items, db), None),
        Command::Llen => (list::llen(items, db), None),
        Command::Lrange => (list::lrange(items, db), None),
        Command::Hset => (hash::hset(items, db), None),
        Command::HsetNx => (hash::hsetnx(items, db), None),
        Command::HincrBy => (hash::hincrby(items, db), None),
        Command::HincrByFloat => (hash::hincrbyfloat(items, db), None),
        Command::Hget => (hash::hget(items, db), None),
        Command::Hgetall => (hash::hgetall(items, db), None),
        Command::Hmset => (hash::hmset(items, db), None),
        Command::Hmget => (hash::hmget(items, db), None),
        Command::Hdel => (hash::hdel(items, db), None),
        Command::Hlen => (hash::hlen(items, db), None),
        Command::Hkeys => (hash::hkeys(items, db), None),
        Command::Hvals => (hash::hvals(items, db), None),
        Command::HstrLen => (hash::hstrlen(items, db), None),
        Command::HRandField => (hash::hrandfield(items, db), None),
        Command::HScan => (hash::hscan(items, db), None),
        Command::Sadd => (set::sadd(items, db), None),
        Command::Srem => (set::srem(items, db), None),
        Command::Sismember => (set::sismember(items, db), None),
        Command::Smembers => (set::smembers(items, db), None),
        Command::Scard => (set::scard(items, db), None),
        Command::SPop => (set::spop(items, db), None),
        Command::SRandMember => (set::srandmember(items, db), None),
        Command::SScan => (set::sscan(items, db), None),
        Command::SMove => (set::smove(items, db), None),
        Command::SInter => (set::sinter(items, db), None),
        Command::SInterStore => (set::sinterstore(items, db), None),
        Command::SUnion => (set::sunion(items, db), None),
        Command::SUnionStore => (set::sunionstore(items, db), None),
        Command::SDiff => (set::sdiff(items, db), None),
        Command::SDiffStore => (set::sdiffstore(items, db), None),
        Command::Zadd => (zset::zadd(items, conn_ctx, server_ctx), None),
        Command::ZIncrBy => (zset::zincrby(items, db), None),
        Command::Zrem => (zset::zrem(items, db), None),
        Command::Zscore => (zset::zscore(items, db), None),
        Command::Zcard => (zset::zcard(items, db), None),
        Command::Zrank => (zset::zrank(items, db), None),
        Command::ZRevRank => (zset::zrevrank(items, db), None),
        Command::Zrange => (zset::zrange(items, db), None),
        Command::ZRevRange => (zset::zrevrange(items, db), None),
        Command::Zrangebyscore => (zset::zrangebyscore(items, db), None),
        Command::Zrangebylex => (zset::zrangebylex(items, db), None),
        Command::Zcount => (zset::zcount(items, db), None),
        Command::Zlexcount => (zset::zlexcount(items, db), None),
        Command::Zpopmin => (zset::zpopmin(items, db), None),
        Command::Bzpopmin => (zset::bzpopmin(items, conn_ctx, server_ctx).await, None),
        Command::Zpopmax => (zset::zpopmax(items, db), None),
        Command::Bzpopmax => (zset::bzpopmax(items, conn_ctx, server_ctx).await, None),
        Command::ZScan => (zset::zscan(items, db), None),
        Command::ZRandMember => (zset::zrandmember(items, db), None),
        Command::Zunion => (zset::zunion(items, db), None),
        Command::Zunionstore => (zset::zunionstore(items, db), None),
        Command::Zinter => (zset::zinter(items, db), None),
        Command::Zinterstore => (zset::zinterstore(items, db), None),
        Command::Zdiff => (zset::zdiff(items, db), None),
        Command::Zdiffstore => (zset::zdiffstore(items, db), None),
        Command::Pfadd => (hll::pfadd(items, db), None),
        Command::Pfcount => (hll::pfcount(items, db), None),
        Command::Pfmerge => (hll::pfmerge(items, db), None),
        Command::GeoAdd => (geo::geoadd(items, db), None),
        Command::GeoDist => (geo::geodist(items, db), None),
        Command::GeoHash => (geo::geohash(items, db), None),
        Command::GeoPos => (geo::geopos(items, db), None),
        Command::GeoRadius => (geo::georadius(items, db), None),
        Command::GeoRadiusByMember => (geo::georadiusbymember(items, db), None),
        Command::Expire => (key::expire(items, db), None),
        Command::PExpire => (key::pexpire(items, db), None),
        Command::ExpireAt => (key::expireat(items, db), None),
        Command::PExpireAt => (key::pexpireat(items, db), None),
        Command::Ttl => (key::ttl(items, db), None),
        Command::PTtl => (key::pttl(items, db), None),
        Command::Exists => (key::exists(items, db), None),
        Command::Type => (key::type_(items, db), None),
        Command::Rename => (key::rename(items, db), None),
        Command::RenameNx => (key::renamenx(items, db), None),
        Command::Persist => (key::persist(items, db), None),
        Command::FlushDb => (key::flushdb(items, db), None),
        Command::FlushAll => (key::flushall(items, &server_ctx.databases), None),
        Command::Dbsize => (key::dbsize(items, db), None),
        Command::Keys => (key::keys(items, db), None),
        Command::Scan => (key::scan(items, db), None),
        Command::Save => (save::save(items, &server_ctx.databases, &server_ctx.config), None),
        Command::Bgsave => (save::bgsave(items, &server_ctx.databases, &server_ctx.config), None),
        Command::Shutdown => {
            std::process::exit(0);
        }
        Command::Command => (command::command(items), None),
        Command::Config => (config::config(items, server_ctx).await, None),
        Command::Info => (info::info(items, server_ctx), None),
        Command::Eval => scripting::eval(items, conn_ctx, server_ctx).await,
        Command::EvalSha => scripting::evalsha(items, conn_ctx, server_ctx).await,
        Command::Script => (scripting::script(items, &server_ctx.script_manager), None),
        Command::Select => {
            if items.len() != 2 {
                (Resp::Error("ERR wrong number of arguments for 'select' command".to_string()), None)
            } else {
                match as_bytes(&items[1]) {
                    Some(b) => match std::str::from_utf8(&b) {
                        Ok(s) => match s.parse::<usize>() {
                            Ok(idx) => {
                                if idx < server_ctx.databases.len() {
                                    conn_ctx.db_index = idx;
                                    (Resp::SimpleString(bytes::Bytes::from_static(b"OK")), None)
                                } else {
                                    (Resp::Error("ERR DB index is out of range".to_string()), None)
                                }
                            }
                            Err(_) => (Resp::Error("ERR value is not an integer or out of range".to_string()), None),
                        },
                        Err(_) => (Resp::Error("ERR value is not an integer or out of range".to_string()), None),
                    },
                    None => (Resp::Error("ERR value is not an integer or out of range".to_string()), None),
                }
            }
        }
        Command::Xadd => stream::xadd(items, db),
        Command::Xlen => (stream::xlen(items, db), None),
        Command::Xrange => (stream::xrange(items, db), None),
        Command::Xrevrange => (stream::xrevrange(items, db), None),
        Command::Xdel => stream::xdel(items, db),
        Command::Xread => (stream::xread_cmd(items, conn_ctx, server_ctx).await, None),
        Command::Xgroup => stream::xgroup(items, db),
        Command::Xreadgroup => stream::xreadgroup_cmd(items, conn_ctx, server_ctx).await,
        Command::Xack => stream::xack(items, db),
        Command::Publish => (pubsub::publish(items.to_vec(), conn_ctx, server_ctx).await, None),
        Command::Subscribe => (pubsub::subscribe(items.to_vec(), conn_ctx, server_ctx).await, None),
        Command::Unsubscribe => (pubsub::unsubscribe(items.to_vec(), conn_ctx, server_ctx).await, None),
        Command::Psubscribe => (pubsub::psubscribe(items.to_vec(), conn_ctx, server_ctx).await, None),
        Command::Punsubscribe => (pubsub::punsubscribe(items.to_vec(), conn_ctx, server_ctx).await, None),
        Command::PubSub => (pubsub::pubsub_command(items.to_vec(), conn_ctx, server_ctx).await, None),
        Command::Client => client::client(items, conn_ctx, server_ctx),
        Command::Monitor => monitor::monitor(conn_ctx, server_ctx),
        Command::Slowlog => slowlog::slowlog(items, server_ctx).await,
        Command::BgRewriteAof => {
            if let Some(aof) = &server_ctx.aof {
                let aof = aof.clone();
                let databases = server_ctx.databases.clone();
                tokio::spawn(async move {
                    if let Err(e) = aof.lock().await.rewrite(&databases).await {
                        error!("Background AOF rewrite failed: {}", e);
                    }
                });
                (Resp::SimpleString(bytes::Bytes::from_static(b"Background append only file rewriting started")), None)
            } else {
                (Resp::Error("ERR AOF is not enabled".to_string()), None)
            }
        }
        Command::Unknown => (Resp::Error("ERR unknown command".to_string()), None),
    }
}

fn command_name(raw: &[u8]) -> Command {
    static COMMAND_MAP: OnceLock<HashMap<String, Command>> = OnceLock::new();
    let map = COMMAND_MAP.get_or_init(|| {
        let mut m = HashMap::new();
        m.insert("PING".to_string(), Command::Ping);
        m.insert("SET".to_string(), Command::Set);
        m.insert("SETNX".to_string(), Command::SetNx);
        m.insert("SETEX".to_string(), Command::SetEx);
        m.insert("PSETEX".to_string(), Command::PSetEx);
        m.insert("GETSET".to_string(), Command::GetSet);
        m.insert("GETDEL".to_string(), Command::GetDel);
        m.insert("GETEX".to_string(), Command::GetEx);
        m.insert("GETRANGE".to_string(), Command::GetRange);
        m.insert("MSET".to_string(), Command::Mset);
        m.insert("MSETNX".to_string(), Command::MsetNx);
        m.insert("SETRANGE".to_string(), Command::SetRange);
        m.insert("DEL".to_string(), Command::Del);
        m.insert("GET".to_string(), Command::Get);
        m.insert("MGET".to_string(), Command::Mget);
        m.insert("INCR".to_string(), Command::Incr);
        m.insert("DECR".to_string(), Command::Decr);
        m.insert("INCRBY".to_string(), Command::IncrBy);
        m.insert("INCRBYFLOAT".to_string(), Command::IncrByFloat);
        m.insert("DECRBY".to_string(), Command::DecrBy);
        m.insert("APPEND".to_string(), Command::Append);
        m.insert("STRALGO".to_string(), Command::StrAlgo);
        m.insert("STRLEN".to_string(), Command::StrLen);
        m.insert("LPUSH".to_string(), Command::Lpush);
        m.insert("LPUSHX".to_string(), Command::Lpushx);
        m.insert("RPUSH".to_string(), Command::Rpush);
        m.insert("RPUSHX".to_string(), Command::Rpushx);
        m.insert("LPOP".to_string(), Command::Lpop);
        m.insert("RPOP".to_string(), Command::Rpop);
        m.insert("BLPOP".to_string(), Command::Blpop);
        m.insert("BRPOP".to_string(), Command::Brpop);
        m.insert("BLMOVE".to_string(), Command::Blmove);
        m.insert("LMOVE".to_string(), Command::Lmove);
        m.insert("LINSERT".to_string(), Command::Linsert);
        m.insert("LREM".to_string(), Command::Lrem);
        m.insert("LPOS".to_string(), Command::Lpos);
        m.insert("LINDEX".to_string(), Command::Lindex);
        m.insert("LTRIM".to_string(), Command::Ltrim);
        m.insert("LLEN".to_string(), Command::Llen);
        m.insert("LRANGE".to_string(), Command::Lrange);
        m.insert("HSET".to_string(), Command::Hset);
        m.insert("HSETNX".to_string(), Command::HsetNx);
        m.insert("HINCRBY".to_string(), Command::HincrBy);
        m.insert("HINCRBYFLOAT".to_string(), Command::HincrByFloat);
        m.insert("HGET".to_string(), Command::Hget);
        m.insert("HGETALL".to_string(), Command::Hgetall);
        m.insert("HMSET".to_string(), Command::Hmset);
        m.insert("HMGET".to_string(), Command::Hmget);
        m.insert("HDEL".to_string(), Command::Hdel);
        m.insert("HLEN".to_string(), Command::Hlen);
        m.insert("HKEYS".to_string(), Command::Hkeys);
        m.insert("HVALS".to_string(), Command::Hvals);
        m.insert("HSTRLEN".to_string(), Command::HstrLen);
        m.insert("HRANDFIELD".to_string(), Command::HRandField);
        m.insert("HSCAN".to_string(), Command::HScan);
        m.insert("SADD".to_string(), Command::Sadd);
        m.insert("SREM".to_string(), Command::Srem);
        m.insert("SISMEMBER".to_string(), Command::Sismember);
        m.insert("SMEMBERS".to_string(), Command::Smembers);
        m.insert("SCARD".to_string(), Command::Scard);
        m.insert("SPOP".to_string(), Command::SPop);
        m.insert("SRANDMEMBER".to_string(), Command::SRandMember);
        m.insert("SSCAN".to_string(), Command::SScan);
        m.insert("SMOVE".to_string(), Command::SMove);
        m.insert("SINTER".to_string(), Command::SInter);
        m.insert("SINTERSTORE".to_string(), Command::SInterStore);
        m.insert("SUNION".to_string(), Command::SUnion);
        m.insert("SUNIONSTORE".to_string(), Command::SUnionStore);
        m.insert("SDIFF".to_string(), Command::SDiff);
        m.insert("SDIFFSTORE".to_string(), Command::SDiffStore);
        m.insert("ZADD".to_string(), Command::Zadd);
        m.insert("ZINCRBY".to_string(), Command::ZIncrBy);
        m.insert("ZREM".to_string(), Command::Zrem);
        m.insert("ZSCORE".to_string(), Command::Zscore);
        m.insert("ZCARD".to_string(), Command::Zcard);
        m.insert("ZRANK".to_string(), Command::Zrank);
        m.insert("ZREVRANK".to_string(), Command::ZRevRank);
        m.insert("ZRANGE".to_string(), Command::Zrange);
        m.insert("ZREVRANGE".to_string(), Command::ZRevRange);
        m.insert("ZRANGEBYSCORE".to_string(), Command::Zrangebyscore);
        m.insert("ZRANGEBYLEX".to_string(), Command::Zrangebylex);
        m.insert("ZCOUNT".to_string(), Command::Zcount);
        m.insert("ZLEXCOUNT".to_string(), Command::Zlexcount);
        m.insert("ZPOPMIN".to_string(), Command::Zpopmin);
        m.insert("BZPOPMIN".to_string(), Command::Bzpopmin);
        m.insert("ZPOPMAX".to_string(), Command::Zpopmax);
        m.insert("BZPOPMAX".to_string(), Command::Bzpopmax);
        m.insert("ZSCAN".to_string(), Command::ZScan);
        m.insert("ZRANDMEMBER".to_string(), Command::ZRandMember);
        m.insert("ZUNION".to_string(), Command::Zunion);
        m.insert("ZUNIONSTORE".to_string(), Command::Zunionstore);
        m.insert("ZINTER".to_string(), Command::Zinter);
        m.insert("ZINTERSTORE".to_string(), Command::Zinterstore);
        m.insert("ZDIFF".to_string(), Command::Zdiff);
        m.insert("ZDIFFSTORE".to_string(), Command::Zdiffstore);
        m.insert("SDIFFSTORE".to_string(), Command::SDiffStore);
        m.insert("PFADD".to_string(), Command::Pfadd);
        m.insert("PFCOUNT".to_string(), Command::Pfcount);
        m.insert("PFMERGE".to_string(), Command::Pfmerge);
        m.insert("GEOADD".to_string(), Command::GeoAdd);
        m.insert("GEODIST".to_string(), Command::GeoDist);
        m.insert("GEOHASH".to_string(), Command::GeoHash);
        m.insert("GEOPOS".to_string(), Command::GeoPos);
        m.insert("GEORADIUS".to_string(), Command::GeoRadius);
        m.insert("GEORADIUSBYMEMBER".to_string(), Command::GeoRadiusByMember);
        m.insert("EXPIRE".to_string(), Command::Expire);
        m.insert("PEXPIRE".to_string(), Command::PExpire);
        m.insert("EXPIREAT".to_string(), Command::ExpireAt);
        m.insert("PEXPIREAT".to_string(), Command::PExpireAt);
        m.insert("TTL".to_string(), Command::Ttl);
        m.insert("PTTL".to_string(), Command::PTtl);
        m.insert("EXISTS".to_string(), Command::Exists);
        m.insert("TYPE".to_string(), Command::Type);
        m.insert("RENAME".to_string(), Command::Rename);
        m.insert("RENAMENX".to_string(), Command::RenameNx);
        m.insert("PERSIST".to_string(), Command::Persist);
        m.insert("FLUSHDB".to_string(), Command::FlushDb);
        m.insert("FLUSHALL".to_string(), Command::FlushAll);
        m.insert("DBSIZE".to_string(), Command::Dbsize);
        m.insert("KEYS".to_string(), Command::Keys);
        m.insert("SCAN".to_string(), Command::Scan);
        m.insert("SAVE".to_string(), Command::Save);
        m.insert("BGSAVE".to_string(), Command::Bgsave);
        m.insert("SHUTDOWN".to_string(), Command::Shutdown);
        m.insert("COMMAND".to_string(), Command::Command);
        m.insert("CONFIG".to_string(), Command::Config);
        m.insert("INFO".to_string(), Command::Info);
        m.insert("EVAL".to_string(), Command::Eval);
        m.insert("EVALSHA".to_string(), Command::EvalSha);
        m.insert("SCRIPT".to_string(), Command::Script);
        m.insert("SELECT".to_string(), Command::Select);
        m.insert("AUTH".to_string(), Command::Auth);
        m.insert("ACL".to_string(), Command::Acl);
        m.insert("XADD".to_string(), Command::Xadd);
        m.insert("XLEN".to_string(), Command::Xlen);
        m.insert("XRANGE".to_string(), Command::Xrange);
        m.insert("XREVRANGE".to_string(), Command::Xrevrange);
        m.insert("XDEL".to_string(), Command::Xdel);
        m.insert("XREAD".to_string(), Command::Xread);
        m.insert("XGROUP".to_string(), Command::Xgroup);
        m.insert("XREADGROUP".to_string(), Command::Xreadgroup);
        m.insert("XACK".to_string(), Command::Xack);
        m.insert("BGREWRITEAOF".to_string(), Command::BgRewriteAof);
        m.insert("MULTI".to_string(), Command::Multi);
        m.insert("EXEC".to_string(), Command::Exec);
        m.insert("DISCARD".to_string(), Command::Discard);
        m.insert("SUBSCRIBE".to_string(), Command::Subscribe);
        m.insert("UNSUBSCRIBE".to_string(), Command::Unsubscribe);
        m.insert("PUBLISH".to_string(), Command::Publish);
        m.insert("PSUBSCRIBE".to_string(), Command::Psubscribe);
        m.insert("PUNSUBSCRIBE".to_string(), Command::Punsubscribe);
        m.insert("PUBSUB".to_string(), Command::PubSub);
        m.insert("CLIENT".to_string(), Command::Client);
        m.insert("MONITOR".to_string(), Command::Monitor);
        m.insert("SLOWLOG".to_string(), Command::Slowlog); //the 110th cmd
        m
    });

    let s = String::from_utf8_lossy(raw).to_uppercase();
    map.get(&s).copied().unwrap_or(Command::Unknown)
}
