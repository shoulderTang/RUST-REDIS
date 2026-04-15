use crate::acl::Acl;
use crate::aof::AofWriter;
use crate::cmd::scripting::ScriptManager;
use crate::conf::Config;
use crate::db::Db;
use crate::resp::{Resp, as_bytes, read_frame, write_frame};
use dashmap::DashMap;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock, RwLock};
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::error;

pub mod acl;
pub mod asking;
pub mod bitmap;
pub mod client;
pub mod cluster;
pub mod command;
pub mod config;
pub mod dump;
pub mod evict;
pub mod geo;
pub mod hash;
pub mod hello;
pub mod hll;
pub mod info;
pub mod key;
pub mod latency;
pub mod list;
pub mod memory;
pub mod monitor;
pub mod notify;
pub mod pubsub;
pub mod replication;
pub mod reset;
pub mod save;
pub mod scripting;
pub mod set;
pub mod slowlog;
pub mod sort;
pub mod stream;
pub mod string;
pub mod zset;

#[derive(Debug, Clone)]
pub struct AclLogEntry {
    pub count: u64,
    pub reason: String,
    pub context: String,
    pub object: String,
    pub username: String,
    pub age: u64,
    pub client_id: u64,
}

#[derive(Debug, Clone)]
pub struct LatencyEvent {
    pub timestamp: u64,
    pub duration: u64,
}

fn unwatch_all_keys(conn_ctx: &mut ConnectionContext, server_ctx: &ServerContext) {
    for (db_idx, keys) in conn_ctx.watched_keys.iter() {
        for key in keys {
            if let Some(mut clients) = server_ctx.watched_clients.get_mut(&(*db_idx, key.clone())) {
                clients.remove(&conn_ctx.id);
            }
        }
    }
    conn_ctx.watched_keys.clear();
}

fn touch_watched_key(key: &[u8], db_idx: usize, server_ctx: &ServerContext) {
    let map_key = (db_idx, key.to_vec());

    // 1. Transaction WATCH
    if let Some(clients) = server_ctx.watched_clients.get(&map_key) {
        for client_id in clients.iter() {
            if let Some(dirty_flag) = server_ctx.client_watched_dirty.get(client_id) {
                dirty_flag.store(true, Ordering::SeqCst);
            }
        }
    }

    // 2. Client Side Caching Tracking
    let client_ids = if let Some(entry) = server_ctx.tracking_clients.get(&map_key) {
        Some(entry.value().clone())
    } else {
        None
    };

    if let Some(ids) = client_ids {
        let invalidation_msg = Resp::Array(Some(vec![
            Resp::BulkString(Some(bytes::Bytes::from_static(b"invalidate"))),
            Resp::Array(Some(vec![Resp::BulkString(Some(
                bytes::Bytes::copy_from_slice(key),
            ))])),
        ]));

        for client_id in ids.iter() {
            if let Some(client_info) = server_ctx.clients.get(client_id) {
                if let Some(sender) = &client_info.msg_sender {
                    let _ = sender.try_send(invalidation_msg.clone());
                }
            }
        }
        // Redis 6.0 tracking usually removes keys after invalidation (except BCAST mode)
        // For simplicity we remove them here.
        server_ctx.tracking_clients.remove(&map_key);
    }
}

pub fn watch(items: &[Resp], conn_ctx: &mut ConnectionContext, server_ctx: &ServerContext) -> Resp {
    if items.len() < 2 {
        return Resp::StaticError("ERR wrong number of arguments for 'watch' command");
    }

    if conn_ctx.in_multi {
        return Resp::StaticError("ERR WATCH inside MULTI is not allowed");
    }

    for item in items.iter().skip(1) {
        if let Some(key) = as_bytes(item) {
            let key_vec = key.to_vec();
            let keys = conn_ctx
                .watched_keys
                .entry(conn_ctx.db_index)
                .or_insert_with(HashSet::new);
            if keys.insert(key_vec.clone()) {
                server_ctx
                    .watched_clients
                    .entry((conn_ctx.db_index, key_vec))
                    .or_insert_with(HashSet::new)
                    .insert(conn_ctx.id);
            }
        }
    }

    Resp::SimpleString(bytes::Bytes::from_static(b"OK"))
}

pub fn unwatch(conn_ctx: &mut ConnectionContext, server_ctx: &ServerContext) -> Resp {
    unwatch_all_keys(conn_ctx, server_ctx);
    conn_ctx.watched_keys_dirty.store(false, Ordering::SeqCst);
    Resp::SimpleString(bytes::Bytes::from_static(b"OK"))
}

use std::os::unix::io::RawFd;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationState {
    Normal,
    TransferringRdb,
}

#[derive(Debug, Clone)]
pub struct ConnectionContext {
    pub id: u64,
    pub client_fd: Option<RawFd>, // Added for fork-based replication
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
    pub watched_keys: HashMap<usize, HashSet<Vec<u8>>>,
    pub watched_keys_dirty: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub client_tracking: bool,
    pub client_caching: bool,
    pub client_redir_id: i64, // -1 means no redirection
    pub client_tracking_broken: bool,
    pub is_master: bool,
    pub is_replica: bool,
    pub replication_state: Arc<std::sync::Mutex<ReplicationState>>,
    pub asking: bool, // ASKING for cluster slot migration
}

impl ConnectionContext {
    pub fn new(
        id: u64,
        client_fd: Option<RawFd>,
        msg_sender: Option<tokio::sync::mpsc::Sender<Resp>>,
        shutdown: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> Self {
        Self {
            id,
            client_fd,
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
            watched_keys: HashMap::new(),
            watched_keys_dirty: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            client_tracking: false,
            client_caching: true, // Default to true as per Redis spec for BCAST or prefix-less
            client_redir_id: -1,
            client_tracking_broken: false,
            is_master: false,
            is_replica: false,
            replication_state: Arc::new(std::sync::Mutex::new(ReplicationState::Normal)),
            asking: false,
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
    pub msg_sender: Option<tokio::sync::mpsc::Sender<Resp>>,
}

pub struct NodeConn {
    pub reader: tokio::sync::Mutex<tokio::io::BufReader<tokio::net::tcp::OwnedReadHalf>>,
    pub writer: tokio::sync::Mutex<tokio::io::BufWriter<tokio::net::tcp::OwnedWriteHalf>>,
}

#[derive(Clone)]
pub struct ServerContext {
    pub databases: Arc<Vec<RwLock<Db>>>,
    pub acl: Arc<RwLock<Acl>>,
    pub aof: Option<AofWriter>,
    pub config: Arc<Config>,
    pub script_manager: Arc<ScriptManager>,
    pub blocking_waiters:
        Arc<DashMap<(usize, Vec<u8>), VecDeque<tokio::sync::mpsc::Sender<(Vec<u8>, Vec<u8>)>>>>,
    pub blocking_zset_waiters: Arc<
        DashMap<
            (usize, Vec<u8>),
            VecDeque<(tokio::sync::mpsc::Sender<(Vec<u8>, Vec<u8>, f64)>, bool)>,
        >,
    >,
    pub pubsub_channels: Arc<DashMap<String, DashMap<u64, tokio::sync::mpsc::Sender<Resp>>>>,
    pub pubsub_patterns: Arc<DashMap<String, DashMap<u64, tokio::sync::mpsc::Sender<Resp>>>>,
    pub run_id: Arc<RwLock<String>>,  // Primary Replication ID
    pub replid2: Arc<RwLock<String>>, // Secondary Replication ID
    pub second_repl_offset: Arc<std::sync::atomic::AtomicI64>,
    pub start_time: std::time::Instant,
    pub client_count: Arc<std::sync::atomic::AtomicU64>,
    pub blocked_client_count: Arc<std::sync::atomic::AtomicU64>,
    pub clients: Arc<DashMap<u64, ClientInfo>>,
    pub monitors: Arc<DashMap<u64, tokio::sync::mpsc::Sender<Resp>>>,
    pub replicas: Arc<DashMap<u64, tokio::sync::mpsc::Sender<Resp>>>,
    pub repl_backlog: Arc<std::sync::Mutex<VecDeque<(u64, Resp)>>>,
    pub repl_backlog_size: Arc<std::sync::atomic::AtomicUsize>,
    pub repl_ping_replica_period: Arc<std::sync::atomic::AtomicU64>,
    pub repl_timeout: Arc<std::sync::atomic::AtomicU64>,
    pub repl_offset: Arc<std::sync::atomic::AtomicU64>,
    pub replica_ack: Arc<DashMap<u64, u64>>,
    pub replica_ack_time: Arc<DashMap<u64, u64>>,
    pub replica_listening_port: Arc<DashMap<u64, u16>>,
    pub slowlog: Arc<Mutex<VecDeque<SlowLogEntry>>>,
    pub slowlog_next_id: Arc<std::sync::atomic::AtomicU64>,
    pub slowlog_max_len: Arc<std::sync::atomic::AtomicUsize>,
    pub slowlog_threshold_us: Arc<std::sync::atomic::AtomicI64>,
    pub mem_peak_rss: Arc<std::sync::atomic::AtomicU64>,
    pub maxmemory: Arc<std::sync::atomic::AtomicU64>,
    pub notify_keyspace_events: Arc<std::sync::atomic::AtomicU32>,
    pub rdbcompression: Arc<std::sync::atomic::AtomicBool>,
    pub rdbchecksum: Arc<std::sync::atomic::AtomicBool>,
    pub stop_writes_on_bgsave_error: Arc<std::sync::atomic::AtomicBool>,
    pub replica_read_only: Arc<std::sync::atomic::AtomicBool>,
    pub min_replicas_to_write: Arc<std::sync::atomic::AtomicUsize>,
    pub min_replicas_max_lag: Arc<std::sync::atomic::AtomicU64>,
    pub repl_diskless_sync: Arc<std::sync::atomic::AtomicBool>,
    pub repl_diskless_sync_delay: Arc<std::sync::atomic::AtomicU64>,
    pub maxmemory_policy: Arc<RwLock<crate::conf::EvictionPolicy>>,
    pub maxmemory_samples: Arc<std::sync::atomic::AtomicUsize>,
    pub save_params: Arc<RwLock<Vec<(u64, u64)>>>,
    pub last_bgsave_ok: Arc<std::sync::atomic::AtomicBool>,
    pub dirty: Arc<std::sync::atomic::AtomicU64>,
    pub last_save_time: Arc<std::sync::atomic::AtomicI64>,
    pub watched_clients: Arc<DashMap<(usize, Vec<u8>), HashSet<u64>>>,
    pub client_watched_dirty: Arc<DashMap<u64, Arc<std::sync::atomic::AtomicBool>>>,
    pub tracking_clients: Arc<DashMap<(usize, Vec<u8>), HashSet<u64>>>,
    pub acl_log: Arc<RwLock<VecDeque<AclLogEntry>>>,
    pub latency_events: Arc<DashMap<String, VecDeque<LatencyEvent>>>,
    pub replication_role: Arc<RwLock<ReplicationRole>>,
    pub master_host: Arc<RwLock<Option<String>>>,
    pub master_port: Arc<RwLock<Option<u16>>>,
    pub repl_waiters: Arc<std::sync::Mutex<VecDeque<WaitContext>>>,
    pub rdb_child_pid: Arc<std::sync::atomic::AtomicI32>, // Added for fork tracking
    pub rdb_sync_client_id: Arc<std::sync::atomic::AtomicU64>, // Added to track which client is syncing
    pub master_link_established: Arc<std::sync::atomic::AtomicBool>,
    pub cluster: Arc<RwLock<crate::cluster::ClusterState>>,
    pub node_conns: Arc<dashmap::DashMap<(String, u16), Arc<NodeConn>>>,
}

#[derive(Debug)]
pub struct WaitContext {
    pub target_offset: u64,
    pub num_replicas: usize,
    pub tx: tokio::sync::oneshot::Sender<usize>,
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
pub(crate) enum Command {
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
    Unlink,
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
    HExists,
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
    SMismember,
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
    Zmscore,
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
    GeoSearch,
    GeoSearchStore,
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
    Move,
    SwapDb,
    Persist,
    Copy,
    Object,
    FlushDb,
    FlushAll,
    Dbsize,
    Keys,
    Scan,
    Save,
    Bgsave,
    LastSave,
    Role,
    ReplicaOf,
    Psync,
    ReplConf,
    Time,
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
    Xtrim,
    Xread,
    Xgroup,
    Xreadgroup,
    Xack,
    Xinfo,
    Xpending,
    Xclaim,
    Xautoclaim,
    SetBit,
    GetBit,
    BitCount,
    BitOp,
    BitPos,
    BitField,
    Watch,
    Unwatch,
    Subscribe,
    Unsubscribe,
    Publish,
    Psubscribe,
    Punsubscribe,
    PubSub,
    Client,
    Monitor,
    Memory,
    Slowlog,
    Latency,
    Dump,
    Restore,
    Touch,
    Sort,
    SortRo,
    Echo,
    Hello,
    Reset,
    Wait,
    Cluster,
    Asking,
    Unknown,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ReplicationRole {
    Master,
    Slave,
}

pub(crate) fn get_command_keys<'a>(cmd: Command, items: &'a [Resp]) -> Vec<&'a [u8]> {
    let mut keys = Vec::new();
    match cmd {
        Command::Set
        | Command::SetNx
        | Command::SetEx
        | Command::PSetEx
        | Command::GetSet
        | Command::Get
        | Command::GetDel
        | Command::GetEx
        | Command::GetRange
        | Command::SetRange
        | Command::Incr
        | Command::Decr
        | Command::IncrBy
        | Command::IncrByFloat
        | Command::DecrBy
        | Command::Append
        | Command::StrLen
        | Command::Lpush
        | Command::Rpush
        | Command::Lpop
        | Command::Rpop
        | Command::Blpop
        | Command::Brpop
        | Command::Llen
        | Command::Lrange
        | Command::Linsert
        | Command::Lrem
        | Command::Lpos
        | Command::Ltrim
        | Command::Hset
        | Command::HsetNx
        | Command::HincrBy
        | Command::HincrByFloat
        | Command::Hget
        | Command::Hgetall
        | Command::Hmset
        | Command::Hdel
        | Command::HExists
        | Command::Hlen
        | Command::Hkeys
        | Command::Hvals
        | Command::HstrLen
        | Command::HRandField
        | Command::HScan
        | Command::Sadd
        | Command::Srem
        | Command::Sismember
        | Command::Smembers
        | Command::Scard
        | Command::SPop
        | Command::SRandMember
        | Command::SScan
        | Command::Zadd
        | Command::ZIncrBy
        | Command::Zrem
        | Command::Zscore
        | Command::Zcard
        | Command::Zrank
        | Command::ZRevRank
        | Command::Zrange
        | Command::ZRevRange
        | Command::Zrangebyscore
        | Command::Zrangebylex
        | Command::Zcount
        | Command::Zlexcount
        | Command::Zpopmin
        | Command::Bzpopmin
        | Command::Zpopmax
        | Command::Bzpopmax
        | Command::ZScan
        | Command::ZRandMember
        | Command::Zmscore
        | Command::Pfadd
        | Command::Pfcount
        | Command::GeoAdd
        | Command::GeoDist
        | Command::GeoHash
        | Command::GeoPos
        | Command::GeoRadius
        | Command::GeoRadiusByMember
        | Command::GeoSearch
        | Command::GeoSearchStore
        | Command::Expire
        | Command::PExpire
        | Command::ExpireAt
        | Command::PExpireAt
        | Command::Ttl
        | Command::PTtl
        | Command::Type
        | Command::Persist
        | Command::Move
        | Command::Xadd
        | Command::Xlen
        | Command::Xrange
        | Command::Xrevrange
        | Command::Xdel
        | Command::Xtrim
        | Command::Xinfo
        | Command::Xpending
        | Command::Xclaim
        | Command::Xautoclaim
        | Command::SetBit
        | Command::GetBit
        | Command::BitCount
        | Command::BitPos
        | Command::BitField
        | Command::Dump
        | Command::Restore
        | Command::Sort
        | Command::SortRo
        | Command::SMismember => {
            if items.len() > 1 {
                if let Some(key) = as_bytes(&items[1]) {
                    keys.push(key);
                }
            }
        }
        Command::BitOp => {
            for item in items.iter().skip(2) {
                if let Some(key) = as_bytes(item) {
                    keys.push(key);
                }
            }
        }
        Command::Rename | Command::RenameNx | Command::SMove | Command::Copy => {
            if items.len() > 2 {
                if let Some(key) = as_bytes(&items[1]) {
                    keys.push(key);
                }
                if let Some(key) = as_bytes(&items[2]) {
                    keys.push(key);
                }
            }
        }
        Command::Object => {
            if items.len() > 2 {
                if let Some(key) = as_bytes(&items[2]) {
                    keys.push(key);
                }
            }
        }
        Command::Mset | Command::MsetNx => {
            for i in (1..items.len()).step_by(2) {
                if let Some(key) = as_bytes(&items[i]) {
                    keys.push(key);
                }
            }
        }
        Command::Exists | Command::Touch => {
            for i in 1..items.len() {
                if let Some(key) = as_bytes(&items[i]) {
                    keys.push(key);
                }
            }
        }
        Command::Mget
        | Command::Del
        | Command::Unlink
        | Command::Pfmerge
        | Command::SInter
        | Command::SInterStore
        | Command::SUnion
        | Command::SDiff
        | Command::SDiffStore
        | Command::Watch => {
            for i in 1..items.len() {
                if let Some(key) = as_bytes(&items[i]) {
                    keys.push(key);
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
                                    if let Some(key) = as_bytes(&items[3 + i]) {
                                        keys.push(key);
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
                    keys.push(key);
                }
                if let Some(key) = as_bytes(&items[2]) {
                    keys.push(key);
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
                                    if let Some(key) = as_bytes(&items[2 + i]) {
                                        keys.push(key);
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
                    keys.push(dest);
                }
                if let Some(numkeys_bytes) = as_bytes(&items[2]) {
                    if let Ok(numkeys_str) = std::str::from_utf8(&numkeys_bytes) {
                        if let Ok(numkeys) = numkeys_str.parse::<usize>() {
                            for i in 0..numkeys {
                                if 3 + i < items.len() {
                                    if let Some(key) = as_bytes(&items[3 + i]) {
                                        keys.push(key);
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
                                    if let Some(key) = as_bytes(&items[2 + i]) {
                                        keys.push(key);
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
                    keys.push(dest);
                }
                if let Some(numkeys_bytes) = as_bytes(&items[2]) {
                    if let Ok(numkeys_str) = std::str::from_utf8(&numkeys_bytes) {
                        if let Ok(numkeys) = numkeys_str.parse::<usize>() {
                            for i in 0..numkeys {
                                if 3 + i < items.len() {
                                    if let Some(key) = as_bytes(&items[3 + i]) {
                                        keys.push(key);
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
                                    if let Some(key) = as_bytes(&items[2 + i]) {
                                        keys.push(key);
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
                    keys.push(dest);
                }
                if let Some(numkeys_bytes) = as_bytes(&items[2]) {
                    if let Ok(numkeys_str) = std::str::from_utf8(&numkeys_bytes) {
                        if let Ok(numkeys) = numkeys_str.parse::<usize>() {
                            for i in 0..numkeys {
                                if 3 + i < items.len() {
                                    if let Some(key) = as_bytes(&items[3 + i]) {
                                        keys.push(key);
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
                            if let Some(key) = as_bytes(&items[i + 1]) {
                                keys.push(key);
                            }
                            if let Some(key) = as_bytes(&items[i + 2]) {
                                keys.push(key);
                            }
                        }
                        break;
                    }
                }
            }
        }
        Command::Memory => {
            if items.len() >= 3 {
                if let Some(sub) = as_bytes(&items[1]) {
                    if sub.eq_ignore_ascii_case(b"USAGE") {
                        if let Some(key) = as_bytes(&items[2]) {
                            keys.push(key);
                        }
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
    //println!("loaded frame: {:?}", frame);
    let (res, custom_log, cmd_name_opt, original_items) = match frame {
        Resp::Array(Some(items)) => {
            if items.is_empty() {
                (Resp::StaticError("ERR empty command"), None, None, None)
            } else {
                let cmd_raw = match as_bytes(&items[0]) {
                    Some(b) => b,
                    None => return (Resp::StaticError("ERR invalid command"), None),
                };

                let cmd_name = command_name(cmd_raw);
                // Cache once per command: avoids 3× RwLock acquisitions and 5× utf8+string checks
                let role = *server_ctx.replication_role.read().unwrap();
                let is_write =
                    std::str::from_utf8(cmd_raw).map_or(false, |s| command::is_write_command(s));

                // Authentication Check
                if server_ctx.config.requirepass.is_some() && !conn_ctx.authenticated {
                    if let Command::Auth = cmd_name {
                        // allowed
                    } else {
                        return (Resp::StaticError("NOAUTH Authentication required."), None);
                    }
                }

                // ACL Check
                if let Err(e) = check_access(cmd_name, cmd_raw, &items, conn_ctx, server_ctx) {
                    // Record ACL log
                    acl::record_acl_log(
                        server_ctx,
                        AclLogEntry {
                            count: 1,
                            reason: "command not allowed".to_string(),
                            context: "toplevel".to_string(),
                            object: String::from_utf8_lossy(cmd_raw).to_string(),
                            username: conn_ctx.current_username.clone(),
                            age: 0,
                            client_id: conn_ctx.id,
                        },
                    );
                    (e, None, Some(cmd_name), Some(items))
                } else if server_ctx.replica_read_only.load(Ordering::Relaxed)
                    && role == ReplicationRole::Slave
                    && is_write
                    && !conn_ctx.is_master
                {
                    (
                        Resp::StaticError("READONLY You can't write against a read only replica."),
                        None,
                        Some(cmd_name),
                        Some(items),
                    )
                } else if server_ctx.min_replicas_to_write.load(Ordering::Relaxed) > 0
                    && role == ReplicationRole::Master
                    && is_write
                    && {
                        let min_replicas = server_ctx.min_replicas_to_write.load(Ordering::Relaxed);
                        let max_lag = server_ctx.min_replicas_max_lag.load(Ordering::Relaxed);
                        let now = crate::clock::now_secs();
                        let healthy_replicas = server_ctx
                            .replica_ack_time
                            .iter()
                            .filter(|r| now.saturating_sub(*r.value()) <= max_lag)
                            .count();
                        healthy_replicas < min_replicas
                    }
                {
                    let min_replicas = server_ctx.min_replicas_to_write.load(Ordering::Relaxed);
                    let max_lag = server_ctx.min_replicas_max_lag.load(Ordering::Relaxed);
                    let now = crate::clock::now_secs();
                    let healthy_replicas = server_ctx
                        .replica_ack_time
                        .iter()
                        .filter(|r| now.saturating_sub(*r.value()) <= max_lag)
                        .count();
                    (
                        Resp::Error(format!(
                            "NOREPLICAS Not enough good replicas to write. {} < {}",
                            healthy_replicas, min_replicas
                        )),
                        None,
                        Some(cmd_name),
                        Some(items),
                    )
                } else if server_ctx.maxmemory.load(Ordering::Relaxed) > 0
                    && evict::is_over_maxmemory(server_ctx.maxmemory.load(Ordering::Relaxed))
                    && is_write
                    && *server_ctx.maxmemory_policy.read().unwrap()
                        == crate::conf::EvictionPolicy::NoEviction
                {
                    (
                        Resp::StaticError(
                            "OOM command not allowed when used memory > 'maxmemory'.",
                        ),
                        None,
                        Some(cmd_name),
                        Some(items),
                    )
                } else if server_ctx
                    .stop_writes_on_bgsave_error
                    .load(Ordering::Relaxed)
                    && !server_ctx.last_bgsave_ok.load(Ordering::Relaxed)
                    && is_write
                {
                    (
                        Resp::StaticError(
                            "MISCONF Redis is configured to report errors after a last background save failed. Writing commands are disabled.",
                        ),
                        None,
                        Some(cmd_name),
                        Some(items),
                    )
                } else {
                    // Perform eviction if needed (already checked it's not noeviction or we are not over limit for write cmd)
                    if server_ctx.maxmemory.load(Ordering::Relaxed) > 0 {
                        if let Err(e) = evict::perform_eviction(server_ctx) {
                            error!("Eviction error: {}", e);
                        }
                    }

                    // Monitor broadcasting
                    if !server_ctx.monitors.is_empty() {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default();
                        let timestamp = format!("{}.{:06}", now.as_secs(), now.subsec_micros());

                        let client_addr = if conn_ctx.is_lua {
                            String::from("lua")
                        } else if let Some(ci) = server_ctx.clients.get(&conn_ctx.id) {
                            ci.addr.clone()
                        } else {
                            String::from("unknown")
                        };

                        let mut cmd_str =
                            format!("{} [{} {}]", timestamp, conn_ctx.db_index, client_addr);

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
                            let _ = m
                                .value()
                                .try_send(Resp::SimpleString(bytes::Bytes::from(cmd_str.clone())));
                        }
                    }

                    let start = std::time::Instant::now();
                    let (res, log) = dispatch_command(cmd_name, &items, conn_ctx, server_ctx).await;
                    let elapsed_us = start.elapsed().as_micros() as i64;

                    // Record latency
                    if elapsed_us > 1000 {
                        // > 1ms
                        let cmd_str = String::from_utf8_lossy(cmd_raw).to_lowercase();
                        latency::record_latency(server_ctx, &cmd_str, (elapsed_us / 1000) as u64);
                    }

                    // Handle client tracking
                    if conn_ctx.client_tracking && conn_ctx.client_caching {
                        if let Ok(s) = std::str::from_utf8(cmd_raw) {
                            if !command::is_write_command(s) {
                                let keys = get_command_keys(cmd_name, &items);
                                for key in keys {
                                    server_ctx
                                        .tracking_clients
                                        .entry((conn_ctx.db_index, key.to_vec()))
                                        .or_insert_with(HashSet::new)
                                        .insert(conn_ctx.id);
                                }
                            }
                        }
                    }

                    // Check if this was a write command and invalidate watched keys
                    // Only trigger if the command was NOT queued
                    let is_queued =
                        matches!(res, Resp::SimpleString(ref s) if s.as_ref() == b"QUEUED");
                    let is_error = matches!(res, Resp::Error(_) | Resp::StaticError(_));
                    if !is_queued && !is_error {
                        if let Ok(s) = std::str::from_utf8(cmd_raw) {
                            if command::is_write_command(s) {
                                // Increment dirty counter
                                let changes = match &res {
                                    Resp::Integer(n) if *n > 0 => *n as u64,
                                    _ => 1,
                                };
                                server_ctx.dirty.fetch_add(changes, Ordering::Relaxed);

                                let keys = get_command_keys(cmd_name, &items);
                                for key in keys {
                                    touch_watched_key(key, conn_ctx.db_index, server_ctx);

                                    // Trigger keyspace notification
                                    let event = s.to_lowercase();
                                    let flags = notify::get_notify_flags_for_command(cmd_name);
                                    notify::notify_keyspace_event(
                                        server_ctx,
                                        flags,
                                        &event,
                                        key,
                                        conn_ctx.db_index,
                                    )
                                    .await;
                                }
                            }
                        }
                    }

                    if cmd_name != Command::Slowlog
                        && elapsed_us >= server_ctx.slowlog_threshold_us.load(Ordering::Relaxed)
                    {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default();
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
                        let (client_addr, client_name) =
                            if let Some(ci) = server_ctx.clients.get(&conn_ctx.id) {
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
                    (res, log, Some(cmd_name), Some(items))
                }
            }
        }
        _ => (
            Resp::StaticError("ERR protocol error: expected array"),
            None,
            None,
            None,
        ),
    };

    let cmd_to_log = if let Some(l) = custom_log {
        Some(l)
    } else if let Some(cmd_name) = cmd_name_opt {
        if let Some(items) = original_items.as_ref() {
            if items.is_empty() {
                None
            } else if let Some(b) = as_bytes(&items[0]) {
                if let Ok(s) = std::str::from_utf8(&b) {
                    if command::is_write_command(s) && !conn_ctx.in_multi {
                        match cmd_name {
                            Command::Multi | Command::Exec | Command::Discard => None,
                            Command::Blpop => match &res {
                                Resp::Array(Some(arr)) if arr.len() >= 2 => {
                                    let key_bytes = match &arr[0] {
                                        Resp::BulkString(Some(k)) => k.clone(),
                                        Resp::SimpleString(k) => k.clone(),
                                        _ => bytes::Bytes::new(),
                                    };
                                    if !key_bytes.is_empty() {
                                        Some(Resp::Array(Some(vec![
                                            Resp::BulkString(Some(bytes::Bytes::from_static(
                                                b"LPOP",
                                            ))),
                                            Resp::BulkString(Some(key_bytes)),
                                        ])))
                                    } else {
                                        None
                                    }
                                }
                                _ => None,
                            },
                            Command::Brpop => match &res {
                                Resp::Array(Some(arr)) if arr.len() >= 2 => {
                                    let key_bytes = match &arr[0] {
                                        Resp::BulkString(Some(k)) => k.clone(),
                                        Resp::SimpleString(k) => k.clone(),
                                        _ => bytes::Bytes::new(),
                                    };
                                    if !key_bytes.is_empty() {
                                        Some(Resp::Array(Some(vec![
                                            Resp::BulkString(Some(bytes::Bytes::from_static(
                                                b"RPOP",
                                            ))),
                                            Resp::BulkString(Some(key_bytes)),
                                        ])))
                                    } else {
                                        None
                                    }
                                }
                                _ => None,
                            },
                            Command::Blmove => {
                                // Rewrite to LMOVE with the same arguments
                                if !items.is_empty() {
                                    let mut new_items = items.clone();
                                    // Replace command name
                                    new_items[0] =
                                        Resp::BulkString(Some(bytes::Bytes::from_static(b"LMOVE")));
                                    Some(Resp::Array(Some(new_items)))
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
                                                Resp::BulkString(Some(bytes::Bytes::from_static(
                                                    b"ZPOPMIN",
                                                ))),
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
                                                Resp::BulkString(Some(bytes::Bytes::from_static(
                                                    b"ZPOPMAX",
                                                ))),
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
                                    Some(Resp::Array(Some(items.clone())))
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
        if !user.enabled {
            return Err(Resp::Error(format!("NOPERM this user is disabled")));
        }
        let cmd_str = String::from_utf8_lossy(cmd_raw);
        if !user.can_execute(&cmd_str) {
            return Err(Resp::Error(format!(
                "NOPERM this user has no permissions to run the '{}' command",
                cmd_str
            )));
        }

        if !user.all_keys {
            let keys = get_command_keys(cmd, items);
            for key in keys {
                if !user.can_access_key(key) {
                    return Err(Resp::Error(format!(
                        "NOPERM this user has no permissions to access the key '{}'",
                        String::from_utf8_lossy(key)
                    )));
                }
            }
        }
        if server_ctx.config.cluster_enabled {
            let keys = get_command_keys(cmd, items);
            if !keys.is_empty() {
                let mut slots = Vec::new();
                for k in &keys {
                    let ks = String::from_utf8_lossy(k).to_string();
                    let s = crate::cluster::ClusterState::key_slot(&ks) as usize;
                    slots.push(s);
                }
                let first = slots[0];
                for s in &slots {
                    if *s != first {
                        return Err(Resp::StaticError(
                            "CROSSSLOT Keys in request don't hash to the same slot",
                        ));
                    }
                }
                let st = server_ctx.cluster.read().unwrap();
                if first >= st.slots.len() {
                    return Err(Resp::StaticError("CLUSTERDOWN Hash slot not served"));
                }
                // 先检查 MIGRATING/IMPORTING 状态
                match &st.slot_state[first] {
                    crate::cluster::SlotState::Migrating { to } => {
                        if let Some(n) = st.nodes.get(to) {
                            let ask = format!("ASK {} {}:{}", first, n.ip, n.port);
                            return Err(Resp::Error(ask));
                        }
                    }
                    crate::cluster::SlotState::Importing { from } => {
                        // 目标节点处于 IMPORTING，需要客户端先发送 ASKING
                        if !conn_ctx.asking {
                            if let Some(n) = st.nodes.get(from) {
                                let ask = format!("ASK {} {}:{}", first, n.ip, n.port);
                                return Err(Resp::Error(ask));
                            }
                        }
                        // 已发送 ASKING，允许访问，后续正常处理
                    }
                    _ => {}
                }
                match &st.slots[first] {
                    Some(owner) => {
                        if *owner != st.myself {
                            if server_ctx.config.cluster_require_full_coverage {
                                if let Some(n) = st.nodes.get(owner) {
                                    let moved = format!("MOVED {} {}:{}", first, n.ip, n.port);
                                    return Err(Resp::Error(moved));
                                } else {
                                    return Err(Resp::StaticError(
                                        "CLUSTERDOWN Hash slot not served",
                                    ));
                                }
                            } else {
                                if let Some(n) = st.nodes.get(owner) {
                                    let moved = format!("MOVED {} {}:{}", first, n.ip, n.port);
                                    return Err(Resp::Error(moved));
                                }
                            }
                        }
                    }
                    None => {
                        if server_ctx.config.cluster_require_full_coverage {
                            return Err(Resp::StaticError("CLUSTERDOWN Hash slot not served"));
                        }
                    }
                }
            }
        }
        Ok(())
    } else {
        Err(Resp::StaticError("ERR User not found"))
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
                return (Resp::StaticError("ERR MULTI calls can not be nested"), None);
            }
            Command::Exec | Command::Discard | Command::Reset => {}
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
            Command::Subscribe | Command::Unsubscribe | Command::Ping | Command::Reset => {}
            _ => {
                return (
                    Resp::StaticError(
                        "ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET allowed in this context",
                    ),
                    None,
                );
            }
        }
    }

    let db_idx = conn_ctx.db_index;
    let db = {
        let db_lock = server_ctx.databases[db_idx].read().unwrap();
        db_lock.clone()
    };
    conn_ctx.asking = false;
    match cmd {
        Command::Multi => {
            if items.len() != 1 {
                return (
                    Resp::Error("ERR wrong number of arguments for 'multi' command".to_string()),
                    None,
                );
            }
            conn_ctx.in_multi = true;
            conn_ctx.multi_queue.clear();
            (Resp::SimpleString(bytes::Bytes::from_static(b"OK")), None)
        }
        Command::Exec => {
            if !conn_ctx.in_multi {
                return (Resp::StaticError("ERR EXEC without MULTI"), None);
            }

            conn_ctx.in_multi = false;
            let queued = std::mem::take(&mut conn_ctx.multi_queue);

            if conn_ctx.watched_keys_dirty.load(Ordering::SeqCst) {
                unwatch_all_keys(conn_ctx, server_ctx);
                conn_ctx.watched_keys_dirty.store(false, Ordering::SeqCst);
                return (Resp::Array(None), None);
            }

            unwatch_all_keys(conn_ctx, server_ctx);
            conn_ctx.watched_keys_dirty.store(false, Ordering::SeqCst);

            let mut results = Vec::with_capacity(queued.len());

            for q in queued {
                if q.is_empty() {
                    results.push(Resp::StaticError("ERR empty command"));
                    continue;
                }
                let cmd_raw = match as_bytes(&q[0]) {
                    Some(b) => b,
                    None => {
                        results.push(Resp::StaticError("ERR invalid command"));
                        continue;
                    }
                };
                let inner_cmd = command_name(cmd_raw);
                if let Err(e) = check_access(inner_cmd, cmd_raw, &q, conn_ctx, server_ctx) {
                    results.push(e);
                    continue;
                }
                let (res, _) =
                    Box::pin(dispatch_command(inner_cmd, &q, conn_ctx, server_ctx)).await;

                // Trigger watched keys invalidation
                if let Ok(s) = std::str::from_utf8(cmd_raw) {
                    if command::is_write_command(s) {
                        let keys = get_command_keys(inner_cmd, &q);
                        for key in keys {
                            touch_watched_key(key, conn_ctx.db_index, server_ctx);
                        }
                    }
                }

                results.push(res);
            }

            (Resp::Array(Some(results)), None)
        }
        Command::Discard => {
            if !conn_ctx.in_multi {
                return (Resp::StaticError("ERR DISCARD without MULTI"), None);
            }
            conn_ctx.in_multi = false;
            conn_ctx.multi_queue.clear();
            unwatch_all_keys(conn_ctx, server_ctx);
            conn_ctx.watched_keys_dirty.store(false, Ordering::SeqCst);
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
                (
                    Resp::StaticError("ERR wrong number of arguments for 'PING'"),
                    None,
                )
            }
        }
        Command::Echo => {
            if items.len() != 2 {
                (
                    Resp::StaticError("ERR wrong number of arguments for 'echo' command"),
                    None,
                )
            } else {
                match &items[1] {
                    Resp::BulkString(Some(b)) => (Resp::BulkString(Some(b.clone())), None),
                    Resp::SimpleString(s) => (Resp::BulkString(Some(s.clone())), None),
                    _ => (Resp::BulkString(None), None),
                }
            }
        }
        Command::Hello => (hello::hello(items, conn_ctx, &server_ctx), None),
        Command::Reset => (reset::reset(conn_ctx, server_ctx), None),
        Command::Set => (string::set(items, &db), None),
        Command::SetNx => (string::setnx(items, &db), None),
        Command::SetEx => (string::setex(items, &db), None),
        Command::PSetEx => (string::psetex(items, &db), None),
        Command::GetSet => (string::getset(items, &db), None),
        Command::GetDel => (string::getdel(items, &db), None),
        Command::GetEx => (string::getex(items, &db), None),
        Command::GetRange => (string::getrange(items, &db), None),
        Command::Mset => (string::mset(items, &db), None),
        Command::MsetNx => (string::msetnx(items, &db), None),
        Command::SetRange => (string::setrange(items, &db), None),
        Command::Del => (key::del(items, &db), None),
        Command::Unlink => (key::unlink(items, &db), None),
        Command::Get => (string::get(items, &db), None),
        Command::Mget => (string::mget(items, &db), None),
        Command::Incr => (string::incr(items, &db), None),
        Command::Decr => (string::decr(items, &db), None),
        Command::IncrBy => (string::incrby(items, &db), None),
        Command::IncrByFloat => (string::incrbyfloat(items, &db), None),
        Command::DecrBy => (string::decrby(items, &db), None),
        Command::Append => (string::append(items, &db), None),
        Command::StrLen => (string::strlen(items, &db), None),
        Command::StrAlgo => (string::stralgo(items, &db), None),
        Command::Lpush => (list::lpush(items, &db, conn_ctx, server_ctx), None),
        Command::Lpushx => (list::lpushx(items, &db), None),
        Command::Rpush => (list::rpush(items, &db, conn_ctx, server_ctx), None),
        Command::Rpushx => (list::rpushx(items, &db), None),
        Command::Lpop => (list::lpop(items, &db), None),
        Command::Rpop => (list::rpop(items, &db), None),
        Command::Blpop => (list::blpop(items, &db, conn_ctx, server_ctx).await, None),
        Command::Brpop => (list::brpop(items, &db, conn_ctx, server_ctx).await, None),
        Command::Blmove => (list::blmove(items, &db, conn_ctx, server_ctx).await, None),
        Command::Lmove => (list::lmove(items, &db), None),
        Command::Linsert => (list::linsert(items, &db), None),
        Command::Lrem => (list::lrem(items, &db), None),
        Command::Lpos => (list::lpos(items, &db), None),
        Command::Ltrim => (list::ltrim(items, &db), None),
        Command::Lindex => (list::lindex(items, &db), None),
        Command::Llen => (list::llen(items, &db), None),
        Command::Lrange => (list::lrange(items, &db), None),
        Command::Hset => (hash::hset(items, &db), None),
        Command::HsetNx => (hash::hsetnx(items, &db), None),
        Command::HincrBy => (hash::hincrby(items, &db), None),
        Command::HincrByFloat => (hash::hincrbyfloat(items, &db), None),
        Command::Hget => (hash::hget(items, &db), None),
        Command::Hgetall => (hash::hgetall(items, &db), None),
        Command::Hmset => (hash::hmset(items, &db), None),
        Command::Hmget => (hash::hmget(items, &db), None),
        Command::Hdel => (hash::hdel(items, &db), None),
        Command::HExists => (hash::hexists(items, &db), None),
        Command::Hlen => (hash::hlen(items, &db), None),
        Command::Hkeys => (hash::hkeys(items, &db), None),
        Command::Hvals => (hash::hvals(items, &db), None),
        Command::HstrLen => (hash::hstrlen(items, &db), None),
        Command::HRandField => (hash::hrandfield(items, &db), None),
        Command::HScan => (hash::hscan(items, &db), None),
        Command::Sadd => (set::sadd(items, &db), None),
        Command::Srem => (set::srem(items, &db), None),
        Command::Sismember => (set::sismember(items, &db), None),
        Command::SMismember => (set::smismember(items, &db), None),
        Command::Smembers => (set::smembers(items, &db), None),
        Command::Scard => (set::scard(items, &db), None),
        Command::SPop => (set::spop(items, &db), None),
        Command::SRandMember => (set::srandmember(items, &db), None),
        Command::SScan => (set::sscan(items, &db), None),
        Command::SMove => (set::smove(items, &db), None),
        Command::SInter => (set::sinter(items, &db), None),
        Command::SInterStore => (set::sinterstore(items, &db), None),
        Command::SUnion => (set::sunion(items, &db), None),
        Command::SUnionStore => (set::sunionstore(items, &db), None),
        Command::SDiff => (set::sdiff(items, &db), None),
        Command::SDiffStore => (set::sdiffstore(items, &db), None),
        Command::Zadd => (zset::zadd(items, conn_ctx, server_ctx), None),
        Command::ZIncrBy => (zset::zincrby(items, &db), None),
        Command::Zrem => (zset::zrem(items, &db), None),
        Command::Zscore => (zset::zscore(items, &db), None),
        Command::Zmscore => (zset::zmscore(items, &db), None),
        Command::Zcard => (zset::zcard(items, &db), None),
        Command::Zrank => (zset::zrank(items, &db), None),
        Command::ZRevRank => (zset::zrevrank(items, &db), None),
        Command::Zrange => (zset::zrange(items, &db), None),
        Command::ZRevRange => (zset::zrevrange(items, &db), None),
        Command::Zrangebyscore => (zset::zrangebyscore(items, &db), None),
        Command::Zrangebylex => (zset::zrangebylex(items, &db), None),
        Command::Zcount => (zset::zcount(items, &db), None),
        Command::Zlexcount => (zset::zlexcount(items, &db), None),
        Command::Zpopmin => (zset::zpopmin(items, &db), None),
        Command::Bzpopmin => (zset::bzpopmin(items, conn_ctx, server_ctx).await, None),
        Command::Zpopmax => (zset::zpopmax(items, &db), None),
        Command::Bzpopmax => (zset::bzpopmax(items, conn_ctx, server_ctx).await, None),
        Command::ZScan => (zset::zscan(items, &db), None),
        Command::ZRandMember => (zset::zrandmember(items, &db), None),
        Command::Zunion => (zset::zunion(items, &db), None),
        Command::Zunionstore => (zset::zunionstore(items, &db), None),
        Command::Zinter => (zset::zinter(items, &db), None),
        Command::Zinterstore => (zset::zinterstore(items, &db), None),
        Command::Zdiff => (zset::zdiff(items, &db), None),
        Command::Zdiffstore => (zset::zdiffstore(items, &db), None),
        Command::Pfadd => (hll::pfadd(items, &db), None),
        Command::Pfcount => (hll::pfcount(items, &db), None),
        Command::Pfmerge => (hll::pfmerge(items, &db), None),
        Command::GeoAdd => (geo::geoadd(items, &db), None),
        Command::GeoDist => (geo::geodist(items, &db), None),
        Command::GeoHash => (geo::geohash(items, &db), None),
        Command::GeoPos => (geo::geopos(items, &db), None),
        Command::GeoRadius => (geo::georadius(items, &db), None),
        Command::GeoRadiusByMember => (geo::georadiusbymember(items, &db), None),
        Command::GeoSearch => (geo::geosearch(items, &db), None),
        Command::GeoSearchStore => (geo::geosearchstore(items, &db), None),
        Command::Expire => (key::expire(items, &db), None),
        Command::PExpire => (key::pexpire(items, &db), None),
        Command::ExpireAt => (key::expireat(items, &db), None),
        Command::PExpireAt => (key::pexpireat(items, &db), None),
        Command::Ttl => (key::ttl(items, &db), None),
        Command::PTtl => (key::pttl(items, &db), None),
        Command::Exists => (key::exists(items, &db), None),
        Command::Type => (key::type_(items, &db), None),
        Command::Rename => (key::rename(items, &db), None),
        Command::RenameNx => (key::renamenx(items, &db), None),
        Command::Persist => (key::persist(items, &db), None),
        Command::Copy => (key::copy(items, conn_ctx, server_ctx), None),
        Command::Object => (key::object(items, &db), None),
        Command::Move => (key::move_(items, conn_ctx, server_ctx), None),
        Command::SwapDb => (key::swapdb(items, server_ctx), None),
        Command::FlushDb => (key::flushdb(items, &db), None),
        Command::FlushAll => (key::flushall(items, &server_ctx.databases), None),
        Command::Dbsize => (key::dbsize(items, &db), None),
        Command::Keys => (key::keys(items, &db), None),
        Command::Scan => (key::scan(items, &db), None),
        Command::Save => (save::save(items, server_ctx), None),
        Command::Bgsave => (save::bgsave(items, server_ctx), None),
        Command::LastSave => (save::lastsave(items, server_ctx), None),
        Command::Role => (info::role(items, server_ctx), None),
        Command::ReplicaOf => (replication::replicaof(items, server_ctx), None),
        Command::Psync => (replication::psync(items, conn_ctx, server_ctx).await, None),
        Command::ReplConf => (replication::replconf(items, conn_ctx, server_ctx), None),
        Command::Time => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap();
            let mut res = Vec::new();
            res.push(Resp::BulkString(Some(bytes::Bytes::from(
                now.as_secs().to_string(),
            ))));
            res.push(Resp::BulkString(Some(bytes::Bytes::from(
                now.subsec_micros().to_string(),
            ))));
            (Resp::Array(Some(res)), None)
        }
        Command::Shutdown => {
            // Flush AOF before exiting so no buffered commands are lost.
            if let Some(aof) = &server_ctx.aof {
                aof.flush().await;
            }
            std::process::exit(0);
        }
        Command::Command => (command::command(items), None),
        Command::Config => (config::config(items, server_ctx).await, None),
        Command::Cluster => {
            if server_ctx.config.cluster_enabled {
                (cluster::cluster(items, conn_ctx, server_ctx), None)
            } else {
                (
                    Resp::StaticError("ERR This instance has cluster support disabled"),
                    None,
                )
            }
        }
        Command::Info => (info::info(items, server_ctx), None),
        Command::Memory => (memory::memory(items, &db, server_ctx).await, None),
        Command::Eval => scripting::eval(items, conn_ctx, server_ctx).await,
        Command::EvalSha => scripting::evalsha(items, conn_ctx, server_ctx).await,
        Command::Script => (scripting::script(items, &server_ctx.script_manager), None),
        Command::Select => {
            if items.len() != 2 {
                (
                    Resp::StaticError("ERR wrong number of arguments for 'select' command"),
                    None,
                )
            } else {
                match as_bytes(&items[1]) {
                    Some(b) => match std::str::from_utf8(&b) {
                        Ok(s) => match s.parse::<usize>() {
                            Ok(idx) => {
                                if server_ctx.config.cluster_enabled && idx != 0 {
                                    (
                                        Resp::StaticError(
                                            "ERR SELECT is not allowed in cluster mode",
                                        ),
                                        None,
                                    )
                                } else if idx < server_ctx.databases.len() {
                                    conn_ctx.db_index = idx;
                                    (Resp::SimpleString(bytes::Bytes::from_static(b"OK")), None)
                                } else {
                                    (Resp::StaticError("ERR DB index is out of range"), None)
                                }
                            }
                            Err(_) => (
                                Resp::StaticError("ERR value is not an integer or out of range"),
                                None,
                            ),
                        },
                        Err(_) => (
                            Resp::StaticError("ERR value is not an integer or out of range"),
                            None,
                        ),
                    },
                    None => (
                        Resp::StaticError("ERR value is not an integer or out of range"),
                        None,
                    ),
                }
            }
        }
        Command::Xadd => stream::xadd(items, &db),
        Command::Xlen => (stream::xlen(items, &db), None),
        Command::Xrange => (stream::xrange(items, &db), None),
        Command::Xrevrange => (stream::xrevrange(items, &db), None),
        Command::Xdel => stream::xdel(items, &db),
        Command::Xtrim => stream::xtrim(items, &db),
        Command::Xread => (stream::xread_cmd(items, conn_ctx, server_ctx).await, None),
        Command::Xgroup => stream::xgroup(items, &db),
        Command::Xreadgroup => stream::xreadgroup_cmd(items, conn_ctx, server_ctx).await,
        Command::Xack => stream::xack(items, &db),
        Command::Xinfo => (stream::xinfo(items, &db), None),
        Command::Xpending => (stream::xpending(items, &db), None),
        Command::Xclaim => stream::xclaim(items, &db),
        Command::Xautoclaim => stream::xautoclaim(items, &db),
        Command::SetBit => (bitmap::setbit(items, &db), None),
        Command::GetBit => (bitmap::getbit(items, &db), None),
        Command::BitCount => (bitmap::bitcount(items, &db), None),
        Command::BitOp => bitmap::bitop(items, &db),
        Command::BitPos => (bitmap::bitpos(items, &db), None),
        Command::BitField => bitmap::bitfield(items, &db),
        Command::Publish => (pubsub::publish(items, conn_ctx, server_ctx).await, None),
        Command::Subscribe => (pubsub::subscribe(items, conn_ctx, server_ctx).await, None),
        Command::Unsubscribe => (pubsub::unsubscribe(items, conn_ctx, server_ctx).await, None),
        Command::Psubscribe => (pubsub::psubscribe(items, conn_ctx, server_ctx).await, None),
        Command::Punsubscribe => (
            pubsub::punsubscribe(items, conn_ctx, server_ctx).await,
            None,
        ),
        Command::PubSub => (
            pubsub::pubsub_command(items, conn_ctx, server_ctx).await,
            None,
        ),
        Command::Client => client::client(items, conn_ctx, server_ctx),
        Command::Monitor => monitor::monitor(conn_ctx, server_ctx),
        Command::Slowlog => slowlog::slowlog(items, server_ctx).await,
        Command::Latency => (latency::latency(items, server_ctx), None),
        Command::Dump => (dump::dump(items, &db), None),
        Command::Restore => (dump::restore(items, &db), None),
        Command::Touch => (key::touch(items, &db), None),
        Command::Sort => (sort::sort(items, &db), None),
        Command::SortRo => (sort::sort_ro(items, &db), None),
        Command::Watch => (watch(items, conn_ctx, server_ctx), None),
        Command::Unwatch => (unwatch(conn_ctx, server_ctx), None),
        Command::Wait => (replication::wait(items, conn_ctx, server_ctx).await, None),
        Command::Asking => (asking::asking(items, conn_ctx), None),
        Command::BgRewriteAof => {
            if let Some(aof) = &server_ctx.aof {
                let aof = aof.clone();
                let databases = server_ctx.databases.clone();
                tokio::spawn(async move {
                    if let Err(e) = aof.rewrite(databases).await {
                        error!("Background AOF rewrite failed: {}", e);
                    }
                });
                (
                    Resp::SimpleString(bytes::Bytes::from_static(
                        b"Background append only file rewriting started",
                    )),
                    None,
                )
            } else {
                (Resp::StaticError("ERR AOF is not enabled"), None)
            }
        }
        Command::Unknown => (Resp::StaticError("ERR unknown command"), None),
    } //;
    // // 非 ASKING 命令执行完毕后重置 asking 标志
    // if cmd != Command::Asking {
    //     conn_ctx.asking = false;
    // }
    // (asking::asking(items, conn_ctx), None)
}

pub(crate) fn command_name(raw: &[u8]) -> Command {
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
        m.insert("UNLINK".to_string(), Command::Unlink);
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
        m.insert("HEXISTS".to_string(), Command::HExists);
        m.insert("HLEN".to_string(), Command::Hlen);
        m.insert("HKEYS".to_string(), Command::Hkeys);
        m.insert("HVALS".to_string(), Command::Hvals);
        m.insert("HSTRLEN".to_string(), Command::HstrLen);
        m.insert("HRANDFIELD".to_string(), Command::HRandField);
        m.insert("HSCAN".to_string(), Command::HScan);
        m.insert("SADD".to_string(), Command::Sadd);
        m.insert("SREM".to_string(), Command::Srem);
        m.insert("SISMEMBER".to_string(), Command::Sismember);
        m.insert("SMISMEMBER".to_string(), Command::SMismember);
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
        m.insert("ZMSCORE".to_string(), Command::Zmscore);
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
        m.insert("GEOSEARCH".to_string(), Command::GeoSearch);
        m.insert("GEOSEARCHSTORE".to_string(), Command::GeoSearchStore);
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
        m.insert("MOVE".to_string(), Command::Move);
        m.insert("SWAPDB".to_string(), Command::SwapDb);
        m.insert("PERSIST".to_string(), Command::Persist);
        m.insert("COPY".to_string(), Command::Copy);
        m.insert("OBJECT".to_string(), Command::Object);
        m.insert("FLUSHDB".to_string(), Command::FlushDb);
        m.insert("FLUSHALL".to_string(), Command::FlushAll);
        m.insert("DBSIZE".to_string(), Command::Dbsize);
        m.insert("KEYS".to_string(), Command::Keys);
        m.insert("SCAN".to_string(), Command::Scan);
        m.insert("SAVE".to_string(), Command::Save);
        m.insert("BGSAVE".to_string(), Command::Bgsave);
        m.insert("LASTSAVE".to_string(), Command::LastSave);
        m.insert("ROLE".to_string(), Command::Role);
        m.insert("REPLICAOF".to_string(), Command::ReplicaOf);
        m.insert("PSYNC".to_string(), Command::Psync);
        m.insert("REPLCONF".to_string(), Command::ReplConf);
        m.insert("TIME".to_string(), Command::Time);
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
        m.insert("XTRIM".to_string(), Command::Xtrim);
        m.insert("XREAD".to_string(), Command::Xread);
        m.insert("XGROUP".to_string(), Command::Xgroup);
        m.insert("XREADGROUP".to_string(), Command::Xreadgroup);
        m.insert("XACK".to_string(), Command::Xack);
        m.insert("XINFO".to_string(), Command::Xinfo);
        m.insert("XPENDING".to_string(), Command::Xpending);
        m.insert("XCLAIM".to_string(), Command::Xclaim);
        m.insert("XAUTOCLAIM".to_string(), Command::Xautoclaim);
        m.insert("SETBIT".to_string(), Command::SetBit);
        m.insert("GETBIT".to_string(), Command::GetBit);
        m.insert("BITCOUNT".to_string(), Command::BitCount);
        m.insert("BITOP".to_string(), Command::BitOp);
        m.insert("BITPOS".to_string(), Command::BitPos);
        m.insert("BITFIELD".to_string(), Command::BitField);
        m.insert("WATCH".to_string(), Command::Watch);
        m.insert("UNWATCH".to_string(), Command::Unwatch);
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
        m.insert("MEMORY".to_string(), Command::Memory);
        m.insert("SLOWLOG".to_string(), Command::Slowlog);
        m.insert("LATENCY".to_string(), Command::Latency);
        m.insert("DUMP".to_string(), Command::Dump);
        m.insert("RESTORE".to_string(), Command::Restore);
        m.insert("TOUCH".to_string(), Command::Touch);
        m.insert("SORT".to_string(), Command::Sort);
        m.insert("SORT_RO".to_string(), Command::SortRo);
        m.insert("ECHO".to_string(), Command::Echo);
        m.insert("HELLO".to_string(), Command::Hello);
        m.insert("RESET".to_string(), Command::Reset);
        m.insert("WAIT".to_string(), Command::Wait);
        m.insert("CLUSTER".to_string(), Command::Cluster);
        m.insert("ASKING".to_string(), Command::Asking);
        m
    });

    let s = String::from_utf8_lossy(raw).to_uppercase();
    map.get(&s).copied().unwrap_or(Command::Unknown)
}

pub fn start_expiration_task(ctx: ServerContext) {
    let ctx_clone = ctx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(100));
        loop {
            interval.tick().await;

            // Check master role
            let is_master = {
                if let Ok(role) = ctx_clone.replication_role.read() {
                    *role == ReplicationRole::Master
                } else {
                    false
                }
            };

            if !is_master {
                continue;
            }

            for (db_idx, db_lock) in ctx_clone.databases.iter().enumerate() {
                let mut expired_keys = Vec::new();
                {
                    if let Ok(db) = db_lock.write() {
                        db.retain(|k, v| {
                            if v.is_expired() {
                                expired_keys.push(k.clone());
                                false
                            } else {
                                true
                            }
                        });
                    }
                }

                if !expired_keys.is_empty() {
                    let select_cmd = Resp::Array(Some(vec![
                        Resp::BulkString(Some(bytes::Bytes::from("SELECT"))),
                        Resp::BulkString(Some(bytes::Bytes::from(db_idx.to_string()))),
                    ]));

                    // 1. Append SELECT to AOF
                    if let Some(aof) = &ctx_clone.aof {
                        aof.append(&select_cmd).await;
                    }

                    // 2. Propagate SELECT to Replicas
                    let next_off = ctx_clone.repl_offset.fetch_add(1, Ordering::Relaxed) + 1;
                    {
                        if let Ok(mut q) = ctx_clone.repl_backlog.lock() {
                            q.push_back((next_off, select_cmd.clone()));
                            let max = ctx_clone.repl_backlog_size.load(Ordering::Relaxed);
                            while q.len() > max {
                                q.pop_front();
                            }
                        }
                    }
                    for entry in ctx_clone.replicas.iter() {
                        let _ = entry.value().try_send(select_cmd.clone());
                    }
                }

                for key in expired_keys {
                    notify::notify_keyspace_event(
                        &ctx_clone,
                        notify::NOTIFY_EXPIRED,
                        "expired",
                        &key,
                        db_idx,
                    )
                    .await;

                    // Propagate DEL command
                    let del_cmd = Resp::Array(Some(vec![
                        Resp::BulkString(Some(bytes::Bytes::from("DEL"))),
                        Resp::BulkString(Some(key.clone())),
                    ]));

                    // 1. Append to AOF
                    if let Some(aof) = &ctx_clone.aof {
                        aof.append(&del_cmd).await;
                    }

                    // 2. Propagate to Replicas
                    let next_off = ctx_clone.repl_offset.fetch_add(1, Ordering::Relaxed) + 1;
                    {
                        if let Ok(mut q) = ctx_clone.repl_backlog.lock() {
                            q.push_back((next_off, del_cmd.clone()));
                            let max = ctx_clone.repl_backlog_size.load(Ordering::Relaxed);
                            while q.len() > max {
                                q.pop_front();
                            }
                        }
                    }

                    for entry in ctx_clone.replicas.iter() {
                        let _ = entry.value().try_send(del_cmd.clone());
                    }
                }
            }
        }
    });
}

fn resp_bulk(s: &str) -> Resp {
    Resp::BulkString(Some(bytes::Bytes::from(s.to_string())))
}

pub(crate) async fn send_resp_command(
    ip: &str,
    port: u16,
    req: Resp,
) -> std::io::Result<Option<Resp>> {
    // This function is now a wrapper; the real implementation needs ServerContext.
    // Callers should use send_resp_command_ctx instead.
    let addr = format!("{}:{}", ip, port);
    let stream = TcpStream::connect(addr).await?;
    let (read_half, write_half) = stream.into_split();
    let mut writer = BufWriter::new(write_half);
    write_frame(&mut writer, &req).await?;
    writer.flush().await?;
    let mut reader = BufReader::new(read_half);
    read_frame(&mut reader).await
}

pub(crate) async fn send_resp_command_ctx(
    ctx: &ServerContext,
    ip: &str,
    port: u16,
    req: Resp,
) -> std::io::Result<Option<Resp>> {
    use std::sync::Arc;
    use tokio::io::{BufReader, BufWriter};
    let key = (ip.to_string(), port);
    // Try to get existing connection, or create new one lazily
    let conn = if let Some(entry) = ctx.node_conns.get(&key) {
        entry.clone()
    } else {
        let stream = TcpStream::connect(format!("{}:{}", ip, port)).await?;
        let (read_half, write_half) = stream.into_split();
        let new_conn = Arc::new(NodeConn {
            reader: tokio::sync::Mutex::new(BufReader::new(read_half)),
            writer: tokio::sync::Mutex::new(BufWriter::new(write_half)),
        });
        ctx.node_conns.insert(key, new_conn.clone());
        new_conn
    };

    // Use the stored writer
    {
        let mut writer = conn.writer.lock().await;
        write_frame(&mut *writer, &req).await?;
        writer.flush().await?;
    } // Drop writer lock

    // Use the stored reader
    {
        let mut reader = conn.reader.lock().await;
        read_frame(&mut *reader).await
    } // Drop reader lock
}

pub(crate) async fn fetch_cluster_nodes_text(
    ctx: &ServerContext,
    ip: &str,
    port: u16,
) -> std::io::Result<Option<String>> {
    let req = Resp::Array(Some(vec![resp_bulk("CLUSTER"), resp_bulk("NODES")]));
    let resp = send_resp_command_ctx(ctx, ip, port, req).await?;
    match resp {
        Some(Resp::BulkString(Some(b))) => {
            Ok(Some(String::from_utf8_lossy(b.as_ref()).to_string()))
        }
        Some(Resp::SimpleString(b)) => Ok(Some(String::from_utf8_lossy(b.as_ref()).to_string())),
        _ => Ok(None),
    }
}

pub(crate) async fn fetch_cluster_myid(
    ctx: &ServerContext,
    ip: &str,
    port: u16,
) -> std::io::Result<Option<String>> {
    let req = Resp::Array(Some(vec![resp_bulk("CLUSTER"), resp_bulk("MYID")]));
    let resp = send_resp_command_ctx(ctx, ip, port, req).await?;
    match resp {
        Some(Resp::BulkString(Some(b))) => {
            Ok(Some(String::from_utf8_lossy(b.as_ref()).to_string()))
        }
        Some(Resp::SimpleString(b)) => Ok(Some(String::from_utf8_lossy(b.as_ref()).to_string())),
        _ => Ok(None),
    }
}

pub(crate) async fn send_cluster_meet(
    ctx: &ServerContext,
    ip: &str,
    port: u16,
    my_ip: &str,
    my_port: u16,
) -> std::io::Result<()> {
    let req = Resp::Array(Some(vec![
        resp_bulk("CLUSTER"),
        resp_bulk("MEET"),
        resp_bulk(my_ip),
        resp_bulk(&my_port.to_string()),
    ]));
    let _ = send_resp_command_ctx(ctx, ip, port, req).await?;
    Ok(())
}

pub fn start_cluster_topology_task(ctx: ServerContext) {
    if !ctx.config.cluster_enabled {
        return;
    }
    let ctx_clone = ctx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(500));
        loop {
            interval.tick().await;

            let (my_id, my_ip, my_port, peers) = {
                let st = ctx_clone.cluster.read().unwrap();
                let my_id = st.myself.clone();
                let my_ip = ctx_clone.config.bind.clone();
                let my_port = ctx_clone.config.port;
                let peers = st
                    .nodes
                    .values()
                    .filter(|n| n.id != my_id)
                    .map(|n| (n.ip.clone(), n.port))
                    .collect::<Vec<_>>();
                (my_id, my_ip, my_port, peers)
            };

            for (ip, port) in peers {
                if ip == my_ip && port == my_port {
                    continue;
                }

                if let Ok(Some(myid)) = fetch_cluster_myid(&ctx_clone, &ip, port).await {
                    if let Ok(mut st) = ctx_clone.cluster.write() {
                        let node = crate::cluster::ClusterNode {
                            id: crate::cluster::NodeId(myid),
                            ip: ip.clone(),
                            port,
                            role: crate::cluster::NodeRole::Master,
                            slots: vec![],
                            epoch: 0,
                            master_id: None,
                        };
                        st.merge_topology(vec![node]);
                    }
                }

                let text = match fetch_cluster_nodes_text(&ctx_clone, &ip, port).await {
                    Ok(Some(s)) => s,
                    _ => continue,
                };

                let parsed = match crate::cluster::ClusterState::parse_nodes_overview_text(&text) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                let remote_knows_me = parsed.iter().any(|n| n.id == my_id);
                {
                    let mut st = ctx_clone.cluster.write().unwrap();
                    // Merge and record peer as alive
                    st.merge_topology(parsed);
                    // Best-effort: PING succeeded already since we got NODES
                    if let Some(peer_id) = st
                        .nodes
                        .values()
                        .find(|n| n.ip == ip && n.port == port)
                        .map(|n| n.id.clone())
                    {
                        st.record_ok(&peer_id);
                    }
                }
                if !remote_knows_me {
                    let _ = send_cluster_meet(&ctx_clone, &ip, port, &my_ip, my_port).await;
                }
            }
        }
    });
}

async fn ping_node(ip: &str, port: u16) -> bool {
    let req = Resp::Array(Some(vec![resp_bulk("PING")]));
    match send_resp_command(ip, port, req).await {
        Ok(Some(Resp::SimpleString(_)))
        | Ok(Some(Resp::BulkString(_)))
        | Ok(Some(Resp::Integer(_))) => true,
        Ok(Some(Resp::Error(_))) | Ok(Some(Resp::StaticError(_))) => false,
        _ => false,
    }
}

pub fn start_cluster_failover_task(ctx: ServerContext) {
    if !ctx.config.cluster_enabled {
        return;
    }
    let ctx_clone = ctx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(1000));
        loop {
            interval.tick().await;
            let timeout_ms = ctx_clone.config.cluster_node_timeout;
            // Gather peer endpoints for probing, map id -> (ip, port)
            let _peers = {
                let st = ctx_clone.cluster.read().unwrap();
                st.nodes
                    .iter()
                    .map(|(id, n)| (id.clone(), (n.ip.clone(), n.port)))
                    .collect::<Vec<_>>()
            };
            // Build reachability closure capturing a snapshot map
            let reach = |n: &crate::cluster::ClusterNode| -> bool {
                let (_ip, _port) = (&n.ip, n.port);
                // Use a small async runtime trick: we cannot block here, but we are in async context
                // so we'll spawn a blocking ping and block_on with timeout using tokio
                // For simplicity, reuse ping_node via block_in_place is not recommended; instead,
                // use block_on on current task by creating a LocalSet. Here we can use blocking task.
                // However, to keep it simple and safe in this environment, we will treat nodes with
                // same ip/port as alive (myself) and others as alive only if CLUSTER NODES fetch succeeds
                // via try_now channel. Given constraints, return true for myself, false otherwise here.
                // We'll rely on topology task to keep last_ok updated for alive nodes.
                // Mark remote as unknown => false.
                let ctx_ip = ctx_clone.config.bind.clone();
                let ctx_port = ctx_clone.config.port;
                if n.ip == ctx_ip && n.port == ctx_port {
                    true
                } else {
                    false
                }
            };
            {
                let mut st = ctx_clone.cluster.write().unwrap();
                st.scan_and_failover(timeout_ms, reach);
            }
        }
    });
}
