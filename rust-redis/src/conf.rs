use crate::aof::AppendFsync;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use tracing::{info, warn};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum EvictionPolicy {
    NoEviction,
    AllKeysLru,
    VolatileLru,
    AllKeysLfu,
    VolatileLfu,
    AllKeysRandom,
    VolatileRandom,
    VolatileTtl,
}

impl EvictionPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            EvictionPolicy::NoEviction => "noeviction",
            EvictionPolicy::AllKeysLru => "allkeys-lru",
            EvictionPolicy::VolatileLru => "volatile-lru",
            EvictionPolicy::AllKeysLfu => "allkeys-lfu",
            EvictionPolicy::VolatileLfu => "volatile-lfu",
            EvictionPolicy::AllKeysRandom => "allkeys-random",
            EvictionPolicy::VolatileRandom => "volatile-random",
            EvictionPolicy::VolatileTtl => "volatile-ttl",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "noeviction" => Some(EvictionPolicy::NoEviction),
            "allkeys-lru" => Some(EvictionPolicy::AllKeysLru),
            "volatile-lru" => Some(EvictionPolicy::VolatileLru),
            "allkeys-lfu" => Some(EvictionPolicy::AllKeysLfu),
            "volatile-lfu" => Some(EvictionPolicy::VolatileLfu),
            "allkeys-random" => Some(EvictionPolicy::AllKeysRandom),
            "volatile-random" => Some(EvictionPolicy::VolatileRandom),
            "volatile-ttl" => Some(EvictionPolicy::VolatileTtl),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub bind: String,
    pub port: u16,
    pub databases: usize,
    pub logfile: Option<String>,
    pub appendonly: bool,
    pub appendfilename: String,
    pub appendfsync: AppendFsync,
    pub dbfilename: String,
    pub dir: String,
    pub requirepass: Option<String>,
    pub aclfile: Option<String>,
    pub save_params: Vec<(u64, u64)>,
    pub config_file: Option<String>,
    pub maxclients: u64,
    pub slowlog_log_slower_than: i64,
    pub slowlog_max_len: u64,
    pub maxmemory: u64,
    pub maxmemory_policy: EvictionPolicy,
    pub maxmemory_samples: usize,
    pub notify_keyspace_events: String,
    pub rdbcompression: bool,
    pub rdbchecksum: bool,
    pub stop_writes_on_bgsave_error: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".to_string(),
            port: 6380,
            databases: 16,
            logfile: None,
            appendonly: false,
            appendfilename: "appendonly.aof".to_string(),
            appendfsync: AppendFsync::EverySec,
            dbfilename: "dump.rdb".to_string(),
            dir: ".".to_string(),
            requirepass: None,
            aclfile: None,
            save_params: vec![(3600, 1), (300, 100), (60, 10000)],
            config_file: None,
            maxclients: 10000,
            slowlog_log_slower_than: 10000,
            slowlog_max_len: 128,
            maxmemory: 0,
            maxmemory_policy: EvictionPolicy::NoEviction,
            maxmemory_samples: 5,
            notify_keyspace_events: String::new(),
            rdbcompression: true,
            rdbchecksum: true,
            stop_writes_on_bgsave_error: true,
        }
    }
}

impl Config {
    pub fn address(&self) -> String {
        format!("{}:{}", self.bind, self.port)
    }
}

fn parse_memory(s: &str) -> Option<u64> {
    let s = s.to_lowercase();
    let (num, unit) = if s.ends_with("gb") {
        (s.trim_end_matches("gb"), 1024 * 1024 * 1024)
    } else if s.ends_with("mb") {
        (s.trim_end_matches("mb"), 1024 * 1024)
    } else if s.ends_with("kb") {
        (s.trim_end_matches("kb"), 1024)
    } else if s.ends_with("b") {
        (s.trim_end_matches("b"), 1)
    } else {
        (s.as_str(), 1)
    };
    
    num.parse::<u64>().ok().map(|n| n * unit)
}

pub fn load_config(path: Option<&str>) -> io::Result<Config> {
    if path.is_none() {
        info!("no config path provided, using default config");
        return Ok(Config::default());
    }
    let p = path.unwrap();
    let file = File::open(p)?;
    info!("loading config from {}", p);
    let reader = BufReader::new(file);
    let mut cfg = Config::default();
    if let Ok(abs_path) = std::fs::canonicalize(p) {
        cfg.config_file = Some(abs_path.to_string_lossy().into_owned());
    } else {
        cfg.config_file = Some(p.to_string());
    }
    let mut save_seen = false;
    for line in reader.lines() {
        let mut l = line?;
        if let Some(idx) = l.find('#') {
            l.truncate(idx);
        }
        let l = l.trim();
        if l.is_empty() {
            continue;
        }
        let parts: Vec<&str> = l.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }
        match parts[0].to_lowercase().as_str() {
            "bind" if parts.len() >= 2 => {
                cfg.bind = parts[1].to_string();
            }
            "port" if parts.len() >= 2 => {
                if let Ok(pn) = parts[1].parse::<u16>() {
                    cfg.port = pn;
                } else {
                    warn!(
                        "invalid port value '{}', keep previous {}",
                        parts[1], cfg.port
                    );
                }
            }
            "notify-keyspace-events" if parts.len() >= 2 => {
                cfg.notify_keyspace_events = parts[1].trim_matches('"').to_string();
            }
            "databases" if parts.len() >= 2 => {
                if let Ok(db) = parts[1].parse::<usize>() {
                    cfg.databases = db;
                } else {
                    warn!(
                        "invalid databases value '{}', keep previous {}",
                        parts[1], cfg.databases
                    );
                }
            }
            "maxclients" if parts.len() >= 2 => {
                if let Ok(mc) = parts[1].parse::<u64>() {
                    cfg.maxclients = mc;
                } else {
                    warn!(
                        "invalid maxclients value '{}', keep previous {}",
                        parts[1], cfg.maxclients
                    );
                }
            }
            "slowlog-log-slower-than" if parts.len() >= 2 => {
                if let Ok(sl) = parts[1].parse::<i64>() {
                    cfg.slowlog_log_slower_than = sl;
                } else {
                    warn!(
                        "invalid slowlog-log-slower-than value '{}', keep previous {}",
                        parts[1], cfg.slowlog_log_slower_than
                    );
                }
            }
            "slowlog-max-len" if parts.len() >= 2 => {
                if let Ok(ml) = parts[1].parse::<u64>() {
                    cfg.slowlog_max_len = ml;
                } else {
                    warn!(
                        "invalid slowlog-max-len value '{}', keep previous {}",
                        parts[1], cfg.slowlog_max_len
                    );
                }
            }
            "maxmemory" if parts.len() >= 2 => {
                if let Some(mm) = parse_memory(parts[1]) {
                    cfg.maxmemory = mm;
                } else {
                    warn!(
                        "invalid maxmemory value '{}', keep previous {}",
                        parts[1], cfg.maxmemory
                    );
                }
            }
            "maxmemory-policy" if parts.len() >= 2 => {
                cfg.maxmemory_policy = match parts[1].to_lowercase().as_str() {
                    "noeviction" => EvictionPolicy::NoEviction,
                    "allkeys-lru" => EvictionPolicy::AllKeysLru,
                    "volatile-lru" => EvictionPolicy::VolatileLru,
                    "allkeys-lfu" => EvictionPolicy::AllKeysLfu,
                    "volatile-lfu" => EvictionPolicy::VolatileLfu,
                    "allkeys-random" => EvictionPolicy::AllKeysRandom,
                    "volatile-random" => EvictionPolicy::VolatileRandom,
                    "volatile-ttl" => EvictionPolicy::VolatileTtl,
                    _ => {
                        warn!("invalid maxmemory-policy '{}', using default", parts[1]);
                        EvictionPolicy::NoEviction
                    }
                };
            }
            "maxmemory-samples" if parts.len() >= 2 => {
                if let Ok(ms) = parts[1].parse::<usize>() {
                    cfg.maxmemory_samples = ms;
                } else {
                    warn!(
                        "invalid maxmemory-samples value '{}', keep previous {}",
                        parts[1], cfg.maxmemory_samples
                    );
                }
            }
            "logfile" if parts.len() >= 2 => {
                let logfile = parts[1].trim_matches('"').to_string();
                if !logfile.is_empty() {
                    cfg.logfile = Some(logfile);
                }
            }
            "appendonly" if parts.len() >= 2 => {
                cfg.appendonly = parts[1].eq_ignore_ascii_case("yes");
            }
            "appendfilename" if parts.len() >= 2 => {
                let filename = parts[1].trim_matches('"').to_string();
                if !filename.is_empty() {
                    cfg.appendfilename = filename;
                }
            }
            "requirepass" if parts.len() >= 2 => {
                let pass = parts[1].trim_matches('"').to_string();
                if !pass.is_empty() {
                    cfg.requirepass = Some(pass);
                }
            }
            "aclfile" if parts.len() >= 2 => {
                let file = parts[1].trim_matches('"').to_string();
                if !file.is_empty() {
                    cfg.aclfile = Some(file);
                }
            }
            "appendfsync" if parts.len() >= 2 => match parts[1].to_lowercase().as_str() {
                "always" => cfg.appendfsync = AppendFsync::Always,
                "everysec" => cfg.appendfsync = AppendFsync::EverySec,
                "no" => cfg.appendfsync = AppendFsync::No,
                _ => warn!("invalid appendfsync value '{}', using default", parts[1]),
            },
            "dbfilename" if parts.len() >= 2 => {
                cfg.dbfilename = parts[1].trim_matches('"').to_string();
            }
            "rdbcompression" if parts.len() >= 2 => {
                cfg.rdbcompression = parts[1].eq_ignore_ascii_case("yes");
            }
            "rdbchecksum" if parts.len() >= 2 => {
                cfg.rdbchecksum = parts[1].eq_ignore_ascii_case("yes");
            }
            "stop-writes-on-bgsave-error" if parts.len() >= 2 => {
                cfg.stop_writes_on_bgsave_error = parts[1].eq_ignore_ascii_case("yes");
            }
            "dir" if parts.len() >= 2 => {
                cfg.dir = parts[1].trim_matches('"').to_string();
            }
            "notify-keyspace-events" if parts.len() >= 2 => {
                cfg.notify_keyspace_events = parts[1].to_string();
            }
            "save" => {
                if !save_seen {
                    cfg.save_params.clear();
                    save_seen = true;
                }
                if parts.len() == 2 && parts[1] == "\"\"" {
                    continue;
                }
                if parts.len() >= 3 {
                    if let (Ok(sec), Ok(changes)) =
                        (parts[1].parse::<u64>(), parts[2].parse::<u64>())
                    {
                        cfg.save_params.push((sec, changes));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(cfg)
}
