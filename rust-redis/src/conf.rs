use std::fs::File;
use std::io::{self, BufRead, BufReader};
use tracing::{info, warn};
use crate::aof::AppendFsync;

pub struct Config {
    pub bind: String,
    pub port: u16,
    pub databases: usize,
    pub logfile: Option<String>,
    pub appendonly: bool,
    pub appendfilename: String,
    pub appendfsync: AppendFsync,
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
        }
    }
}

impl Config {
    pub fn address(&self) -> String {
        format!("{}:{}", self.bind, self.port)
    }
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
                    warn!("invalid port value '{}', keep previous {}", parts[1], cfg.port);
                }
            }
            "databases" if parts.len() >= 2 => {
                if let Ok(db) = parts[1].parse::<usize>() {
                    cfg.databases = db;
                } else {
                    warn!("invalid databases value '{}', keep previous {}", parts[1], cfg.databases);
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
                cfg.appendfilename = parts[1].trim_matches('"').to_string();
            }
            "appendfsync" if parts.len() >= 2 => {
                match parts[1].to_lowercase().as_str() {
                    "always" => cfg.appendfsync = AppendFsync::Always,
                    "everysec" => cfg.appendfsync = AppendFsync::EverySec,
                    "no" => cfg.appendfsync = AppendFsync::No,
                    _ => warn!("invalid appendfsync value '{}', using default", parts[1]),
                }
            }
            _ => {}
        }
    }
    Ok(cfg)
}
