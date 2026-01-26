use crate::aof::Aof;
use crate::conf::Config;
use crate::db::Db;
use crate::resp::Resp;
use bytes::Bytes;
use dashmap::DashMap;
use mlua::prelude::*;
use sha1::{Digest, Sha1};
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as TokioMutex;

pub struct ScriptManager {
    pub cache: DashMap<String, String>,
    pub lua: Mutex<Lua>,
}

pub fn create_script_manager() -> Arc<ScriptManager> {
    Arc::new(ScriptManager {
        cache: DashMap::new(),
        lua: Mutex::new(Lua::new()),
    })
}

pub fn calc_sha1(script: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(script.as_bytes());
    hex::encode(hasher.finalize())
}

fn resp_to_lua<'lua>(lua: &'lua Lua, resp: &Resp) -> LuaResult<LuaValue<'lua>> {
    match resp {
        Resp::SimpleString(s) => {
            let table = lua.create_table()?;
            let s_str = std::str::from_utf8(s).unwrap_or("");
            table.set("ok", s_str)?;
            Ok(LuaValue::Table(table))
        }
        Resp::Error(e) => {
            let table = lua.create_table()?;
            table.set("err", e.as_str())?;
            Ok(LuaValue::Table(table))
        }
        Resp::Integer(i) => Ok(LuaValue::Integer(*i)),
        Resp::BulkString(Some(b)) => Ok(LuaValue::String(lua.create_string(b)?)),
        Resp::BulkString(None) => Ok(LuaValue::Boolean(false)),
        Resp::Array(Some(arr)) => {
            let table = lua.create_table()?;
            for (i, item) in arr.iter().enumerate() {
                table.set(i + 1, resp_to_lua(lua, item)?)?;
            }
            Ok(LuaValue::Table(table))
        }
        Resp::Array(None) => Ok(LuaValue::Boolean(false)),
    }
}

fn lua_to_resp(value: LuaValue) -> Resp {
    match value {
        LuaValue::String(s) => Resp::BulkString(Some(Bytes::from(s.as_bytes().to_vec()))),
        LuaValue::Integer(i) => Resp::Integer(i),
        LuaValue::Number(n) => Resp::Integer(n as i64),
        LuaValue::Boolean(b) => {
            if b {
                Resp::Integer(1)
            } else {
                Resp::BulkString(None)
            }
        }
        LuaValue::Table(t) => {
            if let Ok(err) = t.get::<_, String>("err") {
                return Resp::Error(err);
            }
            if let Ok(ok) = t.get::<_, String>("ok") {
                return Resp::SimpleString(Bytes::from(ok));
            }

            let len = t.len().unwrap_or(0) as usize;
            let mut items = Vec::with_capacity(len);
            for i in 1..=len {
                if let Ok(val) = t.get::<_, LuaValue>(i) {
                    items.push(lua_to_resp(val));
                } else {
                    items.push(Resp::BulkString(None));
                }
            }
            Resp::Array(Some(items))
        }
        LuaValue::Nil => Resp::BulkString(None),
        _ => Resp::BulkString(None),
    }
}

fn redis_call_handler<'lua>(
    lua: &'lua Lua,
    args: LuaMultiValue<'lua>,
    raise_error: bool,
    db: &Db,
    aof: &Option<Arc<TokioMutex<Aof>>>,
    config: &Config,
    script_manager: &Arc<ScriptManager>,
) -> LuaResult<LuaValue<'lua>> {
    let mut resp_args = Vec::new();
    for arg in args {
        match arg {
            LuaValue::String(s) => {
                resp_args.push(Resp::BulkString(Some(Bytes::from(s.as_bytes().to_vec()))))
            }
            LuaValue::Integer(i) => {
                resp_args.push(Resp::BulkString(Some(Bytes::from(i.to_string()))))
            }
            LuaValue::Number(n) => {
                resp_args.push(Resp::BulkString(Some(Bytes::from(n.to_string()))))
            }
            _ => {
                return Err(LuaError::external(
                    "Lua redis() command arguments must be strings or integers",
                ));
            }
        }
    }

    let frame = Resp::Array(Some(resp_args));
    let (res, _) = super::process_frame(frame, db, aof, config, script_manager);

    if raise_error {
        if let Resp::Error(msg) = &res {
            return Err(LuaError::external(msg.clone()));
        }
    }

    resp_to_lua(lua, &res)
}

fn eval_script(
    script: &str,
    items: &[Resp],
    keys_start: usize,
    keys_end: usize,
    args_start: usize,
    db: &Db,
    aof: &Option<Arc<TokioMutex<Aof>>>,
    config: &Config,
    script_manager: &Arc<ScriptManager>,
) -> Resp {
    let keys: Vec<String> = items[keys_start..keys_end]
        .iter()
        .map(|item| match item {
            Resp::BulkString(Some(b)) => std::str::from_utf8(b).unwrap_or("").to_string(),
            _ => "".to_string(),
        })
        .collect();

    let args: Vec<String> = items[args_start..]
        .iter()
        .map(|item| match item {
            Resp::BulkString(Some(b)) => std::str::from_utf8(b).unwrap_or("").to_string(),
            _ => "".to_string(),
        })
        .collect();

    let lua_guard = script_manager.lua.lock().unwrap();
    let lua = &*lua_guard;

    let globals = lua.globals();
    let lua_keys = lua.create_table().unwrap();
    for (i, k) in keys.iter().enumerate() {
        lua_keys.set(i + 1, k.as_str()).unwrap();
    }
    globals.set("KEYS", lua_keys).unwrap();

    let lua_args = lua.create_table().unwrap();
    for (i, a) in args.iter().enumerate() {
        lua_args.set(i + 1, a.as_str()).unwrap();
    }
    globals.set("ARGV", lua_args).unwrap();

    let db_clone = db.clone();
    let aof_clone = aof.clone();
    let config_clone = config.clone();
    let script_manager_clone = script_manager.clone();
    let redis_call = lua
        .create_function(move |lua, args| {
            redis_call_handler(
                lua,
                args,
                true,
                &db_clone,
                &aof_clone,
                &config_clone,
                &script_manager_clone,
            )
        })
        .unwrap();

    let db_clone = db.clone();
    let aof_clone = aof.clone();
    let config_clone = config.clone();
    let script_manager_clone = script_manager.clone();
    let redis_pcall = lua
        .create_function(move |lua, args| {
            redis_call_handler(
                lua,
                args,
                false,
                &db_clone,
                &aof_clone,
                &config_clone,
                &script_manager_clone,
            )
        })
        .unwrap();

    let redis_table = lua.create_table().unwrap();
    redis_table.set("call", redis_call).unwrap();
    redis_table.set("pcall", redis_pcall).unwrap();

    globals.set("redis", redis_table).unwrap();

    match lua.load(script).eval::<LuaValue>() {
        Ok(val) => lua_to_resp(val),
        Err(e) => Resp::Error(format!("ERR error running script: {}", e)),
    }
}

pub fn eval(
    items: &[Resp],
    db: &Db,
    aof: &Option<Arc<TokioMutex<Aof>>>,
    config: &Config,
    script_manager: &Arc<ScriptManager>,
) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'eval' command".to_string());
    }

    let script = match &items[1] {
        Resp::BulkString(Some(b)) => match std::str::from_utf8(b) {
            Ok(s) => s,
            Err(_) => return Resp::Error("ERR script is not valid utf8".to_string()),
        },
        _ => return Resp::Error("ERR script must be a string".to_string()),
    };

    let numkeys = match &items[2] {
        Resp::BulkString(Some(b)) => match std::str::from_utf8(b).unwrap_or("0").parse::<usize>() {
            Ok(n) => n,
            Err(_) => {
                return Resp::Error("ERR value is not an integer or out of range".to_string());
            }
        },
        _ => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    if items.len() < 3 + numkeys {
        return Resp::Error("ERR wrong number of arguments for 'eval' command".to_string());
    }

    let keys_start = 3;
    let keys_end = 3 + numkeys;
    let args_start = keys_end;

    eval_script(
        script,
        items,
        keys_start,
        keys_end,
        args_start,
        db,
        aof,
        config,
        script_manager,
    )
}

pub fn evalsha(
    items: &[Resp],
    db: &Db,
    aof: &Option<Arc<TokioMutex<Aof>>>,
    config: &Config,
    script_manager: &Arc<ScriptManager>,
) -> Resp {
    if items.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'evalsha' command".to_string());
    }

    let sha = match &items[1] {
        Resp::BulkString(Some(b)) => match std::str::from_utf8(b) {
            Ok(s) => s,
            Err(_) => return Resp::Error("ERR sha1 is not valid utf8".to_string()),
        },
        _ => return Resp::Error("ERR sha1 must be a string".to_string()),
    };

    let script = if let Some(s) = script_manager.cache.get(sha) {
        s.clone()
    } else {
        return Resp::Error("NOSCRIPT No matching script. Please use EVAL.".to_string());
    };

    let numkeys = match &items[2] {
        Resp::BulkString(Some(b)) => match std::str::from_utf8(b).unwrap_or("0").parse::<usize>() {
            Ok(n) => n,
            Err(_) => {
                return Resp::Error("ERR value is not an integer or out of range".to_string());
            }
        },
        _ => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    if items.len() < 3 + numkeys {
        return Resp::Error("ERR wrong number of arguments for 'evalsha' command".to_string());
    }

    let keys_start = 3;
    let keys_end = 3 + numkeys;
    let args_start = keys_end;

    eval_script(
        &script,
        items,
        keys_start,
        keys_end,
        args_start,
        db,
        aof,
        config,
        script_manager,
    )
}

pub fn script(items: &[Resp], script_manager: &Arc<ScriptManager>) -> Resp {
    if items.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'script' command".to_string());
    }

    let subcommand = match &items[1] {
        Resp::BulkString(Some(b)) => match std::str::from_utf8(b) {
            Ok(s) => s.to_uppercase(),
            Err(_) => return Resp::Error("ERR subcommand is not valid utf8".to_string()),
        },
        _ => return Resp::Error("ERR subcommand must be a string".to_string()),
    };

    match subcommand.as_str() {
        "LOAD" => {
            if items.len() != 3 {
                return Resp::Error(
                    "ERR wrong number of arguments for 'script|load' command".to_string(),
                );
            }
            let script_content = match &items[2] {
                Resp::BulkString(Some(b)) => match std::str::from_utf8(b) {
                    Ok(s) => s,
                    Err(_) => return Resp::Error("ERR script is not valid utf8".to_string()),
                },
                _ => return Resp::Error("ERR script must be a string".to_string()),
            };

            let sha = calc_sha1(script_content);
            script_manager
                .cache
                .insert(sha.clone(), script_content.to_string());
            Resp::BulkString(Some(Bytes::from(sha)))
        }
        "EXISTS" => {
            if items.len() < 3 {
                return Resp::Error(
                    "ERR wrong number of arguments for 'script|exists' command".to_string(),
                );
            }
            let mut results = Vec::new();
            for item in &items[2..] {
                let sha = match item {
                    Resp::BulkString(Some(b)) => std::str::from_utf8(b).unwrap_or(""),
                    _ => "",
                };
                if script_manager.cache.contains_key(sha) {
                    results.push(Resp::Integer(1));
                } else {
                    results.push(Resp::Integer(0));
                }
            }
            Resp::Array(Some(results))
        }
        "FLUSH" => {
            script_manager.cache.clear();
            Resp::SimpleString(Bytes::from("OK"))
        }
        _ => Resp::Error("ERR unknown subcommand".to_string()),
    }
}
