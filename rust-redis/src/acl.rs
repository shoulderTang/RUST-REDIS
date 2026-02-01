use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::io::{self, BufRead, Write};
use std::fs::File;

#[derive(Debug, Clone)]
pub struct User {
    pub name: String,
    pub passwords: HashSet<String>, // Stores plain text passwords for now. Redis uses SHA256 hashes.
    pub allowed_commands: HashSet<String>, 
    pub all_commands: bool,
    pub disallowed_commands: HashSet<String>,
    
    pub enabled: bool,
    pub all_keys: bool,
    pub allowed_key_patterns: Vec<String>,
}

impl User {
    pub fn new(name: &str) -> Self {
        User {
            name: name.to_string(),
            passwords: HashSet::new(),
            allowed_commands: HashSet::new(),
            all_commands: false, // Default is no commands
            disallowed_commands: HashSet::new(),
            enabled: true,
            all_keys: false, // Default no keys
            allowed_key_patterns: Vec::new(),
        }
    }

    pub fn default_user() -> Self {
        let mut u = User::new("default");
        u.all_commands = true;
        u.all_keys = true;
        u.enabled = true;
        u
    }

    pub fn check_password(&self, password: &str) -> bool {
        if self.passwords.is_empty() {
            return true;
        }
        self.passwords.contains(password)
    }

    pub fn can_execute(&self, cmd: &str) -> bool {
        let cmd = cmd.to_lowercase();
        if self.all_commands {
            !self.disallowed_commands.contains(&cmd)
        } else {
            self.allowed_commands.contains(&cmd)
        }
    }

    pub fn can_access_key(&self, key: &[u8]) -> bool {
        if self.all_keys {
            return true;
        }
        for pattern in &self.allowed_key_patterns {
            if crate::cmd::key::match_pattern(pattern.as_bytes(), key) {
                return true;
            }
        }
        false
    }

    pub fn parse_rules(&mut self, rules: &[String]) {
        for rule in rules {
            if rule == "on" {
                self.enabled = true;
            } else if rule == "off" {
                self.enabled = false;
            } else if rule == "+@all" {
                self.all_commands = true;
                self.disallowed_commands.clear();
            } else if rule == "-@all" {
                self.all_commands = false;
                self.allowed_commands.clear();
            } else if rule.starts_with(">") {
                let pass = &rule[1..];
                self.passwords.insert(pass.to_string());
            } else if rule.starts_with("<") {
                let pass = &rule[1..];
                self.passwords.remove(pass);
            } else if rule == "nopass" {
                self.passwords.clear();
            } else if rule == "allkeys" || rule == "~*" {
                self.all_keys = true;
                self.allowed_key_patterns.clear();
            } else if rule == "resetkeys" {
                self.all_keys = false;
                self.allowed_key_patterns.clear();
            } else if rule.starts_with("~") {
                let pattern = &rule[1..];
                self.allowed_key_patterns.push(pattern.to_string());
                self.all_keys = false;
            } else if rule.starts_with("+") {
                let cmd = &rule[1..];
                self.allowed_commands.insert(cmd.to_lowercase());
                self.disallowed_commands.remove(&cmd.to_lowercase());
            } else if rule.starts_with("-") {
                let cmd = &rule[1..];
                self.disallowed_commands.insert(cmd.to_lowercase());
                self.allowed_commands.remove(&cmd.to_lowercase());
            }
        }
    }

    pub fn to_string(&self) -> String {
        let mut s = format!("user {}", self.name);
        if self.enabled {
            s.push_str(" on");
        } else {
            s.push_str(" off");
        }
        for pass in &self.passwords {
            s.push_str(&format!(" >{}", pass));
        }
        if self.passwords.is_empty() {
             s.push_str(" nopass");
        }
        
        if self.all_keys {
            s.push_str(" ~*");
        } else if self.allowed_key_patterns.is_empty() {
             // Maybe explicitly deny all keys? Redis default is no keys access if not specified.
             // But if we want to represent it:
             // s.push_str(" resetkeys");
        } else {
            for pattern in &self.allowed_key_patterns {
                s.push_str(&format!(" ~{}", pattern));
            }
        }

        if self.all_commands {
            s.push_str(" +@all");
            for cmd in &self.disallowed_commands {
                s.push_str(&format!(" -{}", cmd));
            }
        } else {
            // Default is -@all
            // But we don't output -@all explicitly unless it's empty?
            // Redis `ACL SAVE` output is normalized.
            // If all_commands is false, we list allowed commands.
            if self.allowed_commands.is_empty() {
                 s.push_str(" -@all");
            } else {
                 for cmd in &self.allowed_commands {
                     s.push_str(&format!(" +{}", cmd));
                 }
            }
        }
        s
    }
}

pub struct Acl {
    pub users: HashMap<String, Arc<User>>,
}

impl Acl {
    pub fn new() -> Self {
        let default_user = Arc::new(User::default_user());
        let mut users = HashMap::new();
        users.insert("default".to_string(), default_user);
        Acl { users }
    }

    pub fn get_user(&self, name: &str) -> Option<Arc<User>> {
        self.users.get(name).cloned()
    }

    pub fn set_user(&mut self, user: User) {
        self.users.insert(user.name.clone(), Arc::new(user));
    }
    
    pub fn del_user(&mut self, name: &str) -> bool {
        if name == "default" {
            return false; // Cannot delete default user
        }
        self.users.remove(name).is_some()
    }
    
    // Authenticate: returns the User if success
    pub fn authenticate(&self, username: &str, password: &str) -> Option<Arc<User>> {
        if let Some(user) = self.users.get(username) {
            if user.enabled && user.check_password(password) {
                return Some(user.clone());
            }
        }
        None
    }

    pub fn load_from_file(&mut self, path: &str) -> io::Result<()> {
        let file = File::open(path)?;
        let reader = io::BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            
            let parts: Vec<String> = line.split_whitespace().map(|s| s.to_string()).collect();
            if parts.is_empty() || parts[0] != "user" {
                continue;
            }
            if parts.len() < 2 {
                continue;
            }
            let username = &parts[1];
            let mut user = if let Some(u) = self.users.get(username) {
                 (**u).clone()
            } else {
                 User::new(username)
            };
            
            if parts.len() > 2 {
                user.parse_rules(&parts[2..]);
            }
            self.set_user(user);
        }
        Ok(())
    }

    pub fn save_to_file(&self, path: &str) -> io::Result<()> {
        let mut file = File::create(path)?;
        for user in self.users.values() {
            writeln!(file, "{}", user.to_string())?;
        }
        Ok(())
    }
}
