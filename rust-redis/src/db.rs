use std::hash::{Hash, Hasher, BuildHasher};
use std::collections::hash_map::RandomState;
use std::mem;

pub struct RehashMap<K, V> {
    old: Vec<Vec<(K, V)>>,
    new: Option<Vec<Vec<(K, V)>>>,
    rehash_idx: usize,
    len: usize,
    hasher: RandomState,
    // use stable hasher to keep bucket index consistent across operations
}

impl<K, V> RehashMap<K, V>
where
    K: Hash + Eq,
{
    pub fn new() -> Self {
        let cap = 64;
        let mut old = Vec::with_capacity(cap);
        old.resize_with(cap, Vec::new);
        Self {
            old,
            new: None,
            rehash_idx: 0,
            len: 0,
            hasher: RandomState::new(),
        }
    }

    fn effective_capacity(&self) -> usize {
        match &self.new {
            Some(n) => n.len(),
            None => self.old.len(),
        }
    }

    fn need_grow(&self) -> bool {
        let cap = self.effective_capacity();
        self.len * 4 >= cap * 3
    }

    fn index_for_with_hasher<H: BuildHasher>(hasher: &H, k: &K, cap: usize) -> usize {
        let mut h = hasher.build_hasher();
        k.hash(&mut h);
        (h.finish() as usize) % cap
    }

    fn start_rehash_if_needed(&mut self) {
        if self.new.is_none() && self.need_grow() {
            let new_cap = self.old.len() * 2;
            let mut n = Vec::with_capacity(new_cap);
            n.resize_with(new_cap, Vec::new);
            self.new = Some(n);
            self.rehash_idx = 0;
        }
    }

    fn rehash_step(&mut self) {
        if let Some(new) = self.new.as_mut() {
            if self.rehash_idx < self.old.len() {
                let mut bucket = mem::take(&mut self.old[self.rehash_idx]);
                let new_len = new.len();
                let hs = self.hasher.clone();
                for (k, v) in bucket.drain(..) {
                    let idx = Self::index_for_with_hasher(&hs, &k, new_len);
                    new[idx].push((k, v));
                }
                self.rehash_idx += 1;
            }
            if self.rehash_idx >= self.old.len() {
                let n = self.new.take().unwrap();
                self.old = n;
                self.rehash_idx = 0;
            }
        }
    }

    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        self.start_rehash_if_needed();
        self.rehash_step();
        let hs = self.hasher.clone();
        if let Some(new) = self.new.as_mut() {
            let idx_new = Self::index_for_with_hasher(&hs, &k, new.len());
            for i in 0..new[idx_new].len() {
                if new[idx_new][i].0 == k {
                    let oldv = std::mem::replace(&mut new[idx_new][i].1, v);
                    return Some(oldv);
                }
            }
            let idx_old = Self::index_for_with_hasher(&hs, &k, self.old.len());
            for i in 0..self.old[idx_old].len() {
                if self.old[idx_old][i].0 == k {
                    let oldv = std::mem::replace(&mut self.old[idx_old][i].1, v);
                    return Some(oldv);
                }
            }
            new[idx_new].push((k, v));
            self.len += 1;
            None
        } else {
            let idx = Self::index_for_with_hasher(&hs, &k, self.old.len());
            for i in 0..self.old[idx].len() {
                if self.old[idx][i].0 == k {
                    let oldv = std::mem::replace(&mut self.old[idx][i].1, v);
                    return Some(oldv);
                }
            }
            self.old[idx].push((k, v));
            self.len += 1;
            None
        }
    }

    pub fn get(&mut self, k: &K) -> Option<&V> {
        self.rehash_step();
        let hs = self.hasher.clone();
        if let Some(new) = self.new.as_ref() {
            let idx = Self::index_for_with_hasher(&hs, k, new.len());
            if let Some(pos) = new[idx].iter().position(|(kk, _)| kk == k) {
                return Some(&new[idx][pos].1);
            }
            let idx_old = Self::index_for_with_hasher(&hs, k, self.old.len());
            if let Some(pos) = self.old[idx_old].iter().position(|(kk, _)| kk == k) {
                return Some(&self.old[idx_old][pos].1);
            }
            None
        } else {
            let idx = Self::index_for_with_hasher(&hs, k, self.old.len());
            if let Some(pos) = self.old[idx].iter().position(|(kk, _)| kk == k) {
                return Some(&self.old[idx][pos].1);
            }
            None
        }
    }

    pub fn get_clone(&mut self, k: &K) -> Option<V>
    where
        V: Clone,
    {
        self.get(k).cloned()
    }

    pub fn remove(&mut self, k: &K) -> Option<V> {
        self.rehash_step();
        let hs = self.hasher.clone();
        if let Some(new) = self.new.as_mut() {
            let idx = Self::index_for_with_hasher(&hs, k, new.len());
            if let Some(pos) = new[idx].iter().position(|(kk, _)| kk == k) {
                self.len -= 1;
                let (_, v) = new[idx].swap_remove(pos);
                return Some(v);
            }
            let idx_old = Self::index_for_with_hasher(&hs, k, self.old.len());
            if let Some(pos) = self.old[idx_old].iter().position(|(kk, _)| kk == k) {
                self.len -= 1;
                let (_, v) = self.old[idx_old].swap_remove(pos);
                return Some(v);
            }
            None
        } else {
            let idx = Self::index_for_with_hasher(&hs, k, self.old.len());
            if let Some(pos) = self.old[idx].iter().position(|(kk, _)| kk == k) {
                self.len -= 1;
                let (_, v) = self.old[idx].swap_remove(pos);
                return Some(v);
            }
            None
        }
    }
}

impl<K, V> Default for RehashMap<K, V>
where
    K: Hash + Eq,
{
    fn default() -> Self {
        Self::new()
    }
}

// #[cfg(feature = "rehash")]
// pub type Db = RehashMap<String, bytes::Bytes>;
// #[cfg(not(feature = "rehash"))]
pub type Db = std::collections::HashMap<String, bytes::Bytes>;
