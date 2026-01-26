use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub struct Rax<T> {
    root: RaxNode<T>,
    size: usize,
}

#[derive(Debug, Clone, PartialEq)]
struct RaxNode<T> {
    is_key: bool,
    data: Option<T>,
    children: BTreeMap<u8, RaxEdge<T>>,
}

#[derive(Debug, Clone, PartialEq)]
struct RaxEdge<T> {
    label: Vec<u8>,
    node: RaxNode<T>,
}

impl<T> Default for Rax<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Rax<T> {
    pub fn new() -> Self {
        Rax {
            root: RaxNode::new(),
            size: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn insert(&mut self, key: &[u8], data: T) -> Option<T> {
        self.root.insert(key, data, &mut self.size)
    }
    
    pub fn get(&self, key: &[u8]) -> Option<&T> {
        self.root.get(key)
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<T> {
        self.root.remove(key, &mut self.size)
    }

    pub fn range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, T)> where T: Clone {
        let mut result = Vec::new();
        let mut current_key = Vec::new();
        self.root.range(start, end, &mut current_key, &mut result);
        result
    }

    pub fn rev_range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, T)> where T: Clone {
        let mut result = Vec::new();
        let mut current_key = Vec::new();
        self.root.rev_range(start, end, &mut current_key, &mut result);
        result
    }
}

impl<T> RaxNode<T> {
    fn new() -> Self {
        RaxNode {
            is_key: false,
            data: None,
            children: BTreeMap::new(),
        }
    }

    fn insert(&mut self, key: &[u8], data: T, size: &mut usize) -> Option<T> {
        if key.is_empty() {
            if !self.is_key {
                self.is_key = true;
                *size += 1;
                self.data = Some(data);
                return None;
            }
            return self.data.replace(data);
        }

        let first_byte = key[0];
        if let Some(edge) = self.children.get_mut(&first_byte) {
            let common_len = common_prefix_len(&edge.label, key);
            
            if common_len == edge.label.len() {
                return edge.node.insert(&key[common_len..], data, size);
            } else {
                let old_edge_node = std::mem::replace(&mut edge.node, RaxNode::new());
                let old_label = std::mem::take(&mut edge.label);
                
                let common_part = old_label[..common_len].to_vec();
                let suffix_part = old_label[common_len..].to_vec();
                let key_suffix = key[common_len..].to_vec();
                
                let mut new_split_node = RaxNode::new();
                
                let suffix_first = suffix_part[0];
                let new_edge_to_old = RaxEdge {
                    label: suffix_part,
                    node: old_edge_node,
                };
                new_split_node.children.insert(suffix_first, new_edge_to_old);
                
                if key_suffix.is_empty() {
                    new_split_node.is_key = true;
                    new_split_node.data = Some(data);
                    *size += 1;
                } else {
                    new_split_node.insert(&key_suffix, data, size);
                }
                
                edge.label = common_part;
                edge.node = new_split_node;
                return None;
            }
        } else {
            let edge = RaxEdge {
                label: key.to_vec(),
                node: {
                    let mut n = RaxNode::new();
                    n.insert(&[], data, size);
                    n
                },
            };
            self.children.insert(first_byte, edge);
            return None;
        }
    }

    fn get(&self, key: &[u8]) -> Option<&T> {
        if key.is_empty() {
            if self.is_key {
                return self.data.as_ref();
            } else {
                return None;
            }
        }

        let first_byte = key[0];
        if let Some(edge) = self.children.get(&first_byte) {
             let common_len = common_prefix_len(&edge.label, key);
             if common_len == edge.label.len() {
                 return edge.node.get(&key[common_len..]);
             }
        }
        None
    }

    fn remove(&mut self, key: &[u8], size: &mut usize) -> Option<T> {
        if key.is_empty() {
            if self.is_key {
                self.is_key = false;
                *size -= 1;
                return self.data.take();
            }
            return None;
        }

        let first_byte = key[0];
        if let Some(edge) = self.children.get_mut(&first_byte) {
             let common_len = common_prefix_len(&edge.label, key);
             if common_len == edge.label.len() {
                 return edge.node.remove(&key[common_len..], size);
             }
        }
        None
    }
    
    fn range(&self, start: &[u8], end: &[u8], current_key: &mut Vec<u8>, result: &mut Vec<(Vec<u8>, T)>) where T: Clone {
        if self.is_key {
            if current_key.as_slice() >= start && current_key.as_slice() <= end {
                if let Some(data) = &self.data {
                    result.push((current_key.clone(), data.clone()));
                }
            }
        }

        for edge in self.children.values() {
            current_key.extend_from_slice(&edge.label);
            edge.node.range(start, end, current_key, result);
            current_key.truncate(current_key.len() - edge.label.len());
        }
    }
    
    fn rev_range(&self, start: &[u8], end: &[u8], current_key: &mut Vec<u8>, result: &mut Vec<(Vec<u8>, T)>) where T: Clone {
         for edge in self.children.values().rev() {
            current_key.extend_from_slice(&edge.label);
            edge.node.rev_range(start, end, current_key, result);
            current_key.truncate(current_key.len() - edge.label.len());
        }
        
        if self.is_key {
            if current_key.as_slice() >= start && current_key.as_slice() <= end {
                if let Some(data) = &self.data {
                    result.push((current_key.clone(), data.clone()));
                }
            }
        }
    }
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}
