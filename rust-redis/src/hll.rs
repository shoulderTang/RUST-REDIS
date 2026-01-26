use std::cmp;

pub const HLL_P: u8 = 14; // The greater is P, the smaller the error.
pub const HLL_REGISTERS: usize = 1 << HLL_P; // With P=14, 16384 registers.
pub const HLL_P_MASK: u64 = (HLL_REGISTERS - 1) as u64;

#[derive(Clone, Debug, PartialEq)]
pub struct HyperLogLog {
    pub registers: Vec<u8>,
}

impl HyperLogLog {
    pub fn new() -> Self {
        HyperLogLog {
            registers: vec![0; HLL_REGISTERS],
        }
    }

    pub fn add(&mut self, element: &[u8]) -> bool {
        let hash = murmurhash64a(element, 0xadc83b19); // Redis seed
        let index = (hash & HLL_P_MASK) as usize;
        let mut remaining = hash >> HLL_P;
        remaining |= 1 << (64 - HLL_P); // Set the 50th bit to 1 to ensure termination
        let run_length = (remaining.trailing_zeros() + 1) as u8;
        
        if run_length > self.registers[index] {
            self.registers[index] = run_length;
            true
        } else {
            false
        }
    }

    pub fn count(&self) -> u64 {
        let mut reghisto = [0u32; 64];
        let mut ez = 0; // Number of registers equal to 0

        for &reg in &self.registers {
            if reg == 0 {
                ez += 1;
            }
            reghisto[reg as usize] += 1;
        }

        let m = HLL_REGISTERS as f64;
        let mut e = 0.0;
        
        // E = alpha * m^2 / sum(2^-M[j])
        for (j, &count) in reghisto.iter().enumerate() {
            if count > 0 {
                e += count as f64 * 2.0_f64.powi(-(j as i32));
            }
        }
        
        // alpha = 0.7213 / (1 + 1.079 / m)
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        e = alpha * m * m / e;

        // Corrections
        if e <= 2.5 * m {
            if ez > 0 {
                e = m * (m / ez as f64).ln();
            }
        } else if e > (1.0 / 30.0) * 4294967296.0 { // 2^32
             e = -4294967296.0 * (1.0 - e / 4294967296.0).ln();
        }

        e as u64
    }

    pub fn merge(&mut self, other: &HyperLogLog) {
        for i in 0..HLL_REGISTERS {
            if other.registers[i] > self.registers[i] {
                self.registers[i] = other.registers[i];
            }
        }
    }
}

// MurmurHash64A implementation
pub fn murmurhash64a(key: &[u8], seed: u64) -> u64 {
    let m: u64 = 0xc6a4a7935bd1e995;
    let r: u8 = 47;
    let len = key.len();
    let mut h: u64 = seed ^ (len as u64).wrapping_mul(m);

    let n_blocks = len / 8;
    for i in 0..n_blocks {
        let offset = i * 8;
        let mut k = u64::from_le_bytes(key[offset..offset + 8].try_into().unwrap());
        k = k.wrapping_mul(m);
        k ^= k >> r;
        k = k.wrapping_mul(m);
        h ^= k;
        h = h.wrapping_mul(m);
    }

    let tail = &key[n_blocks * 8..];
    match tail.len() {
        7 => { h ^= (tail[6] as u64) << 48; h ^= (tail[5] as u64) << 40; h ^= (tail[4] as u64) << 32; h ^= (tail[3] as u64) << 24; h ^= (tail[2] as u64) << 16; h ^= (tail[1] as u64) << 8; h ^= tail[0] as u64; h = h.wrapping_mul(m); }
        6 => { h ^= (tail[5] as u64) << 40; h ^= (tail[4] as u64) << 32; h ^= (tail[3] as u64) << 24; h ^= (tail[2] as u64) << 16; h ^= (tail[1] as u64) << 8; h ^= tail[0] as u64; h = h.wrapping_mul(m); }
        5 => { h ^= (tail[4] as u64) << 32; h ^= (tail[3] as u64) << 24; h ^= (tail[2] as u64) << 16; h ^= (tail[1] as u64) << 8; h ^= tail[0] as u64; h = h.wrapping_mul(m); }
        4 => { h ^= (tail[3] as u64) << 24; h ^= (tail[2] as u64) << 16; h ^= (tail[1] as u64) << 8; h ^= tail[0] as u64; h = h.wrapping_mul(m); }
        3 => { h ^= (tail[2] as u64) << 16; h ^= (tail[1] as u64) << 8; h ^= tail[0] as u64; h = h.wrapping_mul(m); }
        2 => { h ^= (tail[1] as u64) << 8; h ^= tail[0] as u64; h = h.wrapping_mul(m); }
        1 => { h ^= tail[0] as u64; h = h.wrapping_mul(m); }
        _ => {}
    }

    h ^= h >> r;
    h = h.wrapping_mul(m);
    h ^= h >> r;
    h
}
