use crate::conf::Config;
use crate::db::{Db, Entry, SortedSet, TotalOrderF64, Value};
use crate::stream::{Stream, StreamID, StreamEntry, ConsumerGroup, Consumer, PendingEntry};
use bytes::{Buf, Bytes};
use std::collections::{VecDeque, HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::sync::Arc;
use dashmap::DashMap;
use std::time::{SystemTime, UNIX_EPOCH};

// Constants for RDB format
const RDB_VERSION: u16 = 9;
const RDB_OPCODE_AUX: u8 = 0xFA;
const RDB_OPCODE_RESIZEDB: u8 = 0xFB;
const RDB_OPCODE_EXPIRETIME_MS: u8 = 0xFC;
const RDB_OPCODE_EXPIRETIME: u8 = 0xFD;
const RDB_OPCODE_SELECTDB: u8 = 0xFE;
const RDB_OPCODE_EOF: u8 = 0xFF;

const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST: u8 = 1;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_ZSET: u8 = 3;
const RDB_TYPE_HASH: u8 = 4;
const RDB_TYPE_STREAM_LISTPACKS: u8 = 15;

// Minimal CRC64 implementation (Placeholder for Redis compatibility)
struct Crc64 {
    crc: u64,
}

impl Crc64 {
    fn new() -> Self {
        Crc64 { crc: 0 }
    }

    fn update(&mut self, _data: &[u8]) {
        // TODO: Implement actual CRC64 ISO
    }

    fn digest(&self) -> u64 {
        self.crc
    }
}

pub struct RdbEncoder {
    writer: BufWriter<File>,
    crc: Crc64,
}

impl RdbEncoder {
    pub fn new(path: &str) -> io::Result<Self> {
        let file = File::create(path)?;
        Ok(RdbEncoder {
            writer: BufWriter::new(file),
            crc: Crc64::new(),
        })
    }

    fn write_magic(&mut self) -> io::Result<()> {
        self.writer.write_all(b"REDIS0009")?;
        Ok(())
    }

    fn write_u8(&mut self, v: u8) -> io::Result<()> {
        self.writer.write_all(&[v])?;
        self.crc.update(&[v]);
        Ok(())
    }

    fn write_u32_be(&mut self, v: u32) -> io::Result<()> {
        let bytes = v.to_be_bytes();
        self.writer.write_all(&bytes)?;
        self.crc.update(&bytes);
        Ok(())
    }

    fn write_u64_be(&mut self, v: u64) -> io::Result<()> {
        let bytes = v.to_be_bytes();
        self.writer.write_all(&bytes)?;
        self.crc.update(&bytes);
        Ok(())
    }

    fn write_u64_le(&mut self, v: u64) -> io::Result<()> {
        let bytes = v.to_le_bytes();
        self.writer.write_all(&bytes)?;
        self.crc.update(&bytes);
        Ok(())
    }

    fn write_u128_be(&mut self, v: u128) -> io::Result<()> {
        let bytes = v.to_be_bytes();
        self.writer.write_all(&bytes)?;
        self.crc.update(&bytes);
        Ok(())
    }

    fn write_len(&mut self, len: u64) -> io::Result<()> {
        if len < 64 {
            self.write_u8((len as u8) & 0x3F)?;
        } else if len < 16384 {
            let b1 = (((len >> 8) as u8) & 0x3F) | 0x40;
            let b2 = (len as u8) & 0xFF;
            self.write_u8(b1)?;
            self.write_u8(b2)?;
        } else {
            self.write_u8(0x80)?;
            self.write_u32_be(len as u32)?;
        }
        Ok(())
    }

    fn write_string(&mut self, s: &[u8]) -> io::Result<()> {
        // LZF compression could go here, but skipping for now
        self.write_len(s.len() as u64)?;
        self.writer.write_all(s)?;
        self.crc.update(s);
        Ok(())
    }

    fn write_aux(&mut self, _key: &str, val: &str) -> io::Result<()> {
        self.write_u8(RDB_OPCODE_AUX)?;
        self.write_string(_key.as_bytes())?;
        self.write_string(val.as_bytes())?;
        Ok(())
    }

    fn save_stream(&mut self, stream: &Stream) -> io::Result<()> {
        // 1. Write number of listpacks
        let entries = stream.range(&StreamID::new(0, 0), &StreamID::new(u64::MAX, u64::MAX));
        if entries.is_empty() {
             self.write_len(0)?;
        } else {
            self.write_len(1)?; // 1 listpack for simplicity

            // Master ID (first entry ID)
            let master_id = entries[0].id;
            let mut master_id_str = Vec::new();
            master_id_str.extend_from_slice(&master_id.ms.to_be_bytes());
            master_id_str.extend_from_slice(&master_id.seq.to_be_bytes());
            self.write_string(&master_id_str)?; 

            // Build Listpack
            let mut lp = ListpackBuilder::new();
            for entry in &entries {
                // Flags (None = 0)
                lp.append_int(0); 
                
                // ms-diff, seq-diff
                let ms_diff = (entry.id.ms as i128 - master_id.ms as i128) as i64;
                let seq_diff = (entry.id.seq as i128 - master_id.seq as i128) as i64;
                lp.append_int(ms_diff);
                lp.append_int(seq_diff);
                
                // Num fields
                lp.append_int(entry.fields.len() as i64);
                
                // Fields
                for (k, v) in &entry.fields {
                    lp.append_string(k);
                    lp.append_string(v);
                }
                
                // LP-count (3 + 1 + 2*fields)
                let lp_count = 3 + 1 + 2 * entry.fields.len() as i64;
                lp.append_int(lp_count);
            }
            
            let lp_bytes = lp.finish();
            self.write_string(&lp_bytes)?;
        }

        // 2. Total number of items
        self.write_len(stream.len() as u64)?;
        
        // 3. Last ID
        self.write_len(stream.last_id.ms)?;
        self.write_len(stream.last_id.seq)?;

        // 4. Consumer Groups
        self.write_len(stream.groups.len() as u64)?;
        for group in stream.groups.values() {
            self.write_string(group.name.as_bytes())?;
            self.write_len(group.last_id.ms)?;
            self.write_len(group.last_id.seq)?;

            // PEL
            self.write_len(group.pel.len() as u64)?;
            for pending in group.pel.values() {
                // Write raw ID (128 bit BE)
                let mut id_bytes = Vec::new();
                id_bytes.extend_from_slice(&pending.id.ms.to_be_bytes());
                id_bytes.extend_from_slice(&pending.id.seq.to_be_bytes());
                self.writer.write_all(&id_bytes)?;
                self.crc.update(&id_bytes);

                self.write_u64_le(pending.delivery_time as u64)?; 
                self.write_len(pending.delivery_count)?;
            }

            // Consumers
            self.write_len(group.consumers.len() as u64)?;
            for consumer in group.consumers.values() {
                self.write_string(consumer.name.as_bytes())?;
                self.write_u64_le(consumer.seen_time as u64)?; 
                
                // Consumer PEL
                self.write_len(consumer.pending_ids.len() as u64)?;
                for pid in &consumer.pending_ids {
                    let mut pid_bytes = Vec::new();
                    pid_bytes.extend_from_slice(&pid.ms.to_be_bytes());
                    pid_bytes.extend_from_slice(&pid.seq.to_be_bytes());
                    self.writer.write_all(&pid_bytes)?;
                    self.crc.update(&pid_bytes);
                }
            }
        }
        Ok(())
    }

    pub fn save(&mut self, databases: &Arc<Vec<Db>>) -> io::Result<()> {
        self.write_magic()?;

        self.write_aux("redis-ver", "6.2.5")?;
        self.write_aux("redis-bits", "64")?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.write_aux("ctime", &now.to_string())?;

        for (i, db) in databases.iter().enumerate() {
            if db.is_empty() {
                continue;
            }

            self.write_u8(RDB_OPCODE_SELECTDB)?;
            self.write_len(i as u64)?;

            let len = db.len() as u64;
            let expires_len = db
                .iter()
                .filter(|entry| entry.value().expires_at.is_some())
                .count() as u64;
            self.write_u8(RDB_OPCODE_RESIZEDB)?;
            self.write_len(len)?;
            self.write_len(expires_len)?;

            for entry in db.iter() {
                let key = entry.key();
                let value = entry.value();

                if let Some(expires_at) = value.expires_at {
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                    if now_ms >= expires_at {
                        continue;
                    }
                    self.write_u8(RDB_OPCODE_EXPIRETIME_MS)?;
                    self.write_u64_le(expires_at)?;
                }

                match &value.value {
                    Value::String(s) => {
                        self.write_u8(RDB_TYPE_STRING)?;
                        self.write_string(key)?;
                        self.write_string(s)?;
                    }
                    Value::List(l) => {
                        self.write_u8(RDB_TYPE_LIST)?;
                        self.write_string(key)?;
                        self.write_len(l.len() as u64)?;
                        for item in l {
                            self.write_string(item)?;
                        }
                    }
                    Value::Set(s) => {
                        self.write_u8(RDB_TYPE_SET)?;
                        self.write_string(key)?;
                        self.write_len(s.len() as u64)?;
                        for item in s {
                            self.write_string(item)?;
                        }
                    }
                    Value::Hash(h) => {
                        self.write_u8(RDB_TYPE_HASH)?;
                        self.write_string(key)?;
                        self.write_len(h.len() as u64)?;
                        for (k, v) in h {
                            self.write_string(k)?;
                            self.write_string(v)?;
                        }
                    }
                    Value::ZSet(z) => {
                        self.write_u8(RDB_TYPE_ZSET)?;
                        self.write_string(key)?;
                        self.write_len(z.scores.len() as u64)?;
                        for (score, member) in &z.scores {
                            self.write_string(member)?;
                            let score_str = score.0.to_string();
                            self.write_string(score_str.as_bytes())?;
                        }
                    }
                    Value::HyperLogLog(hll) => {
                        self.write_u8(RDB_TYPE_STRING)?;
                        self.write_string(key)?;
                        self.write_string(&hll.registers)?;
                    }
                    Value::Stream(stream) => {
                        self.write_u8(RDB_TYPE_STREAM_LISTPACKS)?;
                        self.write_string(key)?;
                        self.save_stream(stream)?;
                    }
                }
            }
        }

        self.write_u8(RDB_OPCODE_EOF)?;
        let checksum = self.crc.digest();
        self.write_u64_le(checksum)?;

        self.writer.flush()?;
        Ok(())
    }
}

// Listpack Builder for Stream RDB compatibility
struct ListpackBuilder {
    buf: Vec<u8>,
    num_elements: u16,
}

impl ListpackBuilder {
    fn new() -> Self {
        ListpackBuilder {
            buf: Vec::new(),
            num_elements: 0,
        }
    }

    fn encode_backlen(&mut self, len: u32) {
        if len <= 127 {
            self.buf.push(len as u8);
        } else if len < 16383 {
            self.buf.push(((len >> 7) as u8) & 127 | 128);
            self.buf.push((len as u8) & 127);
        } else if len < 2097151 {
            self.buf.push(((len >> 14) as u8) & 127 | 128);
            self.buf.push(((len >> 7) as u8) & 127 | 128);
            self.buf.push((len as u8) & 127);
        } else if len < 268435455 {
            self.buf.push(((len >> 21) as u8) & 127 | 128);
            self.buf.push(((len >> 14) as u8) & 127 | 128);
            self.buf.push(((len >> 7) as u8) & 127 | 128);
            self.buf.push((len as u8) & 127);
        } else {
            self.buf.push(((len >> 28) as u8) & 127 | 128);
            self.buf.push(((len >> 21) as u8) & 127 | 128);
            self.buf.push(((len >> 14) as u8) & 127 | 128);
            self.buf.push(((len >> 7) as u8) & 127 | 128);
            self.buf.push((len as u8) & 127);
        }
    }

    fn append_string(&mut self, s: &[u8]) {
        let len = s.len();
        let start_len = self.buf.len();
        
        if len < 64 {
            self.buf.push(0x80 | (len as u8));
        } else if len < 4096 {
            self.buf.push(0xE0 | ((len >> 8) as u8));
            self.buf.push(len as u8);
        } else if len <= u32::MAX as usize {
            self.buf.push(0xF0);
            self.buf.extend_from_slice(&(len as u32).to_le_bytes()); 
        } else {
            panic!("String too large for listpack");
        }
        self.buf.extend_from_slice(s);
        
        let encoded_len = (self.buf.len() - start_len) as u32;
        self.encode_backlen(encoded_len);
        self.num_elements += 1;
    }

    fn append_int(&mut self, v: i64) {
        let start_len = self.buf.len();
        
        if v >= 0 && v <= 127 {
            self.buf.push(v as u8);
        } else if v >= -4096 && v <= 4095 {
            let v = v as u16; 
            self.buf.push(0xC0 | (((v >> 8) as u8) & 0x1F));
            self.buf.push(v as u8);
        } else if v >= -32768 && v <= 32767 {
            self.buf.push(0xF1);
            self.buf.extend_from_slice(&(v as i16).to_le_bytes());
        } else if v >= -8388608 && v <= 8388607 {
            self.buf.push(0xF2);
            let v = v as i32; 
            self.buf.push((v & 0xFF) as u8);
            self.buf.push(((v >> 8) & 0xFF) as u8);
            self.buf.push(((v >> 16) & 0xFF) as u8);
        } else if v >= i32::MIN as i64 && v <= i32::MAX as i64 {
            self.buf.push(0xF3);
            self.buf.extend_from_slice(&(v as i32).to_le_bytes());
        } else {
            self.buf.push(0xF4);
            self.buf.extend_from_slice(&v.to_le_bytes());
        }
        
        let encoded_len = (self.buf.len() - start_len) as u32;
        self.encode_backlen(encoded_len);
        self.num_elements += 1;
    }

    fn finish(self) -> Vec<u8> {
        let total_bytes = 4 + 2 + self.buf.len() + 1;
        let mut res = Vec::with_capacity(total_bytes);
        res.extend_from_slice(&(total_bytes as u32).to_le_bytes());
        res.extend_from_slice(&self.num_elements.to_le_bytes());
        res.extend_from_slice(&self.buf);
        res.push(0xFF); // EOF
        res
    }
}

enum LpElement {
    Int(i64),
    String(Vec<u8>),
}

struct ListpackReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> ListpackReader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        ListpackReader { buf, pos: 0 }
    }

    fn read_u8(&mut self) -> io::Result<u8> {
        if self.pos < self.buf.len() {
            let b = self.buf[self.pos];
            self.pos += 1;
            Ok(b)
        } else {
            Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Listpack EOF"))
        }
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        if self.pos + buf.len() <= self.buf.len() {
            buf.copy_from_slice(&self.buf[self.pos..self.pos+buf.len()]);
            self.pos += buf.len();
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Listpack EOF"))
        }
    }

    fn read_bytes(&mut self, len: usize) -> io::Result<Vec<u8>> {
        if self.pos + len <= self.buf.len() {
            let bytes = self.buf[self.pos..self.pos+len].to_vec();
            self.pos += len;
            Ok(bytes)
        } else {
            Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Listpack EOF"))
        }
    }

    fn read_u16_le(&mut self) -> io::Result<u16> {
        let mut buf = [0u8; 2];
        self.read_exact(&mut buf)?;
        Ok(u16::from_le_bytes(buf))
    }

    fn read_u32_le(&mut self) -> io::Result<u32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }

    fn read_i16_le(&mut self) -> io::Result<i16> {
        let mut buf = [0u8; 2];
        self.read_exact(&mut buf)?;
        Ok(i16::from_le_bytes(buf))
    }

    fn read_i32_le(&mut self) -> io::Result<i32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(i32::from_le_bytes(buf))
    }

    fn read_i64_le(&mut self) -> io::Result<i64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(i64::from_le_bytes(buf))
    }

    fn skip_backlen(&mut self) -> io::Result<()> {
        loop {
            let b = self.read_u8()?;
            if b & 128 == 0 {
                break;
            }
        }
        Ok(())
    }

    fn read_element(&mut self) -> io::Result<Option<LpElement>> {
        let b = match self.read_u8() {
            Ok(b) => b,
            Err(_) => return Ok(None),
        };

        if b == 0xFF {
            return Ok(None);
        }

        let val = if b & 0x80 == 0 {
            // 7-bit uint
            LpElement::Int((b & 0x7F) as i64)
        } else if b & 0xC0 == 0x80 {
            // 6-bit string
            let len = (b & 0x3F) as usize;
            let bytes = self.read_bytes(len)?;
            LpElement::String(bytes)
        } else if b & 0xE0 == 0xC0 {
            // 13-bit int
            let b2 = self.read_u8()?;
            let val = (((b & 0x1F) as u16) << 8) | (b2 as u16);
            // Sign extension for 13-bit
            let mut val_i16 = val as i16;
            if val_i16 & 0x1000 != 0 {
                val_i16 |= 0xF000u16 as i16;
            }
            LpElement::Int(val_i16 as i64)
        } else if b & 0xF0 == 0xE0 {
             // 12-bit string
             let b2 = self.read_u8()?;
             let len = (((b & 0x0F) as usize) << 8) | (b2 as usize);
             let bytes = self.read_bytes(len)?;
             LpElement::String(bytes)
        } else if b == 0xF0 {
             // 32-bit string
             let len = self.read_u32_le()? as usize;
             let bytes = self.read_bytes(len)?;
             LpElement::String(bytes)
        } else if b == 0xF1 {
             // 16-bit int
             let val = self.read_i16_le()?;
             LpElement::Int(val as i64)
        } else if b == 0xF2 {
             // 24-bit int
             let mut buf = [0u8; 3];
             self.read_exact(&mut buf)?;
             let val = (buf[0] as i32) | ((buf[1] as i32) << 8) | ((buf[2] as i32) << 16);
             // Sign extend 24-bit
             let val = if val & 0x800000 != 0 {
                 val | 0xFF000000u32 as i32
             } else {
                 val
             };
             LpElement::Int(val as i64)
        } else if b == 0xF3 {
             // 32-bit int
             let val = self.read_i32_le()?;
             LpElement::Int(val as i64)
        } else if b == 0xF4 {
             // 64-bit int
             let val = self.read_i64_le()?;
             LpElement::Int(val)
        } else {
            return Err(io::Error::new(io::ErrorKind::InvalidData, format!("Invalid listpack entry: {:02x}", b)));
        };

        self.skip_backlen()?;
        Ok(Some(val))
    }
}

pub struct RdbLoader {
    reader: BufReader<File>,
}

impl RdbLoader {
    pub fn new(path: &str) -> io::Result<Self> {
        let file = File::open(path)?;
        Ok(RdbLoader {
            reader: BufReader::new(file),
        })
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.reader.read_exact(buf)
    }

    fn read_u8(&mut self) -> io::Result<u8> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn read_u32_be(&mut self) -> io::Result<u32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(u32::from_be_bytes(buf))
    }

    fn read_u64_be(&mut self) -> io::Result<u64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(u64::from_be_bytes(buf))
    }

    fn read_u64_le(&mut self) -> io::Result<u64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    fn read_u128_be(&mut self) -> io::Result<u128> {
        let mut buf = [0u8; 16];
        self.read_exact(&mut buf)?;
        Ok(u128::from_be_bytes(buf))
    }

    fn read_len(&mut self) -> io::Result<(u64, bool)> {
        let b = self.read_u8()?;
        let type_code = (b & 0xC0) >> 6;
        match type_code {
            0 => Ok(((b & 0x3F) as u64, false)),
            1 => {
                let next = self.read_u8()?;
                let len = (((b & 0x3F) as u64) << 8) | (next as u64);
                Ok((len, false))
            }
            2 => {
                let len = self.read_u32_be()?;
                Ok((len as u64, false))
            }
            3 => {
                // Special encoding
                Ok(((b & 0x3F) as u64, true))
            }
            _ => unreachable!(),
        }
    }

    fn read_string(&mut self) -> io::Result<Bytes> {
        let (len, is_encoded) = self.read_len()?;
        if is_encoded {
            match len {
                0 => {
                    let val = self.read_u8()? as i8;
                    Ok(Bytes::from(val.to_string()))
                }
                1 => {
                    let mut buf = [0u8; 2];
                    self.read_exact(&mut buf)?;
                    let val = i16::from_le_bytes(buf);
                    Ok(Bytes::from(val.to_string()))
                }
                2 => {
                    let mut buf = [0u8; 4];
                    self.read_exact(&mut buf)?;
                    let val = i32::from_le_bytes(buf);
                    Ok(Bytes::from(val.to_string()))
                }
                3 => {
                    // LZF compressed - SKIP for now
                    let (clen, _) = self.read_len()?;
                    let (_ulen, _) = self.read_len()?;
                    let mut compressed = vec![0u8; clen as usize];
                    self.read_exact(&mut compressed)?;
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "LZF compression not supported",
                    ));
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Unknown string encoding",
                    ));
                }
            }
        } else {
            let mut buf = vec![0u8; len as usize];
            self.read_exact(&mut buf)?;
            Ok(Bytes::from(buf))
        }
    }

    fn load_stream(&mut self) -> io::Result<Stream> {
        let mut stream = Stream::new();
        
        // 1. Listpacks
        let (num_listpacks, _) = self.read_len()?;
        for _ in 0..num_listpacks {
            let master_id_bytes = self.read_string()?;
            let lp_bytes = self.read_string()?;
            
            let mut lp = ListpackReader::new(&lp_bytes);
            
            // Skip Header (Total Bytes 4, Num Elements 2)
            lp.read_u32_le()?;
            lp.read_u16_le()?;
            
            loop {
                // Read Flags
                let _flags_elem = match lp.read_element()? {
                    Some(e) => e,
                    None => break, // EOF
                };
                
                // Read ms_diff
                let ms_diff = match lp.read_element()? {
                     Some(LpElement::Int(v)) => v,
                     _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "Expected ms_diff int")),
                };
                // Read seq_diff
                let seq_diff = match lp.read_element()? {
                     Some(LpElement::Int(v)) => v,
                     _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "Expected seq_diff int")),
                };
                // Read num_fields
                let num_fields = match lp.read_element()? {
                     Some(LpElement::Int(v)) => v,
                     _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "Expected num_fields int")),
                };
                
                let mut fields = Vec::new();
                for _ in 0..num_fields {
                    let k = match lp.read_element()? {
                        Some(LpElement::String(v)) => v,
                        _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "Expected field key string")),
                    };
                    let v = match lp.read_element()? {
                        Some(LpElement::String(v)) => v,
                        _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "Expected field value string")),
                    };
                    fields.push((Bytes::from(k), Bytes::from(v)));
                }
                
                // Read lp_count
                let _lp_count = lp.read_element()?;
                
                // Reconstruct ID
                let master_ms = u64::from_be_bytes(master_id_bytes[0..8].try_into().unwrap());
                let master_seq = u64::from_be_bytes(master_id_bytes[8..16].try_into().unwrap());
                
                let id = StreamID {
                    ms: (master_ms as i128 + ms_diff as i128) as u64,
                    seq: (master_seq as i128 + seq_diff as i128) as u64,
                };
                
                // Insert into stream
                if let Err(e) = stream.insert(id, fields) {
                     // Since RDB load should trust data, maybe we shouldn't fail?
                     // But if data is corrupted, we should fail.
                     // The only expected error is order violation if we process listpacks out of order,
                     // or if we have duplicates.
                     return Err(io::Error::new(io::ErrorKind::InvalidData, e));
                }
            }
        }

        // 2. Total number of items
        let (_total_items, _) = self.read_len()?;
        
        // 3. Last ID
        let ms = self.read_len()?.0;
        let seq = self.read_len()?.0;
        stream.last_id = StreamID::new(ms, seq);

        // 4. Consumer Groups
        let (num_groups, _) = self.read_len()?;
        for _ in 0..num_groups {
            let name_bytes = self.read_string()?;
            let name = String::from_utf8(name_bytes.to_vec()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            
            let ms = self.read_len()?.0;
            let seq = self.read_len()?.0;
            let last_id = StreamID::new(ms, seq);
            
            let mut group = ConsumerGroup::new(name.clone(), last_id);
            
            // PEL
            let (pel_len, _) = self.read_len()?;
            for _ in 0..pel_len {
                // Read raw ID (16 bytes)
                let mut id_bytes = [0u8; 16];
                self.read_exact(&mut id_bytes)?;
                let ms = u64::from_be_bytes(id_bytes[0..8].try_into().unwrap());
                let seq = u64::from_be_bytes(id_bytes[8..16].try_into().unwrap());
                let pid = StreamID::new(ms, seq);
                
                let delivery_time = self.read_u64_le()? as u128; // Assuming 64-bit LE
                let delivery_count = self.read_len()?.0;
                
                let pending = PendingEntry {
                    id: pid,
                    delivery_time,
                    delivery_count,
                    owner: String::new(), // Placeholder
                };
                group.pel.insert(pid, pending);
            }
            
            // Consumers
            let (cons_len, _) = self.read_len()?;
            for _ in 0..cons_len {
                let cname_bytes = self.read_string()?;
                let cname = String::from_utf8(cname_bytes.to_vec()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                
                let mut consumer = Consumer::new(cname.clone());
                consumer.seen_time = self.read_u64_le()? as u128;
                
                // Consumer PEL
                let (cpel_len, _) = self.read_len()?;
                for _ in 0..cpel_len {
                    let mut pid_bytes = [0u8; 16];
                    self.read_exact(&mut pid_bytes)?;
                    let ms = u64::from_be_bytes(pid_bytes[0..8].try_into().unwrap());
                    let seq = u64::from_be_bytes(pid_bytes[8..16].try_into().unwrap());
                    let pid = StreamID::new(ms, seq);
                    
                    consumer.pending_ids.insert(pid);
                    
                    // Update global PEL owner
                    if let Some(pending) = group.pel.get_mut(&pid) {
                        pending.owner = cname.clone();
                    }
                }
                group.consumers.insert(cname, consumer);
            }
            stream.groups.insert(name, group);
        }
        
        Ok(stream)
    }

    pub fn load(&mut self, databases: &Arc<Vec<Db>>) -> io::Result<()> {
        // Simple loader: assumes only one DB and reads until EOF
        // Real RDB loader handles opcodes
        
        let mut magic = [0u8; 9];
        self.read_exact(&mut magic)?;
        if &magic != b"REDIS0009" {
             // For now assume version 9 or fail
             // return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid Magic"));
        }

        let mut current_db_index = 0;
        let mut expire_at: Option<u64> = None;

        loop {
            let opcode = self.read_u8()?;
            match opcode {
                RDB_OPCODE_AUX => {
                    let _key = self.read_string()?;
                    let _val = self.read_string()?;
                }
                RDB_OPCODE_RESIZEDB => {
                    let _db_size = self.read_len()?;
                    let _expires_size = self.read_len()?;
                }
                RDB_OPCODE_EXPIRETIME_MS => {
                    let expires = self.read_u64_le()?;
                    expire_at = Some(expires);
                }
                RDB_OPCODE_EXPIRETIME => {
                     let expires = self.read_u32_be()?;
                     expire_at = Some(expires as u64 * 1000);
                }
                RDB_OPCODE_SELECTDB => {
                    let (id, _) = self.read_len()?;
                    current_db_index = id as usize;
                }
                RDB_OPCODE_EOF => {
                    break;
                }
                type_code => {
                    // It's a value type (0..14)
                    let key = self.read_string()?;
                    let val = match type_code {
                        RDB_TYPE_STRING => {
                            let s = self.read_string()?;
                    Value::String(s)
                }
                RDB_TYPE_LIST => {
                            let (len, _) = self.read_len()?;
                            let mut list = VecDeque::new();
                            for _ in 0..len {
                                list.push_back(self.read_string()?);
                            }
                            Value::List(list)
                        }
                        RDB_TYPE_SET => {
                            let (len, _) = self.read_len()?;
                            let mut set = HashSet::new();
                            for _ in 0..len {
                                set.insert(self.read_string()?);
                            }
                            Value::Set(set)
                        }
                        RDB_TYPE_HASH => {
                            let (len, _) = self.read_len()?;
                            let mut hash = HashMap::new();
                            for _ in 0..len {
                                let k = self.read_string()?;
                                let v = self.read_string()?;
                                hash.insert(k, v);
                            }
                            Value::Hash(hash)
                        }
                        RDB_TYPE_ZSET => {
                            let (len, _) = self.read_len()?;
                            let mut zset = SortedSet::new();
                    for _ in 0..len {
                        let member = self.read_string()?;
                        let score_bytes = self.read_string()?;
                        let score_str = String::from_utf8_lossy(&score_bytes);
                        let score = score_str.parse::<f64>().unwrap_or(0.0);
                        zset.members.insert(member.clone(), score);
                        zset.scores.insert((TotalOrderF64(score), member));
                    }
                    Value::ZSet(zset)
                }
                        RDB_TYPE_STREAM_LISTPACKS => {
                            let stream = self.load_stream()?;
                            Value::Stream(stream)
                        }
                        _ => {
                            return Err(io::Error::new(io::ErrorKind::InvalidData, format!("Unknown value type: {}", type_code)));
                        }
                    };
                    
                    if current_db_index < databases.len() {
                        databases[current_db_index].insert(key, Entry::new(val, expire_at));
                    }
                    expire_at = None;
                }
            }
        }
        
        Ok(())
    }
}

pub fn rdb_save(databases: &Arc<Vec<Db>>, conf: &Config) -> io::Result<()> {
    let mut encoder = RdbEncoder::new(&conf.dbfilename)?;
    encoder.save(databases)
}

pub fn rdb_load(databases: &Arc<Vec<Db>>, conf: &Config) -> io::Result<()> {
    if !std::path::Path::new(&conf.dbfilename).exists() {
        return Ok(());
    }
    let mut loader = RdbLoader::new(&conf.dbfilename)?;
    loader.load(databases)
}
