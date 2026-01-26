use std::f64::consts::PI;

pub const GEO_LAT_MIN: f64 = -85.05112878;
pub const GEO_LAT_MAX: f64 = 85.05112878;
pub const GEO_LONG_MIN: f64 = -180.0;
pub const GEO_LONG_MAX: f64 = 180.0;

const D_R: f64 = PI / 180.0;
// const R_MAJOR: f64 = 6378137.0;
// const R_MINOR: f64 = 6356752.3142;

#[derive(Debug, Clone, Copy)]
pub struct GeoHashBits {
    pub bits: u64,
    pub step: u8,
}

pub fn geohash_encode(lat: f64, lon: f64, step: u8) -> GeoHashBits {
    let mut lat_offset = (lat - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN);
    let mut lon_offset = (lon - GEO_LONG_MIN) / (GEO_LONG_MAX - GEO_LONG_MIN);

    lat_offset = lat_offset.clamp(0.0, 1.0);
    lon_offset = lon_offset.clamp(0.0, 1.0);

    let mut bits: u64 = 0;
    
    // Interleave bits
    // We want 'step' bits for each dimension? 
    // Redis uses 26 steps for 52 bits total.
    
    for _ in 0..step {
        lat_offset *= 2.0;
        lon_offset *= 2.0;
        
        let lat_bit = if lat_offset >= 1.0 {
            lat_offset -= 1.0;
            1
        } else {
            0
        };
        
        let lon_bit = if lon_offset >= 1.0 {
            lon_offset -= 1.0;
            1
        } else {
            0
        };
        
        bits <<= 1;
        bits |= lon_bit;
        bits <<= 1;
        bits |= lat_bit;
    }
    
    GeoHashBits { bits, step }
}

pub fn geohash_decode(hash: GeoHashBits) -> (f64, f64) {
    let mut lat_range = (GEO_LAT_MIN, GEO_LAT_MAX);
    let mut lon_range = (GEO_LONG_MIN, GEO_LONG_MAX);
    
    for i in (0..hash.step).rev() {
        let lat_bit = (hash.bits >> (i * 2)) & 1;
        let lon_bit = (hash.bits >> (i * 2 + 1)) & 1;
        
        if lat_bit == 1 {
            lat_range.0 = (lat_range.0 + lat_range.1) / 2.0;
        } else {
            lat_range.1 = (lat_range.0 + lat_range.1) / 2.0;
        }
        
        if lon_bit == 1 {
            lon_range.0 = (lon_range.0 + lon_range.1) / 2.0;
        } else {
            lon_range.1 = (lon_range.0 + lon_range.1) / 2.0;
        }
    }
    
    let lat = (lat_range.0 + lat_range.1) / 2.0;
    let lon = (lon_range.0 + lon_range.1) / 2.0;
    
    (lat, lon)
}

// Earth radius in meters
const EARTH_RADIUS_METERS: f64 = 6372797.560856;

pub fn geodist(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let lat1_rad = lat1 * D_R;
    let lon1_rad = lon1 * D_R;
    let lat2_rad = lat2 * D_R;
    let lon2_rad = lon2 * D_R;

    let u = ((lat2_rad - lat1_rad) / 2.0).sin();
    let v = ((lon2_rad - lon1_rad) / 2.0).sin();
    
    let a = u * u + lat1_rad.cos() * lat2_rad.cos() * v * v;
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    
    EARTH_RADIUS_METERS * c
}

// Base32 for Geohash
const BASE32_CHARS: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

pub fn geohash_to_base32(lat: f64, lon: f64) -> String {
    // Standard Geohash uses 5 bits per char.
    // We typically want a precision of 11 characters or so.
    // Let's implement standard geohash string encoding.
    // It's slightly different from the 52-bit interleave used for ZSET score.
    // But conceptually similar.
    
    let mut lat_range = (-90.0, 90.0);
    let mut lon_range = (-180.0, 180.0);
    let mut bits = 0;
    let mut bits_count = 0;
    let mut result = String::new();
    let precision = 11; // Standard length
    
    let mut is_even = true;
    
    while result.len() < precision {
        let mid;
        if is_even {
            mid = (lon_range.0 + lon_range.1) / 2.0;
            if lon > mid {
                bits = (bits << 1) | 1;
                lon_range.0 = mid;
            } else {
                bits = (bits << 1) | 0;
                lon_range.1 = mid;
            }
        } else {
            mid = (lat_range.0 + lat_range.1) / 2.0;
            if lat > mid {
                bits = (bits << 1) | 1;
                lat_range.0 = mid;
            } else {
                bits = (bits << 1) | 0;
                lat_range.1 = mid;
            }
        }
        
        is_even = !is_even;
        bits_count += 1;
        
        if bits_count == 5 {
            result.push(BASE32_CHARS[bits as usize] as char);
            bits = 0;
            bits_count = 0;
        }
    }
    
    result
}
