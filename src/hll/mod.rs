//! Test

use std::io;
use std::io::Read;

use murmur3::murmur3_x64_128;

pub mod array4;
pub mod array6;
pub mod array8;
pub mod aux_map;
pub mod composite_interpolation;
pub mod container;
pub mod coupon_mapping;
pub mod cubic_interpolation;
pub mod estimator;
pub mod harmonic_numbers;
pub mod hash_set;
pub mod list;
pub mod serialization;
pub mod sketch;

/// Target HLL type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HllType {
    Hll4 = 0,
    Hll6 = 1,
    Hll8 = 2,
}

const KEY_BITS_26: u32 = 26;
const KEY_MASK_26: u32 = (1 << KEY_BITS_26) - 1;

const COUPON_RSE_FACTOR: f64 = 0.409; // at transition point not the asymptote
const COUPON_RSE: f64 = COUPON_RSE_FACTOR / (1 << 13) as f64;

// Constants
const RESIZE_NUMER: u32 = 3; // Resize at 3/4 = 75% load factor
const RESIZE_DENOM: u32 = 4;

/// Extract slot number (low 26 bits) from coupon
#[inline]
fn get_slot(coupon: u32) -> u32 {
    coupon & KEY_MASK_26
}

/// Extract value (upper 6 bits) from coupon
#[inline]
fn get_value(coupon: u32) -> u8 {
    (coupon >> KEY_BITS_26) as u8
}

/// Pack slot number and value into a coupon
///
/// Format: [value (6 bits) << 26] | [slot (26 bits)]
#[inline]
fn pack_coupon(slot: u32, value: u8) -> u32 {
    ((value as u32) << KEY_BITS_26) | (slot & KEY_MASK_26)
}

pub fn coupon<R: Read>(v: &mut R) -> io::Result<u32> {
    const DEFAULT_SEED: u32 = 9001;
    let hash = murmur3_x64_128(v, DEFAULT_SEED)?;

    let lo: u64 = hash as u64;
    let hi: u64 = (hash >> 64) as u64;

    let addr26 = lo as u32 & KEY_MASK_26;
    let lz = hi.leading_zeros();
    let capped = lz.min(62);
    let value = capped + 1;

    Ok(value << KEY_BITS_26 | addr26)
}

#[cfg(test)]
mod tests {
    use crate::hll::{get_slot, get_value, pack_coupon};

    #[test]
    fn test_pack_unpack_coupon() {
        let slot = 12345u32;
        let value = 42u8;
        let coupon = pack_coupon(slot, value);
        assert_eq!(get_slot(coupon), slot);
        assert_eq!(get_value(coupon), value);
    }
}
