//! HyperLogLog sketch implementation for cardinality estimation.
//!
//! This module provides a probabilistic data structure for estimating the cardinality
//! (number of distinct elements) of large datasets with high accuracy and low memory usage.
//!
//! # Overview
//!
//! HyperLogLog (HLL) sketches use hash functions to estimate cardinality in logarithmic space.
//! This implementation follows the Apache DataSketches specification and supports multiple
//! storage modes that automatically adapt based on cardinality:
//!
//! - **List mode**: Stores individual coupons for small cardinalities
//! - **Set mode**: Uses a hash set for medium cardinalities
//! - **HLL mode**: Uses compact arrays for large cardinalities
//!
//! # HLL Types
//!
//! Three target HLL types are supported, trading precision for memory:
//!
//! - [`HllType::Hll4`]: 4 bits per bucket (most compact)
//! - [`HllType::Hll6`]: 6 bits per bucket (balanced)
//! - [`HllType::Hll8`]: 8 bits per bucket (highest precision)
//!
//! # Coupons
//!
//! A coupon is a 32-bit value encoding both a slot number (26 bits) and a value (6 bits).
//! The slot identifies which bucket to update, and the value represents the number of
//! leading zeros in the hash plus one.

use std::hash::Hash;

mod array4;
mod array6;
mod array8;
mod aux_map;
mod composite_interpolation;
mod container;
mod coupon_mapping;
mod cubic_interpolation;
mod estimator;
mod harmonic_numbers;
mod hash_set;
mod list;
mod serialization;
mod sketch;

// Re-export public API
pub use sketch::HllSketch;

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

pub fn coupon<H: Hash>(v: H) -> u32 {
    const DEFAULT_SEED: u32 = 9001;

    let mut hasher = mur3::Hasher128::with_seed(DEFAULT_SEED);
    v.hash(&mut hasher);
    let (lo, hi) = hasher.finish128();

    let addr26 = lo as u32 & KEY_MASK_26;
    let lz = hi.leading_zeros();
    let capped = lz.min(62);
    let value = capped + 1;

    value << KEY_BITS_26 | addr26
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
