// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
//! * **List mode**: Stores individual values for small cardinalities
//! * **Set mode**: Uses a hash set for medium cardinalities
//! * **HLL mode**: Uses compact arrays for large cardinalities
//!
//! Mode transitions are automatic and transparent to the user. Each promotion preserves
//! all previously observed values and maintains estimation accuracy.
//!
//! # Core Types
//!
//! The primary type for cardinality estimation is [`HllSketch`], which maintains a single
//! sketch and provides methods to update with new values and retrieve cardinality estimates.
//! For combining multiple sketches, use [`HllUnion`], which efficiently merges sketches
//! that may have different configurations.
//!
//! # HLL Types
//!
//! Three target HLL types are supported, trading precision for memory:
//!
//! * [`HllType::Hll4`]: 4 bits per bucket (most compact)
//! * [`HllType::Hll6`]: 6 bits per bucket (balanced)
//! * [`HllType::Hll8`]: 8 bits per bucket (highest precision)
//!
//! # Union Operations
//!
//! The [`HllUnion`] type enables combining multiple HLL sketches into a unified estimate.
//! It maintains an internal "gadget" sketch that accumulates the union of all input sketches
//! and automatically handles:
//!
//! * Sketches with different `lg_k` precision levels (resizes/downsamples as needed)
//! * Sketches in different modes (List, Set, or Array)
//! * Sketches with different target HLL types
//!
//! The union operation preserves cardinality estimation accuracy while enabling distributed
//! computation patterns where sketches are built independently and merged later.
//!
//! # Serialization
//!
//! Sketches can be serialized and deserialized while preserving all state, including:
//! * Current mode and HLL type
//! * All observed values (coupons or register values)
//! * HIP accumulator state for accurate estimation
//! * Out-of-order flag for merged/deserialized sketches
//!
//! The serialization format is compatible with Apache DataSketches implementations
//! in Java and C++, enabling cross-platform sketch exchange.
//!
//! # Usage
//!
//! ```
//! # use datasketches::hll::HllSketch;
//! # use datasketches::hll::HllType;
//! # use datasketches::common::NumStdDev;
//! let mut sketch = HllSketch::new(12, HllType::Hll8);
//! sketch.update("apple");
//! let upper = sketch.upper_bound(NumStdDev::Two);
//! assert!(upper >= sketch.estimate());
//! ```
//!
//! # Union
//!
//! ```
//! # use datasketches::hll::HllSketch;
//! # use datasketches::hll::HllType;
//! # use datasketches::hll::HllUnion;
//! let mut left = HllSketch::new(10, HllType::Hll8);
//! let mut right = HllSketch::new(10, HllType::Hll8);
//! left.update("apple");
//! right.update("banana");
//!
//! let mut union = HllUnion::new(10);
//! union.update(&left);
//! union.update(&right);
//!
//! let result = union.to_sketch(HllType::Hll8);
//! assert!(result.estimate() >= 2.0);
//! ```

use std::hash::Hash;

use crate::hash::MurmurHash3X64128;

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
mod mode;
mod serialization;
mod sketch;
mod union;

pub use self::sketch::HllSketch;
pub use self::union::HllUnion;

/// Target HLL type.
///
/// See [module level documentation](self) for more details.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HllType {
    /// Uses a 4-bit field per HLL bucket and for large counts may require the use of a
    /// small internal auxiliary array for storing statistical exceptions, which are rare.
    /// For the values of lgConfigK > 13 (K = 8192), this additional array adds about 3%
    /// to the overall storage.
    ///
    /// It is generally the slowest in terms of update time, but has the smallest storage
    /// footprint of about K/2 * 1.03 bytes.
    Hll4,
    /// Uses a 6-bit field per HLL bucket. It is generally the next fastest in terms
    /// of update time with a storage footprint of about 3/4 * K bytes.
    Hll6,
    /// Uses an 8-bit byte per HLL bucket. It is generally the fastest in terms of update
    /// time but has the largest storage footprint of about K bytes.
    Hll8,
}

const KEY_BITS_26: u32 = 26;
const KEY_MASK_26: u32 = (1 << KEY_BITS_26) - 1;

const COUPON_RSE_FACTOR: f64 = 0.409; // At transition point not the asymptote
const COUPON_RSE: f64 = COUPON_RSE_FACTOR / (1 << 13) as f64;

const RESIZE_NUMERATOR: u32 = 3; // Resize at 3/4 = 75% load factor
const RESIZE_DENOMINATOR: u32 = 4;

/// A coupon encodes a (slot, value) pair derived from hashing an input.
///
/// Format: `[value (6 bits) << 26] | [slot (26 bits)]`
///
/// The slot identifies an HLL register (derived from the lower bits of the hash),
/// and the value represents the number of leading zeros plus one (from the upper bits).
///
/// Pre-computing coupons is useful when the same logical value must be inserted into
/// multiple independent sketches, because the (relatively expensive) hash step is paid
/// only once.  A common pattern is dictionary-encoded data: compute the coupon for each
/// term id up front, cache it, and then call [`HllSketch::update_with_coupon`] for
/// each per-bucket sketch rather than calling [`HllSketch::update`] repeatedly with the
/// decoded string.
///
/// # Examples
///
/// ```
/// # use datasketches::hll::{HllSketch, HllType, Coupon};
/// let c = Coupon::from_hash("hello");
///
/// let mut sketch1 = HllSketch::new(10, HllType::Hll8);
/// let mut sketch2 = HllSketch::new(12, HllType::Hll8);
/// sketch1.update_with_coupon(c);
/// sketch2.update_with_coupon(c);
///
/// assert!(sketch1.estimate() >= 1.0);
/// assert!(sketch2.estimate() >= 1.0);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Coupon(u32);

impl Coupon {
    /// Sentinel value indicating an empty coupon slot.
    const EMPTY: Self = Coupon(0);

    /// Returns `true` if this coupon is the empty sentinel (value `0`).
    #[inline(always)]
    fn is_empty(self) -> bool {
        self == Self::EMPTY
    }

    /// Returns the raw 32-bit representation of the coupon.
    #[inline(always)]
    fn raw(self) -> u32 {
        self.0
    }

    /// Compute the HLL coupon for a hashable value.
    ///
    /// Hashes `v` using MurmurHash3 128-bit and packs the result into a coupon:
    /// the low 26 bits of the low hash word become the slot index, and the
    /// leading-zero count of the high hash word (capped at 62, then plus one)
    /// becomes the 6-bit register value.
    #[inline(always)]
    pub fn from_hash(v: impl Hash) -> Self {
        let mut hasher = MurmurHash3X64128::default();
        v.hash(&mut hasher);
        let (lo, hi) = hasher.finish128();

        let addr26 = lo as u32 & KEY_MASK_26;
        let lz = hi.leading_zeros();
        let capped = lz.min(62);
        let value = capped + 1;

        Coupon((value << KEY_BITS_26) | addr26)
    }

    /// Pack a slot index and register value into a coupon.
    #[inline(always)]
    fn pack(slot: u32, value: u8) -> Self {
        Coupon(((value as u32) << KEY_BITS_26) | (slot & KEY_MASK_26))
    }

    /// Extract the slot index (low 26 bits).
    #[inline(always)]
    fn slot(self) -> u32 {
        self.0 & KEY_MASK_26
    }

    /// Extract the register value (upper 6 bits).
    #[inline(always)]
    fn value(self) -> u8 {
        (self.0 >> KEY_BITS_26) as u8
    }
}

#[cfg(test)]
mod tests {
    use crate::hll::Coupon;

    #[test]
    fn test_pack_unpack_coupon() {
        let slot = 12345u32;
        let value = 42u8;
        let coupon = Coupon::pack(slot, value);
        assert_eq!(coupon.slot(), slot);
        assert_eq!(coupon.value(), value);
    }
}
