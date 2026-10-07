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

//! HyperLogLog sketches for estimating the number of distinct values.
//!
//! Use [`HllSketch`] to collect values and [`HllUnion`] to combine sketches.
//! [`HllType`] controls register storage; `lg_k` controls the register count and accuracy.
//!
//! Serialized sketches are compatible with Apache DataSketches in Java and C++.
//! Cross-language updates must also use compatible hashing; see
//! [`hash::value`](crate::hash::value).
//!
//! # Usage
//!
//! ```
//! use datasketches::common::NumStdDev;
//! use datasketches::hll::HllSketch;
//! use datasketches::hll::HllType;
//!
//! let mut sketch = HllSketch::new(12, HllType::Hll8).unwrap();
//! sketch.update("apple");
//! let upper = sketch.upper_bound(NumStdDev::Two);
//! assert!(upper >= sketch.estimate());
//! ```
//!
//! # Union
//!
//! ```
//! use datasketches::hll::HllSketch;
//! use datasketches::hll::HllType;
//! use datasketches::hll::HllUnion;
//!
//! let mut left = HllSketch::new(10, HllType::Hll8).unwrap();
//! let mut right = HllSketch::new(10, HllType::Hll8).unwrap();
//! left.update("apple");
//! right.update("banana");
//!
//! let mut union = HllUnion::new(10).unwrap();
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

/// Storage representation for HLL registers.
///
/// For the same `lg_k` and input, all three types produce the same estimates.
/// They trade memory usage against update speed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HllType {
    /// Uses 4 bits per HLL bucket and has the smallest storage footprint.
    ///
    /// It is generally the slowest representation to update.
    Hll4,
    /// Uses 6 bits per HLL bucket and provides a middle ground for storage and update speed.
    Hll6,
    /// Uses 8 bits per HLL bucket and has the largest storage footprint.
    ///
    /// It is generally the fastest representation to update.
    Hll8,
}

const KEY_BITS_26: u32 = 26;
const KEY_MASK_26: u32 = (1 << KEY_BITS_26) - 1;

const COUPON_RSE_FACTOR: f64 = 0.409; // At transition point not the asymptote
const COUPON_RSE: f64 = COUPON_RSE_FACTOR / (1 << 13) as f64;

const RESIZE_NUMERATOR: u32 = 3; // Resize at 3/4 = 75% load factor
const RESIZE_DENOMINATOR: u32 = 4;

/// A reusable hash of an input value for HLL sketches.
///
/// Compute a coupon once with [`Coupon::from_value`] and pass it to
/// [`HllSketch::update_with_coupon`] on sketches with any `lg_k` or [`HllType`].
///
/// # Examples
///
/// ```
/// use datasketches::hll::Coupon;
/// use datasketches::hll::HllSketch;
/// use datasketches::hll::HllType;
///
/// let c = Coupon::from_value("hello");
///
/// let mut sketch1 = HllSketch::new(10, HllType::Hll8).unwrap();
/// let mut sketch2 = HllSketch::new(12, HllType::Hll8).unwrap();
/// sketch1.update_with_coupon(c);
/// sketch2.update_with_coupon(c);
///
/// assert!(sketch1.estimate() >= 1.0);
/// assert!(sketch2.estimate() >= 1.0);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Coupon(u32); // Upper six bits: register value; lower 26 bits: slot.

impl Coupon {
    /// Sentinel value indicating an empty coupon slot.
    const EMPTY: Self = Coupon(0);

    #[inline(always)]
    fn is_empty(self) -> bool {
        self == Self::EMPTY
    }

    #[inline(always)]
    fn raw(self) -> u32 {
        self.0
    }

    /// Computes the HLL coupon for a hashable value.
    ///
    /// Use [`hash::value`](crate::hash::value) wrappers when another DataSketches
    /// implementation requires a specific value hashing strategy.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::hll::Coupon;
    /// use datasketches::hll::HllSketch;
    /// use datasketches::hll::HllType;
    ///
    /// let coupon = Coupon::from_value("apple");
    /// let mut sketch = HllSketch::new(10, HllType::Hll8).unwrap();
    /// sketch.update_with_coupon(coupon);
    /// assert!(sketch.estimate() >= 1.0);
    /// ```
    #[inline(always)]
    pub fn from_value<T: Hash>(value: T) -> Self {
        let mut hasher = MurmurHash3X64128::default();
        value.hash(&mut hasher);
        let (lo, hi) = hasher.finish128();

        let addr26 = lo as u32 & KEY_MASK_26;
        let lz = hi.leading_zeros();
        // Register values must fit in six bits; zero is reserved for empty slots.
        let capped = lz.min(62);
        let value = capped + 1;

        Coupon((value << KEY_BITS_26) | addr26)
    }

    #[inline(always)]
    fn pack(slot: u32, value: u8) -> Self {
        Coupon(((value as u32) << KEY_BITS_26) | (slot & KEY_MASK_26))
    }

    #[inline(always)]
    fn slot(self) -> u32 {
        self.0 & KEY_MASK_26
    }

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
