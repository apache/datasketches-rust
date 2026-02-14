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

use super::BloomFilter;
use crate::codec::family::Family;
use crate::hash::DEFAULT_UPDATE_SEED;

/// Builder for creating [`BloomFilter`] instances.
///
/// Provides two construction modes:
/// - [`with_accuracy()`](Self::with_accuracy): Specify target items and false positive rate
///   (recommended)
/// - [`with_size()`](Self::with_size): Specify requested bit count and hash functions (manual)
#[derive(Debug, Clone)]
pub struct BloomFilterBuilder {
    num_bits: u64,
    num_hashes: u16,
    seed: u64,
}

impl BloomFilterBuilder {
    /// Minimum allowed requested Bloom filter size, in bits.
    pub const MIN_NUM_BITS: u64 = 1;
    /// Maximum allowed requested Bloom filter size, in bits.
    ///
    /// Derived from serialization limits so the encoded sketch length fits in a signed 32-bit size
    /// field.
    pub const MAX_NUM_BITS: u64 = (i32::MAX as u64 - Family::BLOOMFILTER.max_pre_longs as u64) * 64;
    /// Minimum allowed number of hash functions.
    pub const MIN_NUM_HASHES: u16 = 1;
    /// Maximum allowed number of hash functions.
    pub const MAX_NUM_HASHES: u16 = i16::MAX as u16;

    /// Creates a builder with optimal parameters for a target accuracy.
    ///
    /// Automatically calculates the optimal number of bits and hash functions
    /// to achieve the desired false positive probability for a given number of items.
    ///
    /// # Arguments
    ///
    /// - `max_items`: Maximum expected number of distinct items
    /// - `fpp`: Target false positive probability (e.g., 0.01 for 1%)
    ///
    /// # Panics
    ///
    /// Panics if `max_items` is 0 or `fpp` is not in (0.0, 1.0].
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::bloom::BloomFilterBuilder;
    /// // Optimal for 10,000 items with 1% FPP
    /// let filter = BloomFilterBuilder::with_accuracy(10_000, 0.01)
    ///     .seed(42)
    ///     .build();
    /// ```
    pub fn with_accuracy(max_items: u64, fpp: f64) -> Self {
        assert!(max_items > 0, "max_items must be greater than 0");
        assert!(
            fpp > 0.0 && fpp <= 1.0,
            "fpp must be between 0.0 and 1.0 (inclusive of 1.0)"
        );

        let num_bits = Self::suggest_num_bits(max_items, fpp);
        let num_hashes = Self::suggest_num_hashes_from_accuracy(max_items, num_bits);

        BloomFilterBuilder {
            num_bits,
            num_hashes,
            seed: DEFAULT_UPDATE_SEED,
        }
    }

    /// Creates a builder with manual size specification.
    ///
    /// Use this when you want precise control over the requested filter size,
    /// or when working with pre-calculated parameters.
    ///
    /// The underlying storage is word-based, so the actual capacity is rounded
    /// up to the next multiple of 64 bits.
    ///
    /// # Arguments
    ///
    /// - `num_bits`: Total number of bits in the filter
    /// - `num_hashes`: Number of hash functions to use
    ///
    /// # Panics
    ///
    /// Panics if any of:
    /// - `num_bits` < [`Self::MIN_NUM_BITS`] or `num_bits` > [`Self::MAX_NUM_BITS`]
    /// - `num_hashes` < [`Self::MIN_NUM_HASHES`] or `num_hashes` > [`Self::MIN_NUM_HASHES`]
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::bloom::BloomFilterBuilder;
    /// let filter = BloomFilterBuilder::with_size(10_000, 7).build();
    /// ```
    pub fn with_size(num_bits: u64, num_hashes: u16) -> Self {
        assert!(
            (Self::MIN_NUM_BITS..=Self::MAX_NUM_BITS).contains(&num_bits),
            "num_bits must be between {} and {}, got {}",
            Self::MIN_NUM_BITS,
            Self::MAX_NUM_BITS,
            num_bits,
        );
        assert!(
            (Self::MIN_NUM_HASHES..=Self::MAX_NUM_HASHES).contains(&num_hashes),
            "num_bits must be between {} and {}, got {}",
            Self::MIN_NUM_HASHES,
            Self::MAX_NUM_HASHES,
            num_hashes
        );

        BloomFilterBuilder {
            num_bits,
            num_hashes,
            seed: DEFAULT_UPDATE_SEED,
        }
    }

    /// Sets a custom hash seed (default: 9001).
    ///
    /// **Important**: Filters with different seeds cannot be merged.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::bloom::BloomFilterBuilder;
    /// let filter = BloomFilterBuilder::with_accuracy(100, 0.01)
    ///     .seed(12345)
    ///     .build();
    /// ```
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Builds the Bloom filter.
    ///
    /// # Panics
    ///
    /// Panics if neither `with_accuracy()` nor `with_size()` was called.
    pub fn build(self) -> BloomFilter {
        let num_hashes = self.num_hashes;
        let num_words = self.num_bits.div_ceil(64) as usize;
        let bit_array = vec![0u64; num_words].into_boxed_slice();

        BloomFilter {
            seed: self.seed,
            num_hashes,
            num_bits_set: 0,
            bit_array,
        }
    }

    /// Suggests optimal number of bits given max items and target FPP.
    ///
    /// Formula: `m = -n * ln(p) / (ln(2)^2)`
    /// where n = max_items, p = fpp
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::bloom::BloomFilterBuilder;
    /// let bits = BloomFilterBuilder::suggest_num_bits(1000, 0.01);
    /// assert!(bits > 9000 && bits < 10000); // ~9585 bits
    /// ```
    pub fn suggest_num_bits(max_items: u64, fpp: f64) -> u64 {
        let n = max_items as f64;
        let p = fpp;
        let ln2_squared = std::f64::consts::LN_2 * std::f64::consts::LN_2;

        let bits = (-n * p.ln() / ln2_squared).ceil() as u64;

        bits.clamp(Self::MIN_NUM_BITS, Self::MAX_NUM_BITS)
    }

    /// Suggests optimal number of hash functions given max items and bit count.
    ///
    /// Formula: `k = (m/n) * ln(2)`
    /// where m = num_bits, n = max_items
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::bloom::BloomFilterBuilder;
    /// let hashes = BloomFilterBuilder::suggest_num_hashes_from_accuracy(1000, 10000);
    /// assert_eq!(hashes, 7); // Optimal k ≈ 6.93
    /// ```
    pub fn suggest_num_hashes_from_accuracy(max_items: u64, num_bits: u64) -> u16 {
        let m = num_bits as f64;
        let n = max_items as f64;

        // Ceil to avoid selecting too few hashes.
        let k = (m / n * std::f64::consts::LN_2).ceil();
        k.clamp(
            f64::from(Self::MIN_NUM_HASHES),
            f64::from(Self::MAX_NUM_HASHES),
        ) as u16
    }

    /// Suggests optimal number of hash functions from target FPP.
    ///
    /// Formula: `k = -log2(p)`
    /// where p = fpp
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::bloom::BloomFilterBuilder;
    /// let hashes = BloomFilterBuilder::suggest_num_hashes_from_fpp(0.01);
    /// assert_eq!(hashes, 7); // -log2(0.01) ≈ 6.64
    /// ```
    pub fn suggest_num_hashes_from_fpp(fpp: f64) -> u16 {
        // Ceil to avoid selecting too few hashes.
        let k = -fpp.log2();
        k.ceil().clamp(
            f64::from(Self::MIN_NUM_HASHES),
            f64::from(Self::MAX_NUM_HASHES),
        ) as u16
    }
}
