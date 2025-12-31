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

use std::hash::Hash;

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::MurmurHash3X64128;

// Serialization constants (compatible with datasketches-cpp)
const PREAMBLE_LONGS_EMPTY: u8 = 3;
const PREAMBLE_LONGS_STANDARD: u8 = 4;
const FAMILY_ID: u8 = 21; // Bloom filter family ID (same as C++)
const SERIAL_VERSION: u8 = 1;
const EMPTY_FLAG_MASK: u8 = 1 << 2;

const MIN_NUM_BITS: u64 = 64;
const MAX_NUM_BITS: u64 = (1u64 << 35) - 64; // ~32 GB - reasonable limit

/// A Bloom filter for probabilistic set membership testing.
///
/// Provides fast membership queries with:
/// - No false negatives (inserted items always return `true`)
/// - Tunable false positive rate
/// - Constant space usage
///
/// Use [`BloomFilterBuilder`] to construct instances.
#[derive(Debug, Clone, PartialEq)]
pub struct BloomFilter {
    /// Hash seed for all hash functions
    seed: u64,
    /// Number of hash functions to use (k)
    num_hashes: u16,
    /// Total number of bits in the filter (m)
    capacity_bits: u64,
    /// Count of bits set to 1 (for statistics)
    num_bits_set: u64,
    /// Bit array packed into u64 words
    /// Length = ceil(capacity_bits / 64)
    bit_array: Vec<u64>,
}

impl BloomFilter {
    /// Returns a builder for creating a Bloom filter.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::bloom::BloomFilter;
    /// use datasketches::bloom::BloomFilterBuilder;
    ///
    /// // By accuracy (recommended)
    /// let filter = BloomFilterBuilder::with_accuracy(1000, 0.01).build();
    ///
    /// // By size (manual)
    /// let filter = BloomFilterBuilder::with_size(10_000, 7).build();
    /// ```
    pub fn builder() -> BloomFilterBuilder {
        BloomFilterBuilder::default()
    }

    // ========================================================================
    // Query Operations
    // ========================================================================

    /// Tests whether an item is possibly in the set.
    ///
    /// Returns:
    /// - `true`: Item was **possibly** inserted (or false positive)
    /// - `false`: Item was **definitely not** inserted
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::bloom::BloomFilterBuilder;
    /// let mut filter = BloomFilterBuilder::with_accuracy(100, 0.01).build();
    /// filter.insert("apple");
    ///
    /// assert!(filter.contains(&"apple")); // true - was inserted
    /// assert!(!filter.contains(&"grape")); // false - never inserted (probably)
    /// ```
    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        if self.is_empty() {
            return false;
        }

        let (h1, h2) = self.compute_hash(item);
        self.check_bits(h1, h2)
    }

    /// Tests and inserts an item in a single operation.
    ///
    /// Returns whether the item was possibly already in the set before insertion.
    /// This is more efficient than calling `contains()` then `insert()` separately.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::bloom::BloomFilterBuilder;
    /// let mut filter = BloomFilterBuilder::with_accuracy(100, 0.01).build();
    ///
    /// let was_present = filter.contains_and_insert(&"apple");
    /// assert!(!was_present); // First insertion
    ///
    /// let was_present = filter.contains_and_insert(&"apple");
    /// assert!(was_present); // Now it's in the set
    /// ```
    pub fn contains_and_insert<T: Hash>(&mut self, item: &T) -> bool {
        let (h1, h2) = self.compute_hash(item);
        let was_present = self.check_bits(h1, h2);
        self.set_bits(h1, h2);
        was_present
    }

    // ========================================================================
    // Update Operations
    // ========================================================================

    /// Inserts an item into the filter.
    ///
    /// After insertion, `contains(item)` will always return `true`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::bloom::BloomFilterBuilder;
    /// let mut filter = BloomFilterBuilder::with_accuracy(100, 0.01).build();
    ///
    /// filter.insert("apple");
    /// filter.insert(42_u64);
    /// filter.insert(&[1, 2, 3]);
    ///
    /// assert!(filter.contains(&"apple"));
    /// ```
    pub fn insert<T: Hash>(&mut self, item: T) {
        let (h1, h2) = self.compute_hash(&item);
        self.set_bits(h1, h2);
    }

    /// Resets the filter to its initial empty state.
    ///
    /// Clears all bits while preserving capacity and configuration.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::bloom::BloomFilterBuilder;
    /// let mut filter = BloomFilterBuilder::with_accuracy(100, 0.01).build();
    /// filter.insert("apple");
    /// assert!(!filter.is_empty());
    ///
    /// filter.reset();
    /// assert!(filter.is_empty());
    /// assert!(!filter.contains(&"apple"));
    /// ```
    pub fn reset(&mut self) {
        for word in &mut self.bit_array {
            *word = 0;
        }
        self.num_bits_set = 0;
    }

    // ========================================================================
    // Set Operations
    // ========================================================================

    /// Merges another filter into this one via bitwise OR (union).
    ///
    /// After merging, this filter will recognize items from either filter
    /// (plus any false positives from either).
    ///
    /// # Panics
    ///
    /// Panics if the filters are not compatible (different size, hashes, or seed).
    /// Use [`is_compatible()`](Self::is_compatible) to check first.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::bloom::BloomFilterBuilder;
    /// let mut f1 = BloomFilterBuilder::with_accuracy(100, 0.01)
    ///     .seed(123)
    ///     .build();
    /// let mut f2 = BloomFilterBuilder::with_accuracy(100, 0.01)
    ///     .seed(123)
    ///     .build();
    ///
    /// f1.insert("a");
    /// f2.insert("b");
    ///
    /// f1.union(&f2);
    /// assert!(f1.contains(&"a"));
    /// assert!(f1.contains(&"b"));
    /// ```
    pub fn union(&mut self, other: &BloomFilter) {
        assert!(
            self.is_compatible(other),
            "Cannot union incompatible Bloom filters"
        );

        for (word, other_word) in self.bit_array.iter_mut().zip(&other.bit_array) {
            *word |= *other_word;
        }

        // Recount set bits (could be optimized)
        self.recount_bits_set();
    }

    /// Intersects this filter with another via bitwise AND.
    ///
    /// After intersection, this filter will recognize only items present in both
    /// filters (plus false positives).
    ///
    /// # Panics
    ///
    /// Panics if the filters are not compatible (different size, hashes, or seed).
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::bloom::BloomFilterBuilder;
    /// let mut f1 = BloomFilterBuilder::with_accuracy(100, 0.01)
    ///     .seed(123)
    ///     .build();
    /// let mut f2 = BloomFilterBuilder::with_accuracy(100, 0.01)
    ///     .seed(123)
    ///     .build();
    ///
    /// f1.insert("a");
    /// f1.insert("b");
    /// f2.insert("b");
    /// f2.insert("c");
    ///
    /// f1.intersect(&f2);
    /// assert!(f1.contains(&"b")); // In both
    /// // "a" and "c" likely return false now
    /// ```
    pub fn intersect(&mut self, other: &BloomFilter) {
        assert!(
            self.is_compatible(other),
            "Cannot intersect incompatible Bloom filters"
        );

        for (word, other_word) in self.bit_array.iter_mut().zip(&other.bit_array) {
            *word &= *other_word;
        }

        self.recount_bits_set();
    }

    /// Inverts all bits in the filter.
    ///
    /// This approximately inverts the notion of set membership, though the false
    /// positive guarantees no longer hold in a well-defined way.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::bloom::BloomFilterBuilder;
    /// let mut filter = BloomFilterBuilder::with_accuracy(100, 0.01).build();
    /// filter.insert("apple");
    ///
    /// filter.invert();
    /// // Now "apple" probably returns false, and most other items return true
    /// ```
    pub fn invert(&mut self) {
        for word in &mut self.bit_array {
            *word = !*word;
        }

        // Mask off excess bits in the last word
        let excess_bits = self.capacity_bits % 64;
        if excess_bits != 0 {
            let last_idx = self.bit_array.len() - 1;
            let mask = (1u64 << excess_bits) - 1;
            self.bit_array[last_idx] &= mask;
        }

        self.recount_bits_set();
    }

    // ========================================================================
    // Statistics and Properties
    // ========================================================================

    /// Returns whether the filter is empty (no items inserted).
    pub fn is_empty(&self) -> bool {
        self.num_bits_set == 0
    }

    /// Returns the number of bits set to 1.
    ///
    /// Useful for monitoring filter saturation.
    pub fn bits_used(&self) -> u64 {
        self.num_bits_set
    }

    /// Returns the total number of bits in the filter (capacity).
    pub fn capacity(&self) -> u64 {
        self.capacity_bits
    }

    /// Returns the number of hash functions used.
    pub fn num_hashes(&self) -> u16 {
        self.num_hashes
    }

    /// Returns the hash seed.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns the current load factor (fraction of bits set).
    ///
    /// Values near 0.5 indicate the filter is approaching saturation.
    /// Values above 0.5 indicate degraded false positive rates.
    pub fn load_factor(&self) -> f64 {
        self.num_bits_set as f64 / self.capacity_bits as f64
    }

    /// Estimates the current false positive probability.
    ///
    /// Based on the formula: `(1 - e^(-k*n/m))^k`
    /// where:
    /// - k = num_hashes
    /// - n = estimated insertions (from bits_used)
    /// - m = capacity_bits
    ///
    /// This is approximate and assumes uniform bit distribution.
    pub fn estimated_fpp(&self) -> f64 {
        let k = self.num_hashes as f64;
        let load = self.load_factor();

        // FPP ≈ (1 - e^(-k*load))^k
        // Using load factor as approximation since exact insertion count is unknown
        (1.0 - (-k * load).exp()).powf(k)
    }

    /// Checks if two filters are compatible for merging.
    ///
    /// Filters are compatible if they have the same:
    /// - Capacity (number of bits)
    /// - Number of hash functions
    /// - Seed
    pub fn is_compatible(&self, other: &BloomFilter) -> bool {
        self.capacity_bits == other.capacity_bits
            && self.num_hashes == other.num_hashes
            && self.seed == other.seed
    }

    // ========================================================================
    // Serialization
    // ========================================================================

    /// Serializes the filter to a byte vector.
    ///
    /// The format is compatible with datasketches-cpp and datasketches-java.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::bloom::BloomFilterBuilder;
    /// let mut filter = BloomFilterBuilder::with_accuracy(100, 0.01).build();
    /// filter.insert("test");
    ///
    /// let bytes = filter.serialize();
    /// let restored = BloomFilter::deserialize(&bytes).unwrap();
    /// assert!(restored.contains(&"test"));
    /// ```
    pub fn serialize(&self) -> Vec<u8> {
        let is_empty = self.is_empty();
        let preamble_longs = if is_empty {
            PREAMBLE_LONGS_EMPTY
        } else {
            PREAMBLE_LONGS_STANDARD
        };

        let capacity = 8 * preamble_longs as usize
            + if is_empty {
                0
            } else {
                self.bit_array.len() * 8
            };
        let mut bytes = SketchBytes::with_capacity(capacity);

        // Preamble
        bytes.write_u8(preamble_longs);
        bytes.write_u8(SERIAL_VERSION);
        bytes.write_u8(FAMILY_ID);
        bytes.write_u8(0); // reserved
        bytes.write_u8(0); // reserved
        bytes.write_u8(if is_empty { EMPTY_FLAG_MASK } else { 0 });
        bytes.write_u16_le(self.num_hashes);

        bytes.write_u64_le(self.seed);
        bytes.write_u64_le(self.capacity_bits);

        if !is_empty {
            bytes.write_u64_le(self.num_bits_set);

            // Bit array
            for &word in &self.bit_array {
                bytes.write_u64_le(word);
            }
        }

        bytes.into_bytes()
    }

    /// Deserializes a filter from bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The data is truncated or corrupted
    /// - The family ID doesn't match (not a Bloom filter)
    /// - The serial version is unsupported
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::bloom::BloomFilterBuilder;
    /// let original = BloomFilterBuilder::with_accuracy(100, 0.01).build();
    /// let bytes = original.serialize();
    ///
    /// let restored = BloomFilter::deserialize(&bytes).unwrap();
    /// assert_eq!(original, restored);
    /// ```
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        let mut cursor = SketchSlice::new(bytes);

        // Read preamble
        let preamble_longs = cursor
            .read_u8()
            .map_err(|_| Error::insufficient_data("preamble_longs"))?;
        let serial_version = cursor
            .read_u8()
            .map_err(|_| Error::insufficient_data("serial_version"))?;
        let family_id = cursor
            .read_u8()
            .map_err(|_| Error::insufficient_data("family_id"))?;

        // Validate
        if family_id != FAMILY_ID {
            return Err(Error::invalid_family(FAMILY_ID, family_id, "BloomFilter"));
        }
        if serial_version != SERIAL_VERSION {
            return Err(Error::unsupported_serial_version(
                SERIAL_VERSION,
                serial_version,
            ));
        }
        if preamble_longs != PREAMBLE_LONGS_EMPTY && preamble_longs != PREAMBLE_LONGS_STANDARD {
            return Err(Error::invalid_preamble_longs(
                PREAMBLE_LONGS_STANDARD,
                preamble_longs,
            ));
        }

        // Skip reserved bytes
        cursor
            .read_u8()
            .map_err(|_| Error::insufficient_data("reserved1"))?;
        cursor
            .read_u8()
            .map_err(|_| Error::insufficient_data("reserved2"))?;

        let flags = cursor
            .read_u8()
            .map_err(|_| Error::insufficient_data("flags"))?;
        let is_empty = (flags & EMPTY_FLAG_MASK) != 0;

        let num_hashes = cursor
            .read_u16_le()
            .map_err(|_| Error::insufficient_data("num_hashes"))?;
        let seed = cursor
            .read_u64_le()
            .map_err(|_| Error::insufficient_data("seed"))?;
        let capacity_bits = cursor
            .read_u64_le()
            .map_err(|_| Error::insufficient_data("capacity_bits"))?;

        let num_words = capacity_bits.div_ceil(64) as usize;
        let mut bit_array = vec![0u64; num_words];
        let num_bits_set;

        if is_empty {
            num_bits_set = 0;
        } else {
            num_bits_set = cursor
                .read_u64_le()
                .map_err(|_| Error::insufficient_data("num_bits_set"))?;

            for word in &mut bit_array {
                *word = cursor
                    .read_u64_le()
                    .map_err(|_| Error::insufficient_data("bit_array"))?;
            }
        }

        Ok(BloomFilter {
            seed,
            num_hashes,
            capacity_bits,
            num_bits_set,
            bit_array,
        })
    }

    // ========================================================================
    // Internal Helpers
    // ========================================================================

    /// Computes the two base hash values using MurmurHash3.
    fn compute_hash<T: Hash>(&self, item: &T) -> (u64, u64) {
        let mut hasher = MurmurHash3X64128::with_seed(self.seed);
        item.hash(&mut hasher);
        hasher.finish128()
    }

    /// Checks if all k bits are set for the given hash values.
    fn check_bits(&self, h1: u64, h2: u64) -> bool {
        for i in 0..self.num_hashes {
            let bit_index = self.compute_bit_index(h1, h2, i);
            if !self.get_bit(bit_index) {
                return false;
            }
        }
        true
    }

    /// Sets all k bits for the given hash values.
    fn set_bits(&mut self, h1: u64, h2: u64) {
        for i in 0..self.num_hashes {
            let bit_index = self.compute_bit_index(h1, h2, i);
            self.set_bit(bit_index);
        }
    }

    /// Computes a bit index using double hashing (Kirsch-Mitzenmacher).
    /// Formula: (h1 + i * h2) mod capacity_bits
    fn compute_bit_index(&self, h1: u64, h2: u64, i: u16) -> u64 {
        // Use wrapping arithmetic to handle overflow
        let hash = h1.wrapping_add(u64::from(i).wrapping_mul(h2));
        hash % self.capacity_bits
    }

    /// Gets the value of a single bit.
    fn get_bit(&self, bit_index: u64) -> bool {
        let word_index = (bit_index / 64) as usize;
        let bit_offset = bit_index % 64;
        let mask = 1u64 << bit_offset;
        (self.bit_array[word_index] & mask) != 0
    }

    /// Sets a single bit and updates the count if it wasn't already set.
    fn set_bit(&mut self, bit_index: u64) {
        let word_index = (bit_index / 64) as usize;
        let bit_offset = bit_index % 64;
        let mask = 1u64 << bit_offset;

        if (self.bit_array[word_index] & mask) == 0 {
            self.bit_array[word_index] |= mask;
            self.num_bits_set += 1;
        }
    }

    /// Recounts all set bits (used after set operations).
    fn recount_bits_set(&mut self) {
        self.num_bits_set = self
            .bit_array
            .iter()
            .map(|word| word.count_ones() as u64)
            .sum();
    }
}

// ============================================================================
// Builder
// ============================================================================

/// Builder for creating [`BloomFilter`] instances.
///
/// Provides two construction modes:
/// - [`with_accuracy()`](Self::with_accuracy): Specify target items and false positive rate
///   (recommended)
/// - [`with_size()`](Self::with_size): Specify exact bit count and hash functions (manual)
#[derive(Debug, Clone)]
pub struct BloomFilterBuilder {
    num_bits: Option<u64>,
    num_hashes: Option<u16>,
    seed: u64,
}

impl Default for BloomFilterBuilder {
    fn default() -> Self {
        BloomFilterBuilder {
            num_bits: None,
            num_hashes: None,
            seed: DEFAULT_UPDATE_SEED,
        }
    }
}

impl BloomFilterBuilder {
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
    /// Panics if `max_items` is 0 or `fpp` is not in (0.0, 1.0).
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
            fpp > 0.0 && fpp < 1.0,
            "fpp must be between 0.0 and 1.0 (exclusive)"
        );

        let num_bits = Self::suggest_num_bits(max_items, fpp);
        let num_hashes = Self::suggest_num_hashes_from_accuracy(max_items, num_bits);

        BloomFilterBuilder {
            num_bits: Some(num_bits),
            num_hashes: Some(num_hashes),
            seed: DEFAULT_UPDATE_SEED,
        }
    }

    /// Creates a builder with manual size specification.
    ///
    /// Use this when you want precise control over the filter size,
    /// or when working with pre-calculated parameters.
    ///
    /// # Arguments
    ///
    /// - `num_bits`: Total number of bits in the filter
    /// - `num_hashes`: Number of hash functions to use
    ///
    /// # Panics
    ///
    /// Panics if parameters are invalid (see [`validate_params()`](Self::validate_params)).
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::bloom::BloomFilterBuilder;
    /// let filter = BloomFilterBuilder::with_size(10_000, 7).build();
    /// ```
    pub fn with_size(num_bits: u64, num_hashes: u16) -> Self {
        Self::validate_params(num_bits, num_hashes);

        BloomFilterBuilder {
            num_bits: Some(num_bits),
            num_hashes: Some(num_hashes),
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
        let num_bits = self
            .num_bits
            .expect("Must call with_accuracy() or with_size() before build()");
        let num_hashes = self
            .num_hashes
            .expect("Must call with_accuracy() or with_size() before build()");

        let num_words = num_bits.div_ceil(64) as usize;
        let bit_array = vec![0u64; num_words];

        BloomFilter {
            seed: self.seed,
            num_hashes,
            capacity_bits: num_bits,
            num_bits_set: 0,
            bit_array,
        }
    }

    // ========================================================================
    // Static Suggestion Methods
    // ========================================================================

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

        // Round up to multiple of 64 for efficiency
        let bits = bits.div_ceil(64) * 64;

        bits.clamp(MIN_NUM_BITS, MAX_NUM_BITS)
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

        let k = (m / n * std::f64::consts::LN_2).round();

        (k as u16).clamp(1, 100) // Reasonable bounds
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
        let k = -fpp.log2();
        (k.round() as u16).clamp(1, 100)
    }

    /// Validates builder parameters.
    fn validate_params(num_bits: u64, num_hashes: u16) {
        assert!(
            num_bits >= MIN_NUM_BITS,
            "num_bits must be at least {}",
            MIN_NUM_BITS
        );
        assert!(
            num_bits <= MAX_NUM_BITS,
            "num_bits must not exceed {}",
            MAX_NUM_BITS
        );
        assert!(num_hashes > 0, "num_hashes must be at least 1");
        assert!(num_hashes <= 100, "num_hashes must not exceed 100");
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_with_accuracy() {
        let filter = BloomFilterBuilder::with_accuracy(1000, 0.01).build();
        assert!(filter.capacity() >= 9000);
        assert_eq!(filter.num_hashes(), 7);
        assert!(filter.is_empty());
    }

    #[test]
    fn test_builder_with_size() {
        let filter = BloomFilterBuilder::with_size(1024, 5).build();
        assert_eq!(filter.capacity(), 1024);
        assert_eq!(filter.num_hashes(), 5);
    }

    #[test]
    fn test_insert_and_contains() {
        let mut filter = BloomFilterBuilder::with_accuracy(100, 0.01).build();

        assert!(!filter.contains(&"apple"));
        filter.insert("apple");
        assert!(filter.contains(&"apple"));
        assert!(!filter.is_empty());
    }

    #[test]
    fn test_contains_and_insert() {
        let mut filter = BloomFilterBuilder::with_accuracy(100, 0.01).build();

        let was_present = filter.contains_and_insert(&42_u64);
        assert!(!was_present);

        let was_present = filter.contains_and_insert(&42_u64);
        assert!(was_present);
    }

    #[test]
    fn test_reset() {
        let mut filter = BloomFilterBuilder::with_accuracy(100, 0.01).build();
        filter.insert("test");
        assert!(!filter.is_empty());

        filter.reset();
        assert!(filter.is_empty());
        assert!(!filter.contains(&"test"));
    }

    #[test]
    fn test_union() {
        let mut f1 = BloomFilterBuilder::with_accuracy(100, 0.01)
            .seed(123)
            .build();
        let mut f2 = BloomFilterBuilder::with_accuracy(100, 0.01)
            .seed(123)
            .build();

        f1.insert("a");
        f2.insert("b");

        f1.union(&f2);
        assert!(f1.contains(&"a"));
        assert!(f1.contains(&"b"));
    }

    #[test]
    fn test_intersect() {
        let mut f1 = BloomFilterBuilder::with_accuracy(100, 0.01)
            .seed(123)
            .build();
        let mut f2 = BloomFilterBuilder::with_accuracy(100, 0.01)
            .seed(123)
            .build();

        f1.insert("a");
        f1.insert("b");
        f2.insert("b");
        f2.insert("c");

        f1.intersect(&f2);
        assert!(f1.contains(&"b"));
    }

    #[test]
    fn test_serialize_deserialize_empty() {
        let filter = BloomFilterBuilder::with_accuracy(100, 0.01).build();
        let bytes = filter.serialize();
        let restored = BloomFilter::deserialize(&bytes).unwrap();

        assert_eq!(filter, restored);
    }

    #[test]
    fn test_serialize_deserialize_with_data() {
        let mut filter = BloomFilterBuilder::with_accuracy(100, 0.01).build();
        filter.insert("test");
        filter.insert(42_u64);

        let bytes = filter.serialize();
        let restored = BloomFilter::deserialize(&bytes).unwrap();

        assert_eq!(filter, restored);
        assert!(restored.contains(&"test"));
        assert!(restored.contains(&42_u64));
    }

    #[test]
    fn test_statistics() {
        let mut filter = BloomFilterBuilder::with_size(1000, 5).build();
        assert_eq!(filter.bits_used(), 0);
        assert_eq!(filter.load_factor(), 0.0);

        filter.insert("test");
        assert!(filter.bits_used() > 0);
        assert!(filter.load_factor() > 0.0);
        assert!(filter.estimated_fpp() > 0.0);
    }

    #[test]
    fn test_is_compatible() {
        let f1 = BloomFilterBuilder::with_accuracy(100, 0.01)
            .seed(123)
            .build();
        let f2 = BloomFilterBuilder::with_accuracy(100, 0.01)
            .seed(123)
            .build();
        let f3 = BloomFilterBuilder::with_accuracy(100, 0.01)
            .seed(456)
            .build();

        assert!(f1.is_compatible(&f2));
        assert!(!f1.is_compatible(&f3));
    }

    #[test]
    #[should_panic(expected = "max_items must be greater than 0")]
    fn test_invalid_max_items() {
        BloomFilterBuilder::with_accuracy(0, 0.01);
    }

    #[test]
    #[should_panic(expected = "fpp must be between")]
    fn test_invalid_fpp() {
        BloomFilterBuilder::with_accuracy(100, 1.5);
    }
}
