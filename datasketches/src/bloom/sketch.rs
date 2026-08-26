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
use std::hash::Hasher;

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::ensure_preamble_longs_in_range;
use crate::codec::assert::ensure_serial_version_is;
use crate::codec::assert::insufficient_data;
use crate::codec::family::Family;
use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::XxHash64;

// Serialization constants
const SERIAL_VERSION: u8 = 1;
const EMPTY_FLAG_MASK: u8 = 1 << 2;

/// A Bloom filter for probabilistic set membership testing.
///
/// Provides fast membership queries with:
/// * No false negatives (inserted items always return `true`)
/// * Tunable false positive rate
/// * Constant space usage
///
/// These guarantees hold until [`invert()`](Self::invert) is called; see its documentation.
#[derive(Debug, Clone, PartialEq)]
pub struct BloomFilter {
    /// Hash seed for all hash functions
    seed: u64,
    /// Number of hash functions to use (k)
    num_hashes: u16,
    /// Count of bits set to 1 (for statistics)
    num_bits_set: u64,
    /// Bit array packed into u64 words
    bit_array: Box<[u64]>,
}

impl BloomFilter {
    /// Returns `true` if an item is possibly in the set.
    ///
    /// A `false` result means the item was definitely not inserted; a `true` result may be a false
    /// positive.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::bloom::BloomFilterBuilder;
    ///
    /// let mut filter = BloomFilterBuilder::with_accuracy(100, 0.01).build();
    /// filter.insert("apple");
    ///
    /// assert!(filter.contains(&"apple")); // true - possibly present (and known to be inserted here)
    /// assert!(!filter.contains(&"grape")); // false - definitely not present
    /// ```
    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        if self.is_empty() {
            return false;
        }

        let (h0, h1) = self.compute_hash(item);
        self.check_bits(h0, h1)
    }

    /// Returns `true` if an item was possibly present before inserting it.
    ///
    /// This is more efficient than calling `contains()` then `insert()` separately.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::bloom::BloomFilterBuilder;
    ///
    /// let mut filter = BloomFilterBuilder::with_accuracy(100, 0.01).build();
    ///
    /// let was_present = filter.contains_and_insert(&"apple");
    /// assert!(!was_present); // First insertion
    ///
    /// let was_present = filter.contains_and_insert(&"apple");
    /// assert!(was_present); // Now it's in the set
    /// ```
    pub fn contains_and_insert<T: Hash>(&mut self, item: &T) -> bool {
        let (h0, h1) = self.compute_hash(item);
        let was_present = self.check_bits(h0, h1);
        self.set_bits(h0, h1);
        was_present
    }

    /// Inserts an item into the filter.
    ///
    /// After insertion, `contains(item)` will always return `true`.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::bloom::BloomFilterBuilder;
    ///
    /// let mut filter = BloomFilterBuilder::with_accuracy(100, 0.01).build();
    ///
    /// filter.insert("apple");
    /// filter.insert(42_u64);
    /// filter.insert(&[1, 2, 3]);
    ///
    /// assert!(filter.contains(&"apple"));
    /// ```
    pub fn insert<T: Hash>(&mut self, item: T) {
        let (h0, h1) = self.compute_hash(&item);
        self.set_bits(h0, h1);
    }

    /// Resets the filter to its initial empty state.
    ///
    /// Clears all bits while preserving capacity and configuration.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::bloom::BloomFilterBuilder;
    ///
    /// let mut filter = BloomFilterBuilder::with_accuracy(100, 0.01).build();
    /// filter.insert("apple");
    /// assert!(!filter.is_empty());
    ///
    /// filter.reset();
    /// assert!(filter.is_empty());
    /// assert!(!filter.contains(&"apple"));
    /// ```
    pub fn reset(&mut self) {
        self.bit_array.fill(0);
        self.num_bits_set = 0
    }

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
    /// use datasketches::bloom::BloomFilterBuilder;
    ///
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

        // Count bits during union operation (single pass)
        let mut num_bits_set = 0;
        for (word, other_word) in self.bit_array.iter_mut().zip(&other.bit_array) {
            *word |= *other_word;
            num_bits_set += word.count_ones() as u64;
        }
        self.num_bits_set = num_bits_set;
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
    /// use datasketches::bloom::BloomFilterBuilder;
    ///
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

        // Count bits during intersect operation (single pass)
        let mut num_bits_set = 0;
        for (word, other_word) in self.bit_array.iter_mut().zip(&other.bit_array) {
            *word &= *other_word;
            num_bits_set += word.count_ones() as u64;
        }
        self.num_bits_set = num_bits_set;
    }

    /// Inverts all bits in the filter.
    ///
    /// This approximately inverts the notion of set membership. After inversion, neither the
    /// no-false-negative nor the false-positive guarantee holds: inserted items may return
    /// `false` from [`contains()`](Self::contains), and [`is_empty()`](Self::is_empty),
    /// [`bits_used()`](Self::bits_used), and [`load_factor()`](Self::load_factor) describe the
    /// raw bit state rather than the inserted items.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::bloom::BloomFilterBuilder;
    ///
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
        self.num_bits_set = self.capacity() as u64 - self.num_bits_set;
    }

    /// Returns whether no bits are set in the filter.
    ///
    /// In normal operation this means no items were inserted. After [`invert()`](Self::invert),
    /// it reports the raw bit state instead.
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
    pub fn capacity(&self) -> usize {
        self.bit_array.len() * 64
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
        self.num_bits_set as f64 / self.capacity() as f64
    }

    /// Returns the estimated current false positive probability.
    ///
    /// Uses the approximation: `load_factor^k`
    /// where:
    /// * load_factor = fraction of bits set (bits_used / capacity)
    /// * k = num_hashes
    ///
    /// This assumes uniform bit distribution and is more accurate than
    /// trying to estimate insertion count from the load factor.
    pub fn estimated_fpp(&self) -> f64 {
        let k = self.num_hashes as f64;
        let load = self.load_factor();

        // FPP ≈ load^k
        // This is the standard approximation when load factor is known directly
        load.powf(k)
    }

    /// Returns `true` if two filters are compatible for merging.
    ///
    /// Filters are compatible if they have the same:
    /// * Capacity (number of bits)
    /// * Number of hash functions
    /// * Seed
    pub fn is_compatible(&self, other: &Self) -> bool {
        self.bit_array.len() == other.bit_array.len()
            && self.num_hashes == other.num_hashes
            && self.seed == other.seed
    }

    /// Serializes the filter to a byte vector.
    ///
    /// The format is compatible with other Apache DataSketches implementations.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::bloom::BloomFilter;
    /// use datasketches::bloom::BloomFilterBuilder;
    ///
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
            Family::BLOOMFILTER.min_pre_longs
        } else {
            Family::BLOOMFILTER.max_pre_longs
        };

        let capacity = 8 * preamble_longs as usize
            + if is_empty {
                0
            } else {
                self.bit_array.len() * 8
            };
        let mut bytes = SketchBytes::with_capacity(capacity);

        // Preamble
        bytes.write_u8(preamble_longs); // Byte 0
        bytes.write_u8(SERIAL_VERSION); // Byte 1
        bytes.write_u8(Family::BLOOMFILTER.id); // Byte 2
        bytes.write_u8(if is_empty { EMPTY_FLAG_MASK } else { 0 }); // Byte 3: flags
        bytes.write_u16_le(self.num_hashes); // Bytes 4-5
        bytes.write_u16_le(0); // Bytes 6-7: unused

        bytes.write_u64_le(self.seed);

        // Bit array capacity is stored as number of 64-bit words (int32) + unused padding (uint32).
        let num_longs = self.bit_array.len() as i32;
        bytes.write_i32_le(num_longs);
        bytes.write_u32_le(0); // unused

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
    /// * The data is truncated or corrupted.
    /// * The family ID does not identify a Bloom filter.
    /// * The serial version is unsupported.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::bloom::BloomFilter;
    /// use datasketches::bloom::BloomFilterBuilder;
    ///
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
            .map_err(insufficient_data("preamble_longs"))?;
        let serial_version = cursor
            .read_u8()
            .map_err(insufficient_data("serial_version"))?;
        let family_id = cursor.read_u8().map_err(insufficient_data("family_id"))?;

        // Byte 3: flags byte (directly after family_id)
        let flags = cursor.read_u8().map_err(insufficient_data("flags"))?;

        // Validate
        Family::BLOOMFILTER.validate_id(family_id)?;
        ensure_serial_version_is(SERIAL_VERSION, serial_version)?;
        ensure_preamble_longs_in_range(
            Family::BLOOMFILTER.min_pre_longs..=Family::BLOOMFILTER.max_pre_longs,
            preamble_longs,
        )?;

        let is_empty = (flags & EMPTY_FLAG_MASK) != 0;

        // Bytes 4-5: num_hashes (u16)
        let num_hashes = cursor
            .read_u16_le()
            .map_err(insufficient_data("num_hashes"))?;
        if num_hashes == 0 || num_hashes > i16::MAX as u16 {
            return Err(Error::deserial(format!(
                "invalid num_hashes: expected [1, {}], got {}",
                i16::MAX,
                num_hashes
            )));
        }
        // Bytes 6-7: unused (u16)
        let _unused = cursor
            .read_u16_le()
            .map_err(insufficient_data("unused_header"))?;
        let seed = cursor.read_u64_le().map_err(insufficient_data("seed"))?;

        // Bit array capacity is stored as number of 64-bit words (int32) + unused padding (uint32).
        let num_longs = cursor
            .read_i32_le()
            .map_err(insufficient_data("num_longs"))?;
        let _unused = cursor.read_u32_le().map_err(insufficient_data("unused"))?;

        if num_longs <= 0 {
            return Err(Error::deserial(format!(
                "invalid num_longs: expected at least 1, got {}",
                num_longs
            )));
        }

        let num_words = num_longs as usize;
        if !is_empty {
            let payload_bytes = num_words
                .checked_add(1)
                .and_then(|words| words.checked_mul(size_of::<u64>()))
                .ok_or_else(|| Error::deserial("Bloom filter payload length overflows"))?;
            if payload_bytes > cursor.remaining().len() {
                return Err(Error::insufficient_data(format!(
                    "Bloom filter payload requires {payload_bytes} bytes, got {}",
                    cursor.remaining().len()
                )));
            }
        }
        let mut bit_array = vec![0u64; num_words].into_boxed_slice();
        let num_bits_set = if is_empty {
            0
        } else {
            let serialized_num_bits_set = cursor
                .read_u64_le()
                .map_err(insufficient_data("num_bits_set"))?;

            let mut count = 0;
            for word in &mut bit_array {
                *word = cursor
                    .read_u64_le()
                    .map_err(insufficient_data("bit_array"))?;
                count += word.count_ones() as u64;
            }
            if serialized_num_bits_set != u64::MAX && serialized_num_bits_set != count {
                return Err(Error::deserial(format!(
                    "invalid num_bits_set: expected {count}, got {serialized_num_bits_set}"
                )));
            }
            count
        };

        Ok(BloomFilter {
            seed,
            num_hashes,
            num_bits_set,
            bit_array,
        })
    }

    /// Computes the two base hash values using XXHash64.
    ///
    /// Uses a two-hash approach:
    /// * h0 = XXHash64(item, seed)
    /// * h1 = XXHash64(item, h0)
    fn compute_hash<T: Hash>(&self, item: &T) -> (u64, u64) {
        // First hash with the configured seed
        let mut hasher = XxHash64::with_seed(self.seed);
        item.hash(&mut hasher);
        let h0 = hasher.finish();

        // Second hash using h0 as the seed
        let mut hasher = XxHash64::with_seed(h0);
        item.hash(&mut hasher);
        let h1 = hasher.finish();

        (h0, h1)
    }

    /// Checks if all k bits are set for the given hash values.
    fn check_bits(&self, h0: u64, h1: u64) -> bool {
        for i in 1..=self.num_hashes {
            let bit_index = self.compute_bit_index(h0, h1, i);
            if !self.get_bit(bit_index) {
                return false;
            }
        }
        true
    }

    /// Sets all k bits for the given hash values.
    fn set_bits(&mut self, h0: u64, h1: u64) {
        for i in 1..=self.num_hashes {
            let bit_index = self.compute_bit_index(h0, h1, i);
            self.set_bit(bit_index);
        }
    }

    /// Computes a bit index using double hashing (Kirsch-Mitzenmacher).
    ///
    /// Formula:
    /// ```text
    /// hash_index = ((h0 + i * h1) >> 1) % capacity_bits
    /// ```
    ///
    /// The right shift by 1 improves bit-distribution. The index `i` is 1-based.
    fn compute_bit_index(&self, h0: u64, h1: u64, i: u16) -> usize {
        let hash = h0.wrapping_add(u64::from(i).wrapping_mul(h1)) as usize;
        (hash >> 1) % self.capacity()
    }

    /// Gets the value of a single bit.
    fn get_bit(&self, bit_index: usize) -> bool {
        let word_index = bit_index >> 6; // Equivalent to bit_index / 64
        let bit_offset = bit_index & 63; // Equivalent to bit_index % 64
        let mask = 1u64 << bit_offset;
        (self.bit_array[word_index] & mask) != 0
    }

    /// Sets a single bit and updates the count if it wasn't already set.
    fn set_bit(&mut self, bit_index: usize) {
        let word_index = bit_index >> 6; // Equivalent to bit_index / 64
        let bit_offset = bit_index & 63; // Equivalent to bit_index % 64
        let mask = 1u64 << bit_offset;

        if (self.bit_array[word_index] & mask) == 0 {
            self.bit_array[word_index] |= mask;
            self.num_bits_set += 1;
        }
    }

    /// Returns the estimated size of the filter in bytes.
    pub fn estimated_size(&self) -> usize {
        size_of::<Self>() + self.bit_array.len() * size_of::<u64>()
    }
}

/// Builder for creating [`BloomFilter`] instances.
///
/// Provides two construction modes:
/// * [`with_accuracy()`](Self::with_accuracy): Specify target items and false positive rate
///   (recommended)
/// * [`with_size()`](Self::with_size): Specify requested bit count and hash functions (manual)
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
    /// * `max_items`: Maximum expected number of distinct items.
    /// * `fpp`: Target false positive probability (for example, `0.01` for `1%`).
    ///
    /// # Panics
    ///
    /// Panics if `max_items` is `0` or `fpp` is outside `(0.0, 1.0]`.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::bloom::BloomFilterBuilder;
    ///
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
    /// * `num_bits`: Total number of bits in the filter.
    /// * `num_hashes`: Number of hash functions to use.
    ///
    /// # Panics
    ///
    /// Panics if any of:
    /// * `num_bits < Self::MIN_NUM_BITS` or `num_bits > Self::MAX_NUM_BITS`.
    /// * `num_hashes < Self::MIN_NUM_HASHES` or `num_hashes > Self::MAX_NUM_HASHES`.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::bloom::BloomFilterBuilder;
    ///
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
            "num_hashes must be between {} and {}, got {}",
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
    /// use datasketches::bloom::BloomFilterBuilder;
    ///
    /// let filter = BloomFilterBuilder::with_accuracy(100, 0.01)
    ///     .seed(12345)
    ///     .build();
    /// ```
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Builds the Bloom filter.
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
    /// use datasketches::bloom::BloomFilterBuilder;
    ///
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
    /// use datasketches::bloom::BloomFilterBuilder;
    ///
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
    /// use datasketches::bloom::BloomFilterBuilder;
    ///
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
