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

use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::ensure_preamble_longs_in_range;
use crate::codec::assert::ensure_serial_version_is;
use crate::codec::assert::insufficient_data;
use crate::codec::family::Family;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::XxHash64;

const SERIAL_VERSION: u8 = 1;
const EMPTY_FLAG_MASK: u8 = 1 << 2;
const MIN_NUM_BITS: u64 = 1;
const MAX_WORDS: u64 = i32::MAX as u64 - Family::BLOOMFILTER.max_pre_longs as u64;
const MAX_NUM_BITS: u64 = MAX_WORDS * 64;
const MAX_ALLOCATABLE_WORDS: u64 = (isize::MAX as usize / size_of::<u64>()) as u64;
const MIN_NUM_HASHES: u16 = 1;
const MAX_NUM_HASHES: u16 = i16::MAX as u16;

/// Builder for creating [`BloomFilter`] instances.
///
/// Construct a builder using [`with_accuracy`](Self::with_accuracy) (recommended) or
/// [`with_size`](Self::with_size), optionally set [`seed`](Self::seed), and finalize with
/// [`build`](Self::build). Parameters are validated when `build` is called.
#[derive(Debug, Clone)]
pub struct BloomFilterBuilder {
    mode: SizingMode,
    seed: u64,
}

#[derive(Debug, Clone)]
enum SizingMode {
    Accuracy { max_items: u64, fpp: f64 },
    Size { num_bits: u64, num_hashes: u16 },
}

impl BloomFilterBuilder {
    /// Creates a builder sized for `max_items` at target false-positive probability `fpp`, using
    /// the default hash seed (`9001`).
    ///
    /// `max_items` is a sizing assumption, not an insertion limit. Inserting more distinct items
    /// remains valid, though the actual false-positive probability can exceed `fpp`. An `fpp` of
    /// `1.0` creates the minimal allocation: 64 bits and 1 hash function.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::bloom::BloomFilterBuilder;
    ///
    /// let filter = BloomFilterBuilder::with_accuracy(10_000, 0.01)
    ///     .seed(42)
    ///     .build()
    ///     .unwrap();
    /// assert_eq!(filter.seed(), 42);
    /// ```
    pub fn with_accuracy(max_items: u64, fpp: f64) -> Self {
        Self {
            mode: SizingMode::Accuracy { max_items, fpp },
            seed: DEFAULT_UPDATE_SEED,
        }
    }

    /// Creates a builder with at least `num_bits` bits and `num_hashes` hash functions, using the
    /// default hash seed (`9001`).
    ///
    /// Capacity is rounded up to a multiple of 64 bits.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::bloom::BloomFilterBuilder;
    ///
    /// let filter = BloomFilterBuilder::with_size(10_000, 7).build().unwrap();
    /// assert_eq!(filter.capacity(), 10_048);
    /// assert_eq!(filter.num_hashes(), 7);
    /// ```
    pub fn with_size(num_bits: u64, num_hashes: u16) -> Self {
        Self {
            mode: SizingMode::Size {
                num_bits,
                num_hashes,
            },
            seed: DEFAULT_UPDATE_SEED,
        }
    }

    /// Sets the hash seed used by the constructed filter (default: `9001`).
    ///
    /// Filters with different seeds cannot be combined via set operations.
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Validates the builder configuration and allocates the [`BloomFilter`].
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](ErrorKind::InvalidArgument) if:
    /// * In accuracy mode, `max_items` is zero, `fpp` is outside `(0.0, 1.0]`, or the target
    ///   exceeds the serialized format or addressable allocation limits.
    /// * In size mode, `num_bits` is zero or exceeds the serialized format or addressable
    ///   allocation limits, or `num_hashes` is outside `1..=32767`.
    pub fn build(self) -> Result<BloomFilter, Error> {
        let (num_bits, num_hashes) = match self.mode {
            SizingMode::Accuracy { max_items, fpp } => {
                if max_items == 0 {
                    return Err(Error::invalid_argument("max_items must be greater than 0"));
                }
                if !(fpp > 0.0 && fpp <= 1.0) {
                    return Err(Error::invalid_argument("fpp must be in (0.0, 1.0]"));
                }

                let n = max_items as f64;
                let log_fpp = fpp.ln();

                // Kirsch and Mitzenmacher (2008), Section 2: https://doi.org/10.1002/rsa.20208
                // For n items, m bits and k independent hash probes, a bit stays unset with
                // probability (1 - 1/m)^(k*n) ≈ exp(-k*n/m), so p ≈ (1 - exp(-k*n/m))^k.
                // At the continuous optimum half the bits are set: p = 2^-k, or k = -ln(p)/ln(2).
                // Since fpp >= 2^-1074 for positive f64, k <= 1074 < MAX_NUM_HASHES.
                let num_hashes = (-log_fpp / std::f64::consts::LN_2).ceil().max(1.0) as u16;
                let k = f64::from(num_hashes);

                // With integer k fixed, solve for m = -k*n / ln(1 - p^(1/k)).
                let bit_set_probability = (log_fpp / k).exp();
                let log_unset_probability = (-bit_set_probability).ln_1p();
                let bits = (-k * n / log_unset_probability).ceil();
                if bits > MAX_NUM_BITS as f64 {
                    return Err(Error::invalid_argument(format!(
                        "target accuracy requires {bits:.0} bits, but at most {MAX_NUM_BITS} are supported"
                    )));
                }

                let num_bits = (bits as u64).max(MIN_NUM_BITS);
                (num_bits, num_hashes)
            }
            SizingMode::Size {
                num_bits,
                num_hashes,
            } => {
                if !(MIN_NUM_BITS..=MAX_NUM_BITS).contains(&num_bits) {
                    return Err(Error::invalid_argument(format!(
                        "num_bits must be within {MIN_NUM_BITS}..={MAX_NUM_BITS}, got {num_bits}"
                    )));
                }
                if !(MIN_NUM_HASHES..=MAX_NUM_HASHES).contains(&num_hashes) {
                    return Err(Error::invalid_argument(format!(
                        "num_hashes must be within {MIN_NUM_HASHES}..={MAX_NUM_HASHES}, got {num_hashes}"
                    )));
                }
                (num_bits, num_hashes)
            }
        };

        let bits = BitArray::new(num_bits)?;
        Ok(BloomFilter {
            seed: self.seed,
            num_hashes,
            bits,
        })
    }
}

/// A Bloom filter for probabilistic set membership testing.
///
/// A `false` result from [`contains`](Self::contains) proves that the item was not inserted; a
/// `true` result may be a false positive. Unlike resizing sketches, a Bloom filter has a fixed
/// bit capacity chosen at construction.
///
/// See [`BloomFilterBuilder`] and the [module level documentation](super) for more.
#[derive(Clone)]
pub struct BloomFilter {
    seed: u64,
    num_hashes: u16,
    bits: BitArray,
}

impl BloomFilter {
    /// Returns `true` if `item` is possibly in the filter, or `false` if it was definitely not
    /// inserted.
    #[inline]
    pub fn contains<T: Hash>(&self, item: T) -> bool {
        if self.is_empty() {
            return false;
        }
        let (h0, h1) = self.hash_pair(item);
        self.bits.contains(h0, h1, self.num_hashes)
    }

    /// Inserts `item` and returns `true` if all of its probe bits were already set.
    #[inline]
    pub fn contains_and_insert<T: Hash>(&mut self, item: T) -> bool {
        let (h0, h1) = self.hash_pair(item);
        self.bits.contains_and_insert(h0, h1, self.num_hashes)
    }

    /// Inserts `item` into the filter.
    #[inline]
    pub fn insert<T: Hash>(&mut self, item: T) {
        let (h0, h1) = self.hash_pair(item);
        self.bits.insert(h0, h1, self.num_hashes);
    }

    /// Clears all bits while preserving capacity, hash count, and seed.
    pub fn reset(&mut self) {
        self.bits.words.fill(0);
        self.bits.bits_used = 0;
    }

    /// Merges `other` into `self` via bitwise OR (union).
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](ErrorKind::InvalidArgument) without modifying `self` if
    /// `self` and `other` have different capacities, hash counts, or seeds.
    pub fn union(&mut self, other: &Self) -> Result<(), Error> {
        self.ensure_compatible(other)?;
        self.bits
            .combine(&other.bits, |word, other_word| *word |= other_word);
        Ok(())
    }

    /// Intersects `self` with `other` via bitwise AND.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](ErrorKind::InvalidArgument) without modifying `self` if
    /// `self` and `other` have different capacities, hash counts, or seeds.
    pub fn intersect(&mut self, other: &Self) -> Result<(), Error> {
        self.ensure_compatible(other)?;
        self.bits
            .combine(&other.bits, |word, other_word| *word &= other_word);
        Ok(())
    }

    /// Excludes `other` from `self` via bitwise AND-NOT (`self & !other`).
    ///
    /// Items inserted into `other` return `false` afterward; items inserted only into `self`
    /// remain present unless one of their bit positions is set in `other`.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](ErrorKind::InvalidArgument) without modifying `self` if
    /// `self` and `other` have different capacities, hash counts, or seeds.
    pub fn difference(&mut self, other: &Self) -> Result<(), Error> {
        self.ensure_compatible(other)?;
        self.bits
            .combine(&other.bits, |word, other_word| *word &= !other_word);
        Ok(())
    }

    /// Returns `true` if no bits are set in the filter.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bits.bits_used == 0
    }

    /// Returns the number of bits currently set to `1`.
    pub fn bits_used(&self) -> u64 {
        self.bits.bits_used
    }

    /// Returns the total bit capacity of the filter (always a positive multiple of 64).
    pub fn capacity(&self) -> u64 {
        self.bits.capacity()
    }

    /// Returns the number of hash probes performed per item (`k`).
    pub fn num_hashes(&self) -> u16 {
        self.num_hashes
    }

    /// Returns the hash seed used by this filter.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns the fraction of bits currently set (`bits_used / capacity`).
    pub fn load_factor(&self) -> f64 {
        self.bits_used() as f64 / self.capacity() as f64
    }

    /// Returns the estimated false-positive probability (`load_factor^num_hashes`).
    pub fn estimated_fpp(&self) -> f64 {
        self.load_factor().powi(i32::from(self.num_hashes))
    }

    /// Returns the estimated heap allocation of the filter's backing bit array in bytes.
    pub fn estimated_size(&self) -> usize {
        size_of_val::<[u64]>(&self.bits.words)
    }

    /// Serializes the filter in the DataSketches binary format.
    ///
    /// The serialized data can be read by other DataSketches implementations.
    pub fn serialize(&self) -> Vec<u8> {
        let is_empty = self.is_empty();
        let preamble_longs = if is_empty {
            Family::BLOOMFILTER.min_pre_longs
        } else {
            Family::BLOOMFILTER.max_pre_longs
        };
        let payload_bytes = if is_empty {
            0
        } else {
            size_of_val::<[u64]>(&self.bits.words)
        };
        let capacity = usize::from(preamble_longs) * size_of::<u64>() + payload_bytes;
        let mut bytes = SketchBytes::with_capacity(capacity);

        bytes.write_u8(preamble_longs);
        bytes.write_u8(SERIAL_VERSION);
        bytes.write_u8(Family::BLOOMFILTER.id);
        bytes.write_u8(if is_empty { EMPTY_FLAG_MASK } else { 0 });
        bytes.write_u16_le(self.num_hashes);
        bytes.write_u16_le(0);
        bytes.write_u64_le(self.seed);
        self.bits.serialize(&mut bytes);

        bytes.into_bytes()
    }

    /// Deserializes a filter from DataSketches binary bytes.
    ///
    /// Also accepts data serialized by other DataSketches implementations.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidData`](ErrorKind::InvalidData) if the header, bit count, or payload is
    /// malformed, truncated, or exceeds supported limits.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        let mut cursor = SketchSlice::new(bytes);

        let preamble_longs = cursor
            .read_u8()
            .map_err(insufficient_data("preamble_longs"))?;
        let serial_version = cursor
            .read_u8()
            .map_err(insufficient_data("serial_version"))?;
        let family_id = cursor.read_u8().map_err(insufficient_data("family_id"))?;
        let flags = cursor.read_u8().map_err(insufficient_data("flags"))?;

        Family::BLOOMFILTER.validate_id(family_id)?;
        ensure_serial_version_is(SERIAL_VERSION, serial_version)?;
        ensure_preamble_longs_in_range(
            Family::BLOOMFILTER.min_pre_longs..=Family::BLOOMFILTER.max_pre_longs,
            preamble_longs,
        )?;

        let is_empty = (flags & EMPTY_FLAG_MASK) != 0;
        let num_hashes = cursor
            .read_u16_le()
            .map_err(insufficient_data("num_hashes"))?;
        if !(MIN_NUM_HASHES..=MAX_NUM_HASHES).contains(&num_hashes) {
            return Err(Error::deserial(format!(
                "invalid num_hashes: expected [{MIN_NUM_HASHES}, {MAX_NUM_HASHES}], got {num_hashes}"
            )));
        }
        let _unused = cursor
            .read_u16_le()
            .map_err(insufficient_data("unused_header"))?;
        let seed = cursor.read_u64_le().map_err(insufficient_data("seed"))?;
        let bits = BitArray::deserialize(cursor, is_empty)?;

        Ok(Self {
            seed,
            num_hashes,
            bits,
        })
    }

    #[inline]
    fn ensure_compatible(&self, other: &Self) -> Result<(), Error> {
        if self.bits.words.len() != other.bits.words.len() {
            return Err(Error::invalid_argument(format!(
                "cannot combine Bloom filters with different capacities: expected {}, got {}",
                self.capacity(),
                other.capacity()
            )));
        }
        if self.num_hashes != other.num_hashes {
            return Err(Error::invalid_argument(format!(
                "cannot combine Bloom filters with different num_hashes: expected {}, got {}",
                self.num_hashes, other.num_hashes
            )));
        }
        if self.seed != other.seed {
            return Err(Error::invalid_argument(format!(
                "cannot combine Bloom filters with different seeds: expected {}, got {}",
                self.seed, other.seed
            )));
        }
        Ok(())
    }

    #[inline(always)]
    fn hash_pair<T: Hash>(&self, item: T) -> (u64, u64) {
        let mut hasher = XxHash64::with_seed(self.seed);
        item.hash(&mut hasher);
        let h0 = hasher.finish();

        let mut hasher = XxHash64::with_seed(h0);
        item.hash(&mut hasher);
        let h1 = hasher.finish();

        (h0, h1)
    }
}

impl fmt::Debug for BloomFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BloomFilter")
            .field("seed", &self.seed)
            .field("num_hashes", &self.num_hashes)
            .field("capacity", &self.capacity())
            .field("bits_used", &self.bits_used())
            .field("is_empty", &self.is_empty())
            .finish()
    }
}

#[derive(Clone)]
struct BitArray {
    bits_used: u64,
    multiplier: u64,
    shift: u32,
    words: Box<[u64]>,
}

impl BitArray {
    fn new(num_bits: u64) -> Result<Self, Error> {
        let num_words = num_bits.div_ceil(64);
        if !(1..=MAX_WORDS).contains(&num_words) {
            return Err(Error::invalid_argument(format!(
                "num_bits must be within {MIN_NUM_BITS}..={MAX_NUM_BITS}, got {num_bits}"
            )));
        }
        let word_len = allocatable_len(num_words, ErrorKind::InvalidArgument)?;
        let words = vec![0_u64; word_len].into_boxed_slice();
        Ok(Self::from_words(0, words))
    }

    fn deserialize(mut cursor: SketchSlice<'_>, is_empty: bool) -> Result<Self, Error> {
        let num_longs = cursor
            .read_i32_le()
            .map_err(insufficient_data("num_longs"))?;
        let _unused = cursor.read_u32_le().map_err(insufficient_data("unused"))?;

        if !(1..=MAX_WORDS as i32).contains(&num_longs) {
            return Err(Error::deserial(format!(
                "invalid num_longs: expected [1, {MAX_WORDS}], got {num_longs}"
            )));
        }
        let num_words = num_longs as u64;
        let word_len = allocatable_len(num_words, ErrorKind::InvalidData)?;

        if !is_empty {
            let payload_bytes = num_words
                .checked_add(1)
                .and_then(|words| words.checked_mul(size_of::<u64>() as u64))
                .and_then(|bytes| usize::try_from(bytes).ok())
                .ok_or_else(|| Error::deserial("Bloom filter payload length overflows"))?;
            let available_bytes = cursor.remaining().len();
            if available_bytes < payload_bytes {
                return Err(Error::insufficient_data_of(
                    "Bloom filter payload",
                    format_args!("expected {payload_bytes} bytes, got {available_bytes}"),
                ));
            }
        }

        let mut words = vec![0_u64; word_len].into_boxed_slice();
        let bits_used = if is_empty {
            0
        } else {
            let serialized_num_bits_set = cursor
                .read_u64_le()
                .map_err(insufficient_data("num_bits_set"))?;
            let mut ones = 0_u64;
            for word in &mut words {
                *word = cursor
                    .read_u64_le()
                    .map_err(insufficient_data("bit_array"))?;
                ones += u64::from(word.count_ones());
            }
            if serialized_num_bits_set != u64::MAX && serialized_num_bits_set != ones {
                return Err(Error::deserial(format!(
                    "invalid num_bits_set: expected {ones}, got {serialized_num_bits_set}"
                )));
            }
            ones
        };

        Ok(Self::from_words(bits_used, words))
    }

    fn serialize(&self, bytes: &mut SketchBytes) {
        bytes.write_i32_le(self.words.len() as i32);
        bytes.write_u32_le(0);
        if self.bits_used != 0 {
            bytes.write_u64_le(self.bits_used);
            for &word in &self.words {
                bytes.write_u64_le(word);
            }
        }
    }

    fn from_words(bits_used: u64, words: Box<[u64]>) -> Self {
        let capacity = (words.len() as u64) << 6;
        let (multiplier, shift) = reduction_parameters(capacity);
        Self {
            bits_used,
            multiplier,
            shift,
            words,
        }
    }

    #[inline]
    fn capacity(&self) -> u64 {
        (self.words.len() as u64) << 6
    }

    #[inline(always)]
    fn contains(&self, h0: u64, h1: u64, num_hashes: u16) -> bool {
        let capacity = self.capacity();
        let multiplier = self.multiplier;
        let shift = self.shift;
        let words = &*self.words;
        for i in 1..=num_hashes {
            let (word_index, mask) = probe_slot(h0, h1, i, capacity, multiplier, shift);
            if (words[word_index] & mask) == 0 {
                return false;
            }
        }
        true
    }

    #[inline(always)]
    fn insert(&mut self, h0: u64, h1: u64, num_hashes: u16) {
        let capacity = self.capacity();
        let multiplier = self.multiplier;
        let shift = self.shift;
        let words = &mut *self.words;
        let mut bits_added = 0_u64;
        for i in 1..=num_hashes {
            let (word_index, mask) = probe_slot(h0, h1, i, capacity, multiplier, shift);
            let word = &mut words[word_index];
            let was_set = (*word & mask) != 0;
            *word |= mask;
            bits_added += u64::from(!was_set);
        }
        self.bits_used += bits_added;
    }

    #[inline(always)]
    fn contains_and_insert(&mut self, mut h0: u64, h1: u64, num_hashes: u16) -> bool {
        let capacity = self.capacity();
        let multiplier = self.multiplier;
        let shift = self.shift;
        let words = &mut *self.words;
        for i in 0..num_hashes {
            let (word_index, mask) = probe_slot(h0, h1, 1, capacity, multiplier, shift);
            h0 = h0.wrapping_add(h1);
            if (words[word_index] & mask) == 0 {
                words[word_index] |= mask;
                self.bits_used += 1;
                // Continue after the first missing probe using the branchless insertion loop.
                self.insert(h0, h1, num_hashes - i - 1);
                return false;
            }
        }
        true
    }

    fn combine(&mut self, other: &Self, op: impl Fn(&mut u64, u64)) {
        debug_assert_eq!(self.words.len(), other.words.len());
        let mut bits_used = 0_u64;
        for (word, &other_word) in self.words.iter_mut().zip(&other.words) {
            op(word, other_word);
            bits_used += u64::from(word.count_ones());
        }
        self.bits_used = bits_used;
    }
}

fn allocatable_len(num_words: u64, kind: ErrorKind) -> Result<usize, Error> {
    if num_words > MAX_ALLOCATABLE_WORDS {
        return Err(Error::new(
            kind,
            format!(
                "requested {num_words} words exceeds maximum addressable allocation ({MAX_ALLOCATABLE_WORDS})"
            ),
        ));
    }
    Ok(num_words as usize)
}

#[inline(always)]
fn probe_slot(
    h0: u64,
    h1: u64,
    i: u16,
    capacity: u64,
    multiplier: u64,
    shift: u32,
) -> (usize, u64) {
    let shifted = h0.wrapping_add(u64::from(i).wrapping_mul(h1)) >> 1;
    let bit_index = bit_index(shifted, capacity, multiplier, shift);
    let word_index = (bit_index >> 6) as usize;
    let mask = 1_u64 << (bit_index & 63);
    (word_index, mask)
}

#[inline(always)]
fn bit_index(shifted: u64, capacity: u64, multiplier: u64, shift: u32) -> u64 {
    debug_assert!(shifted < 1 << 63);
    // Granlund-Montgomery invariant division: (multiplier * shifted) >> (63 + l).
    let quotient = ((u128::from(shifted) * u128::from(multiplier)) >> 64) as u64 >> shift;
    shifted - quotient * capacity
}

fn reduction_parameters(capacity: u64) -> (u64, u32) {
    // `capacity` is a positive multiple of 64, so `2^(l - 1) < capacity <= 2^l`
    // and `ceil(2^(63 + l) / capacity)` fits in `u64`.
    let l = 64 - (capacity - 1).leading_zeros();
    let multiplier = (1_u128 << (63 + l)).div_ceil(u128::from(capacity)) as u64;
    (multiplier, l - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bit_indexes_match_remainder_at_boundaries() {
        for capacity in [64, 192, 95_872, (1 << 36) + 64, MAX_NUM_BITS] {
            let (multiplier, shift) = reduction_parameters(capacity);
            let max_hash = (1_u64 << 63) - 1;
            let last_multiple = max_hash - max_hash % capacity;
            for hash in [
                0,
                capacity - 1,
                capacity,
                capacity + 1,
                last_multiple - 1,
                last_multiple,
                max_hash,
            ] {
                assert_eq!(
                    bit_index(hash, capacity, multiplier, shift),
                    hash % capacity,
                    "capacity {capacity}, hash {hash}"
                );
            }
        }
    }
}
