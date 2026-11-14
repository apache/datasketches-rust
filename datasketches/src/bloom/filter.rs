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

use crate::bloom::bit_array::BitArray;
use crate::bloom::bit_array::MAX_NUM_BITS;
use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::ensure_preamble_longs_in_range;
use crate::codec::assert::ensure_serial_version_is;
use crate::codec::assert::insufficient_data;
use crate::codec::family::Family;
use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::XxHash64;

const SERIAL_VERSION: u8 = 1;
const EMPTY_FLAG_MASK: u8 = 1 << 2;
const MIN_NUM_BITS: u64 = 1;
const MIN_NUM_HASHES: u16 = 1;
const MAX_NUM_HASHES: u16 = i16::MAX as u16;

/// A Bloom filter for probabilistic set membership testing.
///
/// A `false` result from [`contains`](Self::contains) proves that the item was not inserted; a
/// `true` result may be a false positive. Unlike resizing sketches, a Bloom filter has a fixed
/// bit capacity chosen at construction.
///
/// See the [module level documentation](super) for more.
#[derive(Clone, PartialEq, Eq)]
pub struct BloomFilter {
    seed: u64,
    num_hashes: u16,
    bits: BitArray,
}

impl BloomFilter {
    /// Creates a filter sized for `max_items` at target false-positive probability `fpp`, using
    /// the default hash seed (`9001`).
    ///
    /// `max_items` is a sizing assumption, not an insertion limit. Inserting more distinct items
    /// remains valid, though the actual false-positive probability can exceed `fpp`. An `fpp` of
    /// `1.0` creates the minimal allocation: 64 bits and 1 hash function.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if `max_items` is
    /// zero, `fpp` is outside `(0.0, 1.0]`, or the target exceeds the serialized format or
    /// addressable allocation limits.
    pub fn with_accuracy(max_items: u64, fpp: f64) -> Result<Self, Error> {
        Self::with_accuracy_and_seed(max_items, fpp, DEFAULT_UPDATE_SEED)
    }

    /// Creates a filter sized for `max_items` at target false-positive probability `fpp` with an
    /// explicit hash `seed`.
    ///
    /// Filters with different seeds cannot be combined via set operations.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if `max_items` is
    /// zero, `fpp` is outside `(0.0, 1.0]`, or the target exceeds the serialized format or
    /// addressable allocation limits.
    pub fn with_accuracy_and_seed(max_items: u64, fpp: f64, seed: u64) -> Result<Self, Error> {
        if max_items == 0 {
            return Err(Error::invalid_argument("max_items must be greater than 0"));
        }
        if !(fpp > 0.0 && fpp <= 1.0) {
            return Err(Error::invalid_argument("fpp must be in (0.0, 1.0]"));
        }

        let n = max_items as f64;
        let ln2_squared = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let bits = (-n * fpp.ln() / ln2_squared).ceil();
        if bits > MAX_NUM_BITS as f64 {
            return Err(Error::invalid_argument(format!(
                "target accuracy requires {bits:.0} bits, but at most {MAX_NUM_BITS} are supported"
            )));
        }

        let num_bits = (bits as u64).max(MIN_NUM_BITS);
        let num_hashes = (num_bits as f64 / n * std::f64::consts::LN_2)
            .ceil()
            .clamp(f64::from(MIN_NUM_HASHES), f64::from(MAX_NUM_HASHES))
            as u16;
        Self::with_size_and_seed(num_bits, num_hashes, seed)
    }

    /// Creates a filter with at least `num_bits` bits and `num_hashes` hash functions, using the
    /// default hash seed (`9001`).
    ///
    /// Capacity is rounded up to a multiple of 64 bits.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if `num_bits` is not
    /// in `1..=MAX_NUM_BITS`, exceeds addressable allocation limits, or `num_hashes` is outside
    /// `1..=32767`.
    pub fn with_size(num_bits: u64, num_hashes: u16) -> Result<Self, Error> {
        Self::with_size_and_seed(num_bits, num_hashes, DEFAULT_UPDATE_SEED)
    }

    /// Creates a filter with at least `num_bits` bits, `num_hashes` hash functions, and an explicit
    /// hash `seed`.
    ///
    /// Capacity is rounded up to a multiple of 64 bits. Filters with different seeds cannot be
    /// combined via set operations.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if `num_bits` is not
    /// in `1..=MAX_NUM_BITS`, exceeds addressable allocation limits, or `num_hashes` is outside
    /// `1..=32767`.
    pub fn with_size_and_seed(num_bits: u64, num_hashes: u16, seed: u64) -> Result<Self, Error> {
        if !(MIN_NUM_HASHES..=MAX_NUM_HASHES).contains(&num_hashes) {
            return Err(Error::invalid_argument(format!(
                "num_hashes must be within {MIN_NUM_HASHES}..={MAX_NUM_HASHES}, got {num_hashes}"
            )));
        }
        let bits = BitArray::new(num_bits)?;
        Ok(Self {
            seed,
            num_hashes,
            bits,
        })
    }

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
        self.bits.reset();
    }

    /// Merges `other` into `self` via bitwise OR (union).
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if
    /// [`is_compatible`](Self::is_compatible) is `false`.
    pub fn union(&mut self, other: &Self) -> Result<(), Error> {
        self.ensure_compatible(other)?;
        self.bits.union(&other.bits);
        Ok(())
    }

    /// Intersects `self` with `other` via bitwise AND.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if
    /// [`is_compatible`](Self::is_compatible) is `false`.
    pub fn intersect(&mut self, other: &Self) -> Result<(), Error> {
        self.ensure_compatible(other)?;
        self.bits.intersect(&other.bits);
        Ok(())
    }

    /// Excludes `other` from `self` via bitwise AND-NOT (`self & !other`).
    ///
    /// Items inserted into `other` return `false` afterward; items inserted only into `self`
    /// remain present unless one of their bit positions is set in `other`.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if
    /// [`is_compatible`](Self::is_compatible) is `false`.
    pub fn difference(&mut self, other: &Self) -> Result<(), Error> {
        self.ensure_compatible(other)?;
        self.bits.difference(&other.bits);
        Ok(())
    }

    /// Returns `true` if no bits are set in the filter.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bits.is_empty()
    }

    /// Returns the number of bits currently set to `1`.
    pub fn bits_used(&self) -> u64 {
        self.bits.bits_used()
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

    /// Returns `true` if `self` and `other` have identical capacity, `num_hashes`, and `seed`.
    pub fn is_compatible(&self, other: &Self) -> bool {
        self.bits.word_len() == other.bits.word_len()
            && self.num_hashes == other.num_hashes
            && self.seed == other.seed
    }

    /// Returns the estimated heap allocation of the filter's backing bit array in bytes.
    pub fn estimated_size(&self) -> usize {
        self.bits.byte_len()
    }

    /// Serializes the filter in the DataSketches binary format.
    pub fn serialize(&self) -> Vec<u8> {
        let is_empty = self.is_empty();
        let preamble_longs = if is_empty {
            Family::BLOOMFILTER.min_pre_longs
        } else {
            Family::BLOOMFILTER.max_pre_longs
        };
        let payload_bytes = if is_empty { 0 } else { self.bits.byte_len() };
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
    /// # Errors
    ///
    /// Returns [`InvalidData`](crate::error::ErrorKind::InvalidData) if the header, bit count, or
    /// payload is malformed, truncated, or exceeds supported limits.
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

    fn ensure_compatible(&self, other: &Self) -> Result<(), Error> {
        if !self.is_compatible(other) {
            return Err(Error::invalid_argument(
                "Bloom filters must have matching capacity, number of hashes, and seed",
            ));
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
