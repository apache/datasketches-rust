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
use crate::codec::assert::ensure_preamble_longs_in;
use crate::codec::assert::ensure_serial_version_is;
use crate::codec::assert::insufficient_data;
use crate::codec::family::Family;
use crate::countmin::CountMinValue;
use crate::countmin::UnsignedCountMinValue;
use crate::countmin::serialization::FLAGS_IS_EMPTY;
use crate::countmin::serialization::LONG_SIZE_BYTES;
use crate::countmin::serialization::PREAMBLE_LONGS_SHORT;
use crate::countmin::serialization::SERIAL_VERSION;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::MurmurHash3X64128;
use crate::hash::check_seed_hash;
use crate::hash::compute_seed_hash;

const MAX_TABLE_ENTRIES: usize = 1 << 30;

/// CountMin sketch for estimating item frequencies.
///
/// The sketch provides upper and lower bounds on estimated item frequencies
/// with configurable relative error and confidence.
#[derive(Debug, Clone, PartialEq)]
pub struct CountMinSketch<T: CountMinValue> {
    num_hashes: u8,
    num_buckets: u32,
    seed: u64,
    seed_hash: u16,
    total_weight: T,
    counts: Vec<T>,
    hash_seeds: Vec<u64>,
}

impl<T: CountMinValue> CountMinSketch<T> {
    /// Creates a new CountMin sketch with the default seed.
    ///
    /// # Errors
    ///
    /// Returns an error if `num_hashes` is `0`, `num_buckets` is less than `3`, or the total table
    /// size exceeds the supported limit.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::countmin::CountMinSketch;
    ///
    /// let sketch = CountMinSketch::<i64>::new(4, 128).unwrap();
    /// assert_eq!(sketch.num_buckets(), 128);
    /// ```
    pub fn new(num_hashes: u8, num_buckets: u32) -> Result<Self, Error> {
        Self::with_seed(num_hashes, num_buckets, DEFAULT_UPDATE_SEED)
    }

    /// Creates a new CountMin sketch with the provided seed.
    ///
    /// # Errors
    ///
    /// Returns an error if any of:
    /// * `num_hashes` is `0`.
    /// * `num_buckets` is less than `3`.
    /// * The total table size exceeds the supported limit.
    /// * The computed seed hash is zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::countmin::CountMinSketch;
    ///
    /// let sketch = CountMinSketch::<i64>::with_seed(4, 64, 42).unwrap();
    /// assert_eq!(sketch.seed(), 42);
    /// ```
    pub fn with_seed(num_hashes: u8, num_buckets: u32, seed: u64) -> Result<Self, Error> {
        let entries = entries_for_config(num_hashes, num_buckets)?;
        let seed_hash = compute_seed_hash(seed, ErrorKind::InvalidArgument)?;
        Ok(Self::make(
            num_hashes,
            num_buckets,
            seed,
            seed_hash,
            entries,
        ))
    }

    /// Returns the number of hash functions used by the sketch.
    pub fn num_hashes(&self) -> u8 {
        self.num_hashes
    }

    /// Returns the number of buckets per hash function.
    pub fn num_buckets(&self) -> u32 {
        self.num_buckets
    }

    /// Returns the seed used by the sketch.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns the total weight inserted into the sketch.
    pub fn total_weight(&self) -> T {
        self.total_weight
    }

    /// Returns the relative error (epsilon) implied by the number of buckets.
    pub fn relative_error(&self) -> f64 {
        std::f64::consts::E / self.num_buckets as f64
    }

    /// Returns `true` if the sketch has not seen any updates.
    pub fn is_empty(&self) -> bool {
        self.total_weight == T::ZERO
    }

    /// Suggests the number of buckets to achieve the given relative error.
    ///
    /// # Errors
    ///
    /// Returns an error if `relative_error` is not finite, is not greater than zero, or would
    /// require more buckets than the sketch supports.
    pub fn suggest_num_buckets(relative_error: f64) -> Result<u32, Error> {
        if !relative_error.is_finite() || relative_error <= 0.0 {
            return Err(Error::invalid_argument(
                "relative_error must be finite and greater than 0",
            ));
        }

        let num_buckets = (std::f64::consts::E / relative_error).ceil();
        if num_buckets >= MAX_TABLE_ENTRIES as f64 {
            return Err(Error::invalid_argument(format!(
                "relative_error requires {num_buckets} buckets, but fewer than {MAX_TABLE_ENTRIES} are supported"
            )));
        }

        Ok((num_buckets as u32).max(3))
    }

    /// Suggests the number of hashes to achieve the given confidence.
    ///
    /// # Errors
    ///
    /// Returns an error if `confidence` is not in `[0, 1]`.
    pub fn suggest_num_hashes(confidence: f64) -> Result<u8, Error> {
        if !(0.0..=1.0).contains(&confidence) {
            return Err(Error::invalid_argument(
                "confidence must be between 0 and 1.0 (inclusive)",
            ));
        }
        if confidence == 1.0 {
            return Ok(127);
        }
        let hashes = (1.0 / (1.0 - confidence)).ln().ceil();
        Ok(hashes.clamp(1.0, 127.0) as u8)
    }

    /// Updates the sketch with a single occurrence of the item.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::countmin::CountMinSketch;
    ///
    /// let mut sketch = CountMinSketch::<i64>::new(4, 128).unwrap();
    /// sketch.update("apple");
    /// assert!(sketch.estimate("apple") >= 1);
    /// ```
    pub fn update<I: Hash>(&mut self, item: I) {
        self.update_with_weight(item, T::ONE);
    }

    /// Updates the sketch with the given item and weight.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::countmin::CountMinSketch;
    ///
    /// let mut sketch = CountMinSketch::<i64>::new(4, 128).unwrap();
    /// sketch.update_with_weight("banana", 3);
    /// assert!(sketch.estimate("banana") >= 3);
    /// ```
    pub fn update_with_weight<I: Hash>(&mut self, item: I, weight: T) {
        if weight == T::ZERO {
            return;
        }
        let abs_weight = weight.abs();
        self.total_weight = self.total_weight + abs_weight;
        let num_buckets = self.num_buckets as usize;
        for (row, seed) in self.hash_seeds.iter().enumerate() {
            let bucket = self.bucket_index(&item, *seed);
            let index = row * num_buckets + bucket;
            self.counts[index] = self.counts[index] + weight;
        }
    }

    /// Returns the estimated frequency of the given item.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::countmin::CountMinSketch;
    ///
    /// let mut sketch = CountMinSketch::<i64>::new(4, 128).unwrap();
    /// sketch.update_with_weight("pear", 2);
    /// assert!(sketch.estimate("pear") >= 2);
    /// ```
    pub fn estimate<I: Hash>(&self, item: I) -> T {
        let num_buckets = self.num_buckets as usize;
        let mut min = T::MAX;
        for (row, seed) in self.hash_seeds.iter().enumerate() {
            let bucket = self.bucket_index(&item, *seed);
            let index = row * num_buckets + bucket;
            let value = self.counts[index];
            if value < min {
                min = value;
            }
        }
        min
    }

    /// Returns the lower bound on the true frequency of the given item.
    pub fn lower_bound<I: Hash>(&self, item: I) -> T {
        self.estimate(item)
    }

    /// Returns the upper bound on the true frequency of the given item.
    pub fn upper_bound<I: Hash>(&self, item: I) -> T {
        let estimate = self.estimate(item);
        let error = self.total_weight.scale(self.relative_error());
        estimate + error
    }

    /// Merges another sketch into this one.
    ///
    /// # Errors
    ///
    /// Returns an error if the sketches have different numbers of hashes, bucket counts, or seeds.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::countmin::CountMinSketch;
    ///
    /// let mut left = CountMinSketch::<i64>::new(4, 128).unwrap();
    /// let mut right = CountMinSketch::<i64>::new(4, 128).unwrap();
    ///
    /// left.update("apple");
    /// right.update_with_weight("banana", 2);
    ///
    /// left.merge(&right).unwrap();
    /// assert!(left.estimate("banana") >= 2);
    /// ```
    pub fn merge(&mut self, other: &CountMinSketch<T>) -> Result<(), Error> {
        if self.num_hashes != other.num_hashes
            || self.num_buckets != other.num_buckets
            || self.seed != other.seed
        {
            return Err(Error::invalid_argument(
                "Count-Min sketches must have matching numbers of hashes, bucket counts, and seeds",
            ));
        }
        for (count, other_count) in self.counts.iter_mut().zip(&other.counts) {
            *count = *count + *other_count;
        }
        self.total_weight = self.total_weight + other.total_weight;
        Ok(())
    }

    /// Serializes this sketch into the DataSketches CountMin format.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::countmin::CountMinSketch;
    ///
    /// let mut sketch = CountMinSketch::<i64>::new(4, 128).unwrap();
    /// sketch.update("apple");
    /// let bytes = sketch.serialize();
    /// let decoded = CountMinSketch::<i64>::deserialize(&bytes).unwrap();
    /// assert!(decoded.estimate("apple") >= 1);
    /// ```
    pub fn serialize(&self) -> Vec<u8> {
        let header_size = PREAMBLE_LONGS_SHORT as usize * LONG_SIZE_BYTES;
        let value_size = LONG_SIZE_BYTES;
        let payload_size = if self.is_empty() {
            0
        } else {
            value_size + (self.counts.len() * value_size)
        };
        let mut bytes = SketchBytes::with_capacity(header_size + payload_size);

        bytes.write_u8(PREAMBLE_LONGS_SHORT);
        bytes.write_u8(SERIAL_VERSION);
        bytes.write_u8(Family::COUNTMIN.id);
        bytes.write_u8(if self.is_empty() { FLAGS_IS_EMPTY } else { 0 });
        bytes.write_u32_le(0); // unused

        bytes.write_u32_le(self.num_buckets);
        bytes.write_u8(self.num_hashes);
        debug_assert_eq!(
            self.seed_hash,
            compute_seed_hash(self.seed, ErrorKind::InvalidArgument).unwrap()
        );
        bytes.write_u16_le(self.seed_hash);
        bytes.write_u8(0);

        if self.is_empty() {
            return bytes.into_bytes();
        }

        bytes.write(&self.total_weight.to_bytes());
        for count in &self.counts {
            bytes.write(&count.to_bytes());
        }
        bytes.into_bytes()
    }

    /// Deserializes a sketch from bytes using the default seed.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::countmin::CountMinSketch;
    ///
    /// let mut sketch = CountMinSketch::<i64>::new(4, 64).unwrap();
    /// sketch.update("apple");
    /// let bytes = sketch.serialize();
    /// let decoded = CountMinSketch::<i64>::deserialize(&bytes).unwrap();
    /// assert!(decoded.estimate("apple") >= 1);
    /// ```
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        Self::deserialize_with_seed(bytes, DEFAULT_UPDATE_SEED)
    }

    /// Deserializes a sketch from bytes using the provided seed.
    ///
    /// # Errors
    ///
    /// Returns `InvalidData` if the image is malformed, its seed hash does not match `seed`, or
    /// `seed` itself computes to the reserved zero seed hash.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::countmin::CountMinSketch;
    ///
    /// let mut sketch = CountMinSketch::<i64>::with_seed(4, 64, 7).unwrap();
    /// sketch.update("apple");
    /// let bytes = sketch.serialize();
    /// let decoded = CountMinSketch::<i64>::deserialize_with_seed(&bytes, 7).unwrap();
    /// assert!(decoded.estimate("apple") >= 1);
    /// ```
    pub fn deserialize_with_seed(bytes: &[u8], seed: u64) -> Result<Self, Error> {
        fn read_value<T: CountMinValue>(
            cursor: &mut SketchSlice<'_>,
            tag: &'static str,
        ) -> Result<T, Error> {
            let mut bs = [0u8; 8];
            cursor.read_exact(&mut bs).map_err(insufficient_data(tag))?;
            T::try_from_bytes(bs)
        }

        let mut cursor = SketchSlice::new(bytes);
        let preamble_longs = cursor
            .read_u8()
            .map_err(insufficient_data("preamble_longs"))?;
        let serial_version = cursor
            .read_u8()
            .map_err(insufficient_data("serial_version"))?;
        let family_id = cursor.read_u8().map_err(insufficient_data("family_id"))?;
        let flags = cursor.read_u8().map_err(insufficient_data("flags"))?;
        cursor
            .read_u32_le()
            .map_err(insufficient_data("<unused>"))?;

        Family::COUNTMIN.validate_id(family_id)?;
        ensure_serial_version_is(SERIAL_VERSION, serial_version)?;
        ensure_preamble_longs_in(&[PREAMBLE_LONGS_SHORT], preamble_longs)?;

        let num_buckets = cursor
            .read_u32_le()
            .map_err(insufficient_data("num_buckets"))?;
        let num_hashes = cursor.read_u8().map_err(insufficient_data("num_hashes"))?;
        let seed_hash = cursor
            .read_u16_le()
            .map_err(insufficient_data("seed_hash"))?;
        cursor.read_u8().map_err(insufficient_data("unused8"))?;

        let expected_seed_hash = compute_seed_hash(seed, ErrorKind::InvalidData)?;
        check_seed_hash(
            expected_seed_hash,
            seed_hash,
            "deserialized CountMinSketch",
            ErrorKind::InvalidData,
        )?;

        let entries = entries_for_config_checked(num_hashes, num_buckets)?;
        let is_empty = (flags & FLAGS_IS_EMPTY) != 0;
        if !is_empty {
            let payload_values = entries
                .checked_add(1)
                .ok_or_else(|| Error::deserial("CountMin payload value count overflows"))?;
            let payload_bytes = payload_values
                .checked_mul(LONG_SIZE_BYTES)
                .ok_or_else(|| Error::deserial("CountMin payload size overflows"))?;
            if payload_bytes > cursor.remaining().len() {
                return Err(Error::insufficient_data(format!(
                    "CountMin payload requires {payload_bytes} bytes, got {}",
                    cursor.remaining().len()
                )));
            }
        }

        let mut sketch = Self::make(num_hashes, num_buckets, seed, expected_seed_hash, entries);
        if is_empty {
            return Ok(sketch);
        }

        sketch.total_weight = read_value(&mut cursor, "total_weight")?;
        for count in &mut sketch.counts {
            *count = read_value(&mut cursor, "counts")?;
        }
        Ok(sketch)
    }

    /// Returns the estimated size of the sketch in bytes.
    pub fn estimated_size(&self) -> usize {
        size_of::<Self>()
            + self.counts.capacity() * size_of::<T>()
            + self.hash_seeds.capacity() * size_of::<u64>()
    }

    fn make(num_hashes: u8, num_buckets: u32, seed: u64, seed_hash: u16, entries: usize) -> Self {
        let counts = vec![T::ZERO; entries];
        let hash_seeds = make_hash_seeds(seed, num_hashes);
        CountMinSketch {
            num_hashes,
            num_buckets,
            seed,
            seed_hash,
            total_weight: T::ZERO,
            counts,
            hash_seeds,
        }
    }

    fn bucket_index<I: Hash>(&self, item: &I, seed: u64) -> usize {
        let mut hasher = MurmurHash3X64128::with_seed(seed);
        item.hash(&mut hasher);
        let (h1, _) = hasher.finish128();
        (h1 % self.num_buckets as u64) as usize
    }
}

impl<T: UnsignedCountMinValue> CountMinSketch<T> {
    /// Divides every counter by two, truncating toward zero.
    ///
    /// Useful for exponential decay where counts represent recent activity.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::countmin::CountMinSketch;
    ///
    /// let mut sketch = CountMinSketch::<u64>::new(4, 128).unwrap();
    /// sketch.update_with_weight("apple", 3);
    /// sketch.halve();
    /// assert!(sketch.estimate("apple") >= 1);
    /// ```
    pub fn halve(&mut self) {
        for c in &mut self.counts {
            *c = c.halve()
        }
        self.total_weight = self.total_weight.halve();
    }

    /// Multiplies every counter by `decay` and truncates back into `T`.
    ///
    /// Values are truncated toward zero after multiplication; choose `decay` in `(0, 1]`.
    /// The total weight is scaled by the same factor to keep bounds consistent.
    ///
    /// # Panics
    ///
    /// Panics if `decay` is not finite or is outside `(0, 1]`.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::countmin::CountMinSketch;
    ///
    /// let mut sketch = CountMinSketch::<u64>::new(4, 128).unwrap();
    /// sketch.update_with_weight("apple", 3);
    /// sketch.decay(0.5);
    /// assert!(sketch.estimate("apple") >= 1);
    /// ```
    pub fn decay(&mut self, decay: f64) {
        assert!(decay > 0.0 && decay <= 1.0, "decay must be within (0, 1]");
        for c in &mut self.counts {
            *c = c.scale(decay)
        }
        self.total_weight = self.total_weight.scale(decay);
    }
}

fn entries_for_config(num_hashes: u8, num_buckets: u32) -> Result<usize, Error> {
    if num_hashes == 0 {
        return Err(Error::invalid_argument("num_hashes must be at least 1"));
    }
    if num_buckets < 3 {
        return Err(Error::invalid_argument("num_buckets must be at least 3"));
    }
    let entries = (num_hashes as usize)
        .checked_mul(num_buckets as usize)
        .ok_or_else(|| Error::invalid_argument("num_hashes * num_buckets overflows usize"))?;
    if entries >= MAX_TABLE_ENTRIES {
        return Err(Error::invalid_argument(format!(
            "num_hashes * num_buckets must be < {MAX_TABLE_ENTRIES}"
        )));
    }
    Ok(entries)
}

fn entries_for_config_checked(num_hashes: u8, num_buckets: u32) -> Result<usize, Error> {
    if num_hashes == 0 {
        return Err(Error::deserial("num_hashes must be at least 1"));
    }
    if num_buckets < 3 {
        return Err(Error::deserial("num_buckets must be at least 3"));
    }
    let entries = (num_hashes as usize)
        .checked_mul(num_buckets as usize)
        .ok_or_else(|| Error::deserial("num_hashes * num_buckets overflows usize"))?;
    if entries >= MAX_TABLE_ENTRIES {
        return Err(Error::deserial(format!(
            "num_hashes * num_buckets must be < {MAX_TABLE_ENTRIES}",
        )));
    }
    Ok(entries)
}

fn make_hash_seeds(seed: u64, num_hashes: u8) -> Vec<u64> {
    let mut seeds = Vec::with_capacity(num_hashes as usize);
    for i in 0..num_hashes {
        // Derive per-row hash seeds deterministically from the sketch seed.
        let mut hasher = MurmurHash3X64128::with_seed(seed);
        hasher.write(&u64::from(i).to_le_bytes());
        let (h1, _) = hasher.finish128();
        seeds.push(h1);
    }
    seeds
}
