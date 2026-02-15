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

//! Theta sketch implementation
//!
//! This module provides ThetaSketch (mutable) and CompactThetaSketch (immutable)
//! for cardinality estimation.

use std::hash::Hash;

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::family::Family;
use crate::codec::utility::ensure_preamble_longs_in_range;
use crate::common::NumStdDev;
use crate::common::ResizeFactor;
use crate::common::binomial_bounds;
use crate::common::canonical_double;
use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::compute_seed_hash;
use crate::theta::bit_pack::pack_bits;
use crate::theta::bit_pack::pack_bits_block8;
use crate::theta::bit_pack::unpack_bits;
use crate::theta::bit_pack::unpack_bits_block8;
use crate::theta::hash_table::DEFAULT_LG_K;
use crate::theta::hash_table::MAX_LG_K;
use crate::theta::hash_table::MAX_THETA;
use crate::theta::hash_table::MIN_LG_K;
use crate::theta::hash_table::ThetaHashTable;
use crate::theta::serialization;

/// Mutable theta sketch for building from input data
#[derive(Debug)]
pub struct ThetaSketch {
    table: ThetaHashTable,
}

impl ThetaSketch {
    /// Create a new builder for ThetaSketch
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::theta::ThetaSketch;
    /// let sketch = ThetaSketch::builder().lg_k(12).build();
    /// assert_eq!(sketch.lg_k(), 12);
    /// ```
    pub fn builder() -> ThetaSketchBuilder {
        ThetaSketchBuilder::default()
    }

    /// Update the sketch with a hashable value.
    ///
    /// For `f32`/`f64` values, use `update_f32`/`update_f64` instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::theta::ThetaSketch;
    /// let mut sketch = ThetaSketch::builder().build();
    /// sketch.update("apple");
    /// assert!(sketch.estimate() >= 1.0);
    /// ```
    pub fn update<T: Hash>(&mut self, value: T) {
        let hash = self.table.hash_and_screen(value);
        if hash != 0 {
            self.table.try_insert(hash);
        }
    }

    /// Update the sketch with a f64 value.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::theta::ThetaSketch;
    /// let mut sketch = ThetaSketch::builder().build();
    /// sketch.update_f64(1.0);
    /// assert!(sketch.estimate() >= 1.0);
    /// ```
    pub fn update_f64(&mut self, value: f64) {
        // Canonicalize double for compatibility with Java
        let canonical = canonical_double(value);
        self.update(canonical);
    }

    /// Update the sketch with a f32 value.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::theta::ThetaSketch;
    /// let mut sketch = ThetaSketch::builder().build();
    /// sketch.update_f32(1.0);
    /// assert!(sketch.estimate() >= 1.0);
    /// ```
    pub fn update_f32(&mut self, value: f32) {
        self.update_f64(value as f64);
    }

    /// Return cardinality estimate
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::theta::ThetaSketch;
    /// # let mut sketch = ThetaSketch::builder().build();
    /// # sketch.update("apple");
    /// assert!(sketch.estimate() >= 1.0);
    /// ```
    pub fn estimate(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let num_retained = self.table.num_entries() as f64;
        let theta = self.table.theta() as f64 / MAX_THETA as f64;
        num_retained / theta
    }

    /// Return theta as a fraction (0.0 to 1.0)
    pub fn theta(&self) -> f64 {
        self.table.theta() as f64 / MAX_THETA as f64
    }

    /// Return theta as u64
    pub fn theta64(&self) -> u64 {
        self.table.theta()
    }

    /// Check if sketch is empty
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Check if sketch is in estimation mode
    pub fn is_estimation_mode(&self) -> bool {
        self.table.theta() < MAX_THETA
    }

    /// Return number of retained entries
    pub fn num_retained(&self) -> usize {
        self.table.num_entries()
    }

    /// Return lg_k
    pub fn lg_k(&self) -> u8 {
        self.table.lg_nom_size()
    }

    /// Trim the sketch to nominal size k
    pub fn trim(&mut self) {
        self.table.trim();
    }

    /// Reset the sketch to empty state
    pub fn reset(&mut self) {
        self.table.reset();
    }

    /// Return iterator over hash values
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::theta::ThetaSketch;
    /// # let mut sketch = ThetaSketch::builder().build();
    /// # sketch.update("apple");
    /// let mut iter = sketch.iter();
    /// assert!(iter.next().is_some());
    /// ```
    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.table.iter()
    }

    /// Return this sketch in compact (immutable) form.
    ///
    /// If `ordered` is true, retained hash values are sorted in ascending order.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::theta::ThetaSketch;
    /// let mut sketch = ThetaSketch::builder().build();
    /// sketch.update("apple");
    /// let compact = sketch.compact(true);
    /// assert_eq!(compact.num_retained(), 1);
    /// ```
    pub fn compact(&self, ordered: bool) -> CompactThetaSketch {
        let mut entries: Vec<u64> = self.iter().collect();

        let empty = entries.is_empty();
        let theta = if empty {
            // Match Java's correctThetaOnCompact() behavior for never-updated sketches
            // initialized with p < 1.0.
            MAX_THETA
        } else {
            self.table.theta()
        };
        let is_single = entries.len() == 1 && theta == MAX_THETA;
        // Empty or Single-item sketches are always ordered (Java compatibility)
        let ordered = ordered || empty || is_single;

        if ordered && entries.len() > 1 {
            entries.sort_unstable();
        }

        CompactThetaSketch {
            entries,
            theta,
            seed_hash: self.table.seed_hash(),
            ordered,
            empty,
        }
    }

    /// Returns the approximate lower error bound given the specified number of Standard Deviations.
    ///
    /// # Arguments
    ///
    /// * `num_std_dev` - The number of standard deviations for confidence bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::common::NumStdDev;
    /// use datasketches::theta::ThetaSketch;
    ///
    /// let mut sketch = ThetaSketch::builder().lg_k(12).build();
    /// for i in 0..10000 {
    ///     sketch.update(i);
    /// }
    ///
    /// let estimate = sketch.estimate();
    /// let lower_bound = sketch.lower_bound(NumStdDev::Two);
    /// let upper_bound = sketch.upper_bound(NumStdDev::Two);
    ///
    /// assert!(lower_bound <= estimate);
    /// assert!(estimate <= upper_bound);
    /// ```
    pub fn lower_bound(&self, num_std_dev: NumStdDev) -> f64 {
        if !self.is_estimation_mode() {
            return self.num_retained() as f64;
        }
        // This is safe because sampling_probability is guaranteed to be > 0,
        // so theta will always be > 0, and binomial_bounds will never fail
        binomial_bounds::lower_bound(self.num_retained() as u64, self.theta(), num_std_dev)
            .expect("theta should always be valid")
    }

    /// Returns the approximate upper error bound given the specified number of Standard Deviations.
    ///
    /// # Arguments
    ///
    /// * `num_std_dev` - The number of standard deviations for confidence bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::common::NumStdDev;
    /// use datasketches::theta::ThetaSketch;
    ///
    /// let mut sketch = ThetaSketch::builder().lg_k(12).build();
    /// for i in 0..10000 {
    ///     sketch.update(i);
    /// }
    ///
    /// let estimate = sketch.estimate();
    /// let lower_bound = sketch.lower_bound(NumStdDev::Two);
    /// let upper_bound = sketch.upper_bound(NumStdDev::Two);
    ///
    /// assert!(lower_bound <= estimate);
    /// assert!(estimate <= upper_bound);
    /// ```
    pub fn upper_bound(&self, num_std_dev: NumStdDev) -> f64 {
        if !self.is_estimation_mode() {
            return self.num_retained() as f64;
        }
        // This is safe because sampling_probability is guaranteed to be > 0,
        // so theta will always be > 0, and binomial_bounds will never fail
        binomial_bounds::upper_bound(
            self.num_retained() as u64,
            self.theta(),
            num_std_dev,
            self.is_empty(),
        )
        .expect("theta should always be valid")
    }
}

/// Compact (immutable) theta sketch.
///
/// This is the serialized-friendly form of a theta sketch: a compact array of retained hash values
/// plus theta and a 16-bit seed hash. It can be ordered (sorted ascending) or unordered.
#[derive(Clone, Debug)]
pub struct CompactThetaSketch {
    entries: Vec<u64>,
    theta: u64,
    seed_hash: u16,
    ordered: bool,
    empty: bool,
}

impl CompactThetaSketch {
    /// Returns the cardinality estimate.
    pub fn estimate(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let num_retained = self.num_retained() as f64;
        if self.theta == MAX_THETA {
            return num_retained;
        }
        let theta = self.theta as f64 / MAX_THETA as f64;
        num_retained / theta
    }

    /// Returns theta as a fraction (0.0 to 1.0).
    pub fn theta(&self) -> f64 {
        self.theta as f64 / MAX_THETA as f64
    }

    /// Returns theta as u64.
    pub fn theta64(&self) -> u64 {
        self.theta
    }

    /// Returns true if this sketch is empty.
    pub fn is_empty(&self) -> bool {
        self.empty
    }

    /// Returns true if this sketch is in estimation mode.
    pub fn is_estimation_mode(&self) -> bool {
        self.theta < MAX_THETA
    }

    /// Returns the number of retained entries.
    pub fn num_retained(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if retained entries are ordered (sorted ascending).
    pub fn is_ordered(&self) -> bool {
        self.ordered
    }

    /// Returns the 16-bit seed hash.
    pub fn seed_hash(&self) -> u16 {
        self.seed_hash
    }

    /// Return iterator over retained hash values.
    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.entries.iter().copied()
    }

    /// Returns the approximate lower error bound given the specified number of Standard Deviations.
    pub fn lower_bound(&self, num_std_dev: NumStdDev) -> f64 {
        if !self.is_estimation_mode() {
            return self.num_retained() as f64;
        }
        binomial_bounds::lower_bound(self.num_retained() as u64, self.theta(), num_std_dev)
            .expect("compact theta should always be valid")
    }

    /// Returns the approximate upper error bound given the specified number of Standard Deviations.
    pub fn upper_bound(&self, num_std_dev: NumStdDev) -> f64 {
        if !self.is_estimation_mode() {
            return self.num_retained() as f64;
        }
        binomial_bounds::upper_bound(
            self.num_retained() as u64,
            self.theta(),
            num_std_dev,
            self.is_empty(),
        )
        .expect("compact theta should always be valid")
    }

    fn preamble_longs(&self, compressed: bool) -> u8 {
        if compressed {
            if self.is_estimation_mode() { 2 } else { 1 }
        } else {
            if self.is_estimation_mode() {
                3
            } else {
                if self.is_empty() || self.entries.len() == 1 {
                    1
                } else {
                    2
                }
            }
        }
    }

    /// Serializes this sketch in compressed form if applicable.
    ///
    /// This uses `serVer = 4` when the sketch is ordered and suitable for compression, and falls
    /// back to uncompressed `serVer = 3` otherwise.
    pub fn serialize_compressed(&self) -> Vec<u8> {
        if self.is_suitable_for_compression() {
            self.serialize_v4()
        } else {
            self.serialize()
        }
    }

    fn is_suitable_for_compression(&self) -> bool {
        self.ordered
            && !self.entries.is_empty()
            && (self.entries.len() != 1 || self.is_estimation_mode())
    }

    /// Serializes this sketch into the uncompressed compact theta format.
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = SketchBytes::with_capacity(64 + self.entries.len() * 8);

        let pre_longs = self.preamble_longs(false);
        bytes.write_u8(pre_longs);
        bytes.write_u8(serialization::UNCOMPRESSED_SERIAL_VERSION);
        bytes.write_u8(Family::THETA.id);
        bytes.write_u16_be(0); // unused for compact

        let mut flags = 0u8;
        flags |= serialization::FLAGS_IS_READ_ONLY;
        flags |= serialization::FLAGS_IS_COMPACT;
        if self.is_empty() {
            flags |= serialization::FLAGS_IS_EMPTY;
        }
        if self.is_ordered() {
            flags |= serialization::FLAGS_IS_ORDERED;
        }
        bytes.write_u8(flags);

        bytes.write_u16_le(self.seed_hash);

        if pre_longs > 1 {
            bytes.write_u32_le(self.entries.len() as u32);
            bytes.write_u32_be(0); // not used by compact sketches; match Java/C++
        }
        if self.is_estimation_mode() {
            bytes.write_u64_le(self.theta64());
        }
        for hash in self.entries.iter() {
            bytes.write_u64_le(*hash);
        }
        bytes.into_bytes()
    }

    fn serialize_v4(&self) -> Vec<u8> {
        let pre_longs = self.preamble_longs(true);
        let entry_bits = Self::compute_entry_bits(&self.entries);
        let num_entries_bytes = Self::num_entries_bytes(self.entries.len());

        // Pre-size exactly: preamble longs (8 bytes each) + num_entries_bytes + packed bits.
        let compressed_bits = entry_bits as usize * self.entries.len();
        let compressed_bytes = compressed_bits.div_ceil(8);
        let out_bytes = (pre_longs as usize * 8) + (num_entries_bytes as usize) + compressed_bytes;
        let mut bytes = SketchBytes::with_capacity(out_bytes);

        bytes.write_u8(pre_longs);
        bytes.write_u8(serialization::COMPRESSED_SERIAL_VERSION);
        bytes.write_u8(Family::THETA.id);
        bytes.write_u8(entry_bits);
        bytes.write_u8(num_entries_bytes);

        let mut flags = 0u8;
        flags |= serialization::FLAGS_IS_READ_ONLY;
        flags |= serialization::FLAGS_IS_COMPACT;
        flags |= serialization::FLAGS_IS_ORDERED;
        bytes.write_u8(flags);

        bytes.write_u16_le(self.seed_hash);
        if self.is_estimation_mode() {
            bytes.write_u64_le(self.theta);
        }

        let mut n = self.entries.len() as u32;
        for _ in 0..num_entries_bytes {
            bytes.write_u8((n & 0xff) as u8);
            n >>= 8;
        }

        // pack deltas
        let mut previous = 0u64;
        let mut i = 0usize;
        let mut block = vec![0u8; entry_bits as usize];
        while i + 7 < self.entries.len() {
            let mut deltas = [0u64; 8];
            for j in 0..8 {
                let entry = self.entries[i + j];
                deltas[j] = entry - previous;
                previous = entry;
            }
            block.fill(0);
            pack_bits_block8(&deltas, &mut block, entry_bits);
            bytes.write(&block);
            i += 8;
        }

        // pack extra deltas if fewer than 8 of them left
        if i < self.entries.len() {
            let mut block = vec![0u8; entry_bits as usize];
            let mut byte_index = 0;
            let mut bit_index = 0;
            let mut pack_delta_to_block = |delta: u64| {
                (byte_index, bit_index) =
                    pack_bits(delta, entry_bits, &mut block, byte_index, bit_index);
            };
            while i < self.entries.len() {
                let delta = self.entries[i] - previous;
                previous = self.entries[i];
                pack_delta_to_block(delta);
                i += 1;
            }
            if bit_index == 0 {
                bytes.write(&block[0..byte_index]);
            } else {
                bytes.write(&block[0..byte_index + 1]);
            }
        }

        bytes.into_bytes()
    }

    fn compute_entry_bits(entries: &[u64]) -> u8 {
        let mut previous = 0u64;
        let mut ored = 0u64;
        for &entry in entries {
            let delta = entry - previous;
            ored |= delta;
            previous = entry;
        }
        (64 - ored.leading_zeros()) as u8
    }

    fn num_entries_bytes(num_entries: usize) -> u8 {
        let n = num_entries as u32;
        let bits = u32::BITS - n.leading_zeros();
        bits.div_ceil(8) as u8
    }

    /// Deserializes a compact theta sketch from bytes.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        Self::deserialize_with_seed(bytes, DEFAULT_UPDATE_SEED)
    }

    /// Deserializes a compact theta sketch from bytes using the provided expected seed.
    pub fn deserialize_with_seed(bytes: &[u8], seed: u64) -> Result<Self, Error> {
        fn make_error(tag: &'static str) -> impl FnOnce(std::io::Error) -> Error {
            move |_| Error::insufficient_data(tag)
        }

        let mut cursor = SketchSlice::new(bytes);
        let pre_longs = cursor.read_u8().map_err(make_error("preamble_longs"))?;
        let ser_ver = cursor.read_u8().map_err(make_error("serial_version"))?;
        let family_id = cursor.read_u8().map_err(make_error("family_id"))?;

        Family::THETA.validate_id(family_id)?;

        // Validate pre_longs is within valid range for Theta sketch
        ensure_preamble_longs_in_range(
            Family::THETA.min_pre_longs..=Family::THETA.max_pre_longs,
            pre_longs,
        )?;

        match ser_ver {
            1 => Self::deserialize_v1(cursor, seed),
            2 => Self::deserialize_v2(pre_longs, cursor, seed),
            3 => Self::deserialize_v3(pre_longs, cursor, seed),
            4 => Self::deserialize_v4(pre_longs, cursor, seed),
            _ => Err(Error::deserial(format!(
                "unsupported serial version: expected 1, 2, 3, or 4, got {ser_ver}",
            ))),
        }
    }

    fn read_entries(
        cursor: &mut SketchSlice<'_>,
        num_entries: usize,
        theta: u64,
    ) -> Result<Vec<u64>, Error> {
        let mut entries = Vec::with_capacity(num_entries);
        for _ in 0..num_entries {
            let hash = cursor
                .read_u64_le()
                .map_err(|_| Error::insufficient_data("entries"))?;
            if hash == 0 || hash >= theta {
                return Err(Error::deserial("corrupted: invalid retained hash value"));
            }
            entries.push(hash);
        }
        Ok(entries)
    }

    fn deserialize_v1(mut cursor: SketchSlice<'_>, expected_seed: u64) -> Result<Self, Error> {
        fn make_error(tag: &'static str) -> impl FnOnce(std::io::Error) -> Error {
            move |_| Error::insufficient_data(tag)
        }

        let seed_hash = compute_seed_hash(expected_seed);
        cursor.read_u8().map_err(make_error("<unused>"))?;
        cursor.read_u32_le().map_err(make_error("<unused_u32_0>"))?;
        let num_entries = cursor.read_u32_le().map_err(make_error("num_entries"))? as usize;
        cursor.read_u32_le().map_err(make_error("<unused_u32_1>"))?;
        let theta = cursor.read_u64_le().map_err(make_error("theta_long"))?;

        let empty = num_entries == 0 && theta == MAX_THETA;
        if empty {
            return Ok(Self {
                entries: vec![],
                theta,
                seed_hash,
                ordered: true,
                empty: true,
            });
        }

        let entries = Self::read_entries(&mut cursor, num_entries, theta)?;

        Ok(Self {
            entries,
            theta,
            seed_hash,
            ordered: true,
            empty: false,
        })
    }

    fn deserialize_v2(
        pre_longs: u8,
        mut cursor: SketchSlice<'_>,
        expected_seed: u64,
    ) -> Result<Self, Error> {
        fn make_error(tag: &'static str) -> impl FnOnce(std::io::Error) -> Error {
            move |_| Error::insufficient_data(tag)
        }

        cursor.read_u8().map_err(make_error("<unused>"))?;
        cursor.read_u16_le().map_err(make_error("<unused_u16>"))?;
        let seed_hash = cursor.read_u16_le().map_err(make_error("seed_hash"))?;
        let expected_seed_hash = compute_seed_hash(expected_seed);
        if seed_hash != expected_seed_hash {
            return Err(Error::deserial(format!(
                "incompatible seed hash: expected {expected_seed_hash}, got {seed_hash}",
            )));
        }

        match pre_longs {
            1 => Ok(Self {
                entries: vec![],
                theta: MAX_THETA,
                seed_hash,
                ordered: true,
                empty: true,
            }),
            2 => {
                let num_entries = cursor.read_u32_le().map_err(make_error("num_entries"))? as usize;
                cursor.read_u32_le().map_err(make_error("<unused_u32>"))?;
                let entries = Self::read_entries(&mut cursor, num_entries, MAX_THETA)?;
                Ok(Self {
                    entries,
                    theta: MAX_THETA,
                    seed_hash,
                    ordered: true,
                    empty: true,
                })
            }
            3 => {
                let num_entries = cursor.read_u32_le().map_err(make_error("num_entries"))? as usize;
                cursor.read_u32_le().map_err(make_error("<unused_u32>"))?;
                let theta = cursor.read_u64_le().map_err(make_error("theta_long"))?;
                let empty = (num_entries == 0) && (theta == MAX_THETA);
                let entries = Self::read_entries(&mut cursor, num_entries, theta)?;
                Ok(Self {
                    entries,
                    theta,
                    seed_hash,
                    ordered: true,
                    empty,
                })
            }
            _ => Err(Error::invalid_preamble_longs(&[1, 2, 3], pre_longs)),
        }
    }

    fn deserialize_v3(
        pre_longs: u8,
        mut cursor: SketchSlice<'_>,
        expected_seed: u64,
    ) -> Result<Self, Error> {
        fn make_error(tag: &'static str) -> impl FnOnce(std::io::Error) -> Error {
            move |_| Error::insufficient_data(tag)
        }
        cursor.read_u16_le().map_err(make_error("<unused_u32>"))?;
        let flags = cursor.read_u8().map_err(make_error("flags"))?;
        let seed_hash = cursor.read_u16_le().map_err(make_error("seed_hash"))?;

        let empty = (flags & serialization::FLAGS_IS_EMPTY) != 0;
        let mut theta = MAX_THETA;
        let num_entries;
        let mut entries = vec![];
        if !empty {
            let expected_seed_hash = compute_seed_hash(expected_seed);
            if seed_hash != expected_seed_hash {
                return Err(Error::deserial(format!(
                    "incompatible seed hash: expected {expected_seed_hash}, got {seed_hash}",
                )));
            }
            if pre_longs == 1 {
                num_entries = 1;
            } else {
                num_entries = cursor.read_u32_le().map_err(make_error("num_entries"))?;
                cursor.read_u32_le().map_err(make_error("<unused_u32>"))?;
                if pre_longs > 2 {
                    theta = cursor.read_u64_le().map_err(make_error("theta_long"))?;
                }
            }
            entries = Self::read_entries(&mut cursor, num_entries as usize, theta)?;
        }
        let ordered = (flags & serialization::FLAGS_IS_ORDERED) != 0;
        Ok(Self {
            entries,
            theta,
            seed_hash,
            ordered,
            empty,
        })
    }

    fn deserialize_v4(
        pre_longs: u8,
        mut cursor: SketchSlice<'_>,
        expected_seed: u64,
    ) -> Result<Self, Error> {
        fn make_error(tag: &'static str) -> impl FnOnce(std::io::Error) -> Error {
            move |_| Error::insufficient_data(tag)
        }
        let entry_bits = cursor.read_u8().map_err(make_error("entry_bits"))?;
        let num_entries_bytes = cursor.read_u8().map_err(make_error("num_entries"))?;
        let flags = cursor.read_u8().map_err(make_error("flags"))?;
        let seed_hash = cursor.read_u16_le().map_err(make_error("seed_hash"))?;
        let empty = (flags & serialization::FLAGS_IS_EMPTY) != 0;
        if !empty {
            let expected_seed_hash = compute_seed_hash(expected_seed);
            if seed_hash != expected_seed_hash {
                return Err(Error::deserial(format!(
                    "incompatible seed hash: expected {expected_seed_hash}, got {seed_hash}",
                )));
            }
        }
        let theta = if pre_longs > 1 {
            cursor.read_u64_le().map_err(make_error("theta_long"))?
        } else {
            MAX_THETA
        };

        // unpack num_entries
        let mut num_entries = 0usize;
        for i in 0..num_entries_bytes {
            let entry_count_byte = cursor.read_u8().map_err(make_error("num_entries_byte"))?;
            num_entries |= (entry_count_byte as usize) << ((i as usize) << 3);
        }

        // unpack blocks of 8 deltas
        let mut i = 0usize;
        let mut entries = vec![0u64; num_entries];
        while i + 7 < num_entries {
            let mut block = vec![0u8; entry_bits as usize];
            cursor
                .read_exact(&mut block)
                .map_err(make_error("delta_block8"))?;
            unpack_bits_block8(&mut entries[i..i + 8], &block, entry_bits);
            i += 8;
        }

        // unpack extra deltas if fewer than 8 of them left
        if i < num_entries {
            // read extra bytes
            let rem = num_entries - i;
            let bytes_needed = (rem * entry_bits as usize).div_ceil(8);
            let mut tail = vec![0u8; bytes_needed];
            cursor
                .read_exact(&mut tail)
                .map_err(make_error("delta_tail"))?;

            let mut byte_index = 0;
            let mut bit_index = 0;
            let mut unpack_next_delta = || {
                let (delta, next_cursor) = unpack_bits(entry_bits, &tail, byte_index, bit_index);
                (byte_index, bit_index) = next_cursor;
                delta
            };

            for slot in entries.iter_mut().take(num_entries).skip(i) {
                *slot = unpack_next_delta();
            }
        }

        // undo deltas
        let mut previous = 0;
        for e in &mut entries {
            *e += previous;
            previous = *e;
            if *e == 0 || *e >= theta {
                return Err(Error::deserial("corrupted: invalid retained hash value"));
            }
        }

        let ordered = (flags & serialization::FLAGS_IS_ORDERED) != 0;

        Ok(Self {
            entries,
            theta,
            seed_hash,
            ordered,
            empty,
        })
    }
}

/// Builder for ThetaSketch
#[derive(Debug)]
pub struct ThetaSketchBuilder {
    lg_k: u8,
    resize_factor: ResizeFactor,
    sampling_probability: f32,
    seed: u64,
}

impl Default for ThetaSketchBuilder {
    fn default() -> Self {
        Self {
            lg_k: DEFAULT_LG_K,
            resize_factor: ResizeFactor::X8,
            sampling_probability: 1.0,
            seed: DEFAULT_UPDATE_SEED,
        }
    }
}

impl ThetaSketchBuilder {
    /// Set lg_k (log2 of nominal size k).
    ///
    /// # Panics
    ///
    /// If lg_k is not in range [5, 26]
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::theta::ThetaSketch;
    /// let sketch = ThetaSketch::builder().lg_k(12).build();
    /// assert_eq!(sketch.lg_k(), 12);
    /// ```
    pub fn lg_k(mut self, lg_k: u8) -> Self {
        assert!(
            (MIN_LG_K..=MAX_LG_K).contains(&lg_k),
            "lg_k must be in [{}, {}], got {}",
            MIN_LG_K,
            MAX_LG_K,
            lg_k
        );
        self.lg_k = lg_k;
        self
    }

    /// Set resize factor.
    pub fn resize_factor(mut self, factor: ResizeFactor) -> Self {
        self.resize_factor = factor;
        self
    }

    /// Set sampling probability p.
    ///
    /// The sampling probability controls the fraction of hashed values that are retained.
    /// Must be greater than 0 to ensure valid theta values for bound calculations.
    ///
    /// # Panics
    ///
    /// Panics if p is not in range (0.0, 1.0]
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::theta::ThetaSketch;
    /// let _sketch = ThetaSketch::builder().sampling_probability(0.5).build();
    /// ```
    pub fn sampling_probability(mut self, probability: f32) -> Self {
        assert!(
            (0.0..=1.0).contains(&probability) && probability > 0.0,
            "sampling_probability must be in (0.0, 1.0], got {probability}"
        );
        self.sampling_probability = probability;
        self
    }

    /// Set hash seed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::theta::ThetaSketch;
    /// let _sketch = ThetaSketch::builder().seed(7).build();
    /// ```
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Build the ThetaSketch.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::theta::ThetaSketch;
    /// let sketch = ThetaSketch::builder().lg_k(10).build();
    /// assert_eq!(sketch.lg_k(), 10);
    /// ```
    pub fn build(self) -> ThetaSketch {
        let table = ThetaHashTable::new(
            self.lg_k,
            self.resize_factor,
            self.sampling_probability,
            self.seed,
        );

        ThetaSketch { table }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sorted_theta_entries(sketch: &ThetaSketch) -> Vec<u64> {
        let mut entries: Vec<u64> = sketch.iter().collect();
        entries.sort_unstable();
        entries
    }

    fn sorted_compact_entries(sketch: &CompactThetaSketch) -> Vec<u64> {
        let mut entries: Vec<u64> = sketch.iter().collect();
        entries.sort_unstable();
        entries
    }

    fn assert_theta_and_compact_equivalent_ordered(theta: &ThetaSketch, ordered: bool) {
        let compact = theta.compact(ordered);
        assert_theta_and_compact_equivalent(theta, &compact);
        if compact.num_retained() > 1 {
            assert_eq!(compact.is_ordered(), ordered);
        }
    }

    fn assert_theta_and_compact_equivalent(theta: &ThetaSketch, compact: &CompactThetaSketch) {
        assert_eq!(theta.is_empty(), compact.is_empty());
        assert_eq!(theta.is_estimation_mode(), compact.is_estimation_mode());
        assert_eq!(theta.num_retained(), compact.num_retained());
        assert_eq!(theta.theta64(), compact.theta64());
        assert_eq!(sorted_theta_entries(theta), sorted_compact_entries(compact));
        assert!((theta.estimate() - compact.estimate()).abs() <= 1e-12);
    }

    fn assert_compact_equivalent(a: &CompactThetaSketch, b: &CompactThetaSketch) {
        assert_eq!(a.is_empty(), b.is_empty());
        assert_eq!(a.is_estimation_mode(), b.is_estimation_mode());
        assert_eq!(a.is_ordered(), b.is_ordered());
        assert_eq!(a.num_retained(), b.num_retained());
        assert_eq!(a.theta64(), b.theta64());
        assert_eq!(a.seed_hash(), b.seed_hash());
        assert_eq!(sorted_compact_entries(a), sorted_compact_entries(b));
        assert!((a.estimate() - b.estimate()).abs() <= 1e-12);
    }

    fn assert_compressed_round_trip(theta: &ThetaSketch, compact: &CompactThetaSketch) {
        let bytes_v4 = compact.serialize_compressed();
        assert_eq!(bytes_v4[1], serialization::COMPRESSED_SERIAL_VERSION);
        let decoded_v4 = CompactThetaSketch::deserialize(&bytes_v4).unwrap();
        assert_compact_equivalent(compact, &decoded_v4);
        assert_theta_and_compact_equivalent(theta, &decoded_v4);
    }

    #[test]
    fn theta_and_compact_theta_equivalent() {
        let mut exact_theta = ThetaSketch::builder().lg_k(12).build();
        for i in 0..2000 {
            exact_theta.update(i);
        }
        assert!(!exact_theta.is_estimation_mode());
        for ordered in [false, true] {
            assert_theta_and_compact_equivalent_ordered(&exact_theta, ordered);
        }

        let mut estimation_theta = ThetaSketch::builder().lg_k(5).build();
        for i in 0..5000 {
            estimation_theta.update(i);
        }
        assert!(estimation_theta.is_estimation_mode());
        for ordered in [false, true] {
            assert_theta_and_compact_equivalent_ordered(&estimation_theta, ordered);
        }
    }

    #[test]
    fn compact_theta_serialize_deserialize_round_trip_equivalent_to_compact_and_theta() {
        let mut theta = ThetaSketch::builder().lg_k(5).build();
        for i in 0..5000 {
            theta.update(i);
        }
        let compact = theta.compact(true);
        assert!(compact.is_ordered());
        assert!(compact.is_estimation_mode());

        let bytes_v3 = compact.serialize();
        let decoded_v3 = CompactThetaSketch::deserialize(&bytes_v3).unwrap();
        assert_compact_equivalent(&compact, &decoded_v3);
        assert_theta_and_compact_equivalent(&theta, &decoded_v3);
    }

    #[test]
    fn compact_theta_serialize_compressed_round_trip_tail_entries() {
        let mut theta = ThetaSketch::builder().lg_k(12).build();
        for i in 0..13 {
            theta.update(i);
        }

        let compact = theta.compact(true);
        assert_eq!(compact.num_retained() % 8, 5);
        assert!(!compact.is_estimation_mode());
        assert!(compact.is_ordered());

        assert_compressed_round_trip(&theta, &compact);
    }

    #[test]
    fn compact_theta_serialize_compressed_round_trip_more_than_255_entries() {
        let mut theta = ThetaSketch::builder().lg_k(12).build();
        for i in 0..300 {
            theta.update(i);
        }

        let compact = theta.compact(true);
        assert!(compact.num_retained() > 255);
        assert!(!compact.is_estimation_mode());
        assert!(compact.is_ordered());

        assert_compressed_round_trip(&theta, &compact);
    }

    #[test]
    fn compact_theta_serialize_compressed_round_trip_estimation_mode() {
        let mut theta = ThetaSketch::builder().lg_k(5).build();
        for i in 0..5000 {
            theta.update(i);
        }

        let compact = theta.compact(true);
        assert!(compact.is_estimation_mode());
        assert!(compact.is_ordered());

        assert_compressed_round_trip(&theta, &compact);
    }

    #[test]
    fn deserialize_rejects_seed_hash_mismatch() {
        let mut theta = ThetaSketch::builder().seed(7).build();
        theta.update("apple");
        let bytes = theta.compact(true).serialize();

        let err = CompactThetaSketch::deserialize_with_seed(&bytes, 8).unwrap_err();
        assert_eq!(err.kind(), crate::error::ErrorKind::InvalidData);
        assert!(err.message().contains("incompatible seed hash"));
    }

    #[test]
    fn deserialize_rejects_invalid_family_id() {
        let mut theta = ThetaSketch::builder().build();
        theta.update("apple");
        let mut bytes = theta.compact(true).serialize();
        bytes[2] = 0;

        let err = CompactThetaSketch::deserialize(&bytes).unwrap_err();
        assert_eq!(err.kind(), crate::error::ErrorKind::InvalidData);
        assert!(err.message().contains("invalid family"));
    }

    #[test]
    fn deserialize_rejects_unsupported_serial_version() {
        let mut theta = ThetaSketch::builder().build();
        theta.update("apple");
        let mut bytes = theta.compact(true).serialize();
        bytes[1] = 99;

        let err = CompactThetaSketch::deserialize(&bytes).unwrap_err();
        assert_eq!(err.kind(), crate::error::ErrorKind::InvalidData);
        assert!(err.message().contains("unsupported serial version"));
    }

    #[test]
    fn deserialize_rejects_truncated_payload() {
        let mut theta = ThetaSketch::builder().lg_k(5).build();
        for i in 0..5000 {
            theta.update(i);
        }
        let mut bytes = theta.compact(true).serialize();
        bytes.pop();

        let err = CompactThetaSketch::deserialize(&bytes).unwrap_err();
        assert_eq!(err.kind(), crate::error::ErrorKind::InvalidData);
        assert!(err.message().contains("insufficient data"));
    }
}
