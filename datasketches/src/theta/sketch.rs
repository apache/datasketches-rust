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
use crate::common::NumStdDev;
use crate::common::ResizeFactor;
use crate::common::binomial_bounds;
use crate::common::canonical_double;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::compute_seed_hash;
use crate::theta::hash_table::DEFAULT_LG_K;
use crate::theta::hash_table::MAX_LG_K;
use crate::theta::hash_table::MAX_THETA;
use crate::theta::hash_table::MIN_LG_K;
use crate::theta::hash_table::ThetaHashTable;
use crate::theta::serialization::FLAG_IS_COMPACT;
use crate::theta::serialization::FLAG_IS_EMPTY;
use crate::theta::serialization::FLAG_IS_ORDERED;
use crate::theta::serialization::FLAG_IS_READ_ONLY;
use crate::theta::serialization::HASH_SIZE_BYTES;
use crate::theta::serialization::PREAMBLE_LONGS_EMPTY;
use crate::theta::serialization::PREAMBLE_LONGS_ESTIMATION;
use crate::theta::serialization::PREAMBLE_LONGS_EXACT;
use crate::theta::serialization::SERIAL_VERSION;
use crate::theta::serialization::THETA_FAMILY_ID;

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

    /// Serialize the sketch to bytes in compact format.
    ///
    /// The serialized format is compatible with Java and C++ DataSketches
    /// implementations.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::theta::ThetaSketch;
    /// let mut sketch = ThetaSketch::builder().build();
    /// sketch.update("apple");
    /// let bytes = sketch.serialize();
    /// let restored = ThetaSketch::deserialize(&bytes).unwrap();
    /// assert_eq!(sketch.estimate(), restored.estimate());
    /// ```
    pub fn serialize(&self) -> Vec<u8> {
        // Determine preamble size based on state
        let is_empty = self.is_empty();
        let is_estimation_mode = self.is_estimation_mode();

        let preamble_longs = if is_empty {
            PREAMBLE_LONGS_EMPTY
        } else if is_estimation_mode {
            PREAMBLE_LONGS_ESTIMATION
        } else {
            PREAMBLE_LONGS_EXACT
        };

        let num_entries = self.num_retained();
        let preamble_bytes = (preamble_longs as usize) * 8;
        let data_bytes = num_entries * HASH_SIZE_BYTES;
        let total_bytes = preamble_bytes + data_bytes;

        let mut bytes = SketchBytes::with_capacity(total_bytes);

        // Build flags byte
        let mut flags: u8 = FLAG_IS_COMPACT | FLAG_IS_READ_ONLY | FLAG_IS_ORDERED;
        if is_empty {
            flags |= FLAG_IS_EMPTY;
        }

        // Write preamble (first 8 bytes always present)
        bytes.write_u8(preamble_longs);
        bytes.write_u8(SERIAL_VERSION);
        bytes.write_u8(THETA_FAMILY_ID);
        bytes.write_u8(self.lg_k());
        bytes.write_u8(self.lg_k()); // lgArr = lgK for compact
        bytes.write_u8(flags);
        bytes.write_u16_le(compute_seed_hash(self.table.seed()));

        // Write second 8 bytes if not empty (retained count + padding)
        if !is_empty {
            bytes.write_u32_le(num_entries as u32);
            bytes.write_u32_le(0); // padding (p field, unused in compact)
        }

        // Write theta if in estimation mode
        if is_estimation_mode {
            bytes.write_u64_le(self.table.theta());
        }

        // Write sorted hash values
        let mut entries: Vec<u64> = self.iter().collect();
        entries.sort_unstable();
        for entry in entries {
            bytes.write_u64_le(entry);
        }

        bytes.into_bytes()
    }

    /// Deserialize a sketch from bytes.
    ///
    /// Uses the default seed (9001). For sketches created with a different seed,
    /// use [`deserialize_with_seed`](Self::deserialize_with_seed).
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes are invalid or corrupted.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::theta::ThetaSketch;
    /// let mut sketch = ThetaSketch::builder().build();
    /// sketch.update("apple");
    /// let bytes = sketch.serialize();
    /// let restored = ThetaSketch::deserialize(&bytes).unwrap();
    /// assert_eq!(sketch.estimate(), restored.estimate());
    /// ```
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        Self::deserialize_with_seed(bytes, DEFAULT_UPDATE_SEED)
    }

    /// Deserialize a sketch from bytes with a specific seed.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The serialized sketch bytes
    /// * `seed` - The seed used during sketch creation
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The bytes are too short
    /// - The format is invalid (wrong family ID, unsupported version)
    /// - The seed hash doesn't match
    pub fn deserialize_with_seed(bytes: &[u8], seed: u64) -> Result<Self, Error> {
        fn make_error(tag: &'static str) -> impl FnOnce(std::io::Error) -> Error {
            move |_| Error::insufficient_data(tag)
        }

        if bytes.len() < 8 {
            return Err(Error::insufficient_data("preamble"));
        }

        let mut cursor = SketchSlice::new(bytes);

        // Read first 8 bytes (always present)
        let preamble_longs = cursor.read_u8().map_err(make_error("preamble_longs"))?;
        let serial_version = cursor.read_u8().map_err(make_error("serial_version"))?;
        let family_id = cursor.read_u8().map_err(make_error("family_id"))?;
        let lg_k = cursor.read_u8().map_err(make_error("lg_k"))?;
        let _lg_arr = cursor.read_u8().map_err(make_error("lg_arr"))?;
        let flags = cursor.read_u8().map_err(make_error("flags"))?;
        let stored_seed_hash = cursor.read_u16_le().map_err(make_error("seed_hash"))?;

        // Validate format
        if family_id != THETA_FAMILY_ID {
            return Err(Error::invalid_family(
                THETA_FAMILY_ID,
                family_id,
                "ThetaSketch",
            ));
        }
        if serial_version != SERIAL_VERSION && serial_version != 1 && serial_version != 2 {
            return Err(Error::unsupported_serial_version(
                SERIAL_VERSION,
                serial_version,
            ));
        }
        if !(MIN_LG_K..=MAX_LG_K).contains(&lg_k) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("lg_k {} is out of range [{}, {}]", lg_k, MIN_LG_K, MAX_LG_K),
            ));
        }

        // Validate seed hash
        let expected_seed_hash = compute_seed_hash(seed);
        if stored_seed_hash != expected_seed_hash {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "seed hash mismatch: expected 0x{:04X}, got 0x{:04X}",
                    expected_seed_hash, stored_seed_hash
                ),
            ));
        }

        // Parse flags
        let is_empty = (flags & FLAG_IS_EMPTY) != 0;
        let _is_compact = (flags & FLAG_IS_COMPACT) != 0;

        // Handle empty sketch
        if is_empty {
            return Ok(ThetaSketch::builder().lg_k(lg_k).seed(seed).build());
        }

        // Read retained count (bytes 8-11)
        let num_entries = cursor.read_u32_le().map_err(make_error("num_entries"))? as usize;
        let _padding = cursor.read_u32_le().map_err(make_error("padding"))?;

        // Read theta if in estimation mode (preamble_longs >= 3)
        let theta = if preamble_longs >= PREAMBLE_LONGS_ESTIMATION {
            cursor.read_u64_le().map_err(make_error("theta"))?
        } else {
            MAX_THETA
        };

        // Read hash entries
        let mut entries = Vec::with_capacity(num_entries);
        for _ in 0..num_entries {
            let hash = cursor.read_u64_le().map_err(make_error("hash_entry"))?;
            entries.push(hash);
        }

        // Reconstruct the hash table
        let table = ThetaHashTable::from_entries(lg_k, seed, theta, entries);

        Ok(ThetaSketch { table })
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
