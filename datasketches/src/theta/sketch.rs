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

use crate::common::NumStdDev;
use crate::common::ResizeFactor;
use crate::common::binomial_bounds;
use crate::common::compute_seed_hash;
use crate::hash::DEFAULT_UPDATE_SEED;
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

    /// Update the sketch with a hashable value
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

    /// Update the sketch with a f64 value
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

    /// Update the sketch with a f32 value
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
        if ordered && entries.len() > 1 {
            entries.sort_unstable();
        }

        let theta = if entries.is_empty() {
            // Match Java's correctThetaOnCompact() behavior for never-updated sketches
            // initialized with p < 1.0.
            MAX_THETA
        } else {
            self.table.theta()
        };
        let empty = entries.is_empty();

        CompactThetaSketch {
            entries,
            theta,
            seed_hash: compute_seed_hash(self.table.hash_seed()),
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
    pub(crate) entries: Vec<u64>,
    pub(crate) theta: u64,
    pub(crate) seed_hash: u16,
    pub(crate) ordered: bool,
    pub(crate) empty: bool,
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
            .expect("theta should always be valid")
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
        .expect("theta should always be valid")
    }

    /// Serializes this sketch into the uncompressed (`serVer = 3`) compact theta format.
    pub fn serialize(&self) -> Vec<u8> {
        serialization::serialize_v3(self)
    }

    /// Deserializes a compact theta sketch from bytes using the default seed.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, crate::error::Error> {
        serialization::deserialize(bytes)
    }

    /// Deserializes a compact theta sketch from bytes using the provided expected seed.
    pub fn deserialize_with_seed(
        bytes: &[u8],
        expected_seed: u64,
    ) -> Result<Self, crate::error::Error> {
        serialization::deserialize_with_seed(bytes, expected_seed)
    }

    /// Serializes this sketch in compressed form if applicable.
    ///
    /// This uses `serVer = 4` when the sketch is ordered and suitable for compression, and falls
    /// back to uncompressed `serVer = 3` otherwise.
    pub fn serialize_compressed(&self) -> Vec<u8> {
        serialization::serialize_compressed(self)
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

/// Canonicalize double value for compatibility with Java
fn canonical_double(value: f64) -> i64 {
    if value.is_nan() {
        // Java's Double.doubleToLongBits() NaN value
        0x7ff8000000000000i64
    } else {
        // -0.0 + 0.0 == +0.0 under IEEE754 roundTiesToEven rounding mode,
        // which Rust guarantees. Thus, by adding a positive zero we
        // canonicalize signed zero without any branches in one instruction.
        (value + 0.0).to_bits() as i64
    }
}

#[cfg(test)]
mod compact_tests {
    use super::*;

    fn to_big_endian_v3(mut bytes: Vec<u8>) -> Vec<u8> {
        // Set big-endian flag bit
        bytes[5] |= crate::theta::serialization::FLAGS_IS_BIG_ENDIAN;
        let pre_longs = bytes[0] & 0x3f;

        // seed hash (bytes 6..8)
        bytes[6..8].reverse();

        match pre_longs {
            1 => {
                if bytes.len() == 16 {
                    bytes[8..16].reverse();
                }
            }
            2 => {
                bytes[8..12].reverse(); // curCount
                bytes[12..16].reverse(); // p float
                for chunk in bytes[16..].chunks_exact_mut(8) {
                    chunk.reverse();
                }
            }
            3 => {
                bytes[8..12].reverse(); // curCount
                bytes[12..16].reverse(); // p float
                bytes[16..24].reverse(); // theta
                for chunk in bytes[24..].chunks_exact_mut(8) {
                    chunk.reverse();
                }
            }
            _ => {}
        }

        bytes
    }

    #[test]
    fn compact_empty_sampling_corrects_theta_on_serialize() {
        let sketch = ThetaSketch::builder().sampling_probability(0.5).build();
        let compact = sketch.compact(true);
        assert!(compact.is_empty());
        assert_eq!(compact.theta64(), MAX_THETA);

        let bytes = compact.serialize();
        assert_eq!(bytes.len(), 8);
        assert_eq!(bytes[0] & 0x3f, 1);
        assert!((bytes[5] & crate::theta::serialization::FLAGS_IS_EMPTY) != 0);
        assert!((bytes[5] & crate::theta::serialization::FLAGS_IS_COMPACT) != 0);
        assert!((bytes[5] & crate::theta::serialization::FLAGS_IS_READ_ONLY) != 0);

        let decoded = CompactThetaSketch::deserialize(&bytes).unwrap();
        assert!(decoded.is_empty());
        assert_eq!(decoded.theta64(), MAX_THETA);
        assert_eq!(decoded.num_retained(), 0);
    }

    #[test]
    fn compact_single_item_round_trip() {
        let mut sketch = ThetaSketch::builder().build();
        sketch.update("apple");
        let compact = sketch.compact(true);
        assert!(!compact.is_empty());
        assert_eq!(compact.num_retained(), 1);
        assert_eq!(compact.theta64(), MAX_THETA);

        let bytes = compact.serialize();
        assert_eq!(bytes.len(), 16);
        assert_eq!(bytes[0] & 0x3f, 1);
        assert!((bytes[5] & crate::theta::serialization::FLAGS_IS_SINGLE_ITEM) != 0);

        let decoded = CompactThetaSketch::deserialize(&bytes).unwrap();
        assert!(!decoded.is_empty());
        assert_eq!(decoded.num_retained(), 1);
        assert!(decoded.is_ordered());
        assert_eq!(decoded.theta64(), MAX_THETA);
        assert_eq!(decoded.estimate(), 1.0);
    }

    #[test]
    fn compact_estimation_round_trip_ordered() {
        let mut sketch = ThetaSketch::builder().lg_k(5).build();
        for i in 0..200 {
            sketch.update(i);
        }
        assert!(sketch.is_estimation_mode());

        let compact = sketch.compact(true);
        assert!(compact.is_estimation_mode());
        assert!(compact.is_ordered());

        let bytes = compact.serialize();
        assert_eq!(bytes[0] & 0x3f, 3);

        let decoded = CompactThetaSketch::deserialize(&bytes).unwrap();
        assert!(decoded.is_estimation_mode());
        assert!(decoded.is_ordered());
        assert_eq!(decoded.num_retained(), compact.num_retained());
        assert_eq!(decoded.theta64(), compact.theta64());
    }

    #[test]
    fn deserialize_big_endian_v3() {
        let mut sketch = ThetaSketch::builder().lg_k(5).build();
        for i in 0..200 {
            sketch.update(i);
        }
        let compact = sketch.compact(true);
        let bytes_le = compact.serialize();
        let bytes_be = to_big_endian_v3(bytes_le);

        let decoded = CompactThetaSketch::deserialize(&bytes_be).unwrap();
        assert_eq!(decoded.num_retained(), compact.num_retained());
        assert_eq!(decoded.theta64(), compact.theta64());
    }

    #[test]
    fn deserialize_rejects_seed_hash_mismatch() {
        let mut sketch = ThetaSketch::builder().seed(7).build();
        sketch.update("apple");
        let bytes = sketch.compact(true).serialize();

        let err =
            CompactThetaSketch::deserialize_with_seed(&bytes, DEFAULT_UPDATE_SEED).unwrap_err();
        assert_eq!(err.kind(), crate::error::ErrorKind::InvalidData);
        assert!(err.message().contains("incompatible seed hash"));
    }

    #[test]
    fn compact_serialize_compressed_uses_v4_when_suitable() {
        let mut sketch = ThetaSketch::builder().lg_k(10).build();
        for i in 0..1000 {
            sketch.update(i);
        }
        let compact = sketch.compact(true);
        assert!(compact.is_ordered());

        let bytes = compact.serialize_compressed();
        assert_eq!(bytes[1], crate::theta::serialization::SERIAL_VERSION_V4);

        let decoded = CompactThetaSketch::deserialize(&bytes).unwrap();
        assert_eq!(decoded.num_retained(), compact.num_retained());
        assert_eq!(decoded.theta64(), compact.theta64());
        assert!(decoded.is_ordered());
    }
}
