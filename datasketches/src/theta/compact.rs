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

//! Compact Theta sketch implementation
//!
//! A CompactThetaSketch is an immutable, serialized form of a Theta sketch.
//! It stores only the essential data needed for estimation and set operations:
//! - Theta value (sampling threshold)
//! - Sorted hash values
//! - Seed hash for validation
//!
//! This format is compatible with the Apache DataSketches "compact" format
//! used by Java, C++, and Python implementations.

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::common::NumStdDev;
use crate::common::binomial_bounds;
use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::compute_seed_hash;
use crate::theta::hash_table::MAX_THETA;
use crate::theta::serialization::*;

/// A compact, immutable Theta sketch.
///
/// This is the serialized form of a Theta sketch, optimized for storage and
/// transmission. It contains sorted hash values and can be used for:
/// - Cardinality estimation
/// - Set operations (union, intersection, difference)
/// - Serialization to/from bytes
///
/// Unlike [`ThetaSketch`](super::ThetaSketch), this sketch cannot be updated
/// with new values.
///
/// # Example
///
/// ```
/// use datasketches::theta::CompactThetaSketch;
/// use datasketches::theta::ThetaSketch;
///
/// let mut sketch = ThetaSketch::builder().build();
/// sketch.update("apple");
/// sketch.update("banana");
///
/// // Convert to compact form for serialization
/// let compact = sketch.compact();
/// let bytes = compact.serialize();
///
/// // Deserialize
/// let restored = CompactThetaSketch::deserialize(&bytes).unwrap();
/// assert_eq!(compact.estimate(), restored.estimate());
/// ```
#[derive(Debug, Clone)]
pub struct CompactThetaSketch {
    theta: u64,
    entries: Vec<u64>,
    seed_hash: u16,
    is_empty: bool,
}

impl CompactThetaSketch {
    /// Create a new compact sketch from components
    pub(crate) fn new(theta: u64, entries: Vec<u64>, seed_hash: u16, is_empty: bool) -> Self {
        Self {
            theta,
            entries,
            seed_hash,
            is_empty,
        }
    }

    /// Check if the sketch is empty (no values have been added)
    pub fn is_empty(&self) -> bool {
        self.is_empty
    }

    /// Get the cardinality estimate
    ///
    /// Returns the estimated number of distinct values that were inserted
    /// into the original sketch.
    pub fn estimate(&self) -> f64 {
        if self.is_empty {
            return 0.0;
        }
        let num_retained = self.entries.len() as f64;
        let theta_fraction = self.theta as f64 / MAX_THETA as f64;
        num_retained / theta_fraction
    }

    /// Return theta as a fraction (0.0 to 1.0)
    pub fn theta(&self) -> f64 {
        self.theta as f64 / MAX_THETA as f64
    }

    /// Return theta as u64
    pub fn theta64(&self) -> u64 {
        self.theta
    }

    /// Check if sketch is in estimation mode
    pub fn is_estimation_mode(&self) -> bool {
        self.theta < MAX_THETA
    }

    /// Return number of retained entries
    pub fn num_retained(&self) -> usize {
        self.entries.len()
    }

    /// Return iterator over hash values
    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.entries.iter().copied()
    }

    /// Get the seed hash
    pub fn seed_hash(&self) -> u16 {
        self.seed_hash
    }

    /// Returns the approximate lower error bound given the specified number of Standard Deviations.
    pub fn lower_bound(&self, num_std_dev: NumStdDev) -> f64 {
        if self.is_empty {
            return 0.0;
        }
        if !self.is_estimation_mode() {
            return self.num_retained() as f64;
        }
        binomial_bounds::lower_bound(self.num_retained() as u64, self.theta(), num_std_dev)
            .expect("theta should always be valid")
    }

    /// Returns the approximate upper error bound given the specified number of Standard Deviations.
    pub fn upper_bound(&self, num_std_dev: NumStdDev) -> f64 {
        if self.is_empty {
            return 0.0;
        }
        if !self.is_estimation_mode() {
            return self.num_retained() as f64;
        }
        binomial_bounds::upper_bound(
            self.num_retained() as u64,
            self.theta(),
            num_std_dev,
            self.is_empty,
        )
        .expect("theta should always be valid")
    }

    /// Serialize the compact sketch to bytes
    ///
    /// The format is compatible with the Apache DataSketches compact sketch format.
    ///
    /// # Example
    ///
    /// ```
    /// use datasketches::theta::CompactThetaSketch;
    /// use datasketches::theta::ThetaSketch;
    ///
    /// let mut sketch = ThetaSketch::builder().build();
    /// sketch.update("test");
    /// let compact = sketch.compact();
    /// let bytes = compact.serialize();
    /// assert!(!bytes.is_empty());
    /// ```
    pub fn serialize(&self) -> Vec<u8> {
        let is_estimation_mode = self.is_estimation_mode();
        let num_entries = self.entries.len();

        let preamble_longs = if self.is_empty {
            PREAMBLE_LONGS_EMPTY
        } else if is_estimation_mode {
            PREAMBLE_LONGS_ESTIMATION
        } else {
            PREAMBLE_LONGS_EXACT
        };

        let preamble_bytes = (preamble_longs as usize) * 8;
        let total_size = preamble_bytes + num_entries * HASH_SIZE_BYTES;
        let mut bytes = SketchBytes::with_capacity(total_size);

        bytes.write_u8(preamble_longs);
        bytes.write_u8(SERIAL_VERSION);
        bytes.write_u8(THETA_FAMILY_ID);
        bytes.write_u8(0);
        bytes.write_u8(0);

        let mut flags = FLAG_READ_ONLY | FLAG_COMPACT | FLAG_ORDERED;
        if self.is_empty {
            flags |= FLAG_EMPTY;
        }
        bytes.write_u8(flags);
        bytes.write_u16_le(self.seed_hash);

        if preamble_longs >= PREAMBLE_LONGS_EXACT {
            bytes.write_u32_le(num_entries as u32);
            bytes.write_u32_le(DEFAULT_P_FLOAT_BITS);
        }

        if preamble_longs >= PREAMBLE_LONGS_ESTIMATION {
            bytes.write_u64_le(self.theta);
        }

        for hash in &self.entries {
            bytes.write_u64_le(*hash);
        }

        bytes.into_bytes()
    }

    /// Deserialize a compact sketch from bytes
    ///
    /// Uses the default seed for validation.
    ///
    /// # Example
    ///
    /// ```
    /// use datasketches::theta::CompactThetaSketch;
    /// use datasketches::theta::ThetaSketch;
    ///
    /// let mut sketch = ThetaSketch::builder().build();
    /// sketch.update("test");
    /// let compact = sketch.compact();
    /// let bytes = compact.serialize();
    ///
    /// let restored = CompactThetaSketch::deserialize(&bytes).unwrap();
    /// assert_eq!(compact.estimate(), restored.estimate());
    /// ```
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        Self::deserialize_with_seed(bytes, DEFAULT_UPDATE_SEED)
    }

    /// Deserialize a compact sketch from bytes with a specific seed
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The data is too short
    /// - The family ID doesn't match
    /// - The serial version is unsupported
    /// - The seed hash doesn't match
    pub fn deserialize_with_seed(bytes: &[u8], seed: u64) -> Result<Self, Error> {
        fn make_error(tag: &'static str) -> impl FnOnce(std::io::Error) -> Error {
            move |_| Error::insufficient_data(tag)
        }

        let mut cursor = SketchSlice::new(bytes);

        let preamble_longs = cursor.read_u8().map_err(make_error("preamble_longs"))?;
        let serial_version = cursor.read_u8().map_err(make_error("serial_version"))?;
        let family_id = cursor.read_u8().map_err(make_error("family_id"))?;
        let _lg_k = cursor.read_u8().map_err(make_error("lg_k"))?;
        let _lg_resize = cursor.read_u8().map_err(make_error("lg_resize"))?;
        let flags = cursor.read_u8().map_err(make_error("flags"))?;
        let seed_hash = cursor.read_u16_le().map_err(make_error("seed_hash"))?;

        if family_id != THETA_FAMILY_ID {
            return Err(Error::invalid_family(THETA_FAMILY_ID, family_id, "Theta"));
        }
        if serial_version != SERIAL_VERSION {
            return Err(Error::unsupported_serial_version(
                SERIAL_VERSION,
                serial_version,
            ));
        }

        // Validate seed hash (seed_hash = 0 means legacy format, skip validation)
        let expected_seed_hash = compute_seed_hash(seed);
        if seed_hash != 0 && seed_hash != expected_seed_hash {
            return Err(Error::deserial(format!(
                "seed hash mismatch: expected {expected_seed_hash}, got {seed_hash}"
            )));
        }
        let seed_hash = if seed_hash == 0 {
            expected_seed_hash
        } else {
            seed_hash
        };

        let is_empty = (flags & FLAG_EMPTY) != 0;
        let is_compact = (flags & FLAG_COMPACT) != 0;
        let is_single_item = (flags & FLAG_SINGLE_ITEM) != 0;

        if !is_compact {
            return Err(Error::deserial(
                "only compact sketches are supported".to_string(),
            ));
        }

        if is_empty {
            return Ok(Self {
                theta: MAX_THETA,
                entries: Vec::new(),
                seed_hash,
                is_empty: true,
            });
        }

        // Handle single-item format: preamble_longs = 1 with exactly one hash entry
        if preamble_longs == PREAMBLE_LONGS_EMPTY && is_single_item {
            let hash = cursor
                .read_u64_le()
                .map_err(make_error("single_item_hash"))?;
            return Ok(Self {
                theta: MAX_THETA,
                entries: vec![hash],
                seed_hash,
                is_empty: false,
            });
        }

        if preamble_longs < PREAMBLE_LONGS_EXACT {
            return Err(Error::deserial(format!(
                "non-empty sketch requires at least {PREAMBLE_LONGS_EXACT} preamble longs, got {preamble_longs}"
            )));
        }

        let num_entries = cursor.read_u32_le().map_err(make_error("num_entries"))? as usize;
        let _p = cursor.read_u32_le().map_err(make_error("p"))?;

        let theta = if preamble_longs >= PREAMBLE_LONGS_ESTIMATION {
            cursor.read_u64_le().map_err(make_error("theta"))?
        } else {
            MAX_THETA
        };

        let mut entries = Vec::with_capacity(num_entries);
        for i in 0..num_entries {
            let hash = cursor.read_u64_le().map_err(|_| {
                Error::insufficient_data(format!(
                    "expected {num_entries} entries, failed at index {i}"
                ))
            })?;
            entries.push(hash);
        }

        Ok(Self {
            theta,
            entries,
            seed_hash,
            is_empty: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_compact_sketch() {
        let sketch = CompactThetaSketch::new(
            MAX_THETA,
            Vec::new(),
            compute_seed_hash(DEFAULT_UPDATE_SEED),
            true,
        );
        assert!(sketch.is_empty());
        assert_eq!(sketch.estimate(), 0.0);
        assert_eq!(sketch.num_retained(), 0);
        assert!(!sketch.is_estimation_mode());
    }

    #[test]
    fn test_compact_sketch_with_entries() {
        let entries = vec![100, 200, 300];
        let sketch = CompactThetaSketch::new(
            MAX_THETA,
            entries.clone(),
            compute_seed_hash(DEFAULT_UPDATE_SEED),
            false,
        );
        assert!(!sketch.is_empty());
        assert_eq!(sketch.num_retained(), 3);
        assert_eq!(sketch.estimate(), 3.0);
        assert!(!sketch.is_estimation_mode());
    }

    #[test]
    fn test_compact_sketch_estimation_mode() {
        let entries = vec![100, 200, 300];
        let theta = MAX_THETA / 2; // Half of max theta
        let sketch = CompactThetaSketch::new(
            theta,
            entries,
            compute_seed_hash(DEFAULT_UPDATE_SEED),
            false,
        );
        assert!(sketch.is_estimation_mode());
        assert!(sketch.estimate() > 3.0); // Should be approximately 6.0
    }

    #[test]
    fn test_serialize_deserialize_empty() {
        let sketch = CompactThetaSketch::new(
            MAX_THETA,
            Vec::new(),
            compute_seed_hash(DEFAULT_UPDATE_SEED),
            true,
        );
        let bytes = sketch.serialize();
        let restored = CompactThetaSketch::deserialize(&bytes).unwrap();

        assert!(restored.is_empty());
        assert_eq!(sketch.theta64(), restored.theta64());
        assert_eq!(sketch.seed_hash(), restored.seed_hash());
    }

    #[test]
    fn test_serialize_deserialize_exact_mode() {
        let entries = vec![100, 200, 300, 400, 500];
        let sketch = CompactThetaSketch::new(
            MAX_THETA,
            entries.clone(),
            compute_seed_hash(DEFAULT_UPDATE_SEED),
            false,
        );
        let bytes = sketch.serialize();
        let restored = CompactThetaSketch::deserialize(&bytes).unwrap();

        assert!(!restored.is_empty());
        assert!(!restored.is_estimation_mode());
        assert_eq!(sketch.num_retained(), restored.num_retained());
        assert_eq!(sketch.estimate(), restored.estimate());
        assert_eq!(sketch.theta64(), restored.theta64());

        // Verify all entries match
        let restored_entries: Vec<u64> = restored.iter().collect();
        assert_eq!(entries, restored_entries);
    }

    #[test]
    fn test_serialize_deserialize_estimation_mode() {
        let entries = vec![100, 200, 300];
        let theta = MAX_THETA / 2;
        let sketch = CompactThetaSketch::new(
            theta,
            entries.clone(),
            compute_seed_hash(DEFAULT_UPDATE_SEED),
            false,
        );
        let bytes = sketch.serialize();
        let restored = CompactThetaSketch::deserialize(&bytes).unwrap();

        assert!(!restored.is_empty());
        assert!(restored.is_estimation_mode());
        assert_eq!(sketch.num_retained(), restored.num_retained());
        assert_eq!(sketch.estimate(), restored.estimate());
        assert_eq!(sketch.theta64(), restored.theta64());
    }

    #[test]
    fn test_deserialize_invalid_family() {
        let mut bytes = vec![
            1,
            SERIAL_VERSION,
            99,
            0,
            0,
            FLAG_EMPTY | FLAG_COMPACT | FLAG_ORDERED,
        ];
        bytes.extend_from_slice(&compute_seed_hash(DEFAULT_UPDATE_SEED).to_le_bytes());

        let result = CompactThetaSketch::deserialize(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_invalid_seed() {
        let mut bytes = vec![
            1,
            SERIAL_VERSION,
            THETA_FAMILY_ID,
            0,
            0,
            FLAG_EMPTY | FLAG_COMPACT | FLAG_ORDERED,
        ];
        bytes.extend_from_slice(&9999u16.to_le_bytes()); // Wrong seed hash

        let result = CompactThetaSketch::deserialize(&bytes);
        assert!(result.is_err());
    }
}
