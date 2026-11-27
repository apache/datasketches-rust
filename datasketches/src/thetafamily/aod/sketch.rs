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

use crate::aod::hash_table::AodHashTable;
use crate::aod::hash_table::AodIter;
use crate::aod::policy::check_num_values;
use crate::common::NumStdDev;
use crate::common::ResizeFactor;
use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::thetafamily::common::binomial_bounds;
use crate::thetafamily::common::constants::DEFAULT_LG_K;
use crate::thetafamily::common::constants::MAX_THETA;
use crate::thetafamily::common::sort::sort_by_hash_key;

/// A retained hash key and its associated slice of `f64` values.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ArrayOfDoublesEntry<'a> {
    hash: u64,
    values: &'a [f64],
}

impl<'a> ArrayOfDoublesEntry<'a> {
    #[inline]
    pub(super) fn new(hash: u64, values: &'a [f64]) -> Self {
        Self { hash, values }
    }

    /// Returns the 63-bit MurmurHash3 key hash for this entry.
    #[inline]
    pub fn hash(&self) -> u64 {
        self.hash
    }

    /// Returns the summary values associated with this entry's key.
    #[inline]
    pub fn values(&self) -> &'a [f64] {
        self.values
    }
}

/// Read-only view of a mutable or compact ArrayOfDoubles sketch.
///
/// Accepted by [`ArrayOfDoublesUnion`](crate::aod::ArrayOfDoublesUnion),
/// [`ArrayOfDoublesIntersection`](crate::aod::ArrayOfDoublesIntersection), and
/// [`ArrayOfDoublesANotB`](crate::aod::ArrayOfDoublesANotB).
#[derive(Clone, Copy)]
pub struct ArrayOfDoublesSketchView<'a> {
    pub(super) table: Option<&'a AodHashTable>,
    pub(super) keys: &'a [u64],
    pub(super) values: &'a [f64],
    num_retained: usize,
    theta: u64,
    seed_hash: u16,
    num_values: u8,
    is_empty: bool,
    is_ordered: bool,
}

impl fmt::Debug for ArrayOfDoublesSketchView<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArrayOfDoublesSketchView")
            .field("seed_hash", &self.seed_hash)
            .field("num_values", &self.num_values)
            .field("is_empty", &self.is_empty)
            .field("is_ordered", &self.is_ordered)
            .field("num_retained", &self.num_retained)
            .field("theta64", &self.theta)
            .finish()
    }
}

impl<'a> ArrayOfDoublesSketchView<'a> {
    /// Returns the number of `f64` values associated with each key.
    #[inline]
    pub fn num_values(&self) -> u8 {
        self.num_values
    }

    /// Returns the 16-bit seed hash.
    #[inline]
    pub fn seed_hash(&self) -> u16 {
        self.seed_hash
    }

    /// Returns `true` if the viewed sketch is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.is_empty
    }

    /// Returns `true` if the viewed sketch is in estimation mode.
    #[inline]
    pub fn is_estimation_mode(&self) -> bool {
        !self.is_empty && self.theta < MAX_THETA
    }

    /// Returns `true` if retained entries are ordered by ascending hash.
    #[inline]
    pub fn is_ordered(&self) -> bool {
        self.is_ordered
    }

    /// Returns the number of retained entries.
    #[inline]
    pub fn num_retained(&self) -> usize {
        self.num_retained
    }

    /// Returns theta as a `u64` threshold.
    #[inline]
    pub fn theta64(&self) -> u64 {
        self.theta
    }

    /// Returns theta as a fraction in `[0.0, 1.0]`.
    #[inline]
    pub fn theta(&self) -> f64 {
        self.theta as f64 / MAX_THETA as f64
    }

    /// Returns the cardinality estimate.
    pub fn estimate(&self) -> f64 {
        if self.is_empty {
            0.0
        } else if self.theta == MAX_THETA {
            self.num_retained as f64
        } else {
            self.num_retained as f64 / self.theta()
        }
    }

    /// Returns an iterator over retained entries.
    pub fn iter(self) -> impl Iterator<Item = ArrayOfDoublesEntry<'a>> + 'a {
        AodIter::new(self.keys, self.values, self.num_values, self.num_retained)
    }

    fn lower_bound(&self, num_std_dev: NumStdDev) -> f64 {
        if !self.is_estimation_mode() {
            return self.num_retained as f64;
        }
        binomial_bounds::lower_bound(self.num_retained as u64, self.theta(), num_std_dev)
            .expect("theta should always be valid")
    }

    fn upper_bound(&self, num_std_dev: NumStdDev) -> f64 {
        if !self.is_estimation_mode() {
            return self.num_retained as f64;
        }
        binomial_bounds::upper_bound(
            self.num_retained as u64,
            self.theta(),
            num_std_dev,
            self.is_empty,
        )
        .expect("theta should always be valid")
    }
}

/// Mutable ArrayOfDoubles sketch for summarizing streams of keyed `f64` vectors.
///
/// Retained hashes and their fixed-width `f64` arrays are stored in contiguous buffers without
/// per-entry heap allocations.
///
/// # Examples
///
/// ```
/// use datasketches::aod::ArrayOfDoublesSketchBuilder;
///
/// let mut sketch = ArrayOfDoublesSketchBuilder::default()
///     .num_values(2)
///     .build()?;
/// sketch.update("user-1", [1.0, 25.0])?;
/// sketch.update("user-1", [1.0, 15.0])?;
/// assert_eq!(sketch.num_retained(), 1);
/// assert_eq!(sketch.iter().next().unwrap().values(), &[2.0, 40.0]);
/// # Ok::<(), datasketches::error::Error>(())
/// ```
pub struct ArrayOfDoublesSketch {
    table: AodHashTable,
    // Public emptiness tracks accepted update calls, not retained entries: sampling may screen
    // all.
    is_empty: bool,
}

impl fmt::Debug for ArrayOfDoublesSketch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArrayOfDoublesSketch")
            .field("lg_k", &self.lg_k())
            .field("seed_hash", &self.seed_hash())
            .field("num_values", &self.num_values())
            .field("is_empty", &self.is_empty)
            .field("is_estimation_mode", &self.is_estimation_mode())
            .field("num_retained", &self.num_retained())
            .field("theta64", &self.theta64())
            .field("estimate", &self.estimate())
            .finish()
    }
}

impl ArrayOfDoublesSketch {
    /// Returns a read-only view accepted by ArrayOfDoubles set operations.
    pub fn as_view(&self) -> ArrayOfDoublesSketchView<'_> {
        ArrayOfDoublesSketchView {
            table: Some(&self.table),
            keys: &self.table.keys,
            values: &self.table.values,
            num_retained: self.table.num_retained,
            theta: self.theta64(),
            seed_hash: self.table.seed_hash,
            num_values: self.table.num_values,
            is_empty: self.is_empty,
            is_ordered: false,
        }
    }

    /// Updates the sketch with a key and its `f64` values.
    ///
    /// Values are added element by element when `key` is already retained. Updates screened out by
    /// theta leave retained summaries unchanged.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` without modifying the sketch if `values.as_ref().len()` does not
    /// equal [`num_values`](Self::num_values).
    pub fn update(&mut self, key: impl Hash, values: impl AsRef<[f64]>) -> Result<(), Error> {
        let values = values.as_ref();
        check_num_values(self.table.num_values, values.len(), "sketch update")?;
        self.is_empty = false;
        let hash = self.table.hash(key);
        self.table.insert_or_add(hash, values);
        Ok(())
    }

    /// Returns the number of `f64` values associated with each key.
    #[inline]
    pub fn num_values(&self) -> u8 {
        self.table.num_values
    }

    /// Returns the cardinality estimate.
    pub fn estimate(&self) -> f64 {
        self.as_view().estimate()
    }

    /// Returns theta as a fraction in `[0.0, 1.0]`.
    #[inline]
    pub fn theta(&self) -> f64 {
        self.theta64() as f64 / MAX_THETA as f64
    }

    /// Returns theta as a `u64` threshold.
    ///
    /// An empty sketch reports `MAX_THETA` even when built with a sampling probability below `1.0`.
    #[inline]
    pub fn theta64(&self) -> u64 {
        if self.is_empty {
            MAX_THETA
        } else {
            self.table.retention_theta
        }
    }

    /// Returns the 16-bit seed hash.
    #[inline]
    pub fn seed_hash(&self) -> u16 {
        self.table.seed_hash
    }

    /// Returns `true` if the sketch is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.is_empty
    }

    /// Returns `true` if the sketch is in estimation mode.
    #[inline]
    pub fn is_estimation_mode(&self) -> bool {
        !self.is_empty && self.table.retention_theta < MAX_THETA
    }

    /// Returns the number of retained entries.
    #[inline]
    pub fn num_retained(&self) -> usize {
        self.table.num_retained
    }

    /// Returns the configured `lg_k`.
    #[inline]
    pub fn lg_k(&self) -> u8 {
        self.table.lg_nom_size
    }

    /// Trims the sketch to the nominal capacity `1 << lg_k`.
    pub fn trim(&mut self) {
        self.table.trim();
    }

    /// Resets the sketch to its initial empty state while preserving configuration.
    pub fn reset(&mut self) {
        self.table.reset();
        self.is_empty = true;
    }

    /// Returns an iterator over retained entries.
    pub fn iter(&self) -> impl Iterator<Item = ArrayOfDoublesEntry<'_>> + '_ {
        self.table.iter()
    }

    /// Returns the approximate lower error bound for `num_std_dev`.
    pub fn lower_bound(&self, num_std_dev: NumStdDev) -> f64 {
        self.as_view().lower_bound(num_std_dev)
    }

    /// Returns the approximate upper error bound for `num_std_dev`.
    pub fn upper_bound(&self, num_std_dev: NumStdDev) -> f64 {
        self.as_view().upper_bound(num_std_dev)
    }

    /// Returns an estimate of heap memory usage in bytes.
    pub fn estimated_size(&self) -> usize {
        self.table.estimated_size()
    }

    /// Returns this sketch in compact, immutable form.
    ///
    /// If `ordered` is `true`, retained entries are sorted by ascending hash.
    #[must_use = "compact returns a new compact sketch without modifying this sketch"]
    pub fn compact(&self, ordered: bool) -> CompactArrayOfDoublesSketch {
        if self.is_empty {
            debug_assert_eq!(self.num_retained(), 0);
            CompactArrayOfDoublesSketch::empty(self.seed_hash(), self.num_values())
        } else {
            self.table.to_non_empty_compact(ordered)
        }
    }
}

impl<'a> From<&'a ArrayOfDoublesSketch> for ArrayOfDoublesSketchView<'a> {
    fn from(sketch: &'a ArrayOfDoublesSketch) -> Self {
        sketch.as_view()
    }
}

/// Compact, immutable ArrayOfDoubles sketch backed by contiguous hash and value arrays.
#[derive(Clone)]
pub struct CompactArrayOfDoublesSketch {
    is_empty: bool,
    is_ordered: bool,
    seed_hash: u16,
    theta: u64,
    num_values: u8,
    pub(super) hashes: Vec<u64>,
    pub(super) values: Vec<f64>,
}

impl CompactArrayOfDoublesSketch {
    pub(super) fn empty(seed_hash: u16, num_values: u8) -> Self {
        Self {
            is_empty: true,
            is_ordered: true,
            seed_hash,
            theta: MAX_THETA,
            num_values,
            hashes: Vec::new(),
            values: Vec::new(),
        }
    }

    pub(super) fn from_raw_parts(
        is_empty: bool,
        is_ordered: bool,
        seed_hash: u16,
        theta: u64,
        num_values: u8,
        hashes: Vec<u64>,
        values: Vec<f64>,
    ) -> Self {
        debug_assert_eq!(values.len(), hashes.len() * usize::from(num_values));
        debug_assert!(!is_empty || (hashes.is_empty() && theta == MAX_THETA && is_ordered));
        Self {
            is_empty,
            is_ordered,
            seed_hash,
            theta,
            num_values,
            hashes,
            values,
        }
    }

    pub(super) fn compact_non_empty(
        hashes: Vec<u64>,
        values: Vec<f64>,
        theta: u64,
        seed_hash: u16,
        num_values: u8,
        already_ordered: bool,
        ordered: bool,
    ) -> Self {
        let stride = usize::from(num_values);
        assert_eq!(values.len(), hashes.len() * stride);
        let is_ordered = ordered || already_ordered || (hashes.len() == 1 && theta == MAX_THETA);
        if ordered && !already_ordered && hashes.len() > 1 {
            let mut pairs: Vec<_> = hashes
                .iter()
                .enumerate()
                .map(|(slot, &hash)| (hash, slot as u32))
                .collect();
            sort_by_hash_key(&mut pairs, |&(hash, _)| hash);
            let mut sorted_hashes = Vec::with_capacity(hashes.len());
            let mut sorted_values = Vec::with_capacity(values.len());
            for (hash, slot) in pairs {
                let start = slot as usize * stride;
                sorted_hashes.push(hash);
                sorted_values.extend_from_slice(&values[start..start + stride]);
            }
            return Self::from_raw_parts(
                false,
                is_ordered,
                seed_hash,
                theta,
                num_values,
                sorted_hashes,
                sorted_values,
            );
        }
        Self::from_raw_parts(
            false, is_ordered, seed_hash, theta, num_values, hashes, values,
        )
    }

    /// Returns a read-only view accepted by ArrayOfDoubles set operations.
    pub fn as_view(&self) -> ArrayOfDoublesSketchView<'_> {
        ArrayOfDoublesSketchView {
            table: None,
            keys: &self.hashes,
            values: &self.values,
            num_retained: self.hashes.len(),
            theta: self.theta,
            seed_hash: self.seed_hash,
            num_values: self.num_values,
            is_empty: self.is_empty,
            is_ordered: self.is_ordered,
        }
    }

    /// Returns the number of `f64` values associated with each key.
    #[inline]
    pub fn num_values(&self) -> u8 {
        self.num_values
    }

    /// Returns the cardinality estimate.
    pub fn estimate(&self) -> f64 {
        self.as_view().estimate()
    }

    /// Returns theta as a fraction in `[0.0, 1.0]`.
    #[inline]
    pub fn theta(&self) -> f64 {
        self.theta as f64 / MAX_THETA as f64
    }

    /// Returns theta as a `u64` threshold.
    #[inline]
    pub fn theta64(&self) -> u64 {
        self.theta
    }

    /// Returns `true` if the sketch is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.is_empty
    }

    /// Returns `true` if the sketch is in estimation mode.
    #[inline]
    pub fn is_estimation_mode(&self) -> bool {
        !self.is_empty && self.theta < MAX_THETA
    }

    /// Returns the number of retained entries.
    #[inline]
    pub fn num_retained(&self) -> usize {
        self.hashes.len()
    }

    /// Returns `true` if retained entries are ordered by ascending hash.
    #[inline]
    pub fn is_ordered(&self) -> bool {
        self.is_ordered
    }

    /// Returns the 16-bit seed hash.
    #[inline]
    pub fn seed_hash(&self) -> u16 {
        self.seed_hash
    }

    /// Returns an iterator over retained entries.
    pub fn iter(&self) -> impl Iterator<Item = ArrayOfDoublesEntry<'_>> + '_ {
        AodIter::new(
            &self.hashes,
            &self.values,
            self.num_values,
            self.hashes.len(),
        )
    }

    /// Returns the approximate lower error bound for `num_std_dev`.
    pub fn lower_bound(&self, num_std_dev: NumStdDev) -> f64 {
        self.as_view().lower_bound(num_std_dev)
    }

    /// Returns the approximate upper error bound for `num_std_dev`.
    pub fn upper_bound(&self, num_std_dev: NumStdDev) -> f64 {
        self.as_view().upper_bound(num_std_dev)
    }

    /// Returns an estimate of heap memory usage in bytes.
    pub fn estimated_size(&self) -> usize {
        self.hashes.capacity() * size_of::<u64>() + self.values.capacity() * size_of::<f64>()
    }

    /// Serializes this sketch into the compact ArrayOfDoubles binary format.
    ///
    /// The serialized data can be read by other DataSketches implementations when `num_values` is
    /// in `1..=127`. Values in `128..=255` can be read by DataSketches C++ and Go, but not by
    /// DataSketches Java. Empty sketches serialize with a zero seed hash.
    pub fn serialize(&self) -> Vec<u8> {
        crate::aod::serialization::serialize_compact(self)
    }

    /// Deserializes a compact ArrayOfDoubles sketch using the default seed.
    ///
    /// Also accepts data serialized by other DataSketches implementations when encoded in the
    /// compact ArrayOfDoubles format (`sketch_type = 3`). DataSketches Java's updatable
    /// (`sketch_type = 2`) and union (`sketch_type = 4`) images must be compacted via
    /// `.compact()` or `.getResult()` before exchange. Images with `num_values` in `128..=255` can
    /// originate from DataSketches C++ and Go, but not from DataSketches Java.
    ///
    /// # Errors
    ///
    /// Returns `InvalidData` if the image is malformed or a non-empty image's seed hash does not
    /// match the default seed.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        Self::deserialize_with_seed(bytes, DEFAULT_UPDATE_SEED)
    }

    /// Deserializes a compact ArrayOfDoubles sketch using `seed`.
    ///
    /// Also accepts data serialized by other DataSketches implementations when encoded in the
    /// compact ArrayOfDoubles format (`sketch_type = 3`). DataSketches Java's updatable
    /// (`sketch_type = 2`) and union (`sketch_type = 4`) images must be compacted via
    /// `.compact()` or `.getResult()` before exchange. Images with `num_values` in `128..=255` can
    /// originate from DataSketches C++ and Go, but not from DataSketches Java. Empty sketches use
    /// the hash of `seed` regardless of the stored seed hash.
    ///
    /// # Errors
    ///
    /// Returns `InvalidData` if the image is malformed, a non-empty image's seed hash does not
    /// match `seed`, or `seed` itself computes to the reserved zero seed hash.
    pub fn deserialize_with_seed(bytes: &[u8], seed: u64) -> Result<Self, Error> {
        crate::aod::serialization::deserialize_compact(bytes, seed)
    }
}

impl fmt::Debug for CompactArrayOfDoublesSketch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompactArrayOfDoublesSketch")
            .field("seed_hash", &self.seed_hash)
            .field("num_values", &self.num_values)
            .field("is_empty", &self.is_empty)
            .field("is_ordered", &self.is_ordered)
            .field("is_estimation_mode", &self.is_estimation_mode())
            .field("num_retained", &self.num_retained())
            .field("theta64", &self.theta)
            .field("estimate", &self.estimate())
            .finish()
    }
}

impl<'a> From<&'a CompactArrayOfDoublesSketch> for ArrayOfDoublesSketchView<'a> {
    fn from(sketch: &'a CompactArrayOfDoublesSketch) -> Self {
        sketch.as_view()
    }
}

/// Builder for [`ArrayOfDoublesSketch`].
///
/// Configuration is stored without validation and checked when [`build`](Self::build) is called.
#[derive(Debug, Clone)]
pub struct ArrayOfDoublesSketchBuilder {
    lg_k: u8,
    resize_factor: ResizeFactor,
    sampling_probability: f32,
    seed: u64,
    num_values: u8,
}

impl Default for ArrayOfDoublesSketchBuilder {
    fn default() -> Self {
        Self {
            lg_k: DEFAULT_LG_K,
            resize_factor: ResizeFactor::X8,
            sampling_probability: 1.0,
            seed: DEFAULT_UPDATE_SEED,
            num_values: 1,
        }
    }
}

impl ArrayOfDoublesSketchBuilder {
    /// Sets `lg_k`, the base-2 logarithm of the nominal capacity (`5..=26`).
    pub fn lg_k(mut self, lg_k: u8) -> Self {
        self.lg_k = lg_k;
        self
    }

    /// Sets the hash-table resize factor.
    pub fn resize_factor(mut self, factor: ResizeFactor) -> Self {
        self.resize_factor = factor;
        self
    }

    /// Sets the initial uniform sampling probability in `(0.0, 1.0]`.
    pub fn sampling_probability(mut self, probability: f32) -> Self {
        self.sampling_probability = probability;
        self
    }

    /// Sets the hash seed.
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Sets the number of `f64` values associated with each key (`1..=255`).
    pub fn num_values(mut self, num_values: u8) -> Self {
        self.num_values = num_values;
        self
    }

    /// Builds an [`ArrayOfDoublesSketch`].
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if `num_values` is zero, `lg_k` is outside `5..=26`,
    /// `sampling_probability` is outside `(0.0, 1.0]`, or the computed seed hash is zero.
    pub fn build(self) -> Result<ArrayOfDoublesSketch, Error> {
        let table = AodHashTable::new(
            self.lg_k,
            self.resize_factor,
            self.sampling_probability,
            self.seed,
            self.num_values,
        )?;
        Ok(ArrayOfDoublesSketch {
            table,
            is_empty: true,
        })
    }
}
