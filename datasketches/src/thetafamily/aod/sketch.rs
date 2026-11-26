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

use crate::aod::policy::ArrayOfDoublesPolicy;
use crate::aod::policy::check_num_values;
use crate::aod::policy::validate_num_values;
use crate::common::NumStdDev;
use crate::common::ResizeFactor;
use crate::error::Error;
use crate::thetafamily::common::sketch_state::CompactSketchState;
use crate::tuple::CompactTupleSketch;
use crate::tuple::TupleEntry;
use crate::tuple::TupleSketch;
use crate::tuple::TupleSketchBuilder;
use crate::tuple::TupleSketchView;

/// A retained hash and its associated array of double values.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ArrayOfDoublesEntry<'a> {
    hash: u64,
    values: &'a [f64],
}

impl<'a> ArrayOfDoublesEntry<'a> {
    fn from_tuple(entry: &'a TupleEntry<Vec<f64>>) -> Self {
        Self {
            hash: entry.hash(),
            values: entry.summary(),
        }
    }

    /// Returns the hash used as this entry's key.
    pub fn hash(&self) -> u64 {
        self.hash
    }

    /// Returns the values associated with this entry's key.
    pub fn values(&self) -> &'a [f64] {
        self.values
    }
}

/// Read-only view of a mutable or compact ArrayOfDoubles sketch.
#[derive(Clone, Copy)]
pub struct ArrayOfDoublesSketchView<'a> {
    inner: TupleSketchView<'a, Vec<f64>>,
    num_values: u8,
}

impl fmt::Debug for ArrayOfDoublesSketchView<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArrayOfDoublesSketchView")
            .field("seed_hash", &self.seed_hash())
            .field("num_values", &self.num_values())
            .field("is_empty", &self.is_empty())
            .field("is_ordered", &self.is_ordered())
            .field("num_retained", &self.num_retained())
            .field("theta64", &self.theta64())
            .finish()
    }
}

impl<'a> ArrayOfDoublesSketchView<'a> {
    pub(in crate::thetafamily) fn into_tuple_view(self) -> TupleSketchView<'a, Vec<f64>> {
        self.inner
    }

    /// Returns the number of values associated with each key.
    pub fn num_values(&self) -> u8 {
        self.num_values
    }

    /// Returns the 16-bit seed hash.
    pub fn seed_hash(&self) -> u16 {
        self.inner.seed_hash()
    }

    /// Returns `true` if the viewed sketch is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns `true` if the viewed sketch is in estimation mode.
    pub fn is_estimation_mode(&self) -> bool {
        !self.is_empty() && self.theta64() < crate::thetafamily::common::constants::MAX_THETA
    }

    /// Returns `true` if retained entries are ordered by ascending hash.
    pub fn is_ordered(&self) -> bool {
        self.inner.is_ordered()
    }

    /// Returns the number of retained entries.
    pub fn num_retained(&self) -> usize {
        self.inner.num_retained()
    }

    /// Returns theta as a `u64` threshold.
    pub fn theta64(&self) -> u64 {
        self.inner.theta64()
    }

    /// Returns theta as a fraction in `[0.0, 1.0]`.
    pub fn theta(&self) -> f64 {
        self.theta64() as f64 / crate::thetafamily::common::constants::MAX_THETA as f64
    }

    /// Returns the cardinality estimate.
    pub fn estimate(&self) -> f64 {
        if self.is_empty() {
            0.0
        } else {
            self.num_retained() as f64 / self.theta()
        }
    }

    /// Returns an iterator over retained entries.
    pub fn iter(self) -> impl Iterator<Item = ArrayOfDoublesEntry<'a>> + 'a {
        self.inner.iter().map(ArrayOfDoublesEntry::from_tuple)
    }
}

/// Mutable ArrayOfDoubles sketch for building from input data.
pub struct ArrayOfDoublesSketch {
    inner: TupleSketch<ArrayOfDoublesPolicy>,
    num_values: u8,
}

impl fmt::Debug for ArrayOfDoublesSketch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArrayOfDoublesSketch")
            .field("lg_k", &self.lg_k())
            .field("seed_hash", &self.seed_hash())
            .field("num_values", &self.num_values())
            .field("is_empty", &self.is_empty())
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
            inner: self.inner.as_view(),
            num_values: self.num_values,
        }
    }

    /// Updates the sketch with a key and its values.
    ///
    /// Values are added element by element when the key is already retained.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` without modifying the sketch if `values.len()` does not equal
    /// `num_values`.
    pub fn update(&mut self, key: impl Hash, values: impl AsRef<[f64]>) -> Result<(), Error> {
        let values = values.as_ref();
        check_num_values(self.num_values, values.len(), "sketch update")?;
        self.inner.update(key, values);
        Ok(())
    }

    /// Returns the number of values associated with each key.
    pub fn num_values(&self) -> u8 {
        self.num_values
    }

    /// Returns the cardinality estimate.
    pub fn estimate(&self) -> f64 {
        self.inner.estimate()
    }

    /// Returns theta as a fraction in `[0.0, 1.0]`.
    pub fn theta(&self) -> f64 {
        self.inner.theta()
    }

    /// Returns theta as a `u64` threshold.
    pub fn theta64(&self) -> u64 {
        self.inner.theta64()
    }

    /// Returns the 16-bit seed hash.
    pub fn seed_hash(&self) -> u16 {
        self.inner.seed_hash()
    }

    /// Returns `true` if the sketch is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns `true` if the sketch is in estimation mode.
    pub fn is_estimation_mode(&self) -> bool {
        self.inner.is_estimation_mode()
    }

    /// Returns the number of retained entries.
    pub fn num_retained(&self) -> usize {
        self.inner.num_retained()
    }

    /// Returns the configured `lg_k`.
    pub fn lg_k(&self) -> u8 {
        self.inner.lg_k()
    }

    /// Trims the sketch to the capacity configured by `lg_k`.
    pub fn trim(&mut self) {
        self.inner.trim();
    }

    /// Resets the sketch to the empty state.
    pub fn reset(&mut self) {
        self.inner.reset();
    }

    /// Returns an iterator over retained entries.
    pub fn iter(&self) -> impl Iterator<Item = ArrayOfDoublesEntry<'_>> + '_ {
        self.inner.iter().map(ArrayOfDoublesEntry::from_tuple)
    }

    /// Returns the approximate lower error bound given the number of standard deviations.
    pub fn lower_bound(&self, num_std_dev: NumStdDev) -> f64 {
        self.inner.lower_bound(num_std_dev)
    }

    /// Returns the approximate upper error bound given the number of standard deviations.
    pub fn upper_bound(&self, num_std_dev: NumStdDev) -> f64 {
        self.inner.upper_bound(num_std_dev)
    }

    /// Returns an estimate of the sketch's memory usage in bytes.
    pub fn estimated_size(&self) -> usize {
        self.inner.estimated_size()
            + self.num_retained() * usize::from(self.num_values) * size_of::<f64>()
            + size_of::<u8>()
    }

    /// Returns this sketch in compact, immutable form.
    ///
    /// If `ordered` is `true`, retained entries are sorted by hash in ascending order.
    #[must_use = "compact returns a new compact sketch without modifying this sketch"]
    pub fn compact(&self, ordered: bool) -> CompactArrayOfDoublesSketch {
        CompactArrayOfDoublesSketch::from_tuple(self.inner.compact(ordered), self.num_values)
    }
}

impl<'a> From<&'a ArrayOfDoublesSketch> for ArrayOfDoublesSketchView<'a> {
    fn from(sketch: &'a ArrayOfDoublesSketch) -> Self {
        sketch.as_view()
    }
}

/// Compact, immutable ArrayOfDoubles sketch.
#[derive(Clone)]
pub struct CompactArrayOfDoublesSketch {
    inner: CompactTupleSketch<Vec<f64>>,
    num_values: u8,
}

impl CompactArrayOfDoublesSketch {
    pub(in crate::thetafamily) fn from_tuple(
        inner: CompactTupleSketch<Vec<f64>>,
        num_values: u8,
    ) -> Self {
        debug_assert!(
            inner
                .iter()
                .all(|entry| entry.summary().len() == usize::from(num_values))
        );
        Self { inner, num_values }
    }

    pub(in crate::thetafamily) fn from_compact_state(
        compact_state: CompactSketchState<TupleEntry<Vec<f64>>>,
        num_values: u8,
    ) -> Self {
        Self::from_tuple(
            CompactTupleSketch::from_compact_state(compact_state),
            num_values,
        )
    }

    /// Returns a read-only view accepted by ArrayOfDoubles set operations.
    pub fn as_view(&self) -> ArrayOfDoublesSketchView<'_> {
        ArrayOfDoublesSketchView {
            inner: self.inner.as_view(),
            num_values: self.num_values,
        }
    }

    /// Returns the number of values associated with each key.
    pub fn num_values(&self) -> u8 {
        self.num_values
    }

    /// Returns the cardinality estimate.
    pub fn estimate(&self) -> f64 {
        self.inner.estimate()
    }

    /// Returns theta as a fraction in `[0.0, 1.0]`.
    pub fn theta(&self) -> f64 {
        self.inner.theta()
    }

    /// Returns theta as a `u64` threshold.
    pub fn theta64(&self) -> u64 {
        self.inner.theta64()
    }

    /// Returns `true` if the sketch is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns `true` if the sketch is in estimation mode.
    pub fn is_estimation_mode(&self) -> bool {
        self.inner.is_estimation_mode()
    }

    /// Returns the number of retained entries.
    pub fn num_retained(&self) -> usize {
        self.inner.num_retained()
    }

    /// Returns `true` if retained entries are ordered by ascending hash.
    pub fn is_ordered(&self) -> bool {
        self.inner.is_ordered()
    }

    /// Returns the 16-bit seed hash.
    pub fn seed_hash(&self) -> u16 {
        self.inner.seed_hash()
    }

    /// Returns an iterator over retained entries.
    pub fn iter(&self) -> impl Iterator<Item = ArrayOfDoublesEntry<'_>> + '_ {
        self.inner.iter().map(ArrayOfDoublesEntry::from_tuple)
    }

    /// Returns the approximate lower error bound given the number of standard deviations.
    pub fn lower_bound(&self, num_std_dev: NumStdDev) -> f64 {
        self.inner.lower_bound(num_std_dev)
    }

    /// Returns the approximate upper error bound given the number of standard deviations.
    pub fn upper_bound(&self, num_std_dev: NumStdDev) -> f64 {
        self.inner.upper_bound(num_std_dev)
    }

    /// Returns an estimate of the sketch's memory usage in bytes.
    pub fn estimated_size(&self) -> usize {
        self.inner.estimated_size()
            + self.num_retained() * usize::from(self.num_values) * size_of::<f64>()
            + size_of::<u8>()
    }

    /// Serializes this sketch into the compact ArrayOfDoubles binary format.
    ///
    /// The serialized data can be read by other DataSketches implementations.
    /// Empty sketches serialize with a zero seed hash.
    pub fn serialize(&self) -> Vec<u8> {
        crate::aod::serialization::serialize_compact(self)
    }

    /// Deserializes a compact ArrayOfDoubles sketch using the default seed.
    ///
    /// Also accepts data serialized by other DataSketches implementations.
    ///
    /// # Errors
    ///
    /// Returns `InvalidData` if the image is malformed or a non-empty image's seed hash does not
    /// match the default seed.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        Self::deserialize_with_seed(bytes, crate::hash::DEFAULT_UPDATE_SEED)
    }

    /// Deserializes a compact ArrayOfDoubles sketch using `seed`.
    ///
    /// Also accepts data serialized by other DataSketches implementations.
    /// Empty sketches use the hash of `seed` regardless of the stored seed hash.
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
            .field("seed_hash", &self.seed_hash())
            .field("num_values", &self.num_values())
            .field("is_empty", &self.is_empty())
            .field("is_ordered", &self.is_ordered())
            .field("is_estimation_mode", &self.is_estimation_mode())
            .field("num_retained", &self.num_retained())
            .field("theta64", &self.theta64())
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
            lg_k: crate::thetafamily::common::constants::DEFAULT_LG_K,
            resize_factor: ResizeFactor::X8,
            sampling_probability: 1.0,
            seed: crate::hash::DEFAULT_UPDATE_SEED,
            num_values: 1,
        }
    }
}

impl ArrayOfDoublesSketchBuilder {
    /// Sets `lg_k`, the base-2 logarithm of the nominal capacity.
    pub fn lg_k(mut self, lg_k: u8) -> Self {
        self.lg_k = lg_k;
        self
    }

    /// Sets the resize factor.
    pub fn resize_factor(mut self, factor: ResizeFactor) -> Self {
        self.resize_factor = factor;
        self
    }

    /// Sets the sampling probability.
    pub fn sampling_probability(mut self, probability: f32) -> Self {
        self.sampling_probability = probability;
        self
    }

    /// Sets the hash seed.
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Sets the number of values associated with each key.
    pub fn num_values(mut self, num_values: u8) -> Self {
        self.num_values = num_values;
        self
    }

    /// Builds an [`ArrayOfDoublesSketch`].
    ///
    /// # Errors
    ///
    /// Returns an error if `num_values` is zero, `lg_k` is outside `[5, 26]`,
    /// `sampling_probability` is outside `(0.0, 1.0]`, or the computed seed hash is zero.
    pub fn build(self) -> Result<ArrayOfDoublesSketch, Error> {
        validate_num_values(self.num_values)?;
        let policy = ArrayOfDoublesPolicy {
            num_values: self.num_values,
        };
        let inner = TupleSketchBuilder::new(policy)
            .lg_k(self.lg_k)
            .resize_factor(self.resize_factor)
            .sampling_probability(self.sampling_probability)
            .seed(self.seed)
            .build()?;
        Ok(ArrayOfDoublesSketch {
            inner,
            num_values: self.num_values,
        })
    }
}
