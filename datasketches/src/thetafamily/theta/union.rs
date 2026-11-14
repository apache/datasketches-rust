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

use crate::common::ResizeFactor;
use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::theta::CompactThetaSketch;
use crate::theta::ThetaSketchView;
use crate::theta::hash_table::ThetaEntry;
use crate::theta::serialization;
use crate::thetafamily::common::constants::DEFAULT_LG_K;
use crate::thetafamily::common::union::UnionMergePolicy;
use crate::thetafamily::common::union::UnionState;

/// Stateful union operator for Theta sketches.
pub struct ThetaUnion {
    state: UnionState<ThetaEntry, NoopUnionPolicy>,
}

impl fmt::Debug for ThetaUnion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.state.fmt_debug(f, "ThetaUnion")
    }
}

#[derive(Debug)]
struct NoopUnionPolicy;

impl UnionMergePolicy<ThetaEntry> for NoopUnionPolicy {
    fn merge(&self, _existing: &mut ThetaEntry, _incoming: ThetaEntry) {}
}

impl ThetaUnion {
    /// Updates this union with the given sketch.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if a non-empty `sketch` has a different seed hash from this
    /// union.
    pub fn update<'a>(&mut self, sketch: impl Into<ThetaSketchView<'a>>) -> Result<(), Error> {
        let sketch = sketch.into();
        self.state.update(sketch)
    }

    /// Returns this union as a compact sketch.
    pub fn to_sketch(&self, ordered: bool) -> CompactThetaSketch {
        CompactThetaSketch::from_compact_state(self.state.to_compact_hash_state(ordered))
    }

    /// Serializes this union result into the uncompressed compact Theta format.
    ///
    /// The result is the same as [`Self::to_sketch`] followed by
    /// [`CompactThetaSketch::serialize`]: the same retained hashes, theta, and seed hash, and
    /// identical bytes when `ordered` is `true` (`to_sketch(true).serialize()`). When `ordered` is
    /// `false`, retained hashes may appear in a different order. Empty results and exact singletons
    /// always use their canonical encoding.
    pub fn serialize(&self, ordered: bool) -> Vec<u8> {
        let state = self.state.to_compact_hash_state(ordered);
        serialization::serialize_compact_uncompressed(&state)
    }

    /// Serializes this union result in compressed compact Theta format if applicable.
    ///
    /// Produces the same bytes as `self.to_sketch(true).serialize_compressed()`. Compressed
    /// serialization requires ordered entries, so retained hashes are sorted ascending. Falls back
    /// to uncompressed canonical encoding for empty sketches, zero-entry estimation sketches, and
    /// exact singletons.
    ///
    /// The serialized data can be read by other DataSketches implementations via
    /// [`CompactThetaSketch::deserialize`] or [`CompactThetaSketch::deserialize_with_seed`].
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::theta::CompactThetaSketch;
    /// use datasketches::theta::ThetaSketchBuilder;
    /// use datasketches::theta::ThetaUnionBuilder;
    ///
    /// let mut sketch = ThetaSketchBuilder::default().lg_k(12).build().unwrap();
    /// for value in 0..100 {
    ///     sketch.update(value);
    /// }
    ///
    /// let mut union = ThetaUnionBuilder::default().lg_k(12).build().unwrap();
    /// union.update(&sketch).unwrap();
    ///
    /// let bytes = union.serialize_compressed();
    /// let restored = CompactThetaSketch::deserialize(&bytes).unwrap();
    /// assert_eq!(restored.num_retained(), 100);
    /// ```
    #[must_use]
    pub fn serialize_compressed(&self) -> Vec<u8> {
        let state = self.state.to_compact_hash_state(true);
        serialization::serialize_compact_compressed(&state)
    }

    /// Resets the union to its empty state.
    pub fn reset(&mut self) {
        self.state.reset();
    }

    /// Returns an estimate of the union's memory usage in bytes.
    pub fn estimated_size(&self) -> usize {
        self.state.estimated_size()
    }
}

/// Builder for [`ThetaUnion`].
///
/// Configuration is stored without validation and checked when [`build()`](Self::build) is called.
#[derive(Debug, Clone)]
pub struct ThetaUnionBuilder {
    lg_k: u8,
    resize_factor: ResizeFactor,
    sampling_probability: f32,
    seed: u64,
}

impl Default for ThetaUnionBuilder {
    fn default() -> Self {
        Self {
            lg_k: DEFAULT_LG_K,
            resize_factor: ResizeFactor::X8,
            sampling_probability: 1.0,
            seed: DEFAULT_UPDATE_SEED,
        }
    }
}

impl ThetaUnionBuilder {
    /// Sets `lg_k`, the base-2 logarithm of the nominal capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::theta::ThetaUnionBuilder;
    ///
    /// ThetaUnionBuilder::default().lg_k(12).build().unwrap();
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::theta::ThetaUnionBuilder;
    ///
    /// ThetaUnionBuilder::default()
    ///     .sampling_probability(0.5)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn sampling_probability(mut self, probability: f32) -> Self {
        self.sampling_probability = probability;
        self
    }

    /// Sets the hash seed.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::theta::ThetaUnionBuilder;
    ///
    /// ThetaUnionBuilder::default().seed(7).build().unwrap();
    /// ```
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Builds the [`ThetaUnion`].
    ///
    /// # Errors
    ///
    /// Returns an error if `lg_k` is outside `[5, 26]`, `sampling_probability` is outside
    /// `(0.0, 1.0]`, or the computed seed hash is zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::theta::ThetaUnionBuilder;
    ///
    /// ThetaUnionBuilder::default().lg_k(10).build().unwrap();
    /// ```
    pub fn build(self) -> Result<ThetaUnion, Error> {
        Ok(ThetaUnion {
            state: UnionState::new(
                self.lg_k,
                self.resize_factor,
                self.sampling_probability,
                self.seed,
                NoopUnionPolicy,
            )?,
        })
    }
}
