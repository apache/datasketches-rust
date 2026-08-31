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

use crate::common::ResizeFactor;
use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::theta::CompactThetaSketch;
use crate::theta::ThetaSketchView;
use crate::theta::hash_table::ThetaEntry;
use crate::thetacommon::constants::DEFAULT_LG_K;
use crate::thetacommon::union::UnionMergePolicy;
use crate::thetacommon::union::UnionState;

/// Stateful union operator for Theta sketches.
#[derive(Debug)]
pub struct ThetaUnion {
    state: UnionState<ThetaEntry, NoopUnionPolicy>,
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
        let compact_state = self
            .state
            .to_compact_sketch_state(ordered)
            .map_retained_entries(|entry| entry.hash());
        CompactThetaSketch::from_compact_state(compact_state)
    }

    /// Resets the union to its empty state.
    pub fn reset(&mut self) {
        self.state.reset();
    }

    /// Returns the estimated size of the union in bytes.
    pub fn estimated_size(&self) -> usize {
        size_of::<Self>() + self.state.estimated_size()
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
