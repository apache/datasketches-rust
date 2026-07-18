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
use crate::thetacommon::constants::MAX_LG_K;
use crate::thetacommon::constants::MIN_LG_K;
use crate::thetacommon::union::RawThetaUnion;
use crate::thetacommon::union::RawThetaUnionPolicy;

/// Stateful union operator for Theta sketches.
#[derive(Debug)]
pub struct ThetaUnion {
    raw: RawThetaUnion<ThetaEntry, NoopUnionPolicy>,
}

#[derive(Debug)]
struct NoopUnionPolicy;

impl RawThetaUnionPolicy<ThetaEntry> for NoopUnionPolicy {
    fn merge(&self, _existing: &mut ThetaEntry, _incoming: ThetaEntry) {}
}

impl ThetaUnion {
    /// Create a new builder for ThetaUnion
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::theta::ThetaUnion;
    /// let _union = ThetaUnion::builder().lg_k(12).build();
    /// ```
    pub fn builder() -> ThetaUnionBuilder {
        ThetaUnionBuilder::default()
    }

    /// Update this union with a given sketch.
    pub fn update<S: ThetaSketchView>(&mut self, sketch: &S) -> Result<(), Error> {
        self.raw.update(sketch)
    }

    /// Return this union as a compact sketch.
    pub fn to_sketch(&self, ordered: bool) -> CompactThetaSketch {
        let result = self.raw.result(ordered);
        CompactThetaSketch::from_parts(
            result
                .entries
                .into_iter()
                .map(|entry| entry.hash())
                .collect(),
            result.theta,
            result.seed_hash,
            result.ordered,
            result.empty,
        )
    }

    /// Reset the union to empty state.
    pub fn reset(&mut self) {
        self.raw.reset();
    }
}

/// Builder for [`ThetaUnion`].
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
    /// Set lg_k (log2 of nominal size k).
    ///
    /// # Panics
    ///
    /// If lg_k is not in range [5, 26]
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::theta::ThetaUnion;
    /// let _union = ThetaUnion::builder().lg_k(12).build();
    /// ```
    pub fn lg_k(mut self, lg_k: u8) -> Self {
        assert!(
            (MIN_LG_K..=MAX_LG_K).contains(&lg_k),
            "lg_k must be in [{MIN_LG_K}, {MAX_LG_K}], got {lg_k}"
        );
        self.lg_k = lg_k;
        self
    }

    /// Set resize factor.
    pub fn resize_factor(mut self, resize_factor: ResizeFactor) -> Self {
        self.resize_factor = resize_factor;
        self
    }

    /// Set sampling probability p.
    ///
    /// # Panics
    ///
    /// Panics if p is not in range `(0.0, 1.0]`
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::theta::ThetaUnion;
    /// let _union = ThetaUnion::builder().sampling_probability(0.5).build();
    /// ```
    pub fn sampling_probability(mut self, p: f32) -> Self {
        assert!(
            (0.0..=1.0).contains(&p) && p > 0.0,
            "sampling_probability must be in (0.0, 1.0], got {p}"
        );
        self.sampling_probability = p;
        self
    }

    /// Set hash seed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::theta::ThetaUnion;
    /// let _union = ThetaUnion::builder().seed(7).build();
    /// ```
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Build the ThetaUnion.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::theta::ThetaUnion;
    /// let _union = ThetaUnion::builder().lg_k(10).build();
    /// ```
    pub fn build(self) -> ThetaUnion {
        ThetaUnion {
            raw: RawThetaUnion::new(
                self.lg_k,
                self.resize_factor,
                self.sampling_probability,
                self.seed,
                NoopUnionPolicy,
            ),
        }
    }
}
