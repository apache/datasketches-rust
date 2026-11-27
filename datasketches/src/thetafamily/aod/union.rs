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

use crate::aod::ArrayOfDoublesSketchView;
use crate::aod::CompactArrayOfDoublesSketch;
use crate::aod::policy::ArrayOfDoublesPolicy;
use crate::aod::policy::check_num_values;
use crate::aod::policy::validate_num_values;
use crate::common::ResizeFactor;
use crate::error::Error;
use crate::thetafamily::tuple::TupleUnion;
use crate::thetafamily::tuple::TupleUnionBuilder;

/// Union of ArrayOfDoubles sketches.
///
/// Values associated with a key present in multiple inputs are added element by element.
pub struct ArrayOfDoublesUnion {
    inner: TupleUnion<ArrayOfDoublesPolicy>,
    num_values: u8,
}

impl fmt::Debug for ArrayOfDoublesUnion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArrayOfDoublesUnion")
            .field("num_values", &self.num_values)
            .field("inner", &self.inner)
            .finish()
    }
}

impl ArrayOfDoublesUnion {
    /// Merges a mutable or compact sketch into the union.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` without modifying the union if the sketch has a different
    /// `num_values` or a non-empty sketch has a different seed hash.
    pub fn update<'a>(
        &mut self,
        sketch: impl Into<ArrayOfDoublesSketchView<'a>>,
    ) -> Result<(), Error> {
        let sketch = sketch.into();
        check_num_values(
            self.num_values,
            usize::from(sketch.num_values()),
            "union update",
        )?;
        self.inner.update(sketch.into_tuple_view())
    }

    /// Returns the union as a compact sketch.
    ///
    /// If `ordered` is `true`, retained entries are sorted ascending by hash.
    pub fn to_sketch(&self, ordered: bool) -> CompactArrayOfDoublesSketch {
        CompactArrayOfDoublesSketch::from_tuple(self.inner.to_sketch(ordered), self.num_values)
    }

    /// Resets the union to its initial empty state.
    pub fn reset(&mut self) {
        self.inner.reset();
    }
}

/// Builder for [`ArrayOfDoublesUnion`].
#[derive(Debug, Clone)]
pub struct ArrayOfDoublesUnionBuilder {
    lg_k: u8,
    resize_factor: ResizeFactor,
    sampling_probability: f32,
    seed: u64,
    num_values: u8,
}

impl Default for ArrayOfDoublesUnionBuilder {
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

impl ArrayOfDoublesUnionBuilder {
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

    /// Builds an [`ArrayOfDoublesUnion`].
    ///
    /// # Errors
    ///
    /// Returns an error if `num_values` is zero, `lg_k` is outside `[5, 26]`,
    /// `sampling_probability` is outside `(0.0, 1.0]`, or the computed seed hash is zero.
    pub fn build(self) -> Result<ArrayOfDoublesUnion, Error> {
        validate_num_values(self.num_values)?;
        let policy = ArrayOfDoublesPolicy {
            num_values: self.num_values,
        };
        let inner = TupleUnionBuilder::new(policy)
            .lg_k(self.lg_k)
            .resize_factor(self.resize_factor)
            .sampling_probability(self.sampling_probability)
            .seed(self.seed)
            .build()?;
        Ok(ArrayOfDoublesUnion {
            inner,
            num_values: self.num_values,
        })
    }
}
