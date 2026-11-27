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

use crate::aod::ArrayOfDoublesCombiner;
use crate::aod::ArrayOfDoublesSketchView;
use crate::aod::CompactArrayOfDoublesSketch;
use crate::aod::policy::ArrayOfDoublesIntersectionPolicy;
use crate::aod::policy::check_num_values;
use crate::aod::policy::validate_num_values;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::check_seed_hash;
use crate::hash::compute_seed_hash;
use crate::tuple::TupleIntersection;

/// Stateful intersection operator for ArrayOfDoubles sketches.
///
/// The caller supplies an [`ArrayOfDoublesCombiner`] for values associated with shared keys.
/// A newly created operator has no result until its first successful update.
pub struct ArrayOfDoublesIntersection<C>
where
    C: ArrayOfDoublesCombiner,
{
    inner: TupleIntersection<ArrayOfDoublesIntersectionPolicy<C>>,
    num_values: u8,
    seed_hash: u16,
}

impl<C> fmt::Debug for ArrayOfDoublesIntersection<C>
where
    C: ArrayOfDoublesCombiner,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArrayOfDoublesIntersection")
            .field("num_values", &self.num_values)
            .field("inner", &self.inner)
            .finish()
    }
}

impl<C> ArrayOfDoublesIntersection<C>
where
    C: ArrayOfDoublesCombiner,
{
    /// Creates an intersection with the default seed and the given configuration.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if `num_values` is zero.
    pub fn new(num_values: u8, combiner: C) -> Result<Self, Error> {
        Self::with_seed(num_values, combiner, crate::hash::DEFAULT_UPDATE_SEED)
    }

    /// Creates an intersection with the given configuration and seed.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if `num_values` is zero or the computed seed hash is zero.
    pub fn with_seed(num_values: u8, combiner: C, seed: u64) -> Result<Self, Error> {
        validate_num_values(num_values)?;
        let seed_hash = compute_seed_hash(seed, ErrorKind::InvalidArgument)?;
        let policy = ArrayOfDoublesIntersectionPolicy {
            num_values,
            combiner,
        };
        Ok(Self {
            inner: TupleIntersection::with_seed(policy, seed)?,
            num_values,
            seed_hash,
        })
    }

    /// Updates the intersection with a mutable or compact sketch.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if the sketch has a different `num_values`, a non-empty sketch
    /// has a different seed hash, or the input appears corrupted.
    pub fn update<'a>(
        &mut self,
        sketch: impl Into<ArrayOfDoublesSketchView<'a>>,
    ) -> Result<(), Error> {
        let sketch = sketch.into();
        check_num_values(
            self.num_values,
            usize::from(sketch.num_values()),
            "intersection update",
        )?;
        if !sketch.is_empty() {
            check_seed_hash(
                self.seed_hash,
                sketch.seed_hash(),
                "ArrayOfDoubles intersection input",
                ErrorKind::InvalidArgument,
            )?;
        }
        self.inner.update(sketch.into_tuple_view())
    }

    /// Returns `true` after the first successful [`update`](Self::update).
    pub fn has_result(&self) -> bool {
        self.inner.has_result()
    }

    /// Returns the current intersection as a compact sketch.
    ///
    /// Returns `None` until the first successful update. After that, returns `Some` even when the
    /// intersection is empty. If `ordered` is `true`, retained entries are sorted by hash.
    pub fn to_sketch(&self, ordered: bool) -> Option<CompactArrayOfDoublesSketch> {
        self.inner
            .to_sketch(ordered)
            .map(|sketch| CompactArrayOfDoublesSketch::from_tuple(sketch, self.num_values))
    }
}
