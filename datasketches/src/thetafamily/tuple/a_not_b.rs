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

//! Tuple sketch set difference (`A and not B`).
//!
//! [`TupleANotB`] computes the set difference of two Tuple sketches: the keys retained in `A` that
//! are not present in `B`. Surviving keys keep their summaries from `A` unchanged, so unlike the
//! union and intersection this operation needs no combine policy. It shares its set-difference
//! implementation with Theta a-not-B.

use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::compute_seed_hash;
use crate::thetacommon::a_not_b;
use crate::tuple::sketch::CompactTupleSketch;
use crate::tuple::sketch::TupleSketchView;

/// Set difference operator (`A and not B`) for Tuple sketches.
///
/// This is a stateless operator (other than the seed): each call to [`compute`](Self::compute)
/// takes two input sketches and returns a new [`CompactTupleSketch`]. Surviving keys carry their
/// summaries straight from `A`.
///
/// # Examples
///
/// ```
/// use datasketches::tuple::DefaultUpdatePolicy;
/// use datasketches::tuple::TupleANotB;
/// use datasketches::tuple::TupleSketchBuilder;
///
/// let update_policy = DefaultUpdatePolicy::<u64>::default();
/// let mut a = TupleSketchBuilder::new(update_policy).build();
/// a.update("apple", 1);
/// a.update("banana", 1);
///
/// let mut b = TupleSketchBuilder::new(update_policy).build();
/// b.update("banana", 1);
///
/// let a_not_b = TupleANotB::default();
/// let result = a_not_b.compute(&a, &b, true).unwrap();
/// assert_eq!(result.num_retained(), 1); // only "apple" survives
/// ```
#[derive(Debug, Clone, Copy)]
pub struct TupleANotB {
    seed_hash: u16,
}

impl Default for TupleANotB {
    fn default() -> Self {
        Self::with_seed(DEFAULT_UPDATE_SEED)
    }
}

impl TupleANotB {
    /// Creates a new set difference operator for the given `seed`.
    pub fn with_seed(seed: u64) -> Self {
        Self {
            seed_hash: compute_seed_hash(seed),
        }
    }

    /// Computes `a and not b`.
    ///
    /// The result retains every key of `a` (below the combined theta) that is not present in `b`,
    /// keeping the summaries from `a`. Summary values in `b` are ignored and need not be
    /// cloneable. If `ordered` is true, the retained entries are sorted ascending by hash.
    ///
    /// # Errors
    ///
    /// Returns an error if either non-trivial input has a seed hash that differs from this
    /// operator's seed.
    pub fn compute<'a, 'b, S, T>(
        &self,
        a: impl Into<TupleSketchView<'a, S>>,
        b: impl Into<TupleSketchView<'b, T>>,
        ordered: bool,
    ) -> Result<CompactTupleSketch<S>, Error>
    where
        S: Clone + 'a,
        T: 'b,
    {
        let a = a.into();
        let b = b.into();
        let parts = a_not_b::compute(self.seed_hash, a, b, ordered)?;
        Ok(CompactTupleSketch::from_parts(
            parts.entries,
            parts.theta,
            parts.seed_hash,
            parts.ordered,
            parts.empty,
        ))
    }
}
