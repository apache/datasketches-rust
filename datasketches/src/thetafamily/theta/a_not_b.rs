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

//! Theta sketch set difference (`A and not B`).
//!
//! [`ThetaANotB`] computes the set difference of two Theta sketches: the keys retained in `A`
//! that are not present in `B`. It shares its set-difference implementation with Tuple a-not-B;
//! Theta entries carry no summary, so nothing needs to be combined.

use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::theta::CompactThetaSketch;
use crate::theta::ThetaSketchView;
use crate::thetacommon::a_not_b::ANotBOperator;

/// Set difference operator (`A and not B`) for Theta sketches.
///
/// This is a stateless operator (other than the seed): each call to [`compute`](Self::compute)
/// takes two input sketches and returns a new [`CompactThetaSketch`].
///
/// # Examples
///
/// ```
/// use datasketches::theta::ThetaANotB;
/// use datasketches::theta::ThetaSketchBuilder;
///
/// let mut a = ThetaSketchBuilder::default().build();
/// a.update("apple");
/// a.update("banana");
///
/// let mut b = ThetaSketchBuilder::default().build();
/// b.update("banana");
///
/// let a_not_b = ThetaANotB::default();
/// let result = a_not_b.compute(&a, &b, true).unwrap();
/// assert_eq!(result.num_retained(), 1); // only "apple" survives
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ThetaANotB {
    op: ANotBOperator,
}

impl Default for ThetaANotB {
    fn default() -> Self {
        Self::with_seed(DEFAULT_UPDATE_SEED)
    }
}

impl ThetaANotB {
    /// Creates a new set difference operator for the given `seed`.
    pub fn with_seed(seed: u64) -> Self {
        Self {
            op: ANotBOperator::new(seed),
        }
    }

    /// Computes `a and not b`.
    ///
    /// The result retains every key of `a` (below the combined theta) that is not present in `b`.
    /// If `ordered` is true, the retained entries are sorted ascending by hash.
    ///
    /// # Errors
    ///
    /// Returns an error if either non-trivial input has a seed hash that differs from this
    /// operator's seed.
    pub fn compute<A, B>(&self, a: &A, b: &B, ordered: bool) -> Result<CompactThetaSketch, Error>
    where
        A: ThetaSketchView,
        B: ThetaSketchView,
    {
        let parts = self.op.compute(a, b, ordered)?;
        Ok(CompactThetaSketch::from_parts(
            parts
                .entries
                .into_iter()
                .map(|entry| entry.hash())
                .collect(),
            parts.theta,
            parts.seed_hash,
            parts.ordered,
            parts.empty,
        ))
    }
}
