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

//! Tuple sketch intersection.
//!
//! [`TupleIntersection`] computes the intersection (set AND) of Tuple sketches. It shares its state
//! machine with the Theta intersection; the only Tuple-specific addition is that for each key
//! retained in both the running result and the incoming sketch, the two summaries are combined
//! with a [`SummaryCombinePolicy`].
//!
//! Unlike the union there is no default policy: how to combine the summaries of keys present in
//! both inputs is application-specific, so a policy must always be supplied.

use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::thetacommon::intersection::IntersectionState;
use crate::tuple::hash_table::TupleEntry;
use crate::tuple::policy::SummaryCombinePolicy;
use crate::tuple::sketch::CompactTupleSketch;
use crate::tuple::sketch::TupleSketchView;

/// Stateful intersection operator for Tuple sketches.
///
/// `P` is the [`SummaryCombinePolicy`] applied to keys present in more than one input. There is no
/// default policy (see the module docs), so one must be supplied at construction.
///
/// Before the first [`update`](Self::update), the result is undefined; use
/// [`has_result`](Self::has_result) to check.
///
/// # Examples
///
/// ```
/// use datasketches::tuple::DefaultUpdatePolicy;
/// use datasketches::tuple::SummaryCombinePolicy;
/// use datasketches::tuple::SummaryPolicy;
/// use datasketches::tuple::TupleIntersection;
/// use datasketches::tuple::TupleSketchBuilder;
///
/// // Sum the summaries of keys that appear in both inputs.
/// #[derive(Default)]
/// struct SumPolicy;
/// impl SummaryPolicy for SumPolicy {
///     type Summary = u64;
///
///     fn create(&self) -> Self::Summary {
///         0
///     }
/// }
/// impl SummaryCombinePolicy for SumPolicy {
///     fn combine(&self, summary: &mut Self::Summary, other: &Self::Summary) {
///         *summary += *other;
///     }
/// }
///
/// let update_policy = DefaultUpdatePolicy::<u64>::default();
/// let mut a = TupleSketchBuilder::new(update_policy).build();
/// a.update("shared", 3);
/// a.update("only_a", 1);
///
/// let mut b = TupleSketchBuilder::new(update_policy).build();
/// b.update("shared", 4);
/// b.update("only_b", 1);
///
/// let mut intersection = TupleIntersection::new(SumPolicy);
/// intersection.update(&a).unwrap();
/// intersection.update(&b).unwrap();
///
/// let result = intersection.to_sketch(true);
/// assert_eq!(result.num_retained(), 1); // only "shared"
/// assert_eq!(result.iter().next().unwrap().1, &7); // 3 + 4
/// ```
#[derive(Debug)]
pub struct TupleIntersection<P>
where
    P: SummaryCombinePolicy,
{
    state: IntersectionState<TupleEntry<P::Summary>, P>,
}

impl<P> TupleIntersection<P>
where
    P: SummaryCombinePolicy,
{
    /// Creates a new intersection operator with the default seed and the given combine `policy`.
    pub fn new(policy: P) -> Self {
        Self::with_seed(policy, DEFAULT_UPDATE_SEED)
    }

    /// Creates a new intersection operator for the given combine `policy` and `seed`.
    pub fn with_seed(policy: P, seed: u64) -> Self {
        Self {
            state: IntersectionState::new(seed, policy),
        }
    }

    /// Updates the intersection with a given sketch.
    ///
    /// The intersection can be viewed as starting from the "universe" set, and every update reduces
    /// the current set to the keys it shares with `sketch`. Summaries of shared keys are combined
    /// via the policy.
    ///
    /// # Errors
    ///
    /// Returns an error if `sketch` (when non-empty) has a different seed hash, or if the input
    /// appears corrupted (entry counts do not match what the sketch reports).
    pub fn update<V>(&mut self, sketch: &V) -> Result<(), Error>
    where
        V: TupleSketchView<P::Summary>,
        P::Summary: Clone,
    {
        self.state.update(sketch)
    }

    /// Returns whether this operator has received at least one update.
    pub fn has_result(&self) -> bool {
        self.state.has_result()
    }

    /// Returns the estimated size of the intersection in bytes.
    pub fn estimated_size(&self) -> usize {
        size_of::<Self>() + self.state.estimated_size()
    }

    /// Returns the intersection result as a compact Tuple sketch.
    ///
    /// If `ordered` is true, retained entries are sorted ascending by hash.
    ///
    /// # Panics
    ///
    /// Panics if called before the first [`update`](Self::update).
    pub fn to_sketch(&self, ordered: bool) -> CompactTupleSketch<P::Summary>
    where
        P::Summary: Clone,
    {
        assert!(
            self.state.has_result(),
            "TupleIntersection::to_sketch() called before first update()"
        );
        let parts = self.state.result(ordered);
        CompactTupleSketch::from_parts(
            parts.entries,
            parts.theta,
            parts.seed_hash,
            parts.ordered,
            parts.empty,
        )
    }
}
