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
/// A newly created operator has no result. [`has_result`](Self::has_result) returns `false` and
/// [`to_sketch`](Self::to_sketch) returns `None` until the first successful
/// [`update`](Self::update).
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
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
/// let mut a = TupleSketchBuilder::new(update_policy).build()?;
/// a.update("shared", 3);
/// a.update("only_a", 1);
///
/// let mut b = TupleSketchBuilder::new(update_policy).build()?;
/// b.update("shared", 4);
/// b.update("only_b", 1);
///
/// let mut intersection = TupleIntersection::new(SumPolicy);
/// intersection.update(&a)?;
/// intersection.update(&b)?;
///
/// let result = intersection
///     .to_sketch(true)
///     .expect("intersection has been updated");
/// assert_eq!(result.num_retained(), 1); // only "shared"
/// assert_eq!(
///     result
///         .iter()
///         .next()
///         .expect("result contains the shared entry")
///         .1,
///     &7
/// );
/// # Ok(())
/// # }
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
            .expect("the default Tuple intersection seed must be valid")
    }

    /// Creates a new intersection operator for the given combine `policy` and `seed`.
    ///
    /// # Errors
    ///
    /// Returns an error if the computed seed hash is zero.
    pub fn with_seed(policy: P, seed: u64) -> Result<Self, Error> {
        Ok(Self {
            state: IntersectionState::new(seed, policy)?,
        })
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
    pub fn update<'a>(
        &mut self,
        sketch: impl Into<TupleSketchView<'a, P::Summary>>,
    ) -> Result<(), Error>
    where
        P::Summary: Clone + 'a,
    {
        let sketch = sketch.into();
        self.state.update(sketch)
    }

    /// Returns `true` after the first successful [`update`](Self::update).
    pub fn has_result(&self) -> bool {
        self.state.has_result()
    }

    /// Returns the estimated size of the intersection in bytes.
    pub fn estimated_size(&self) -> usize {
        size_of::<Self>() + self.state.estimated_size()
    }

    /// Returns the current intersection as a compact Tuple sketch.
    ///
    /// Returns `None` until the first successful [`update`](Self::update). After that, returns
    /// `Some` even when the intersection is empty.
    ///
    /// If `ordered` is `true`, retained entries are sorted in ascending hash order.
    pub fn to_sketch(&self, ordered: bool) -> Option<CompactTupleSketch<P::Summary>>
    where
        P::Summary: Clone,
    {
        if !self.state.has_result() {
            return None;
        }
        let parts = self.state.to_compact_parts(ordered);
        Some(CompactTupleSketch::from_parts(
            parts.entries,
            parts.theta,
            parts.seed_hash,
            parts.ordered,
            parts.empty,
        ))
    }
}
