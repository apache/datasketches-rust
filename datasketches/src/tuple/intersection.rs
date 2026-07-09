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
//! [`TupleIntersection`] computes the intersection (set AND) of Tuple sketches. It reuses the raw
//! intersection state machine (`RawThetaIntersection`) that also drives the Theta intersection;
//! the only Tuple-specific addition is that for each key retained in both the running result and
//! the incoming sketch, the two summaries are combined with a [`SummaryCombinePolicy`].
//!
//! Unlike the union there is no default policy: how to combine the summaries of keys present in
//! both inputs is application-specific, so a policy must always be supplied.

use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::thetacommon::intersection::RawThetaIntersection;
use crate::tuple::hash_table::TupleEntry;
use crate::tuple::policy::CombinePolicyAdapter;
use crate::tuple::policy::SummaryCombinePolicy;
use crate::tuple::sketch::CompactTupleSketch;
use crate::tuple::sketch::TupleSketchView;

/// Stateful intersection operator for Tuple sketches.
///
/// `S` is the summary type and `P` is the [`SummaryCombinePolicy`] applied to keys present in more
/// than one input. There is no default policy (see the module docs), so one must be supplied at
/// construction.
///
/// Before the first [`update`](Self::update), the result is undefined; use
/// [`has_result`](Self::has_result) to check.
///
/// # Examples
///
/// ```
/// use datasketches::tuple::SummaryCombinePolicy;
/// use datasketches::tuple::TupleIntersection;
/// use datasketches::tuple::UpdatableTupleSketch;
///
/// // Sum the summaries of keys that appear in both inputs.
/// #[derive(Default)]
/// struct SumPolicy;
/// impl SummaryCombinePolicy<u64> for SumPolicy {
///     fn combine(&self, summary: &mut u64, other: &u64) {
///         *summary += *other;
///     }
/// }
///
/// let mut a = UpdatableTupleSketch::<u64>::builder().build();
/// a.update("shared", 3);
/// a.update("only_a", 1);
///
/// let mut b = UpdatableTupleSketch::<u64>::builder().build();
/// b.update("shared", 4);
/// b.update("only_b", 1);
///
/// let mut intersection = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
/// intersection.update(&a).unwrap();
/// intersection.update(&b).unwrap();
///
/// let result = intersection.to_sketch(true);
/// assert_eq!(result.num_retained(), 1); // only "shared"
/// assert_eq!(result.iter().next().unwrap().1, &7); // 3 + 4
/// ```
#[derive(Debug)]
pub struct TupleIntersection<S, P> {
    raw: RawThetaIntersection<TupleEntry<S>, CombinePolicyAdapter<P>>,
}

impl<S, P> TupleIntersection<S, P> {
    /// Creates a new intersection operator for the given `seed` and combine `policy`.
    pub fn new(seed: u64, policy: P) -> Self {
        Self {
            raw: RawThetaIntersection::new(seed, CombinePolicyAdapter(policy)),
        }
    }

    /// Creates a new intersection operator with the default seed and the given combine `policy`.
    pub fn new_with_default_seed(policy: P) -> Self {
        Self::new(DEFAULT_UPDATE_SEED, policy)
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
        V: TupleSketchView<S>,
        P: SummaryCombinePolicy<S>,
        S: Clone,
    {
        self.raw.update(sketch)
    }

    /// Returns whether this operator has received at least one update.
    pub fn has_result(&self) -> bool {
        self.raw.has_result()
    }

    /// Returns the intersection result as a compact Tuple sketch.
    ///
    /// If `ordered` is true, retained entries are sorted ascending by hash.
    ///
    /// # Panics
    ///
    /// Panics if called before the first [`update`](Self::update).
    pub fn to_sketch(&self, ordered: bool) -> CompactTupleSketch<S>
    where
        S: Clone,
    {
        assert!(
            self.raw.has_result(),
            "TupleIntersection::to_sketch() called before first update()"
        );
        let parts = self.raw.result(ordered);
        CompactTupleSketch::from_parts(
            parts
                .entries
                .into_iter()
                .map(TupleEntry::into_parts)
                .collect(),
            parts.theta,
            parts.seed_hash,
            parts.ordered,
            parts.empty,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tuple::UpdatableTupleSketch;

    #[derive(Debug, Default, Clone, Copy)]
    struct SumPolicy;

    impl SummaryCombinePolicy<u64> for SumPolicy {
        fn combine(&self, summary: &mut u64, other: &u64) {
            *summary += *other;
        }
    }

    fn sorted_entries(sketch: &CompactTupleSketch<u64>) -> Vec<(u64, u64)> {
        let mut entries: Vec<(u64, u64)> = sketch.iter().map(|(h, &s)| (h, s)).collect();
        entries.sort_unstable();
        entries
    }

    #[test]
    fn intersection_of_overlapping_sketches() {
        let mut a = UpdatableTupleSketch::<u64>::builder().build();
        for i in 0..1000 {
            a.update(i, 1u64);
        }
        let mut b = UpdatableTupleSketch::<u64>::builder().build();
        for i in 500..1500 {
            b.update(i, 1u64);
        }

        let mut intersection = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
        intersection.update(&a).unwrap();
        intersection.update(&b).unwrap();

        let result = intersection.to_sketch(true);
        // Keys 500..1000 are shared (exact mode), each summary is 1 + 1 = 2.
        assert_eq!(result.num_retained(), 500);
        assert_eq!(result.estimate(), 500.0);
        assert!(result.iter().all(|(_, &s)| s == 2));
    }

    #[test]
    fn intersection_combines_summaries_of_shared_keys() {
        let mut a = UpdatableTupleSketch::<u64>::builder().build();
        a.update("shared", 3u64);
        a.update("only_a", 100u64);
        let mut b = UpdatableTupleSketch::<u64>::builder().build();
        b.update("shared", 4u64);
        b.update("only_b", 200u64);

        let mut intersection = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
        intersection.update(&a).unwrap();
        intersection.update(&b).unwrap();

        let result = intersection.to_sketch(true);
        assert_eq!(sorted_entries(&result).len(), 1);
        assert_eq!(result.iter().next().unwrap().1, &7); // 3 + 4
    }

    #[test]
    fn intersection_is_order_independent() {
        let mut a = UpdatableTupleSketch::<u64>::builder().build();
        for i in 0..1000 {
            a.update(i, 1u64);
        }
        let mut b = UpdatableTupleSketch::<u64>::builder().build();
        for i in 500..1500 {
            b.update(i, 1u64);
        }

        let mut a_then_b = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
        a_then_b.update(&a).unwrap();
        a_then_b.update(&b).unwrap();

        let mut b_then_a = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
        b_then_a.update(&b).unwrap();
        b_then_a.update(&a).unwrap();

        assert_eq!(
            sorted_entries(&a_then_b.to_sketch(true)),
            sorted_entries(&b_then_a.to_sketch(true))
        );
    }

    #[test]
    fn intersection_accepts_updatable_and_compact_inputs() {
        let mut a = UpdatableTupleSketch::<u64>::builder().build();
        for i in 0..1000 {
            a.update(i, 1u64);
        }
        let mut b = UpdatableTupleSketch::<u64>::builder().build();
        for i in 500..1500 {
            b.update(i, 1u64);
        }
        let b_compact = b.compact(true);

        let mut intersection = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
        intersection.update(&a).unwrap();
        intersection.update(&b_compact).unwrap();

        assert_eq!(intersection.to_sketch(true).num_retained(), 500);
    }

    #[test]
    fn intersection_with_disjoint_sketches_is_empty() {
        let mut a = UpdatableTupleSketch::<u64>::builder().build();
        for i in 0..1000 {
            a.update(i, 1u64);
        }
        let mut b = UpdatableTupleSketch::<u64>::builder().build();
        for i in 1000..2000 {
            b.update(i, 1u64);
        }

        let mut intersection = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
        intersection.update(&a).unwrap();
        intersection.update(&b).unwrap();

        let result = intersection.to_sketch(true);
        assert_eq!(result.num_retained(), 0);
        assert_eq!(result.estimate(), 0.0);
    }

    #[test]
    fn intersection_with_empty_input_is_empty() {
        let mut a = UpdatableTupleSketch::<u64>::builder().build();
        for i in 0..1000 {
            a.update(i, 1u64);
        }
        let empty = UpdatableTupleSketch::<u64>::builder().build();

        let mut intersection = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
        intersection.update(&a).unwrap();
        intersection.update(&empty).unwrap();

        let result = intersection.to_sketch(true);
        assert!(result.is_empty());
        assert_eq!(result.num_retained(), 0);
        assert_eq!(result.estimate(), 0.0);
    }

    #[test]
    fn intersection_single_update_returns_input() {
        let mut a = UpdatableTupleSketch::<u64>::builder().build();
        for i in 0..100 {
            a.update(i, 5u64);
        }

        let mut intersection = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
        intersection.update(&a).unwrap();

        let result = intersection.to_sketch(true);
        assert_eq!(result.num_retained(), 100);
        // A single update copies the input unchanged (summaries not combined with anything).
        assert!(result.iter().all(|(_, &s)| s == 5));
    }

    #[test]
    fn has_result_reflects_first_update() {
        let mut intersection = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
        assert!(!intersection.has_result());

        let mut a = UpdatableTupleSketch::<u64>::builder().build();
        a.update(1, 1u64);
        intersection.update(&a).unwrap();
        assert!(intersection.has_result());
    }

    #[test]
    #[should_panic(expected = "before first update")]
    fn result_before_update_panics() {
        let intersection = TupleIntersection::<u64, SumPolicy>::new_with_default_seed(SumPolicy);
        let _ = intersection.to_sketch(true);
    }

    #[test]
    fn intersection_rejects_seed_mismatch() {
        let mut a = UpdatableTupleSketch::<u64>::builder().seed(1).build();
        a.update(1, 1u64);

        let mut intersection = TupleIntersection::<u64, _>::new(2, SumPolicy);
        let err = intersection.update(&a).unwrap_err();
        assert_eq!(err.kind(), crate::error::ErrorKind::InvalidArgument);
    }

    #[test]
    fn intersection_in_estimation_mode_estimates_within_bounds() {
        let mut a = UpdatableTupleSketch::<u64>::builder().lg_k(8).build();
        for i in 0..50000 {
            a.update(i, 1u64);
        }
        let mut b = UpdatableTupleSketch::<u64>::builder().lg_k(8).build();
        for i in 25000..75000 {
            b.update(i, 1u64);
        }

        let mut intersection = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
        intersection.update(&a).unwrap();
        intersection.update(&b).unwrap();

        let result = intersection.to_sketch(true);
        assert!(result.is_estimation_mode());
        // True intersection size is 25000 (keys 25000..50000).
        let lower = result.lower_bound(crate::common::NumStdDev::Three);
        let upper = result.upper_bound(crate::common::NumStdDev::Three);
        assert!(
            lower <= 25000.0 && 25000.0 <= upper,
            "expected 25000 in [{lower}, {upper}]"
        );
    }
}
