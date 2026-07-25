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
//! [`TupleAnotB`] computes the set difference of two Tuple sketches: the keys retained in `A` that
//! are not present in `B`. Surviving keys keep their summaries from `A` unchanged, so unlike the
//! union and intersection this operation needs no combine policy. The logic lives in the shared
//! raw operator (`RawAnotB`) that also drives the Theta a-not-B.

use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::thetacommon::a_not_b::RawAnotB;
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
/// # use datasketches::tuple::{DefaultUpdatePolicy, TupleAnotB, TupleSketchBuilder};
/// let update_policy = DefaultUpdatePolicy::<u64>::default();
/// let mut a = TupleSketchBuilder::new(update_policy).build();
/// a.update("apple", 1);
/// a.update("banana", 1);
///
/// let mut b = TupleSketchBuilder::new(update_policy).build();
/// b.update("banana", 1);
///
/// let a_not_b = TupleAnotB::new_with_default_seed();
/// let result = a_not_b.compute(&a, &b, true).unwrap();
/// assert_eq!(result.num_retained(), 1); // only "apple" survives
/// ```
#[derive(Debug, Clone, Copy)]
pub struct TupleAnotB {
    raw: RawAnotB,
}

impl TupleAnotB {
    /// Creates a new set difference operator for the given `seed`.
    pub fn new(seed: u64) -> Self {
        Self {
            raw: RawAnotB::new(seed),
        }
    }

    /// Creates a new set difference operator with the default seed.
    pub fn new_with_default_seed() -> Self {
        Self::new(DEFAULT_UPDATE_SEED)
    }

    /// Computes `a and not b`.
    ///
    /// The result retains every key of `a` (below the combined theta) that is not present in `b`,
    /// keeping the summaries from `a`. If `ordered` is true, the retained entries are sorted
    /// ascending by hash.
    ///
    /// # Errors
    ///
    /// Returns an error if either non-trivial input has a seed hash that differs from this
    /// operator's seed.
    pub fn compute<S, A, B>(
        &self,
        a: &A,
        b: &B,
        ordered: bool,
    ) -> Result<CompactTupleSketch<S>, Error>
    where
        A: TupleSketchView<S>,
        B: TupleSketchView<S>,
    {
        let parts = self.raw.compute(a, b, ordered)?;
        Ok(CompactTupleSketch::from_parts(
            parts.entries,
            parts.theta,
            parts.seed_hash,
            parts.ordered,
            parts.empty,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::NumStdDev;
    use crate::error::ErrorKind;
    use crate::tuple::DefaultUpdatePolicy;
    use crate::tuple::TupleSketchBuilder;

    fn default_sketch_builder() -> TupleSketchBuilder<DefaultUpdatePolicy<u64>> {
        TupleSketchBuilder::new(DefaultUpdatePolicy::<u64>::default())
    }

    fn sorted_entries(sketch: &CompactTupleSketch<u64>) -> Vec<(u64, u64)> {
        let mut entries: Vec<(u64, u64)> = sketch.iter().map(|(h, &s)| (h, s)).collect();
        entries.sort_unstable();
        entries
    }

    #[test]
    fn a_not_b_basic_difference() {
        let mut a = default_sketch_builder().build();
        for i in 0..1000 {
            a.update(i, 1u64);
        }
        let mut b = default_sketch_builder().build();
        for i in 500..1500 {
            b.update(i, 1u64);
        }

        let result = TupleAnotB::new_with_default_seed()
            .compute(&a, &b, true)
            .unwrap();
        // Keys 0..500 are only in A (exact mode).
        assert_eq!(result.num_retained(), 500);
        assert_eq!(result.estimate(), 500.0);
    }

    #[test]
    fn a_not_b_keeps_summaries_from_a() {
        let mut a = default_sketch_builder().build();
        a.update("only_a", 7u64);
        a.update("shared", 7u64);
        let mut b = default_sketch_builder().build();
        b.update("shared", 99u64);

        let result = TupleAnotB::new_with_default_seed()
            .compute(&a, &b, true)
            .unwrap();
        assert_eq!(result.num_retained(), 1);
        // The surviving key keeps A's summary; B's summary is never combined in.
        assert_eq!(result.iter().next().unwrap().1, &7);
    }

    #[test]
    fn a_not_b_with_empty_b_returns_a() {
        let mut a = default_sketch_builder().build();
        for i in 0..100 {
            a.update(i, 3u64);
        }
        let b = default_sketch_builder().build();

        let result = TupleAnotB::new_with_default_seed()
            .compute(&a, &b, true)
            .unwrap();
        assert_eq!(result.num_retained(), 100);
        assert!(result.iter().all(|(_, &s)| s == 3));
    }

    #[test]
    fn a_not_b_with_empty_a_is_empty() {
        let a = default_sketch_builder().build();
        let mut b = default_sketch_builder().build();
        for i in 0..100 {
            b.update(i, 1u64);
        }

        let result = TupleAnotB::new_with_default_seed()
            .compute(&a, &b, true)
            .unwrap();
        assert!(result.is_empty());
        assert_eq!(result.num_retained(), 0);
        assert_eq!(result.estimate(), 0.0);
    }

    #[test]
    fn a_not_b_with_superset_b_is_empty() {
        let mut a = default_sketch_builder().build();
        for i in 0..500 {
            a.update(i, 1u64);
        }
        let mut b = default_sketch_builder().build();
        for i in 0..1000 {
            b.update(i, 1u64);
        }

        let result = TupleAnotB::new_with_default_seed()
            .compute(&a, &b, true)
            .unwrap();
        assert_eq!(result.num_retained(), 0);
        assert_eq!(result.estimate(), 0.0);
    }

    #[test]
    fn a_not_b_with_disjoint_b_returns_a() {
        let mut a = default_sketch_builder().build();
        for i in 0..500 {
            a.update(i, 1u64);
        }
        let mut b = default_sketch_builder().build();
        for i in 500..1000 {
            b.update(i, 1u64);
        }

        let result = TupleAnotB::new_with_default_seed()
            .compute(&a, &b, true)
            .unwrap();
        assert_eq!(result.num_retained(), 500);
    }

    #[test]
    fn a_not_b_accepts_updatable_and_compact_inputs() {
        let mut a = default_sketch_builder().build();
        for i in 0..1000 {
            a.update(i, 1u64);
        }
        let mut b = default_sketch_builder().build();
        for i in 500..1500 {
            b.update(i, 1u64);
        }
        let b_compact = b.compact(true);

        // a (updatable) not b (compact)
        let result = TupleAnotB::new_with_default_seed()
            .compute(&a, &b_compact, true)
            .unwrap();
        assert_eq!(result.num_retained(), 500);

        // a (compact) not b (compact)
        let a_compact = a.compact(true);
        let result2 = TupleAnotB::new_with_default_seed()
            .compute(&a_compact, &b_compact, true)
            .unwrap();
        assert_eq!(result2.num_retained(), 500);
    }

    #[test]
    fn a_not_b_ordered_merge_matches_hash_set_path() {
        // In estimation mode, the both-ordered merge-scan path must produce the same result as
        // the hash-set path taken for unordered inputs.
        let mut a = default_sketch_builder().lg_k(8).build();
        for i in 0..75000 {
            a.update(i, 1u64);
        }
        let mut b = default_sketch_builder().lg_k(8).build();
        for i in 25000..75000 {
            b.update(i, 2u64);
        }
        assert!(a.is_estimation_mode() && b.is_estimation_mode());

        let op = TupleAnotB::new_with_default_seed();
        let unordered = op.compute(&a, &b, true).unwrap();
        let ordered = op
            .compute(&a.compact(true), &b.compact(true), true)
            .unwrap();

        assert!(ordered.is_ordered());
        assert_eq!(unordered.theta64(), ordered.theta64());
        assert_eq!(sorted_entries(&unordered), sorted_entries(&ordered));
    }

    #[test]
    fn a_not_b_result_is_ordered_when_requested() {
        let mut a = default_sketch_builder().build();
        for i in 0..1000 {
            a.update(i, 1u64);
        }
        let mut b = default_sketch_builder().build();
        for i in 500..1500 {
            b.update(i, 1u64);
        }

        let result = TupleAnotB::new_with_default_seed()
            .compute(&a, &b, true)
            .unwrap();
        assert!(result.is_ordered());
        let entries = sorted_entries(&result);
        let iter_order: Vec<u64> = result.iter().map(|(h, _)| h).collect();
        let sorted_order: Vec<u64> = entries.iter().map(|(h, _)| *h).collect();
        assert_eq!(iter_order, sorted_order);
    }

    #[test]
    fn a_not_b_rejects_seed_mismatch() {
        let mut a = default_sketch_builder().seed(1).build();
        a.update(1, 1u64);
        let mut b = default_sketch_builder().seed(1).build();
        b.update(2, 1u64);

        let err = TupleAnotB::new(2).compute(&a, &b, true).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidArgument);
    }

    #[test]
    fn a_not_b_validates_a_seed_even_when_b_is_empty() {
        // A is non-empty with a seed that does not match the operator; B is empty. The empty-B fast
        // path must not bypass A's seed check.
        let mut a = default_sketch_builder().seed(1).build();
        a.update(1, 1u64);
        let b = default_sketch_builder().seed(1).build(); // empty

        let err = TupleAnotB::new(2).compute(&a, &b, true).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidArgument);
    }

    #[test]
    fn a_not_b_empty_b_returns_a_for_non_empty_zero_retained_a() {
        // A is logically non-empty but retains no keys (the single update is screened out by the
        // sampling theta).
        let mut a = default_sketch_builder().sampling_probability(0.001).build();
        a.update(1u64, 1u64);
        assert!(!a.is_empty());
        assert_eq!(a.num_retained(), 0);

        // B is empty and built with a different seed. Since an empty B subtracts nothing, the
        // result must be a copy of A: no seed error, and A's theta is preserved (not
        // lowered by B).
        let b = default_sketch_builder().seed(999).build();

        let result = TupleAnotB::new_with_default_seed()
            .compute(&a, &b, true)
            .unwrap();
        assert!(!result.is_empty());
        assert_eq!(result.num_retained(), 0);
        assert_eq!(result.theta64(), a.theta64());
    }

    #[test]
    fn a_not_b_in_estimation_mode_estimates_within_bounds() {
        let mut a = default_sketch_builder().lg_k(8).build();
        for i in 0..75000 {
            a.update(i, 1u64);
        }
        let mut b = default_sketch_builder().lg_k(8).build();
        for i in 25000..75000 {
            b.update(i, 1u64);
        }

        let result = TupleAnotB::new_with_default_seed()
            .compute(&a, &b, true)
            .unwrap();
        assert!(result.is_estimation_mode());
        // True difference size is 25000 (keys 0..25000).
        let lower = result.lower_bound(NumStdDev::Three);
        let upper = result.upper_bound(NumStdDev::Three);
        assert!(
            lower <= 25000.0 && 25000.0 <= upper,
            "expected 25000 in [{lower}, {upper}]"
        );
    }
}
