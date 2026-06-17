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
//! union and intersection this operation needs no combine policy.

use std::collections::HashSet;

use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::compute_seed_hash;
use crate::theta::MAX_THETA;
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
/// # use datasketches::tuple::{TupleAnotB, UpdatableTupleSketch};
/// let mut a = UpdatableTupleSketch::<u64>::builder().build();
/// a.update("apple", 1);
/// a.update("banana", 1);
///
/// let mut b = UpdatableTupleSketch::<u64>::builder().build();
/// b.update("banana", 1);
///
/// let a_not_b = TupleAnotB::new_with_default_seed();
/// let result = a_not_b.compute(&a, &b).unwrap();
/// assert_eq!(result.num_retained(), 1); // only "apple" survives
/// ```
#[derive(Debug, Clone, Copy)]
pub struct TupleAnotB {
    seed_hash: u16,
}

impl TupleAnotB {
    /// Creates a new set difference operator for the given `seed`.
    pub fn new(seed: u64) -> Self {
        Self {
            seed_hash: compute_seed_hash(seed),
        }
    }

    /// Creates a new set difference operator with the default seed.
    pub fn new_with_default_seed() -> Self {
        Self::new(DEFAULT_UPDATE_SEED)
    }

    /// Computes `a and not b`, returning an ordered compact sketch.
    ///
    /// # Errors
    ///
    /// See [`compute_with_ordered`](Self::compute_with_ordered).
    pub fn compute<S, A, B>(&self, a: &A, b: &B) -> Result<CompactTupleSketch<S>, Error>
    where
        A: TupleSketchView<S>,
        B: TupleSketchView<S>,
        S: Clone,
    {
        self.compute_with_ordered(a, b, true)
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
    pub fn compute_with_ordered<S, A, B>(
        &self,
        a: &A,
        b: &B,
        ordered: bool,
    ) -> Result<CompactTupleSketch<S>, Error>
    where
        A: TupleSketchView<S>,
        B: TupleSketchView<S>,
        S: Clone,
    {
        // If A is empty the result is an (empty) copy of A. As with the union and intersection, an
        // empty input carries no keys, so its seed is not validated.
        if a.is_empty() {
            return Ok(Self::compact_from_view(a, ordered));
        }

        // A is non-empty, so its seed must be compatible.
        if a.seed_hash() != self.seed_hash {
            return Err(Error::invalid_argument(format!(
                "A seed hash mismatch: expected {}, got {}",
                self.seed_hash,
                a.seed_hash()
            )));
        }

        // An empty B subtracts nothing, so the result is simply a copy of A. This also covers the
        // "A is non-empty but has no retained keys" state: B's seed and theta must not influence
        // the result, so we return before touching them.
        if b.is_empty() {
            return Ok(Self::compact_from_view(a, ordered));
        }

        // B is non-empty, so its seed must be compatible.
        if b.seed_hash() != self.seed_hash {
            return Err(Error::invalid_argument(format!(
                "B seed hash mismatch: expected {}, got {}",
                self.seed_hash,
                b.seed_hash()
            )));
        }

        let theta = a.theta64().min(b.theta64());
        // A is non-empty here; the result only becomes empty if everything is subtracted in exact
        // mode (handled below).
        let mut is_empty = false;

        let entries: Vec<(u64, S)> = if b.num_retained() == 0 {
            a.iter()
                .filter(|(hash, _)| *hash < theta)
                .map(|(hash, summary)| (hash, summary.clone()))
                .collect()
        } else {
            let mut b_keys: HashSet<u64> = HashSet::with_capacity(b.num_retained());
            for (hash, _) in b.iter() {
                if hash < theta {
                    b_keys.insert(hash);
                } else if b.is_ordered() {
                    break;
                }
            }

            let mut entries = Vec::new();
            for (hash, summary) in a.iter() {
                if hash < theta {
                    if !b_keys.contains(&hash) {
                        entries.push((hash, summary.clone()));
                    }
                } else if a.is_ordered() {
                    break;
                }
            }
            entries
        };

        if entries.is_empty() && theta == MAX_THETA {
            is_empty = true;
        }

        let out_ordered = ordered || a.is_ordered();
        let mut entries = entries;
        if ordered && !a.is_ordered() && entries.len() > 1 {
            entries.sort_unstable_by_key(|(hash, _)| *hash);
        }

        Ok(CompactTupleSketch::from_parts(
            entries,
            theta,
            self.seed_hash,
            out_ordered,
            is_empty,
        ))
    }

    /// Builds a compact sketch that is a copy of the view `a`.
    fn compact_from_view<S, V>(a: &V, ordered: bool) -> CompactTupleSketch<S>
    where
        V: TupleSketchView<S>,
        S: Clone,
    {
        let mut entries: Vec<(u64, S)> = a
            .iter()
            .map(|(hash, summary)| (hash, summary.clone()))
            .collect();
        let out_ordered = ordered || a.is_ordered();
        if ordered && !a.is_ordered() && entries.len() > 1 {
            entries.sort_unstable_by_key(|(hash, _)| *hash);
        }
        CompactTupleSketch::from_parts(
            entries,
            a.theta64(),
            a.seed_hash(),
            out_ordered,
            a.is_empty(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::NumStdDev;
    use crate::error::ErrorKind;
    use crate::tuple::UpdatableTupleSketch;

    fn sorted_entries(sketch: &CompactTupleSketch<u64>) -> Vec<(u64, u64)> {
        let mut entries: Vec<(u64, u64)> = sketch.iter().map(|(h, &s)| (h, s)).collect();
        entries.sort_unstable();
        entries
    }

    #[test]
    fn a_not_b_basic_difference() {
        let mut a = UpdatableTupleSketch::<u64>::builder().build();
        for i in 0..1000 {
            a.update(i, 1u64);
        }
        let mut b = UpdatableTupleSketch::<u64>::builder().build();
        for i in 500..1500 {
            b.update(i, 1u64);
        }

        let result = TupleAnotB::new_with_default_seed().compute(&a, &b).unwrap();
        // Keys 0..500 are only in A (exact mode).
        assert_eq!(result.num_retained(), 500);
        assert_eq!(result.estimate(), 500.0);
    }

    #[test]
    fn a_not_b_keeps_summaries_from_a() {
        let mut a = UpdatableTupleSketch::<u64>::builder().build();
        a.update("only_a", 7u64);
        a.update("shared", 7u64);
        let mut b = UpdatableTupleSketch::<u64>::builder().build();
        b.update("shared", 99u64);

        let result = TupleAnotB::new_with_default_seed().compute(&a, &b).unwrap();
        assert_eq!(result.num_retained(), 1);
        // The surviving key keeps A's summary; B's summary is never combined in.
        assert_eq!(result.iter().next().unwrap().1, &7);
    }

    #[test]
    fn a_not_b_with_empty_b_returns_a() {
        let mut a = UpdatableTupleSketch::<u64>::builder().build();
        for i in 0..100 {
            a.update(i, 3u64);
        }
        let b = UpdatableTupleSketch::<u64>::builder().build();

        let result = TupleAnotB::new_with_default_seed().compute(&a, &b).unwrap();
        assert_eq!(result.num_retained(), 100);
        assert!(result.iter().all(|(_, &s)| s == 3));
    }

    #[test]
    fn a_not_b_with_empty_a_is_empty() {
        let a = UpdatableTupleSketch::<u64>::builder().build();
        let mut b = UpdatableTupleSketch::<u64>::builder().build();
        for i in 0..100 {
            b.update(i, 1u64);
        }

        let result = TupleAnotB::new_with_default_seed().compute(&a, &b).unwrap();
        assert!(result.is_empty());
        assert_eq!(result.num_retained(), 0);
        assert_eq!(result.estimate(), 0.0);
    }

    #[test]
    fn a_not_b_with_superset_b_is_empty() {
        let mut a = UpdatableTupleSketch::<u64>::builder().build();
        for i in 0..500 {
            a.update(i, 1u64);
        }
        let mut b = UpdatableTupleSketch::<u64>::builder().build();
        for i in 0..1000 {
            b.update(i, 1u64);
        }

        let result = TupleAnotB::new_with_default_seed().compute(&a, &b).unwrap();
        assert_eq!(result.num_retained(), 0);
        assert_eq!(result.estimate(), 0.0);
    }

    #[test]
    fn a_not_b_with_disjoint_b_returns_a() {
        let mut a = UpdatableTupleSketch::<u64>::builder().build();
        for i in 0..500 {
            a.update(i, 1u64);
        }
        let mut b = UpdatableTupleSketch::<u64>::builder().build();
        for i in 500..1000 {
            b.update(i, 1u64);
        }

        let result = TupleAnotB::new_with_default_seed().compute(&a, &b).unwrap();
        assert_eq!(result.num_retained(), 500);
    }

    #[test]
    fn a_not_b_accepts_updatable_and_compact_inputs() {
        let mut a = UpdatableTupleSketch::<u64>::builder().build();
        for i in 0..1000 {
            a.update(i, 1u64);
        }
        let mut b = UpdatableTupleSketch::<u64>::builder().build();
        for i in 500..1500 {
            b.update(i, 1u64);
        }
        let b_compact = b.compact(true);

        // a (updatable) not b (compact)
        let result = TupleAnotB::new_with_default_seed()
            .compute(&a, &b_compact)
            .unwrap();
        assert_eq!(result.num_retained(), 500);

        // a (compact) not b (compact)
        let a_compact = a.compact(true);
        let result2 = TupleAnotB::new_with_default_seed()
            .compute(&a_compact, &b_compact)
            .unwrap();
        assert_eq!(result2.num_retained(), 500);
    }

    #[test]
    fn a_not_b_result_is_ordered_when_requested() {
        let mut a = UpdatableTupleSketch::<u64>::builder().build();
        for i in 0..1000 {
            a.update(i, 1u64);
        }
        let mut b = UpdatableTupleSketch::<u64>::builder().build();
        for i in 500..1500 {
            b.update(i, 1u64);
        }

        let result = TupleAnotB::new_with_default_seed()
            .compute_with_ordered(&a, &b, true)
            .unwrap();
        assert!(result.is_ordered());
        let entries = sorted_entries(&result);
        let iter_order: Vec<u64> = result.iter().map(|(h, _)| h).collect();
        let sorted_order: Vec<u64> = entries.iter().map(|(h, _)| *h).collect();
        assert_eq!(iter_order, sorted_order);
    }

    #[test]
    fn a_not_b_rejects_seed_mismatch() {
        let mut a = UpdatableTupleSketch::<u64>::builder().seed(1).build();
        a.update(1, 1u64);
        let mut b = UpdatableTupleSketch::<u64>::builder().seed(1).build();
        b.update(2, 1u64);

        let err = TupleAnotB::new(2).compute(&a, &b).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidArgument);
    }

    #[test]
    fn a_not_b_validates_a_seed_even_when_b_is_empty() {
        // A is non-empty with a seed that does not match the operator; B is empty. The empty-B fast
        // path must not bypass A's seed check.
        let mut a = UpdatableTupleSketch::<u64>::builder().seed(1).build();
        a.update(1, 1u64);
        let b = UpdatableTupleSketch::<u64>::builder().seed(1).build(); // empty

        let err = TupleAnotB::new(2).compute(&a, &b).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidArgument);
    }

    #[test]
    fn a_not_b_empty_b_returns_a_for_non_empty_zero_retained_a() {
        // A is logically non-empty but retains no keys (the single update is screened out by the
        // sampling theta).
        let mut a = UpdatableTupleSketch::<u64>::builder()
            .sampling_probability(0.001)
            .build();
        a.update(1u64, 1u64);
        assert!(!a.is_empty());
        assert_eq!(a.num_retained(), 0);

        // B is empty and built with a different seed. Since an empty B subtracts nothing, the
        // result must be a copy of A: no seed error, and A's theta is preserved (not
        // lowered by B).
        let b = UpdatableTupleSketch::<u64>::builder().seed(999).build();

        let result = TupleAnotB::new_with_default_seed().compute(&a, &b).unwrap();
        assert!(!result.is_empty());
        assert_eq!(result.num_retained(), 0);
        assert_eq!(result.theta64(), a.theta64());
    }

    #[test]
    fn a_not_b_in_estimation_mode_estimates_within_bounds() {
        let mut a = UpdatableTupleSketch::<u64>::builder().lg_k(8).build();
        for i in 0..75000 {
            a.update(i, 1u64);
        }
        let mut b = UpdatableTupleSketch::<u64>::builder().lg_k(8).build();
        for i in 25000..75000 {
            b.update(i, 1u64);
        }

        let result = TupleAnotB::new_with_default_seed().compute(&a, &b).unwrap();
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
