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
//! [`TupleIntersection`] computes the intersection (set AND) of Tuple sketches. The hash-table
//! bookkeeping mirrors the [Theta intersection](crate::theta), with one Tuple-specific addition:
//! for each key retained in both the running result and the incoming sketch, the two summaries are
//! combined with a [`SummaryCombinePolicy`].
//!
//! Unlike the union there is no default policy: how to combine the summaries of keys present in
//! both inputs is application-specific, so a policy must always be supplied.

use crate::common::ResizeFactor;
use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::theta::HASH_TABLE_REBUILD_THRESHOLD;
use crate::theta::MAX_THETA;
use crate::tuple::hash_table::TupleHashTable;
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
/// let result = intersection.result();
/// assert_eq!(result.num_retained(), 1); // only "shared"
/// assert_eq!(result.iter().next().unwrap().1, &7); // 3 + 4
/// ```
#[derive(Debug)]
pub struct TupleIntersection<S, P> {
    is_valid: bool,
    table: TupleHashTable<S>,
    policy: P,
}

impl<S, P> TupleIntersection<S, P> {
    /// Creates a new intersection operator for the given `seed` and combine `policy`.
    pub fn new(seed: u64, policy: P) -> Self {
        Self {
            is_valid: false,
            table: TupleHashTable::from_raw_parts(
                0,
                0,
                ResizeFactor::X1,
                1.0,
                MAX_THETA,
                seed,
                false,
            ),
            policy,
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
        let new_default_table = |table: &TupleHashTable<S>| {
            TupleHashTable::from_raw_parts(
                0,
                0,
                ResizeFactor::X1,
                1.0,
                table.theta(),
                table.hash_seed(),
                table.is_empty(),
            )
        };

        if self.table.is_empty() {
            return Ok(());
        }

        if !sketch.is_empty() && sketch.seed_hash() != self.table.seed_hash() {
            return Err(Error::invalid_argument(format!(
                "incompatible seed hash: expected {}, got {}",
                self.table.seed_hash(),
                sketch.seed_hash()
            )));
        }

        if sketch.is_empty() {
            self.table.set_empty(true);
        }

        self.table.set_theta(if self.table.is_empty() {
            MAX_THETA
        } else {
            self.table.theta().min(sketch.theta64())
        });

        if self.is_valid && self.table.num_retained() == 0 {
            return Ok(());
        }

        if sketch.num_retained() == 0 {
            self.is_valid = true;
            self.table = new_default_table(&self.table);
            return Ok(());
        }

        // first update, copy the incoming sketch's entries (hash + summary)
        if !self.is_valid {
            self.is_valid = true;
            let lg_size = TupleHashTable::<S>::lg_size_from_count_for_rebuild(
                sketch.num_retained(),
                HASH_TABLE_REBUILD_THRESHOLD,
            );
            let mut new_table = TupleHashTable::from_raw_parts(
                lg_size,
                lg_size - 1,
                ResizeFactor::X1,
                1.0,
                self.table.theta(),
                self.table.hash_seed(),
                self.table.is_empty(),
            );
            for (hash, summary) in sketch.iter() {
                if !new_table.try_insert(hash, summary.clone()) {
                    return Err(Error::invalid_argument(
                        "duplicate key, possibly corrupted input sketch",
                    ));
                }
            }
            // Safety check.
            if new_table.num_retained() != sketch.num_retained() {
                return Err(Error::invalid_argument(
                    "num entries mismatch, possibly corrupted input sketch",
                ));
            }
            self.table = new_table;
        } else {
            let max_matches = self.table.num_retained().min(sketch.num_retained());
            let mut matched_entries: Vec<(u64, S)> = Vec::with_capacity(max_matches);
            let mut count = 0;
            let policy = &self.policy;
            for (hash, incoming) in sketch.iter() {
                if hash < self.table.theta() {
                    if let Some(existing) = self.table.get(hash) {
                        if matched_entries.len() == max_matches {
                            return Err(Error::invalid_argument(
                                "max matches exceeded, possibly corrupted input sketch",
                            ));
                        }
                        let mut combined = existing.clone();
                        policy.combine(&mut combined, incoming);
                        matched_entries.push((hash, combined));
                    }
                } else if sketch.is_ordered() {
                    break; // early stop for ordered sketches
                }
                count += 1;
            }
            // Safety check.
            if count > sketch.num_retained() {
                return Err(Error::invalid_argument(
                    "more keys than expected, possibly corrupted input sketch",
                ));
            } else if !sketch.is_ordered() && count < sketch.num_retained() {
                return Err(Error::invalid_argument(
                    "fewer keys than expected, possibly corrupted input sketch",
                ));
            }
            if matched_entries.is_empty() {
                self.table = new_default_table(&self.table);
                if self.table.theta() == MAX_THETA {
                    self.table.set_empty(true);
                }
            } else {
                let lg_size = TupleHashTable::<S>::lg_size_from_count_for_rebuild(
                    matched_entries.len(),
                    HASH_TABLE_REBUILD_THRESHOLD,
                );
                let mut new_table = TupleHashTable::from_raw_parts(
                    lg_size,
                    lg_size - 1,
                    ResizeFactor::X1,
                    1.0,
                    self.table.theta(),
                    self.table.hash_seed(),
                    self.table.is_empty(),
                );
                for (hash, summary) in matched_entries {
                    if !new_table.try_insert(hash, summary) {
                        return Err(Error::invalid_argument(
                            "duplicate key, possibly corrupted input sketch",
                        ));
                    }
                }
                self.table = new_table;
            }
        }
        Ok(())
    }

    /// Returns whether this operator has received at least one update.
    pub fn has_result(&self) -> bool {
        self.is_valid
    }

    /// Returns the intersection result as a compact Tuple sketch (ordered).
    ///
    /// # Panics
    ///
    /// Panics if called before the first [`update`](Self::update).
    pub fn result(&self) -> CompactTupleSketch<S>
    where
        S: Clone,
    {
        self.result_with_ordered(true)
    }

    /// Returns the intersection result as a compact Tuple sketch.
    ///
    /// # Panics
    ///
    /// Panics if called before the first [`update`](Self::update).
    pub fn result_with_ordered(&self, ordered: bool) -> CompactTupleSketch<S>
    where
        S: Clone,
    {
        assert!(
            self.is_valid,
            "TupleIntersection::result() called before first update()"
        );
        let mut entries: Vec<(u64, S)> = self
            .table
            .iter()
            .map(|(hash, summary)| (hash, summary.clone()))
            .collect();
        if ordered {
            entries.sort_unstable_by_key(|(hash, _)| *hash);
        }
        CompactTupleSketch::from_parts(
            entries,
            self.table.theta(),
            self.table.seed_hash(),
            ordered,
            self.table.is_empty(),
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

        let result = intersection.result();
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

        let result = intersection.result();
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
            sorted_entries(&a_then_b.result()),
            sorted_entries(&b_then_a.result())
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

        assert_eq!(intersection.result().num_retained(), 500);
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

        let result = intersection.result();
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

        let result = intersection.result();
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

        let result = intersection.result();
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
        let _ = intersection.result();
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

        let result = intersection.result();
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
