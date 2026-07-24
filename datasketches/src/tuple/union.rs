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

//! Tuple sketch union.
//!
//! [`TupleUnion`] computes the union (set OR) of any number of Tuple sketches. It reuses the raw
//! union state machine (`RawThetaUnion`) that also drives the Theta union; the only Tuple-specific
//! behavior is that when an incoming key already exists in the union, the two summaries are
//! combined with a [`SummaryCombinePolicy`] instead of one being dropped.

use crate::common::ResizeFactor;
use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::thetacommon::constants::DEFAULT_LG_K;
use crate::thetacommon::constants::MAX_LG_K;
use crate::thetacommon::constants::MIN_LG_K;
use crate::thetacommon::union::RawThetaUnion;
use crate::thetacommon::union::RawThetaUnionPolicy;
use crate::tuple::hash_table::TupleEntry;
use crate::tuple::policy::SummaryCombinePolicy;
use crate::tuple::sketch::CompactTupleSketch;
use crate::tuple::sketch::TupleSketchView;

/// Adapts a [`SummaryCombinePolicy`] to the raw union's entry-merge policy.
#[derive(Debug)]
struct CombinePolicyAdapter<P>(P);

impl<P> RawThetaUnionPolicy<TupleEntry<P::Summary>> for CombinePolicyAdapter<P>
where
    P: SummaryCombinePolicy,
{
    fn merge(&self, existing: &mut TupleEntry<P::Summary>, incoming: TupleEntry<P::Summary>) {
        self.0.combine(existing.summary_mut(), incoming.summary());
    }
}

/// Union (set OR) of Tuple sketches.
///
/// `P` is the [`SummaryCombinePolicy`] applied when a key is present in more than one input. For
/// additive summaries the built-in
/// [`DefaultUnionPolicy`](crate::tuple::DefaultUnionPolicy) can be used.
///
/// # Examples
///
/// ```
/// # use datasketches::tuple::{DefaultUnionPolicy, DefaultUpdatePolicy, TupleSketchBuilder, TupleUnionBuilder};
/// let update_policy = DefaultUpdatePolicy::<u64>::default();
/// let mut a = TupleSketchBuilder::new(update_policy).build();
/// a.update("apple", 1);
/// a.update("banana", 1);
///
/// let mut b = TupleSketchBuilder::new(update_policy).build();
/// b.update("banana", 1);
/// b.update("cherry", 1);
///
/// let mut union = TupleUnionBuilder::new(DefaultUnionPolicy::<u64>::default()).build();
/// union.update(&a).unwrap();
/// union.update(&b).unwrap();
///
/// let result = union.to_sketch(true);
/// assert_eq!(result.num_retained(), 3); // apple, banana, cherry
/// ```
#[derive(Debug)]
pub struct TupleUnion<P>
where
    P: SummaryCombinePolicy,
{
    raw: RawThetaUnion<TupleEntry<P::Summary>, CombinePolicyAdapter<P>>,
}

impl<P> TupleUnion<P>
where
    P: SummaryCombinePolicy,
{
    /// Merges a sketch into the union.
    ///
    /// Accepts either an [`TupleSketch`](crate::tuple::TupleSketch) or a
    /// [`CompactTupleSketch`] through the shared [`TupleSketchView`] trait. Keys present in both
    /// the running union and `sketch` have their summaries combined via the union policy.
    ///
    /// # Errors
    ///
    /// Returns an error if `sketch` was built with a different seed than this union (its seed hash
    /// does not match).
    pub fn update<V>(&mut self, sketch: &V) -> Result<(), Error>
    where
        V: TupleSketchView<P::Summary>,
    {
        self.raw.update(sketch)
    }

    /// Returns the union as a [`CompactTupleSketch`].
    ///
    /// If `ordered` is true, retained entries are sorted ascending by hash.
    pub fn to_sketch(&self, ordered: bool) -> CompactTupleSketch<P::Summary>
    where
        P::Summary: Clone,
    {
        let result = self.raw.to_compact_parts(ordered);
        CompactTupleSketch::from_parts(
            result.entries,
            result.theta,
            result.seed_hash,
            result.ordered,
            result.empty,
        )
    }

    /// Resets the union to its initial empty state.
    pub fn reset(&mut self) {
        self.raw.reset();
    }
}

/// Builder for [`TupleUnion`].
///
/// Every builder carries a concrete [`SummaryCombinePolicy`]. Use
/// [`DefaultUnionPolicy`](crate::tuple::DefaultUnionPolicy) for additive summaries, or supply a
/// custom combine policy.
#[derive(Debug)]
pub struct TupleUnionBuilder<P>
where
    P: SummaryCombinePolicy,
{
    lg_k: u8,
    resize_factor: ResizeFactor,
    sampling_probability: f32,
    seed: u64,
    policy: P,
}

impl<P> TupleUnionBuilder<P>
where
    P: SummaryCombinePolicy,
{
    /// Creates a builder with the given combine policy.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tuple::DefaultUnionPolicy;
    /// use datasketches::tuple::TupleUnionBuilder;
    ///
    /// let union = TupleUnionBuilder::new(DefaultUnionPolicy::<u64>::default())
    ///     .lg_k(12)
    ///     .build();
    /// ```
    pub fn new(policy: P) -> Self {
        Self {
            lg_k: DEFAULT_LG_K,
            resize_factor: ResizeFactor::X8,
            sampling_probability: 1.0,
            seed: DEFAULT_UPDATE_SEED,
            policy,
        }
    }

    /// Sets lg_k (log2 of the nominal size k).
    ///
    /// # Panics
    ///
    /// Panics if lg_k is not in range [5, 26].
    pub fn lg_k(mut self, lg_k: u8) -> Self {
        assert!(
            (MIN_LG_K..=MAX_LG_K).contains(&lg_k),
            "lg_k must be in [{MIN_LG_K}, {MAX_LG_K}], got {lg_k}"
        );
        self.lg_k = lg_k;
        self
    }

    /// Sets the resize factor.
    pub fn resize_factor(mut self, factor: ResizeFactor) -> Self {
        self.resize_factor = factor;
        self
    }

    /// Sets the sampling probability p.
    ///
    /// # Panics
    ///
    /// Panics if p is not in range `(0.0, 1.0]`.
    pub fn sampling_probability(mut self, probability: f32) -> Self {
        assert!(
            (0.0..=1.0).contains(&probability) && probability > 0.0,
            "sampling_probability must be in (0.0, 1.0], got {probability}"
        );
        self.sampling_probability = probability;
        self
    }

    /// Sets the hash seed.
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Builds the [`TupleUnion`].
    pub fn build(self) -> TupleUnion<P> {
        TupleUnion {
            raw: RawThetaUnion::new(
                self.lg_k,
                self.resize_factor,
                self.sampling_probability,
                self.seed,
                CombinePolicyAdapter(self.policy),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::NumStdDev;
    use crate::error::ErrorKind;
    use crate::tuple::DefaultUnionPolicy;
    use crate::tuple::DefaultUpdatePolicy;
    use crate::tuple::SummaryPolicy;
    use crate::tuple::TupleSketchBuilder;

    fn default_union_builder() -> TupleUnionBuilder<DefaultUnionPolicy<u64>> {
        TupleUnionBuilder::new(DefaultUnionPolicy::<u64>::default())
    }

    fn default_sketch_builder() -> TupleSketchBuilder<DefaultUpdatePolicy<u64>> {
        TupleSketchBuilder::new(DefaultUpdatePolicy::<u64>::default())
    }

    fn sorted_entries(sketch: &CompactTupleSketch<u64>) -> Vec<(u64, u64)> {
        let mut entries: Vec<(u64, u64)> = sketch.iter().map(|(h, &s)| (h, s)).collect();
        entries.sort_unstable();
        entries
    }

    #[test]
    fn union_of_disjoint_sketches_sums_cardinality() {
        let mut a = default_sketch_builder().build();
        for i in 0..1000 {
            a.update(i, 1u64);
        }
        let mut b = default_sketch_builder().build();
        for i in 1000..2000 {
            b.update(i, 1u64);
        }

        let mut union = default_union_builder().build();
        union.update(&a).unwrap();
        union.update(&b).unwrap();

        let result = union.to_sketch(true);
        // 2000 distinct keys < k (4096), so the union is in exact mode.
        assert!(!result.is_estimation_mode());
        assert_eq!(result.num_retained(), 2000);
        assert_eq!(result.estimate(), 2000.0);
        // Every summary stays at 1 because the inputs are disjoint.
        assert!(result.iter().all(|(_, &s)| s == 1));
    }

    #[test]
    fn union_combines_overlapping_summaries() {
        let mut a = default_sketch_builder().build();
        a.update("shared", 3u64);
        a.update("only_a", 1u64);
        let mut b = default_sketch_builder().build();
        b.update("shared", 4u64);
        b.update("only_b", 1u64);

        let mut union = default_union_builder().build();
        union.update(&a).unwrap();
        union.update(&b).unwrap();

        let result = union.to_sketch(true);
        assert_eq!(result.num_retained(), 3);

        // The shared key's summary is the default-policy sum (3 + 4 = 7); the rest are 1.
        let summaries: Vec<u64> = sorted_entries(&result)
            .into_iter()
            .map(|(_, s)| s)
            .collect();
        let mut sorted = summaries.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, vec![1, 1, 7]);
    }

    #[test]
    fn union_result_is_order_independent() {
        let mut a = default_sketch_builder().build();
        a.update("shared", 3u64);
        a.update("only_a", 5u64);
        let mut b = default_sketch_builder().build();
        b.update("shared", 4u64);
        b.update("only_b", 6u64);

        let mut a_then_b = default_union_builder().build();
        a_then_b.update(&a).unwrap();
        a_then_b.update(&b).unwrap();

        let mut b_then_a = default_union_builder().build();
        b_then_a.update(&b).unwrap();
        b_then_a.update(&a).unwrap();

        assert_eq!(
            sorted_entries(&a_then_b.to_sketch(true)),
            sorted_entries(&b_then_a.to_sketch(true))
        );
    }

    #[test]
    fn union_accepts_updatable_and_compact_inputs() {
        let mut a = default_sketch_builder().build();
        for i in 0..500 {
            a.update(i, 1u64);
        }
        let mut b = default_sketch_builder().build();
        for i in 250..750 {
            b.update(i, 1u64);
        }
        let b_compact = b.compact(true);

        let mut union = default_union_builder().build();
        union.update(&a).unwrap(); // updatable input
        union.update(&b_compact).unwrap(); // compact input

        let result = union.to_sketch(true);
        assert_eq!(result.num_retained(), 750); // 0..750 distinct
    }

    #[test]
    fn union_of_empty_inputs_is_empty() {
        let empty = default_sketch_builder().build();

        let mut union = default_union_builder().build();
        union.update(&empty).unwrap();

        let result = union.to_sketch(true);
        assert!(result.is_empty());
        assert!(result.is_ordered());
        assert_eq!(result.estimate(), 0.0);
        assert_eq!(result.num_retained(), 0);
    }

    #[test]
    fn union_never_updated_is_empty() {
        let union = default_union_builder().build();
        let result = union.to_sketch(true);
        assert!(result.is_empty());
        assert_eq!(result.estimate(), 0.0);
    }

    #[test]
    fn union_rejects_seed_mismatch() {
        let mut a = default_sketch_builder().seed(1).build();
        a.update("k", 1u64);

        let mut union = default_union_builder().seed(2).build();
        let err = union.update(&a).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidArgument);
    }

    #[test]
    fn union_in_estimation_mode_estimates_within_bounds() {
        let mut a = default_sketch_builder().lg_k(8).build();
        for i in 0..50000 {
            a.update(i, 1u64);
        }
        let mut b = default_sketch_builder().lg_k(8).build();
        for i in 25000..75000 {
            b.update(i, 1u64);
        }

        let mut union = default_union_builder().lg_k(8).build();
        union.update(&a).unwrap();
        union.update(&b).unwrap();

        let result = union.to_sketch(true);
        assert!(result.is_estimation_mode());
        // Union of 0..75000 distinct keys.
        let lower = result.lower_bound(NumStdDev::Three);
        let upper = result.upper_bound(NumStdDev::Three);
        assert!(
            lower <= 75000.0 && 75000.0 <= upper,
            "expected 75000 in [{lower}, {upper}]"
        );
    }

    #[derive(Debug, Default, Clone, Copy)]
    struct MaxUnionPolicy;

    impl SummaryPolicy for MaxUnionPolicy {
        type Summary = u64;

        fn create(&self) -> Self::Summary {
            0
        }
    }

    impl SummaryCombinePolicy for MaxUnionPolicy {
        fn combine(&self, summary: &mut Self::Summary, other: &Self::Summary) {
            *summary = (*summary).max(*other);
        }
    }

    #[test]
    fn union_uses_custom_combine_policy() {
        let mut a = default_sketch_builder().build();
        a.update("shared", 3u64);
        let mut b = default_sketch_builder().build();
        b.update("shared", 9u64);

        let mut union = TupleUnionBuilder::new(MaxUnionPolicy).build();
        union.update(&a).unwrap();
        union.update(&b).unwrap();

        let result = union.to_sketch(true);
        assert_eq!(result.num_retained(), 1);
        assert_eq!(result.iter().next().unwrap().1, &9); // max(3, 9)
    }

    #[test]
    fn union_reset_clears_state() {
        let mut a = default_sketch_builder().build();
        for i in 0..100 {
            a.update(i, 1u64);
        }

        let mut union = default_union_builder().build();
        union.update(&a).unwrap();
        assert!(!union.to_sketch(true).is_empty());

        union.reset();
        assert!(union.to_sketch(true).is_empty());
    }
}
