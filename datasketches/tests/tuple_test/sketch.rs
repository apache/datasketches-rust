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

use datasketches::common::NumStdDev;
use datasketches::hash_value;
use datasketches::tuple::CompactTupleSketch;
use datasketches::tuple::DefaultUpdatePolicy;
use datasketches::tuple::SummaryPolicy;
use datasketches::tuple::SummaryUpdatePolicy;
use datasketches::tuple::TupleSketch;
use datasketches::tuple::TupleSketchBuilder;

use super::default_tuple_sketch_builder;

#[test]
fn updates_distinct_keys_and_accumulates_summaries() {
    let mut sketch = default_tuple_sketch_builder().build();
    sketch.update("shared", 2u64);
    sketch.update("shared", 3u64);
    sketch.update("other", 7u64);

    assert_eq!(sketch.estimate(), 2.0);
    assert_eq!(sketch.num_retained(), 2);

    let mut summaries: Vec<u64> = sketch.iter().map(|(_, &summary)| summary).collect();
    summaries.sort_unstable();
    assert_eq!(summaries, [5, 7]);
}

#[test]
fn accepts_supported_hash_representations() {
    let mut sketch = default_tuple_sketch_builder().build();
    sketch.update("string", 1u64);
    sketch.update(42i64, 1u64);
    sketch.update(42u64, 1u64);
    sketch.update(hash_value::canonical_float::from_f64(5.0), 1u64);
    sketch.update(hash_value::canonical_float::from_f32(5.0), 1u64);
    sketch.update([1u8, 2, 3], 1u64);

    assert_eq!(sketch.estimate(), 4.0);
}

#[test]
fn default_update_policy_accepts_distinct_rhs_type() {
    let mut sketch = TupleSketchBuilder::new(DefaultUpdatePolicy::<String>::default()).build();
    sketch.update("key", "hello");
    sketch.update("key", " world");

    assert_eq!(sketch.iter().next().unwrap().1, "hello world");
}

struct ArraySumPolicy {
    num_values: usize,
}

impl SummaryPolicy for ArraySumPolicy {
    type Summary = Vec<f64>;

    fn create(&self) -> Self::Summary {
        vec![0.0; self.num_values]
    }
}

impl<U> SummaryUpdatePolicy<U> for ArraySumPolicy
where
    U: AsRef<[f64]>,
{
    fn update(&self, summary: &mut Self::Summary, value: U) {
        let value = value.as_ref();
        assert_eq!(value.len(), self.num_values);
        for (summary, value) in summary.iter_mut().zip(value) {
            *summary += value;
        }
    }
}

#[test]
fn custom_update_policy_accepts_multiple_value_representations() {
    let mut sketch = TupleSketchBuilder::new(ArraySumPolicy { num_values: 2 }).build();
    sketch.update("key", &[1.0, 2.0]);
    sketch.update("key", vec![3.0, 4.0]);

    assert_eq!(sketch.num_retained(), 1);
    assert_eq!(sketch.iter().next().unwrap().1.as_slice(), [4.0, 6.0]);
}

#[test]
fn trim_and_reset_update_public_state() {
    let mut sketch = default_tuple_sketch_builder().lg_k(5).build();
    for value in 0..1000 {
        sketch.update(value, 1u64);
    }

    sketch.trim();
    assert_eq!(sketch.num_retained(), 32);
    assert!(sketch.is_estimation_mode());

    sketch.reset();
    assert!(sketch.is_empty());
    assert_eq!(sketch.num_retained(), 0);
    assert_eq!(sketch.estimate(), 0.0);
    assert_eq!(sketch.theta(), 1.0);
    assert!(!sketch.is_estimation_mode());
}

#[test]
fn bounds_cover_exact_and_estimation_results() {
    let mut exact = default_tuple_sketch_builder().build();
    for value in 0..100 {
        exact.update(value, 1u64);
    }
    assert_eq!(exact.lower_bound(NumStdDev::One), 100.0);
    assert_eq!(exact.upper_bound(NumStdDev::Three), 100.0);

    let mut estimated = default_tuple_sketch_builder().lg_k(8).build();
    for value in 0..50_000 {
        estimated.update(value, 1u64);
    }
    let estimate = estimated.estimate();
    let lower_one = estimated.lower_bound(NumStdDev::One);
    let lower_three = estimated.lower_bound(NumStdDev::Three);
    let upper_one = estimated.upper_bound(NumStdDev::One);
    let upper_three = estimated.upper_bound(NumStdDev::Three);

    assert!(estimated.is_estimation_mode());
    assert!(lower_three <= lower_one);
    assert!(lower_one < estimate);
    assert!(estimate < upper_one);
    assert!(upper_one <= upper_three);
}

#[test]
fn empty_sampled_sketch_has_zero_bounds() {
    let sketch = default_tuple_sketch_builder()
        .sampling_probability(0.1)
        .build();

    assert!(sketch.is_empty());
    assert!(sketch.is_estimation_mode());
    assert_eq!(sketch.estimate(), 0.0);
    assert_eq!(sketch.lower_bound(NumStdDev::Three), 0.0);
    assert_eq!(sketch.upper_bound(NumStdDev::Three), 0.0);
}

fn sorted_entries<'a>(entries: impl Iterator<Item = (u64, &'a u64)>) -> Vec<(u64, u64)> {
    let mut entries: Vec<_> = entries.map(|(hash, &summary)| (hash, summary)).collect();
    entries.sort_unstable();
    entries
}

fn assert_compact_preserves_state(
    updatable: &TupleSketch<DefaultUpdatePolicy<u64>>,
    compact: &CompactTupleSketch<u64>,
    ordered: bool,
) {
    assert_eq!(compact.is_estimation_mode(), updatable.is_estimation_mode());
    assert_eq!(compact.theta64(), updatable.theta64());
    assert_eq!(compact.seed_hash(), updatable.seed_hash());
    assert_eq!(
        sorted_entries(compact.iter()),
        sorted_entries(updatable.iter())
    );
    assert_eq!(compact.estimate(), updatable.estimate());
    assert_eq!(compact.is_ordered(), ordered);
}

#[test]
fn compact_preserves_state_in_exact_and_estimation_modes() {
    for (lg_k, num_updates, expected_estimation_mode) in [(12, 2_000, false), (5, 5_000, true)] {
        let mut sketch = default_tuple_sketch_builder().lg_k(lg_k).build();
        for key in 0..num_updates {
            sketch.update(key, key + 1);
            sketch.update(key, 10u64);
        }
        assert_eq!(sketch.is_estimation_mode(), expected_estimation_mode);

        for ordered in [false, true] {
            let compact = sketch.compact(ordered);
            assert_compact_preserves_state(&sketch, &compact, ordered);
        }
    }
}

#[test]
fn compact_preserves_logical_non_empty_after_screened_update() {
    let screened_value = (0u64..)
        .find(|candidate| {
            let mut sketch = default_tuple_sketch_builder()
                .sampling_probability(0.5)
                .build();
            sketch.update(*candidate, 1u64);
            !sketch.is_empty() && sketch.num_retained() == 0
        })
        .expect("failed to find a value screened out by the sampling theta");

    let mut sketch = default_tuple_sketch_builder()
        .sampling_probability(0.5)
        .build();
    sketch.update(screened_value, 1u64);
    let compact = sketch.compact(false);

    assert!(!compact.is_empty());
    assert_eq!(compact.num_retained(), 0);
    assert_eq!(compact.theta64(), sketch.theta64());
}
