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
use datasketches::tuple::SummaryPolicy;
use datasketches::tuple::SummaryUpdatePolicy;
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

#[derive(Default)]
struct MaxPolicy;

impl SummaryPolicy for MaxPolicy {
    type Summary = u64;

    fn create(&self) -> Self::Summary {
        0
    }
}

impl SummaryUpdatePolicy<u64> for MaxPolicy {
    fn update(&self, summary: &mut Self::Summary, value: u64) {
        *summary = (*summary).max(value);
    }
}

#[test]
fn custom_update_policy_controls_retained_summaries() {
    let mut sketch = TupleSketchBuilder::new(MaxPolicy).build();
    sketch.update("key", 3);
    sketch.update("key", 9);
    sketch.update("key", 5);

    assert_eq!(sketch.num_retained(), 1);
    assert_eq!(sketch.iter().next().unwrap().1, &9);
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
