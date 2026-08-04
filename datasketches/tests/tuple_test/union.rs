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
use datasketches::error::ErrorKind;
use datasketches::tuple::DefaultUnionPolicy;
use datasketches::tuple::SummaryCombinePolicy;
use datasketches::tuple::SummaryPolicy;
use datasketches::tuple::TupleUnionBuilder;

use super::default_tuple_sketch_builder;
use super::tuple_sketch_with_range;

fn default_union_builder() -> TupleUnionBuilder<DefaultUnionPolicy<u64>> {
    TupleUnionBuilder::new(DefaultUnionPolicy::<u64>::default())
}

#[test]
fn union_combines_overlapping_summaries() {
    let mut a = default_tuple_sketch_builder().build();
    a.update("shared", 3u64);
    a.update("only_a", 1u64);
    let mut b = default_tuple_sketch_builder().build();
    b.update("shared", 4u64);
    b.update("only_b", 1u64);

    let mut union = default_union_builder().build();
    union.update(&a).unwrap();
    union.update(&b).unwrap();
    let result = union.to_sketch(true);

    let mut summaries: Vec<u64> = result.iter().map(|(_, &summary)| summary).collect();
    summaries.sort_unstable();
    assert_eq!(result.num_retained(), 3);
    assert_eq!(summaries, [1, 1, 7]);
}

#[test]
fn accepts_mutable_and_compact_inputs() {
    let a = tuple_sketch_with_range(0, 500);
    let b = tuple_sketch_with_range(250, 500);

    let mut union = default_union_builder().build();
    union.update(&a).unwrap();
    union.update(&b.compact(true)).unwrap();

    assert_eq!(union.to_sketch(true).num_retained(), 750);
}

#[test]
fn reset_restores_the_initial_empty_state() {
    let input = tuple_sketch_with_range(0, 100);
    let mut union = default_union_builder().build();

    assert!(union.to_sketch(true).is_empty());
    union.update(&input).unwrap();
    assert!(!union.to_sketch(true).is_empty());

    union.reset();
    let result = union.to_sketch(true);
    assert!(result.is_empty());
    assert_eq!(result.estimate(), 0.0);
}

#[test]
fn non_empty_input_requires_the_union_seed() {
    let mut input = default_tuple_sketch_builder().seed(1).build();
    input.update("value", 1u64);
    let mut union = default_union_builder().seed(2).build();

    let err = union.update(&input).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidArgument);
}

#[derive(Debug, Default, Clone, Copy)]
struct MaxPolicy;

impl SummaryPolicy for MaxPolicy {
    type Summary = u64;

    fn create(&self) -> Self::Summary {
        0
    }
}

impl SummaryCombinePolicy for MaxPolicy {
    fn combine(&self, summary: &mut Self::Summary, other: &Self::Summary) {
        *summary = (*summary).max(*other);
    }
}

#[test]
fn custom_combine_policy_controls_overlapping_summaries() {
    let mut a = default_tuple_sketch_builder().build();
    a.update("shared", 3u64);
    let mut b = default_tuple_sketch_builder().build();
    b.update("shared", 9u64);

    let mut union = TupleUnionBuilder::new(MaxPolicy).build();
    union.update(&a).unwrap();
    union.update(&b).unwrap();

    assert_eq!(union.to_sketch(true).iter().next().unwrap().1, &9);
}

#[test]
fn result_ordering_follows_the_request() {
    let input = tuple_sketch_with_range(0, 100);
    let mut union = default_union_builder().build();
    union.update(&input).unwrap();

    assert!(union.to_sketch(true).is_ordered());
    assert!(!union.to_sketch(false).is_ordered());
}

#[test]
fn estimation_bounds_cover_the_true_union() {
    let mut a = default_tuple_sketch_builder().lg_k(8).build();
    let mut b = default_tuple_sketch_builder().lg_k(8).build();
    for value in 0..50_000 {
        a.update(value, 1u64);
    }
    for value in 25_000..75_000 {
        b.update(value, 1u64);
    }

    let mut union = default_union_builder().lg_k(8).build();
    union.update(&a).unwrap();
    union.update(&b).unwrap();
    let result = union.to_sketch(true);
    let lower = result.lower_bound(NumStdDev::Three);
    let upper = result.upper_bound(NumStdDev::Three);

    assert!(result.is_estimation_mode());
    assert!(
        lower <= 75_000.0 && 75_000.0 <= upper,
        "expected 75000 in [{lower}, {upper}]"
    );
}

#[test]
fn union_estimated_size_grows_with_updates() {
    let mut union = default_union_builder().build();
    assert_eq!(union.estimated_size(), 2120);

    let sketch = tuple_sketch_with_range(0, 1000);
    union.update(&sketch).unwrap();
    assert_eq!(union.estimated_size(), 131144);
}
