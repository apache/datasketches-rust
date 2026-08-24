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
use datasketches::tuple::SummaryCombinePolicy;
use datasketches::tuple::SummaryPolicy;
use datasketches::tuple::TupleIntersection;

use super::default_tuple_sketch_builder;
use super::tuple_sketch_with_range;

#[derive(Debug, Default, Clone, Copy)]
struct SumPolicy;

impl SummaryPolicy for SumPolicy {
    type Summary = u64;

    fn create(&self) -> Self::Summary {
        0
    }
}

impl SummaryCombinePolicy for SumPolicy {
    fn combine(&self, summary: &mut Self::Summary, other: &Self::Summary) {
        *summary += *other;
    }
}

#[test]
fn has_result_tracks_the_first_update() {
    let sketch = tuple_sketch_with_range(0, 10);
    let mut intersection = TupleIntersection::new(SumPolicy);

    assert!(!intersection.has_result());
    intersection.update(&sketch).unwrap();
    assert!(intersection.has_result());
    assert_eq!(intersection.to_sketch(true).unwrap().num_retained(), 10);
}

#[test]
fn result_before_first_update_returns_none() {
    let intersection = TupleIntersection::<IdentityPolicy>::new();
    assert!(intersection.to_sketch(true).is_none());
}

#[test]
fn overlap_combines_summaries() {
    let mut a = default_tuple_sketch_builder().build();
    a.update("shared", 3u64);
    a.update("only_a", 100u64);
    let mut b = default_tuple_sketch_builder().build();
    b.update("shared", 4u64);
    b.update("only_b", 200u64);

    let mut intersection = TupleIntersection::new(SumPolicy);
    intersection.update(&a).unwrap();
    intersection.update(&b).unwrap();
    let result = intersection.to_sketch(true).unwrap();

    assert_eq!(result.num_retained(), 1);
    assert_eq!(result.iter().next().unwrap().1, &7);
}

#[test]
fn accepts_mutable_and_compact_inputs() {
    let a = tuple_sketch_with_range(0, 1000);
    let b = tuple_sketch_with_range(500, 1000);

    let mut intersection = TupleIntersection::new(SumPolicy);
    intersection.update(&a).unwrap();
    intersection.update(&b.compact(true)).unwrap();

    assert_eq!(intersection.to_sketch(true).unwrap().num_retained(), 500);
}

#[test]
fn disjoint_result_is_terminally_empty() {
    let a = tuple_sketch_with_range(0, 100);
    let b = tuple_sketch_with_range(100, 100);
    let later = tuple_sketch_with_range(0, 100);

    let mut intersection = TupleIntersection::new(SumPolicy);
    intersection.update(&a).unwrap();
    intersection.update(&b).unwrap();
    intersection.update(&later).unwrap();

    let result = intersection.to_sketch(true).unwrap();
    assert!(result.is_empty());
    assert_eq!(result.num_retained(), 0);
}

#[test]
fn logically_non_empty_input_without_retained_entries_is_preserved() {
    let screened_value = (0u64..)
        .find(|candidate| {
            let mut sketch = default_tuple_sketch_builder()
                .sampling_probability(0.001)
                .build();
            sketch.update(*candidate, 1u64);
            !sketch.is_empty() && sketch.num_retained() == 0
        })
        .expect("failed to find a value screened out by the sampling theta");

    let mut sketch = default_tuple_sketch_builder()
        .sampling_probability(0.001)
        .build();
    sketch.update(screened_value, 1u64);

    let mut intersection = TupleIntersection::new(SumPolicy);
    intersection.update(&sketch).unwrap();
    let result = intersection.to_sketch(true).unwrap();

    assert!(!result.is_empty());
    assert_eq!(result.num_retained(), 0);
    assert_eq!(result.theta64(), sketch.theta64());
}

#[test]
fn only_non_empty_inputs_require_the_operator_seed() {
    let empty_other_seed = default_tuple_sketch_builder().seed(2).build();
    let mut non_empty_other_seed = default_tuple_sketch_builder().seed(2).build();
    non_empty_other_seed.update("value", 1u64);

    let mut intersection = TupleIntersection::with_seed(SumPolicy, 1);
    intersection.update(&empty_other_seed).unwrap();

    let mut intersection = TupleIntersection::with_seed(SumPolicy, 1);
    let err = intersection.update(&non_empty_other_seed).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn result_ordering_follows_the_request() {
    let input = tuple_sketch_with_range(0, 100);
    let mut intersection = TupleIntersection::new(SumPolicy);
    intersection.update(&input).unwrap();

    assert!(intersection.to_sketch(true).unwrap().is_ordered());
    assert!(!intersection.to_sketch(false).unwrap().is_ordered());
}

#[test]
fn estimation_bounds_cover_the_true_intersection() {
    let mut a = default_tuple_sketch_builder().lg_k(8).build();
    let mut b = default_tuple_sketch_builder().lg_k(8).build();
    for value in 0..50_000 {
        a.update(value, 1u64);
    }
    for value in 25_000..75_000 {
        b.update(value, 1u64);
    }

    let mut intersection = TupleIntersection::new(SumPolicy);
    intersection.update(&a).unwrap();
    intersection.update(&b).unwrap();
    let result = intersection.to_sketch(true).unwrap();
    let lower = result.lower_bound(NumStdDev::Three);
    let upper = result.upper_bound(NumStdDev::Three);

    assert!(result.is_estimation_mode());
    assert!(
        lower <= 25_000.0 && 25_000.0 <= upper,
        "expected 25000 in [{lower}, {upper}]"
    );
}

#[test]
fn intersection_estimated_size_grows_with_updates() {
    let mut intersection = TupleIntersection::new(SumPolicy);
    assert_eq!(intersection.estimated_size(), 72);

    let sketch = tuple_sketch_with_range(0, 1000);
    intersection.update(&sketch).unwrap();
    assert_eq!(intersection.estimated_size(), 32840);
}
