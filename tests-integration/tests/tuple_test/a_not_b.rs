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
use datasketches::tuple::CompactTupleSketch;
use datasketches::tuple::TupleANotB;
use googletest::assert_that;
use googletest::prelude::all;
use googletest::prelude::ge;
use googletest::prelude::le;
use tests_integration::MAX_THETA;

use crate::default_tuple_sketch_builder;
use crate::tuple_sketch_with_range;

fn sorted_entries(sketch: &CompactTupleSketch<u64>) -> Vec<(u64, u64)> {
    let mut entries: Vec<_> = sketch
        .iter()
        .map(|entry| (entry.hash(), *entry.summary()))
        .collect();
    entries.sort_unstable();
    entries
}

#[test]
fn difference_keeps_only_a_summaries() {
    let mut a = default_tuple_sketch_builder().build().unwrap();
    a.update("shared", 3u64);
    a.update("only_a", 5u64);
    let mut b = default_tuple_sketch_builder().build().unwrap();
    b.update("shared", 9u64);
    b.update("only_b", 7u64);

    let result = TupleANotB::default().compute(&a, &b, true).unwrap();

    assert_eq!(result.num_retained(), 1);
    assert_eq!(result.estimate(), 1.0);
    assert_eq!(result.iter().next().unwrap().summary(), &5);
}

#[test]
fn accepts_mutable_and_compact_inputs() {
    let a = tuple_sketch_with_range(0, 1000);
    let b = tuple_sketch_with_range(500, 1000);
    let op = TupleANotB::default();

    let mutable_result = op.compute(&a, &b, true).unwrap();
    let compact_result = op
        .compute(&a.compact(true), &b.compact(false), true)
        .unwrap();

    assert_eq!(
        sorted_entries(&mutable_result),
        sorted_entries(&compact_result)
    );
    assert_eq!(mutable_result.num_retained(), 500);
}

#[test]
fn input_and_result_ordering_preserve_entries() {
    let mut a = default_tuple_sketch_builder().lg_k(8).build().unwrap();
    let mut b = default_tuple_sketch_builder().lg_k(8).build().unwrap();
    for value in 0..20_000 {
        a.update(value, 1u64);
    }
    for value in 10_000..30_000 {
        b.update(value, 1u64);
    }

    let op = TupleANotB::default();
    let from_unordered_inputs = op.compute(&a, &b, true).unwrap();
    let from_ordered_inputs = op
        .compute(&a.compact(true), &b.compact(true), true)
        .unwrap();
    let unordered_result = op.compute(&a, &b, false).unwrap();

    assert!(from_unordered_inputs.is_ordered());
    assert!(from_ordered_inputs.is_ordered());
    assert!(!unordered_result.is_ordered());
    assert_eq!(
        from_unordered_inputs.theta64(),
        from_ordered_inputs.theta64()
    );
    assert_eq!(from_unordered_inputs.theta64(), unordered_result.theta64());
    assert_eq!(
        sorted_entries(&from_unordered_inputs),
        sorted_entries(&from_ordered_inputs)
    );
    assert_eq!(
        sorted_entries(&from_unordered_inputs),
        sorted_entries(&unordered_result)
    );
}

#[test]
fn empty_inputs_do_not_impose_a_seed() {
    let empty_other_seed = default_tuple_sketch_builder()
        .sampling_probability(0.5)
        .seed(2)
        .build()
        .unwrap();
    let non_empty = tuple_sketch_with_range(0, 10);
    let op = TupleANotB::default();

    let result = op.compute(&empty_other_seed, &non_empty, true).unwrap();
    assert!(result.is_empty());
    assert_eq!(result.theta64(), MAX_THETA);
    assert!(!result.is_estimation_mode());
    assert_eq!(
        op.compute(&non_empty, &empty_other_seed, true)
            .unwrap()
            .num_retained(),
        10
    );
}

#[test]
fn non_empty_inputs_require_the_operator_seed() {
    let mut other_seed = default_tuple_sketch_builder().seed(2).build().unwrap();
    other_seed.update("value", 1u64);
    let empty = default_tuple_sketch_builder().build().unwrap();
    let good = tuple_sketch_with_range(0, 10);
    let op = TupleANotB::default();

    let err = op.compute(&other_seed, &empty, true).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidArgument);

    let err = op.compute(&good, &other_seed, true).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn empty_b_preserves_logically_non_empty_a_without_retained_entries() {
    let screened_value = (0u64..)
        .find(|candidate| {
            let mut sketch = default_tuple_sketch_builder()
                .sampling_probability(0.001)
                .build()
                .unwrap();
            sketch.update(*candidate, 1u64);
            !sketch.is_empty() && sketch.num_retained() == 0
        })
        .expect("failed to find a value screened out by the sampling theta");

    let mut a = default_tuple_sketch_builder()
        .sampling_probability(0.001)
        .build()
        .unwrap();
    a.update(screened_value, 1u64);
    let empty_b = default_tuple_sketch_builder().seed(999).build().unwrap();

    let result = TupleANotB::default().compute(&a, &empty_b, true).unwrap();

    assert!(!result.is_empty());
    assert_eq!(result.num_retained(), 0);
    assert_eq!(result.theta64(), a.theta64());
}

#[test]
fn estimation_bounds_cover_the_true_difference() {
    let mut a = default_tuple_sketch_builder().lg_k(8).build().unwrap();
    let mut b = default_tuple_sketch_builder().lg_k(8).build().unwrap();
    for value in 0..50_000 {
        a.update(value, 1u64);
    }
    for value in 25_000..75_000 {
        b.update(value, 1u64);
    }

    let result = TupleANotB::default().compute(&a, &b, true).unwrap();
    let lower = result.lower_bound(NumStdDev::Three);
    let upper = result.upper_bound(NumStdDev::Three);

    assert!(result.is_estimation_mode());
    assert_that!(25_000.0, all!(ge(lower), le(upper)));
}
