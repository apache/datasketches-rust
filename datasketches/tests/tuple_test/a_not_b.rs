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

//! Behavioral tests for the Tuple a-not-B (set difference) operator.
//!
//! The result of `a and not b` retains the keys of `a` that are absent from `b`, keeping the
//! summaries from `a`. These tests use a `u64` summary with the default additive update policy,
//! so the distinct-count behavior matches a plain Theta a-not-B.

use datasketches::tuple::CompactTupleSketch;
use datasketches::tuple::TupleAnotB;

use super::default_tuple_sketch_builder;
use super::tuple_sketch_with_range;

#[test]
fn test_basic_difference_keeps_summaries_from_a() {
    let mut a = default_tuple_sketch_builder().build();
    a.update("shared", 3u64);
    a.update("only_a", 5u64);
    let mut b = default_tuple_sketch_builder().build();
    b.update("shared", 9u64);
    b.update("only_b", 7u64);

    let a_not_b = TupleAnotB::default();
    let r = a_not_b.compute(&a, &b, true).unwrap();

    // "shared" is subtracted; "only_a" survives with A's summary.
    assert_eq!(r.num_retained(), 1);
    assert_eq!(r.iter().next().unwrap().1, &5);
    assert!(!r.is_estimation_mode());
    assert_eq!(r.estimate(), 1.0);
}

#[test]
fn test_accepts_updatable_and_compact_inputs() {
    let a = tuple_sketch_with_range(0, 1000);
    let b = tuple_sketch_with_range(500, 1000);

    let a_not_b = TupleAnotB::default();
    let r = a_not_b.compute(&a.compact(true), &b, true).unwrap();
    assert_eq!(r.num_retained(), 500);

    let r = a_not_b.compute(&a, &b.compact(false), true).unwrap();
    assert_eq!(r.num_retained(), 500);
}

#[test]
fn test_seed_mismatch_returns_error() {
    let mut one_other_seed = default_tuple_sketch_builder().seed(2).build();
    one_other_seed.update("value", 1u64);
    let good = tuple_sketch_with_range(0, 10);

    let a_not_b = TupleAnotB::with_seed(1);
    assert!(a_not_b.compute(&one_other_seed, &good, true).is_err());
    assert!(a_not_b.compute(&good, &one_other_seed, true).is_err());
}

#[test]
fn test_seed_mismatch_ignored_for_empty_inputs() {
    // Empty inputs carry no keys, so their seeds are not validated.
    let empty_other_seed = default_tuple_sketch_builder().seed(2).build();
    let good = tuple_sketch_with_range(0, 10);

    let a_not_b = TupleAnotB::default();

    let r = a_not_b.compute(&empty_other_seed, &good, true).unwrap();
    assert!(r.is_empty());

    let r = a_not_b.compute(&good, &empty_other_seed, true).unwrap();
    assert_eq!(r.num_retained(), 10);
}

#[test]
fn test_empty_a_returns_empty() {
    let empty = default_tuple_sketch_builder().build();
    let b = tuple_sketch_with_range(0, 1000);

    let a_not_b = TupleAnotB::default();
    let r = a_not_b.compute(&empty, &b, true).unwrap();

    assert!(r.is_empty());
    assert_eq!(r.num_retained(), 0);
    assert_eq!(r.estimate(), 0.0);
}

#[test]
fn test_empty_b_returns_a() {
    let a = tuple_sketch_with_range(0, 1000);
    let empty = default_tuple_sketch_builder().build();

    let a_not_b = TupleAnotB::default();
    let r = a_not_b.compute(&a, &empty, true).unwrap();

    assert_eq!(r.num_retained(), 1000);
    assert_eq!(r.estimate(), 1000.0);
}

#[test]
fn test_exact_partial_overlap_unordered() {
    let a = tuple_sketch_with_range(0, 1000);
    let b = tuple_sketch_with_range(500, 1000);

    let a_not_b = TupleAnotB::default();
    let r = a_not_b.compute(&a, &b, true).unwrap();

    // Keys 0..500 survive (exact mode).
    assert!(!r.is_empty());
    assert!(!r.is_estimation_mode());
    assert_eq!(r.num_retained(), 500);
    assert_eq!(r.estimate(), 500.0);
}

#[test]
fn test_exact_partial_overlap_ordered() {
    let a = tuple_sketch_with_range(0, 1000);
    let b = tuple_sketch_with_range(500, 1000);

    let a_not_b = TupleAnotB::default();
    let r = a_not_b
        .compute(&a.compact(true), &b.compact(true), true)
        .unwrap();

    assert!(!r.is_empty());
    assert!(!r.is_estimation_mode());
    assert_eq!(r.num_retained(), 500);
    assert_eq!(r.estimate(), 500.0);
}

#[test]
fn test_exact_disjoint_returns_a() {
    let a = tuple_sketch_with_range(0, 1000);
    let b = tuple_sketch_with_range(1000, 1000);

    let a_not_b = TupleAnotB::default();
    let r = a_not_b.compute(&a, &b, true).unwrap();

    assert!(!r.is_estimation_mode());
    assert_eq!(r.num_retained(), 1000);
    assert_eq!(r.estimate(), 1000.0);
}

#[test]
fn test_exact_superset_b_returns_empty() {
    let a = tuple_sketch_with_range(0, 1000);
    let b = tuple_sketch_with_range(0, 2000);

    let a_not_b = TupleAnotB::default();
    let r = a_not_b.compute(&a, &b, true).unwrap();

    assert!(r.is_empty());
    assert_eq!(r.num_retained(), 0);
    assert_eq!(r.estimate(), 0.0);
}

#[test]
fn test_result_ordering() {
    let mut a = default_tuple_sketch_builder().build();
    for i in 0..64 {
        a.update(i, 1u64);
    }
    let empty = default_tuple_sketch_builder().build();

    let a_not_b = TupleAnotB::default();

    let r = a_not_b.compute(&a, &empty, true).unwrap();
    assert!(r.is_ordered());

    let r = a_not_b.compute(&a, &empty, false).unwrap();
    assert!(!r.is_ordered());
}

#[test]
fn test_estimation_partial_overlap_unordered() {
    let a = tuple_sketch_with_range(0, 10000);
    let b = tuple_sketch_with_range(5000, 10000);

    let a_not_b = TupleAnotB::default();
    let r = a_not_b.compute(&a, &b, true).unwrap();

    // True difference size is 5000 (keys 0..5000).
    assert!(!r.is_empty());
    assert!(r.is_estimation_mode());
    assert!((r.estimate() - 5000.0).abs() <= 5000.0 * 0.02);
}

#[test]
fn test_estimation_partial_overlap_ordered() {
    let a = tuple_sketch_with_range(0, 10000);
    let b = tuple_sketch_with_range(5000, 10000);

    let a_not_b = TupleAnotB::default();
    let r = a_not_b
        .compute(&a.compact(true), &b.compact(true), true)
        .unwrap();

    assert!(!r.is_empty());
    assert!(r.is_estimation_mode());
    assert!((r.estimate() - 5000.0).abs() <= 5000.0 * 0.02);
}

#[test]
fn test_estimation_partial_overlap_deserialized_compact() {
    let a = tuple_sketch_with_range(0, 10000);
    let b = tuple_sketch_with_range(5000, 10000);
    let c1 = CompactTupleSketch::<u64>::deserialize(&a.compact(true).serialize()).unwrap();
    let c2 = CompactTupleSketch::<u64>::deserialize(&b.compact(true).serialize()).unwrap();

    let a_not_b = TupleAnotB::default();
    let r = a_not_b.compute(&c1, &c2, true).unwrap();

    assert!(!r.is_empty());
    assert!(r.is_estimation_mode());
    assert!((r.estimate() - 5000.0).abs() <= 5000.0 * 0.02);
}

#[test]
fn test_estimation_disjoint_returns_a() {
    let a = tuple_sketch_with_range(0, 10000);
    let b = tuple_sketch_with_range(10000, 10000);

    let a_not_b = TupleAnotB::default();
    let r = a_not_b.compute(&a, &b, true).unwrap();

    assert!(!r.is_empty());
    assert!(r.is_estimation_mode());
    assert!((r.estimate() - 10000.0).abs() <= 10000.0 * 0.02);
}
