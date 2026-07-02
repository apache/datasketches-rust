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

//! Behavioral tests for the Tuple intersection, mirroring `theta_intersection_test.rs`.
//!
//! Unlike Theta, a Tuple intersection requires an explicit [`SummaryCombinePolicy`] for keys that
//! appear in more than one input. These tests use a `u64` summary and a summing policy, so the
//! distinct-count behavior matches the Theta intersection.

#![cfg(feature = "tuple")]

use datasketches::tuple::CompactTupleSketch;
use datasketches::tuple::PrimitiveSummarySerde;
use datasketches::tuple::SummaryCombinePolicy;
use datasketches::tuple::TupleIntersection;
use datasketches::tuple::UpdatableTupleSketch;

#[derive(Debug, Default, Clone, Copy)]
struct SumPolicy;

impl SummaryCombinePolicy<u64> for SumPolicy {
    fn combine(&self, summary: &mut u64, other: &u64) {
        *summary += *other;
    }
}

fn sketch_with_range(start: u64, count: u64) -> UpdatableTupleSketch<u64> {
    let mut sketch = UpdatableTupleSketch::<u64>::builder().build();
    for i in 0..count {
        sketch.update(start + i, 1u64);
    }
    sketch
}

#[test]
fn test_has_result_state_machine() {
    let mut a = UpdatableTupleSketch::<u64>::builder().build();
    a.update("x", 1u64);

    let mut i = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
    assert!(!i.has_result());
    i.update(&a).unwrap();
    assert!(i.has_result());
    assert!(i.result().estimate() >= 1.0);
}

#[test]
fn test_result_before_update_panics() {
    let i = TupleIntersection::<u64, _>::new(123, SumPolicy);
    let result = std::panic::catch_unwind(|| {
        let _ = i.result();
    });
    assert!(result.is_err());
}

#[test]
fn test_update_accepts_compact_sketch() {
    let mut a = UpdatableTupleSketch::<u64>::builder().build();
    a.update("x", 1u64);
    a.update("y", 1u64);

    let mut b = UpdatableTupleSketch::<u64>::builder().build();
    b.update("y", 1u64);
    b.update("z", 1u64);

    let mut i = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
    i.update(&a.compact(true)).unwrap();
    i.update(&b).unwrap();

    let r = i.result();
    assert!(r.estimate() == 1.0);
    assert!(r.is_ordered());

    let mut c = UpdatableTupleSketch::<u64>::builder().build();
    c.update("a", 1u64);
    c.update("b", 1u64);
    c.update("c", 1u64);

    i.update(&c.compact(false)).unwrap();

    let r = i.result_with_ordered(false);
    assert!(r.estimate() == 0.0);
    assert!(!r.is_ordered());
}

#[test]
fn test_seed_mismatch_behaviour_for_empty_sketch() {
    let empty_other_seed = UpdatableTupleSketch::<u64>::builder().seed(2).build();
    let mut i = TupleIntersection::<u64, _>::new(1, SumPolicy);

    i.update(&empty_other_seed).unwrap();
    assert!(i.has_result());
    let r = i.result();
    assert!(r.is_empty());
}

#[test]
fn test_seed_mismatch_behaviour() {
    let mut one_other_seed = UpdatableTupleSketch::<u64>::builder().seed(2).build();
    one_other_seed.update("value", 1u64);
    let mut i = TupleIntersection::<u64, _>::new(1, SumPolicy);

    assert!(i.update(&one_other_seed).is_err());
}

#[test]
fn test_terminal_empty_state_ignores_future_updates() {
    let empty = UpdatableTupleSketch::<u64>::builder().build();

    let mut non_empty = UpdatableTupleSketch::<u64>::builder().build();
    non_empty.update("x", 1u64);

    let mut i = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
    i.update(&empty).unwrap();
    i.update(&non_empty).unwrap();

    let r = i.result();
    assert!(r.is_empty());
}

#[test]
fn test_result_with_ordered_false_is_not_ordered() {
    let mut a = UpdatableTupleSketch::<u64>::builder().build();
    for i in 0..64 {
        a.update(i, 1u64);
    }
    let mut i = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
    i.update(&a).unwrap();

    let r = i.result_with_ordered(false);
    assert!(!r.is_ordered());
}

#[test]
fn test_empty_update_twice() {
    let empty = UpdatableTupleSketch::<u64>::builder().build();
    let mut i = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);

    i.update(&empty).unwrap();
    let r1 = i.result();
    assert_eq!(r1.num_retained(), 0);
    assert!(r1.is_empty());
    assert!(!r1.is_estimation_mode());
    assert_eq!(r1.estimate(), 0.0);

    i.update(&empty).unwrap();
    let r2 = i.result();
    assert_eq!(r2.num_retained(), 0);
    assert!(r2.is_empty());
    assert!(!r2.is_estimation_mode());
    assert_eq!(r2.estimate(), 0.0);
}

#[test]
fn test_non_empty_no_retained_keys() {
    let mut s = UpdatableTupleSketch::<u64>::builder()
        .sampling_probability(0.001)
        .build();
    s.update(1u64, 1u64);

    let mut i = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
    i.update(&s).unwrap();
    let r1 = i.result();
    assert_eq!(r1.num_retained(), 0);
    assert!(!r1.is_empty());
    assert!(r1.is_estimation_mode());
    assert!((r1.theta() - 0.001).abs() < 1e-10);
    assert_eq!(r1.estimate(), 0.0);

    i.update(&s).unwrap();
    let r2 = i.result();
    assert_eq!(r2.num_retained(), 0);
    assert!(!r2.is_empty());
    assert!(r2.is_estimation_mode());
    assert!((r2.theta() - 0.001).abs() < 1e-10);
    assert_eq!(r2.estimate(), 0.0);
}

#[test]
fn test_exact_half_overlap_unordered() {
    let s1 = sketch_with_range(0, 1000);
    let s2 = sketch_with_range(500, 1000);

    let mut i = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
    i.update(&s1).unwrap();
    i.update(&s2).unwrap();
    let r = i.result();

    assert!(!r.is_empty());
    assert!(!r.is_estimation_mode());
    assert_eq!(r.estimate(), 500.0);
}

#[test]
fn test_exact_half_overlap_ordered() {
    let s1 = sketch_with_range(0, 1000);
    let s2 = sketch_with_range(500, 1000);

    let mut i = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
    i.update(&s1.compact(true)).unwrap();
    i.update(&s2.compact(true)).unwrap();
    let r = i.result();

    assert!(!r.is_empty());
    assert!(!r.is_estimation_mode());
    assert_eq!(r.estimate(), 500.0);
}

#[test]
fn test_exact_disjoint_unordered() {
    let s1 = sketch_with_range(0, 1000);
    let s2 = sketch_with_range(1000, 1000);

    let mut i = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
    i.update(&s1).unwrap();
    i.update(&s2).unwrap();
    let r = i.result();

    assert!(r.is_empty());
    assert!(!r.is_estimation_mode());
    assert_eq!(r.estimate(), 0.0);
}

#[test]
fn test_exact_disjoint_ordered() {
    let s1 = sketch_with_range(0, 1000);
    let s2 = sketch_with_range(1000, 1000);

    let mut i = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
    i.update(&s1.compact(true)).unwrap();
    i.update(&s2.compact(true)).unwrap();
    let r = i.result();

    assert!(r.is_empty());
    assert!(!r.is_estimation_mode());
    assert_eq!(r.estimate(), 0.0);
}

#[test]
fn test_estimation_half_overlap_unordered() {
    let s1 = sketch_with_range(0, 10000);
    let s2 = sketch_with_range(5000, 10000);

    let mut i = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
    i.update(&s1).unwrap();
    i.update(&s2).unwrap();
    let r = i.result();

    assert!(!r.is_empty());
    assert!(r.is_estimation_mode());
    assert!((r.estimate() - 5000.0).abs() <= 5000.0 * 0.02);
}

#[test]
fn test_estimation_half_overlap_ordered() {
    let s1 = sketch_with_range(0, 10000);
    let s2 = sketch_with_range(5000, 10000);

    let mut i = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
    i.update(&s1.compact(true)).unwrap();
    i.update(&s2.compact(true)).unwrap();
    let r = i.result();

    assert!(!r.is_empty());
    assert!(r.is_estimation_mode());
    assert!((r.estimate() - 5000.0).abs() <= 5000.0 * 0.02);
}

#[test]
fn test_estimation_half_overlap_ordered_deserialized_compact() {
    let serde = PrimitiveSummarySerde;
    let s1 = sketch_with_range(0, 10000);
    let s2 = sketch_with_range(5000, 10000);
    let c1 = CompactTupleSketch::<u64>::deserialize(&s1.compact(true).serialize(&serde), &serde)
        .unwrap();
    let c2 = CompactTupleSketch::<u64>::deserialize(&s2.compact(true).serialize(&serde), &serde)
        .unwrap();

    let mut i = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
    i.update(&c1).unwrap();
    i.update(&c2).unwrap();
    let r = i.result();

    assert!(!r.is_empty());
    assert!(r.is_estimation_mode());
    assert!((r.estimate() - 5000.0).abs() <= 5000.0 * 0.02);
}

#[test]
fn test_estimation_disjoint_unordered() {
    let s1 = sketch_with_range(0, 10000);
    let s2 = sketch_with_range(10000, 10000);

    let mut i = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
    i.update(&s1).unwrap();
    i.update(&s2).unwrap();
    let r = i.result();

    assert!(!r.is_empty());
    assert!(r.is_estimation_mode());
    assert_eq!(r.estimate(), 0.0);
}

#[test]
fn test_estimation_disjoint_ordered() {
    let s1 = sketch_with_range(0, 10000);
    let s2 = sketch_with_range(10000, 10000);

    let mut i = TupleIntersection::<u64, _>::new_with_default_seed(SumPolicy);
    i.update(&s1.compact(true)).unwrap();
    i.update(&s2.compact(true)).unwrap();
    let r = i.result();

    assert!(!r.is_empty());
    assert!(r.is_estimation_mode());
    assert_eq!(r.estimate(), 0.0);
}

#[test]
fn test_seed_mismatch_non_empty_returns_error() {
    let mut s = UpdatableTupleSketch::<u64>::builder().build();
    s.update(1u64, 1u64);

    let mut i = TupleIntersection::<u64, _>::new(123, SumPolicy);
    assert!(i.update(&s).is_err());
}
