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

use datasketches::theta::CompactThetaSketch;
use datasketches::theta::ThetaIntersection;
use datasketches::theta::ThetaSketch;

fn sketch_with_range(start: u64, count: u64) -> ThetaSketch {
    let mut sketch = ThetaSketch::builder().build();
    for i in 0..count {
        sketch.update(start + i);
    }
    sketch
}

#[test]
fn test_has_result_state_machine() {
    let mut a = ThetaSketch::builder().build();
    a.update("x");

    let mut i = ThetaIntersection::new_with_default_seed();
    assert!(!i.has_result());
    i.update(&a).unwrap();
    assert!(i.has_result());
    assert!(i.result().estimate() >= 1.0);
}

#[test]
fn test_result_before_update_panics() {
    let i = ThetaIntersection::new(123);
    let result = std::panic::catch_unwind(|| {
        let _ = i.result();
    });
    assert!(result.is_err());
}

#[test]
fn test_update_accepts_compact_sketch() {
    let mut a = ThetaSketch::builder().build();
    a.update("x");
    a.update("y");

    let mut b = ThetaSketch::builder().build();
    b.update("y");
    b.update("z");

    let mut i = ThetaIntersection::new_with_default_seed();
    i.update(&a.compact(true)).unwrap();
    i.update(&b).unwrap();

    let r = i.result();
    assert!(r.estimate() == 1.0);
    assert!(r.is_ordered());

    let mut c = ThetaSketch::builder().build();
    c.update("a");
    c.update("b");
    c.update("c");

    i.update(&c.compact(false)).unwrap();

    let r = i.result_with_ordered(false);
    assert!(r.estimate() == 0.0);
    assert!(!r.is_ordered());
}

#[test]
fn test_seed_mismatch_behaviour_for_empty_sketch() {
    let empty_other_seed = ThetaSketch::builder().seed(2).build();
    let mut i = ThetaIntersection::new(1);

    i.update(&empty_other_seed).unwrap();
    assert!(i.has_result());
    let r = i.result();
    assert!(r.is_empty());
}

#[test]
fn test_seed_mismatch_behaviour() {
    let mut one_other_seed = ThetaSketch::builder().seed(2).build();
    one_other_seed.update("value");
    let mut i = ThetaIntersection::new(1);

    assert!(i.update(&one_other_seed).is_err());
}

#[test]
fn test_terminal_empty_state_ignores_future_updates() {
    let empty = ThetaSketch::builder().build();

    let mut non_empty = ThetaSketch::builder().build();
    non_empty.update("x");

    let mut i = ThetaIntersection::new_with_default_seed();
    i.update(&empty).unwrap();
    i.update(&non_empty).unwrap();

    let r = i.result();
    assert!(r.is_empty());
}

#[test]
fn test_result_with_ordered_false_is_not_ordered() {
    let mut a = ThetaSketch::builder().build();
    for i in 0..64 {
        a.update(i);
    }
    let mut i = ThetaIntersection::new_with_default_seed();
    i.update(&a).unwrap();

    let r = i.result_with_ordered(false);
    assert!(!r.is_ordered());
}

#[test]
fn test_empty_update_twice() {
    let empty = ThetaSketch::builder().build();
    let mut i = ThetaIntersection::new_with_default_seed();

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
    let mut s = ThetaSketch::builder().sampling_probability(0.001).build();
    s.update(1u64);

    let mut i = ThetaIntersection::new_with_default_seed();
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

    let mut i = ThetaIntersection::new_with_default_seed();
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

    let mut i = ThetaIntersection::new_with_default_seed();
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

    let mut i = ThetaIntersection::new_with_default_seed();
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

    let mut i = ThetaIntersection::new_with_default_seed();
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

    let mut i = ThetaIntersection::new_with_default_seed();
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

    let mut i = ThetaIntersection::new_with_default_seed();
    i.update(&s1.compact(true)).unwrap();
    i.update(&s2.compact(true)).unwrap();
    let r = i.result();

    assert!(!r.is_empty());
    assert!(r.is_estimation_mode());
    assert!((r.estimate() - 5000.0).abs() <= 5000.0 * 0.02);
}

#[test]
fn test_estimation_half_overlap_ordered_deserialized_compact() {
    let s1 = sketch_with_range(0, 10000);
    let s2 = sketch_with_range(5000, 10000);
    let c1 = CompactThetaSketch::deserialize(&s1.compact(true).serialize()).unwrap();
    let c2 = CompactThetaSketch::deserialize(&s2.compact(true).serialize()).unwrap();

    let mut i = ThetaIntersection::new_with_default_seed();
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

    let mut i = ThetaIntersection::new_with_default_seed();
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

    let mut i = ThetaIntersection::new_with_default_seed();
    i.update(&s1.compact(true)).unwrap();
    i.update(&s2.compact(true)).unwrap();
    let r = i.result();

    assert!(!r.is_empty());
    assert!(r.is_estimation_mode());
    assert_eq!(r.estimate(), 0.0);
}

#[test]
fn test_seed_mismatch_non_empty_returns_error() {
    let mut s = ThetaSketch::builder().build();
    s.update(1u64);

    let mut i = ThetaIntersection::new(123);
    assert!(i.update(&s).is_err());
}
