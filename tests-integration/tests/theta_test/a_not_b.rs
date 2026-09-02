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

//! Behavioral tests for the Theta a-not-B (set difference) operator, mirroring
//! `theta_intersection_test.rs`.
//!
//! The result of `a and not b` retains the keys of `a` that are absent from `b`.

use datasketches::theta::CompactThetaSketch;
use datasketches::theta::ThetaANotB;
use datasketches::theta::ThetaSketch;
use datasketches::theta::ThetaSketchBuilder;
use googletest::assert_that;
use googletest::prelude::anything;
use googletest::prelude::err;
use googletest::prelude::lt;
use googletest::prelude::near;
use tests_integration::MAX_THETA;

fn sketch_with_range(start: u64, count: u64) -> ThetaSketch {
    let mut sketch = ThetaSketchBuilder::default().build().unwrap();
    for i in 0..count {
        sketch.update(start + i);
    }
    sketch
}

#[test]
fn test_basic_difference() {
    let mut a = ThetaSketchBuilder::default().build().unwrap();
    a.update("shared");
    a.update("only_a");
    let mut b = ThetaSketchBuilder::default().build().unwrap();
    b.update("shared");
    b.update("only_b");

    let a_not_b = ThetaANotB::default();
    let r = a_not_b.compute(&a, &b, true).unwrap();

    // "shared" is subtracted; only "only_a" survives.
    assert_eq!(r.num_retained(), 1);
    assert!(!r.is_estimation_mode());
    assert_eq!(r.estimate(), 1.0);
}

#[test]
fn test_accepts_updatable_and_compact_inputs() {
    let a = sketch_with_range(0, 1000);
    let b = sketch_with_range(500, 1000);

    let a_not_b = ThetaANotB::default();
    let r = a_not_b.compute(&a.compact(true), &b, true).unwrap();
    assert_eq!(r.num_retained(), 500);

    let r = a_not_b.compute(&a, &b.compact(false), true).unwrap();
    assert_eq!(r.num_retained(), 500);
}

#[test]
fn test_seed_mismatch_returns_error() {
    let mut one_other_seed = ThetaSketchBuilder::default().seed(2).build().unwrap();
    one_other_seed.update("value");
    let good = sketch_with_range(0, 10);

    let a_not_b = ThetaANotB::with_seed(1).unwrap();
    assert_that!(
        a_not_b.compute(&one_other_seed, &good, true),
        err(anything())
    );
    assert_that!(
        a_not_b.compute(&good, &one_other_seed, true),
        err(anything())
    );
}

#[test]
fn test_seed_mismatch_ignored_for_empty_inputs() {
    // Empty inputs carry no keys, so their seeds are not validated.
    let empty_other_seed = ThetaSketchBuilder::default().seed(2).build().unwrap();
    let good = sketch_with_range(0, 10);

    let a_not_b = ThetaANotB::default();

    let r = a_not_b.compute(&empty_other_seed, &good, true).unwrap();
    assert!(r.is_empty());

    let r = a_not_b.compute(&good, &empty_other_seed, true).unwrap();
    assert_eq!(r.num_retained(), 10);
}

#[test]
fn test_empty_a_returns_empty() {
    let empty = ThetaSketchBuilder::default()
        .sampling_probability(0.5)
        .build()
        .unwrap();
    let b = sketch_with_range(0, 1000);

    let a_not_b = ThetaANotB::default();
    let r = a_not_b.compute(&empty, &b, true).unwrap();

    assert!(r.is_empty());
    assert_eq!(r.num_retained(), 0);
    assert_eq!(r.estimate(), 0.0);
    assert_eq!(r.theta64(), MAX_THETA);
    assert!(!r.is_estimation_mode());
}

#[test]
fn test_empty_b_returns_a() {
    let a = sketch_with_range(0, 1000);
    let empty = ThetaSketchBuilder::default().build().unwrap();

    let a_not_b = ThetaANotB::default();
    let r = a_not_b.compute(&a, &empty, true).unwrap();

    assert_eq!(r.num_retained(), 1000);
    assert_eq!(r.estimate(), 1000.0);
}

#[test]
fn test_exact_partial_overlap_unordered() {
    let a = sketch_with_range(0, 1000);
    let b = sketch_with_range(500, 1000);

    let a_not_b = ThetaANotB::default();
    let r = a_not_b.compute(&a, &b, true).unwrap();

    // Keys 0..500 survive (exact mode).
    assert!(!r.is_empty());
    assert!(!r.is_estimation_mode());
    assert_eq!(r.num_retained(), 500);
    assert_eq!(r.estimate(), 500.0);
}

#[test]
fn test_exact_partial_overlap_ordered() {
    let a = sketch_with_range(0, 1000);
    let b = sketch_with_range(500, 1000);

    let a_not_b = ThetaANotB::default();
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
    let a = sketch_with_range(0, 1000);
    let b = sketch_with_range(1000, 1000);

    let a_not_b = ThetaANotB::default();
    let r = a_not_b.compute(&a, &b, true).unwrap();

    assert!(!r.is_estimation_mode());
    assert_eq!(r.num_retained(), 1000);
    assert_eq!(r.estimate(), 1000.0);
}

#[test]
fn test_exact_superset_b_returns_empty() {
    let a = sketch_with_range(0, 1000);
    let b = sketch_with_range(0, 2000);

    let a_not_b = ThetaANotB::default();
    let r = a_not_b.compute(&a, &b, true).unwrap();

    assert!(r.is_empty());
    assert_eq!(r.num_retained(), 0);
    assert_eq!(r.estimate(), 0.0);
}

#[test]
fn test_result_ordering() {
    let a = sketch_with_range(0, 64);
    let empty = ThetaSketchBuilder::default().build().unwrap();

    let a_not_b = ThetaANotB::default();

    let r = a_not_b.compute(&a, &empty, true).unwrap();
    assert!(r.is_ordered());

    let r = a_not_b.compute(&a, &empty, false).unwrap();
    assert!(!r.is_ordered());
}

#[test]
fn test_estimation_lower_theta_b_unordered() {
    let a = sketch_with_range(0, 10000);
    let b = sketch_with_range(5000, 25000);
    assert_that!(b.theta64(), lt(a.theta64()));

    let a_not_b = ThetaANotB::default();
    let r = a_not_b.compute(&a, &b, true).unwrap();

    // B is deliberately larger so its lower theta constrains the difference.
    assert!(!r.is_empty());
    assert!(r.is_estimation_mode());
    assert_eq!(r.theta64(), b.theta64());
    assert_that!(r.estimate(), near(5000.0, 5000.0 * 0.03));
}

#[test]
fn test_estimation_lower_theta_b_ordered() {
    let a = sketch_with_range(0, 10000);
    let b = sketch_with_range(5000, 25000);
    assert_that!(b.theta64(), lt(a.theta64()));

    let a_not_b = ThetaANotB::default();
    let r = a_not_b
        .compute(&a.compact(true), &b.compact(true), true)
        .unwrap();

    assert!(!r.is_empty());
    assert!(r.is_estimation_mode());
    assert_eq!(r.theta64(), b.theta64());
    assert_that!(r.estimate(), near(5000.0, 5000.0 * 0.03));
}

#[test]
fn test_estimation_partial_overlap_deserialized_compact() {
    let a = sketch_with_range(0, 10000);
    let b = sketch_with_range(5000, 10000);
    let c1 = CompactThetaSketch::deserialize(&a.compact(true).serialize()).unwrap();
    let c2 = CompactThetaSketch::deserialize(&b.compact(true).serialize()).unwrap();

    let a_not_b = ThetaANotB::default();
    let r = a_not_b.compute(&c1, &c2, true).unwrap();

    assert!(!r.is_empty());
    assert!(r.is_estimation_mode());
    assert_that!(r.estimate(), near(5000.0, 5000.0 * 0.02));
}

#[test]
fn test_estimation_disjoint_returns_a() {
    let a = sketch_with_range(0, 10000);
    let b = sketch_with_range(10000, 10000);

    let a_not_b = ThetaANotB::default();
    let r = a_not_b.compute(&a, &b, true).unwrap();

    assert!(!r.is_empty());
    assert!(r.is_estimation_mode());
    assert_that!(r.estimate(), near(10000.0, 10000.0 * 0.02));
}
