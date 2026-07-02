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

#![cfg(feature = "theta")]

use datasketches::theta::ThetaJaccardSimilarity;
use datasketches::theta::ThetaSketch;

fn assert_jaccard_exact(actual: datasketches::theta::JaccardSimilarity, expected: f64) {
    assert_eq!(actual.lower_bound, expected);
    assert_eq!(actual.estimate, expected);
    assert_eq!(actual.upper_bound, expected);
}

fn assert_close(actual: f64, expected: f64, margin: f64) {
    assert!(
        (actual - expected).abs() <= margin,
        "actual={actual}, expected={expected}, margin={margin}"
    );
}

fn sketch_with_range(start: u64, count: u64) -> ThetaSketch {
    let mut sketch = ThetaSketch::builder().build();
    for value in start..start + count {
        sketch.update(value);
    }
    sketch
}

fn sketch_with_range_and_seed(start: u64, count: u64, seed: u64) -> ThetaSketch {
    let mut sketch = ThetaSketch::builder().seed(seed).build();
    for value in start..start + count {
        sketch.update(value);
    }
    sketch
}

#[test]
fn test_empty() {
    let sketch_a = ThetaSketch::builder().build();
    let sketch_b = ThetaSketch::builder().build();

    let jaccard = ThetaJaccardSimilarity::jaccard(&sketch_a, &sketch_b).unwrap();

    assert_jaccard_exact(jaccard, 1.0);
}

#[test]
fn test_same_sketch_exact_mode() {
    let sketch = sketch_with_range(0, 1000);

    let jaccard = ThetaJaccardSimilarity::jaccard(&sketch, &sketch).unwrap();
    assert_jaccard_exact(jaccard, 1.0);

    let jaccard =
        ThetaJaccardSimilarity::jaccard(&sketch.compact(true), &sketch.compact(true)).unwrap();
    assert_jaccard_exact(jaccard, 1.0);
}

#[test]
fn test_full_overlap_exact_mode() {
    let sketch_a = sketch_with_range(0, 1000);
    let sketch_b = sketch_with_range(0, 1000);

    let jaccard = ThetaJaccardSimilarity::jaccard(&sketch_a, &sketch_b).unwrap();
    assert_jaccard_exact(jaccard, 1.0);

    let jaccard =
        ThetaJaccardSimilarity::jaccard(&sketch_a.compact(true), &sketch_b.compact(true)).unwrap();
    assert_jaccard_exact(jaccard, 1.0);
}

#[test]
fn test_disjoint_exact_mode() {
    let sketch_a = sketch_with_range(0, 1000);
    let sketch_b = sketch_with_range(1000, 1000);

    let jaccard = ThetaJaccardSimilarity::jaccard(&sketch_a, &sketch_b).unwrap();
    assert_jaccard_exact(jaccard, 0.0);

    let jaccard =
        ThetaJaccardSimilarity::jaccard(&sketch_a.compact(true), &sketch_b.compact(true)).unwrap();
    assert_jaccard_exact(jaccard, 0.0);
}

#[test]
fn test_half_overlap_estimation_mode() {
    let sketch_a = sketch_with_range(0, 10000);
    let sketch_b = sketch_with_range(5000, 10000);

    let jaccard = ThetaJaccardSimilarity::jaccard(&sketch_a, &sketch_b).unwrap();
    assert_close(jaccard.lower_bound, 0.33, 0.01);
    assert_close(jaccard.estimate, 0.33, 0.01);
    assert_close(jaccard.upper_bound, 0.33, 0.01);

    let jaccard =
        ThetaJaccardSimilarity::jaccard(&sketch_a.compact(true), &sketch_b.compact(true)).unwrap();
    assert_close(jaccard.lower_bound, 0.33, 0.01);
    assert_close(jaccard.estimate, 0.33, 0.01);
    assert_close(jaccard.upper_bound, 0.33, 0.01);
}

#[test]
fn test_half_overlap_estimation_mode_custom_seed() {
    let seed = 123;
    let sketch_a = sketch_with_range_and_seed(0, 10000, seed);
    let sketch_b = sketch_with_range_and_seed(5000, 10000, seed);

    let jaccard = ThetaJaccardSimilarity::jaccard_with_seed(&sketch_a, &sketch_b, seed).unwrap();
    assert_close(jaccard.lower_bound, 0.33, 0.01);
    assert_close(jaccard.estimate, 0.33, 0.01);
    assert_close(jaccard.upper_bound, 0.33, 0.01);

    let jaccard = ThetaJaccardSimilarity::jaccard_with_seed(
        &sketch_a.compact(true),
        &sketch_b.compact(true),
        seed,
    )
    .unwrap();
    assert_close(jaccard.lower_bound, 0.33, 0.01);
    assert_close(jaccard.estimate, 0.33, 0.01);
    assert_close(jaccard.upper_bound, 0.33, 0.01);
}

#[test]
fn test_seed_mismatch() {
    let mut sketch_a = ThetaSketch::builder().build();
    sketch_a.update(1u64);
    let mut sketch_b = ThetaSketch::builder().seed(123).build();
    sketch_b.update(1u64);

    assert!(ThetaJaccardSimilarity::jaccard(&sketch_a, &sketch_b).is_err());
}
