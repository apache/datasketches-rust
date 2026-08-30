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

use datasketches::theta::JaccardSimilarity;
use datasketches::theta::ThetaJaccardSimilarity;
use datasketches::theta::ThetaSketch;
use datasketches::theta::ThetaSketchBuilder;
use googletest::assert_that;
use googletest::prelude::anything;
use googletest::prelude::err;
use googletest::prelude::near;

fn assert_jaccard_exact(actual: JaccardSimilarity, expected: f64) {
    assert_eq!(actual.lower_bound(), expected);
    assert_eq!(actual.estimate(), expected);
    assert_eq!(actual.upper_bound(), expected);
}

fn assert_close(actual: f64, expected: f64, margin: f64) {
    assert_that!(actual, near(expected, margin));
}

fn assert_jaccard_estimate(actual: JaccardSimilarity, expected: f64) {
    assert_close(actual.lower_bound(), expected, 0.01);
    assert_close(actual.estimate(), expected, 0.01);
    assert_close(actual.upper_bound(), expected, 0.01);
}

fn sketch_with_range(start: u64, count: u64) -> ThetaSketch {
    let mut sketch = ThetaSketchBuilder::default().build().unwrap();
    for value in start..start + count {
        sketch.update(value);
    }
    sketch
}

fn sketch_with_range_and_seed(start: u64, count: u64, seed: u64) -> ThetaSketch {
    let mut sketch = ThetaSketchBuilder::default().seed(seed).build().unwrap();
    for value in start..start + count {
        sketch.update(value);
    }
    sketch
}

#[test]
fn test_empty() {
    let sketch_a = ThetaSketchBuilder::default().build().unwrap();
    let sketch_b = ThetaSketchBuilder::default().build().unwrap();

    let operator = ThetaJaccardSimilarity::default();
    let jaccard = operator.compute(&sketch_a, &sketch_b).unwrap();

    assert_jaccard_exact(jaccard, 1.0);
    assert!(operator.exactly_equal(&sketch_a, &sketch_b).unwrap());
}

#[test]
fn test_exactly_equal() {
    let empty = ThetaSketchBuilder::default().build().unwrap();
    let sketch_a = sketch_with_range(0, 1000);
    let sketch_b = sketch_with_range(0, 1000);
    let sketch_c = sketch_with_range(1000, 1000);
    let compact_a = sketch_a.compact(true);
    let compact_b = sketch_b.compact(true);

    let operator = ThetaJaccardSimilarity::default();
    assert!(!operator.exactly_equal(&empty, &sketch_a).unwrap());
    assert!(operator.exactly_equal(&sketch_a, &sketch_b).unwrap());
    assert!(operator.exactly_equal(&sketch_a, &compact_b).unwrap());
    assert!(operator.exactly_equal(&compact_a, &sketch_b).unwrap());
    assert!(!operator.exactly_equal(&sketch_a, &sketch_c).unwrap());
}

#[test]
fn test_same_sketch_exact_mode() {
    let sketch = sketch_with_range(0, 1000);

    let operator = ThetaJaccardSimilarity::default();
    let jaccard = operator.compute(&sketch, &sketch).unwrap();
    assert_jaccard_exact(jaccard, 1.0);

    let jaccard = operator
        .compute(&sketch.compact(true), &sketch.compact(true))
        .unwrap();
    assert_jaccard_exact(jaccard, 1.0);
}

#[test]
fn test_full_overlap_exact_mode() {
    let sketch_a = sketch_with_range(0, 1000);
    let sketch_b = sketch_with_range(0, 1000);

    let operator = ThetaJaccardSimilarity::default();
    let jaccard = operator.compute(&sketch_a, &sketch_b).unwrap();
    assert_jaccard_exact(jaccard, 1.0);

    let jaccard = operator
        .compute(&sketch_a.compact(true), &sketch_b.compact(true))
        .unwrap();
    assert_jaccard_exact(jaccard, 1.0);
}

#[test]
fn test_disjoint_exact_mode() {
    let sketch_a = sketch_with_range(0, 1000);
    let sketch_b = sketch_with_range(1000, 1000);

    let operator = ThetaJaccardSimilarity::default();
    let jaccard = operator.compute(&sketch_a, &sketch_b).unwrap();
    assert_jaccard_exact(jaccard, 0.0);

    let jaccard = operator
        .compute(&sketch_a.compact(true), &sketch_b.compact(true))
        .unwrap();
    assert_jaccard_exact(jaccard, 0.0);
}

#[test]
fn test_half_overlap_estimation_mode() {
    let sketch_a = sketch_with_range(0, 10000);
    let sketch_b = sketch_with_range(5000, 10000);

    let operator = ThetaJaccardSimilarity::default();
    let jaccard = operator.compute(&sketch_a, &sketch_b).unwrap();
    assert_jaccard_estimate(jaccard, 0.33);

    let jaccard = operator
        .compute(&sketch_a.compact(true), &sketch_b.compact(true))
        .unwrap();
    assert_jaccard_estimate(jaccard, 0.33);
}

#[test]
fn test_half_overlap_estimation_mode_custom_seed() {
    let seed = 123;
    let sketch_a = sketch_with_range_and_seed(0, 10000, seed);
    let sketch_b = sketch_with_range_and_seed(5000, 10000, seed);

    let operator = ThetaJaccardSimilarity::with_seed(seed).unwrap();
    let jaccard = operator.compute(&sketch_a, &sketch_b).unwrap();
    assert_jaccard_estimate(jaccard, 0.33);

    let jaccard = operator
        .compute(&sketch_a.compact(true), &sketch_b.compact(true))
        .unwrap();
    assert_jaccard_estimate(jaccard, 0.33);
}

#[test]
fn test_seed_mismatch() {
    let empty = ThetaSketchBuilder::default().build().unwrap();
    let mut sketch_a = ThetaSketchBuilder::default().build().unwrap();
    sketch_a.update(1u64);
    let mut sketch_b = ThetaSketchBuilder::default().seed(123).build().unwrap();
    sketch_b.update(1u64);

    assert_that!(
        ThetaJaccardSimilarity::default().compute(&sketch_a, &sketch_b),
        err(anything())
    );
    assert_that!(
        ThetaJaccardSimilarity::default().exactly_equal(&sketch_a, &sketch_b),
        err(anything())
    );
    assert!(
        !ThetaJaccardSimilarity::default()
            .exactly_equal(&empty, &sketch_b)
            .unwrap()
    );
}

#[test]
fn test_distinct_non_empty_sketches_with_no_retained_entries_are_uncertain() {
    let mut sketch_a = ThetaSketchBuilder::default()
        .sampling_probability(1e-12)
        .build()
        .unwrap();
    let mut sketch_b = ThetaSketchBuilder::default()
        .sampling_probability(1e-12)
        .build()
        .unwrap();
    let mut different_theta = ThetaSketchBuilder::default()
        .sampling_probability(2e-12)
        .build()
        .unwrap();
    sketch_a.update("apple");
    sketch_b.update("banana");
    different_theta.update("orange");

    assert!(!sketch_a.is_empty());
    assert!(!sketch_b.is_empty());
    assert_eq!(sketch_a.num_retained(), 0);
    assert_eq!(sketch_b.num_retained(), 0);
    assert_eq!(different_theta.num_retained(), 0);

    let operator = ThetaJaccardSimilarity::default();
    let jaccard = operator.compute(&sketch_a, &sketch_b).unwrap();
    assert_eq!(jaccard.lower_bound(), 0.0);
    assert_eq!(jaccard.estimate(), 0.5);
    assert_eq!(jaccard.upper_bound(), 1.0);

    assert!(operator.exactly_equal(&sketch_a, &sketch_b).unwrap());
    assert!(!operator.exactly_equal(&sketch_a, &different_theta).unwrap());
}
