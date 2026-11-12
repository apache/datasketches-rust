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

use datasketches::common::JaccardSimilarity;
use datasketches::error::ErrorKind;
use datasketches::theta::ThetaSketch;
use datasketches::theta::ThetaSketchBuilder;
use googletest::assert_that;
use googletest::prelude::near;
use tests_integration::ZERO_HASH_SEED;

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

fn non_empty_sketch_without_retained_entries(
    sampling_probability: f32,
    value: &str,
) -> ThetaSketch {
    let mut sketch = ThetaSketchBuilder::default()
        .sampling_probability(sampling_probability)
        .build()
        .unwrap();
    sketch.update(value);

    assert!(!sketch.is_empty());
    assert_eq!(sketch.num_retained(), 0);
    sketch
}

#[test]
fn test_empty() {
    let sketch_a = ThetaSketchBuilder::default().build().unwrap();
    let sketch_b = ThetaSketchBuilder::default().build().unwrap();

    let jaccard = JaccardSimilarity::compute(&sketch_a, &sketch_b).unwrap();

    assert_jaccard_exact(jaccard, 1.0);
    assert!(JaccardSimilarity::exactly_equal(&sketch_a, &sketch_b).unwrap());
}

#[test]
fn test_exactly_equal() {
    let empty = ThetaSketchBuilder::default().build().unwrap();
    let sketch_a = sketch_with_range(0, 1000);
    let sketch_b = sketch_with_range(0, 1000);
    let sketch_c = sketch_with_range(1000, 1000);
    let compact_a = sketch_a.compact(true);
    let compact_b = sketch_b.compact(true);

    assert!(!JaccardSimilarity::exactly_equal(&empty, &sketch_a).unwrap());
    assert!(JaccardSimilarity::exactly_equal(&sketch_a, &sketch_b).unwrap());
    assert!(JaccardSimilarity::exactly_equal(&sketch_a, &compact_b).unwrap());
    assert!(JaccardSimilarity::exactly_equal(&compact_a, &sketch_b).unwrap());
    assert!(!JaccardSimilarity::exactly_equal(&sketch_a, &sketch_c).unwrap());
}

#[test]
fn test_same_sketch_exact_mode() {
    let sketch = sketch_with_range(0, 1000);

    let jaccard = JaccardSimilarity::compute(&sketch, &sketch).unwrap();
    assert_jaccard_exact(jaccard, 1.0);

    let jaccard = JaccardSimilarity::compute(&sketch.compact(true), &sketch.compact(true)).unwrap();
    assert_jaccard_exact(jaccard, 1.0);
}

#[test]
fn test_full_overlap_exact_mode() {
    let sketch_a = sketch_with_range(0, 1000);
    let sketch_b = sketch_with_range(0, 1000);

    let jaccard = JaccardSimilarity::compute(&sketch_a, &sketch_b).unwrap();
    assert_jaccard_exact(jaccard, 1.0);

    let jaccard =
        JaccardSimilarity::compute(&sketch_a.compact(true), &sketch_b.compact(true)).unwrap();
    assert_jaccard_exact(jaccard, 1.0);
}

#[test]
fn test_disjoint_exact_mode() {
    let sketch_a = sketch_with_range(0, 1000);
    let sketch_b = sketch_with_range(1000, 1000);

    let jaccard = JaccardSimilarity::compute(&sketch_a, &sketch_b).unwrap();
    assert_jaccard_exact(jaccard, 0.0);

    let jaccard =
        JaccardSimilarity::compute(&sketch_a.compact(true), &sketch_b.compact(true)).unwrap();
    assert_jaccard_exact(jaccard, 0.0);
}

#[test]
fn test_half_overlap_estimation_mode() {
    let sketch_a = sketch_with_range(0, 10000);
    let sketch_b = sketch_with_range(5000, 10000);

    let jaccard = JaccardSimilarity::compute(&sketch_a, &sketch_b).unwrap();
    assert_jaccard_estimate(jaccard, 0.33);

    let jaccard =
        JaccardSimilarity::compute(&sketch_a.compact(true), &sketch_b.compact(true)).unwrap();
    assert_jaccard_estimate(jaccard, 0.33);
}

#[test]
fn test_half_overlap_estimation_mode_custom_seed() {
    let seed = 123;
    let sketch_a = sketch_with_range_and_seed(0, 10000, seed);
    let sketch_b = sketch_with_range_and_seed(5000, 10000, seed);

    let jaccard = JaccardSimilarity::compute_with_seed(&sketch_a, &sketch_b, seed).unwrap();
    assert_jaccard_estimate(jaccard, 0.33);

    let jaccard = JaccardSimilarity::compute_with_seed(
        &sketch_a.compact(true),
        &sketch_b.compact(true),
        seed,
    )
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

    assert_eq!(
        JaccardSimilarity::compute_with_seed(&empty, &empty, ZERO_HASH_SEED)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        JaccardSimilarity::exactly_equal_with_seed(&empty, &empty, ZERO_HASH_SEED)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        JaccardSimilarity::compute(&sketch_a, &sketch_b)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        JaccardSimilarity::compute(&sketch_b, &sketch_a)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        JaccardSimilarity::exactly_equal(&sketch_a, &sketch_b)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        JaccardSimilarity::exactly_equal(&sketch_b, &sketch_a)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
    assert_jaccard_exact(JaccardSimilarity::compute(&empty, &sketch_b).unwrap(), 0.0);
    assert!(!JaccardSimilarity::exactly_equal(&empty, &sketch_b).unwrap());
}

#[test]
fn test_equal_theta_non_empty_sketches_with_no_retained_entries_are_identical() {
    let sketch_a = non_empty_sketch_without_retained_entries(1e-12, "apple");
    let sketch_b = non_empty_sketch_without_retained_entries(1e-12, "banana");

    assert_eq!(sketch_a.theta64(), sketch_b.theta64());

    assert_jaccard_exact(
        JaccardSimilarity::compute(&sketch_a, &sketch_b).unwrap(),
        1.0,
    );
    assert_jaccard_exact(
        JaccardSimilarity::compute(&sketch_a, &sketch_a).unwrap(),
        1.0,
    );
    assert!(JaccardSimilarity::exactly_equal(&sketch_a, &sketch_b).unwrap());
}

#[test]
fn test_distinct_theta_non_empty_sketches_with_no_retained_entries_are_uncertain() {
    let sketch_a = non_empty_sketch_without_retained_entries(1e-12, "apple");
    let different_theta = non_empty_sketch_without_retained_entries(2e-12, "orange");

    assert_ne!(sketch_a.theta64(), different_theta.theta64());

    let jaccard = JaccardSimilarity::compute(&sketch_a, &different_theta).unwrap();
    assert_eq!(jaccard.lower_bound(), 0.0);
    assert_eq!(jaccard.estimate(), 0.5);
    assert_eq!(jaccard.upper_bound(), 1.0);

    assert!(!JaccardSimilarity::exactly_equal(&sketch_a, &different_theta).unwrap());
}

#[test]
fn test_representation_combinations_and_asymmetric_theta() {
    // `sketch_a` (lg_k = 5, 5000 items) enters estimation mode with a low theta, while
    // `sketch_b` (lg_k = 12, 2000 items) stays in exact mode (`MAX_THETA`) with partial overlap.
    let mut sketch_a = ThetaSketchBuilder::default().lg_k(5).build().unwrap();
    for value in 0..5000u64 {
        sketch_a.update(value);
    }
    let mut sketch_b = ThetaSketchBuilder::default().lg_k(12).build().unwrap();
    for value in 2500..4500u64 {
        sketch_b.update(value);
    }
    assert!(sketch_a.is_estimation_mode());
    assert!(!sketch_b.is_estimation_mode());

    let compact_ordered_a = sketch_a.compact(true);
    let compact_unordered_a = sketch_a.compact(false);
    let compact_ordered_b = sketch_b.compact(true);
    let compact_unordered_b = sketch_b.compact(false);
    assert!(!compact_unordered_a.is_ordered());
    assert!(!compact_unordered_b.is_ordered());

    let expected = JaccardSimilarity::compute(&sketch_a, &sketch_b).unwrap();
    assert_close(expected.estimate(), 2000.0 / 5000.0, 0.2);

    let views_a = [
        sketch_a.as_view(),
        compact_ordered_a.as_view(),
        compact_unordered_a.as_view(),
    ];
    let views_b = [
        sketch_b.as_view(),
        compact_ordered_b.as_view(),
        compact_unordered_b.as_view(),
    ];
    for &va in &views_a {
        for &vb in &views_b {
            assert_eq!(JaccardSimilarity::compute(va, vb).unwrap(), expected);
            assert_eq!(JaccardSimilarity::compute(vb, va).unwrap(), expected);
            assert!(!JaccardSimilarity::exactly_equal(va, vb).unwrap());
        }
        for &va2 in &views_a {
            assert!(JaccardSimilarity::exactly_equal(va, va2).unwrap());
            assert_jaccard_exact(JaccardSimilarity::compute(va, va2).unwrap(), 1.0);
        }
    }
}

#[test]
fn test_similarity_and_dissimilarity_thresholds() {
    let sketch_a = sketch_with_range(0, 10000);
    let sketch_b = sketch_with_range(5000, 10000);
    let jaccard = JaccardSimilarity::compute(&sketch_a, &sketch_b).unwrap();
    assert!(jaccard.lower_bound() < jaccard.estimate());
    assert!(jaccard.estimate() < jaccard.upper_bound());

    // Inclusive bounds pass; interior thresholds are inconclusive (`false` for both tests).
    assert!(jaccard.is_similar(0.0).unwrap());
    assert!(jaccard.is_similar(jaccard.lower_bound()).unwrap());
    assert!(!jaccard.is_similar(jaccard.estimate()).unwrap());
    assert!(!jaccard.is_dissimilar(jaccard.estimate()).unwrap());
    assert!(jaccard.is_dissimilar(jaccard.upper_bound()).unwrap());
    assert!(jaccard.is_dissimilar(1.0).unwrap());

    let uncertain_a = non_empty_sketch_without_retained_entries(1e-12, "apple");
    let uncertain_b = non_empty_sketch_without_retained_entries(2e-12, "orange");
    let uncertain = JaccardSimilarity::compute(&uncertain_a, &uncertain_b).unwrap();
    assert!(!uncertain.is_similar(0.01).unwrap());
    assert!(!uncertain.is_dissimilar(0.99).unwrap());

    for invalid in [-0.01, 1.01, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_eq!(
            jaccard.is_similar(invalid).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(
            jaccard.is_dissimilar(invalid).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
    }
}
