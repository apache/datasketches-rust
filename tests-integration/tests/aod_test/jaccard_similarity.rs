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

use datasketches::aod::ArrayOfDoublesJaccardSimilarity;
use datasketches::aod::ArrayOfDoublesSketch;
use datasketches::aod::ArrayOfDoublesSketchBuilder;
use datasketches::common::JaccardSimilarity;
use datasketches::error::ErrorKind;
use googletest::assert_that;
use googletest::prelude::near;
use tests_integration::ZERO_HASH_SEED;

use crate::sketch_with_range;

fn assert_jaccard_exact(actual: JaccardSimilarity, expected: f64) {
    assert_eq!(actual.lower_bound(), expected);
    assert_eq!(actual.estimate(), expected);
    assert_eq!(actual.upper_bound(), expected);
}

fn non_empty_sketch_without_entries(sampling_probability: f32, key: &str) -> ArrayOfDoublesSketch {
    let mut sketch = ArrayOfDoublesSketchBuilder::default()
        .sampling_probability(sampling_probability)
        .build()
        .unwrap();
    sketch.update(key, [1.0]).unwrap();
    assert!(!sketch.is_empty());
    assert_eq!(sketch.num_retained(), 0);
    sketch
}

#[test]
fn empty_sketches_are_identical() {
    let a = ArrayOfDoublesSketchBuilder::default().build().unwrap();
    let b = ArrayOfDoublesSketchBuilder::default()
        .num_values(3)
        .build()
        .unwrap();

    let op = ArrayOfDoublesJaccardSimilarity::default();
    assert_jaccard_exact(op.compute(&a, &b).unwrap(), 1.0);
    assert!(op.exactly_equal(&a, &b).unwrap());
}

#[test]
fn summary_values_and_dimensions_do_not_affect_key_similarity() {
    let mut a = ArrayOfDoublesSketchBuilder::default()
        .num_values(1)
        .build()
        .unwrap();
    let mut b = ArrayOfDoublesSketchBuilder::default()
        .num_values(3)
        .build()
        .unwrap();
    for key in 0..1_000_u64 {
        a.update(key, [1.0]).unwrap();
        b.update(key, [-7.0, 2.0, 9.0]).unwrap();
    }

    let op = ArrayOfDoublesJaccardSimilarity::default();
    assert_jaccard_exact(op.compute(&a, &b).unwrap(), 1.0);
    assert!(op.exactly_equal(&a, &b).unwrap());

    let compact_a = a.compact(true);
    let compact_b = b.compact(false);
    assert_jaccard_exact(op.compute(&compact_a, &compact_b).unwrap(), 1.0);
    assert!(op.exactly_equal(&a, &compact_b).unwrap());
}

#[test]
fn half_overlap_in_estimation_mode() {
    let a = sketch_with_range(0, 10_000, 2);
    let b = sketch_with_range(5_000, 10_000, 2);

    let op = ArrayOfDoublesJaccardSimilarity::default();
    let result = op.compute(&a, &b).unwrap();
    assert_that!(result.lower_bound(), near(1.0 / 3.0, 0.02));
    assert_that!(result.estimate(), near(1.0 / 3.0, 0.02));
    assert_that!(result.upper_bound(), near(1.0 / 3.0, 0.02));
    assert!(!op.exactly_equal(&a, &b).unwrap());
}

#[test]
fn seed_validation_and_mismatch() {
    assert_eq!(
        ArrayOfDoublesJaccardSimilarity::with_seed(ZERO_HASH_SEED)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );

    let seed = 123;
    let empty = ArrayOfDoublesSketchBuilder::default().build().unwrap();
    let mut a = ArrayOfDoublesSketchBuilder::default()
        .seed(seed)
        .build()
        .unwrap();
    let mut b = ArrayOfDoublesSketchBuilder::default()
        .seed(seed)
        .build()
        .unwrap();
    for key in 0..500_u64 {
        a.update(key, [1.0]).unwrap();
        b.update(key, [2.0]).unwrap();
    }

    let matching = ArrayOfDoublesJaccardSimilarity::with_seed(seed).unwrap();
    assert_jaccard_exact(matching.compute(&a, &b).unwrap(), 1.0);

    let default_op = ArrayOfDoublesJaccardSimilarity::default();
    assert_eq!(
        default_op.compute(&a, &b).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        default_op.exactly_equal(&a, &b).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert_jaccard_exact(default_op.compute(&empty, &a).unwrap(), 0.0);
    assert!(!default_op.exactly_equal(&empty, &a).unwrap());
}

#[test]
fn non_empty_sketches_without_entries_compare_by_theta() {
    let a = non_empty_sketch_without_entries(1e-12, "apple");
    let same_theta = non_empty_sketch_without_entries(1e-12, "banana");
    let different_theta = non_empty_sketch_without_entries(2e-12, "cherry");

    let op = ArrayOfDoublesJaccardSimilarity::default();
    assert_jaccard_exact(op.compute(&a, &same_theta).unwrap(), 1.0);
    assert!(op.exactly_equal(&a, &same_theta).unwrap());

    let uncertain = op.compute(&a, &different_theta).unwrap();
    assert_eq!(uncertain.lower_bound(), 0.0);
    assert_eq!(uncertain.estimate(), 0.5);
    assert_eq!(uncertain.upper_bound(), 1.0);
    assert!(!op.exactly_equal(&a, &different_theta).unwrap());
}
