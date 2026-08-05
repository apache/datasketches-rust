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

use datasketches::thetacommon::JaccardSimilarity;
use datasketches::tuple::DefaultUpdatePolicy;
use datasketches::tuple::TupleJaccardSimilarity;
use datasketches::tuple::TupleSketchBuilder;

use crate::default_tuple_sketch_builder;
use crate::tuple_sketch_with_range;

fn assert_jaccard_exact(actual: JaccardSimilarity, expected: f64) {
    assert_eq!(actual.lower_bound(), expected);
    assert_eq!(actual.estimate(), expected);
    assert_eq!(actual.upper_bound(), expected);
}

fn assert_close(actual: f64, expected: f64, margin: f64) {
    assert!(
        (actual - expected).abs() <= margin,
        "actual={actual}, expected={expected}, margin={margin}"
    );
}

fn assert_jaccard_estimate(actual: JaccardSimilarity, expected: f64) {
    assert_close(actual.lower_bound(), expected, 0.01);
    assert_close(actual.estimate(), expected, 0.01);
    assert_close(actual.upper_bound(), expected, 0.01);
}

#[test]
fn test_empty() {
    let sketch_a = default_tuple_sketch_builder().build();
    let sketch_b = default_tuple_sketch_builder().build();

    let operator = TupleJaccardSimilarity::default();
    let jaccard = operator.compute(&sketch_a, &sketch_b).unwrap();

    assert_jaccard_exact(jaccard, 1.0);
    assert!(operator.exactly_equal(&sketch_a, &sketch_b).unwrap());
}

#[test]
fn test_summary_values_and_types_do_not_affect_similarity() {
    let mut sketch_a = TupleSketchBuilder::new(DefaultUpdatePolicy::<u64>::default()).build();
    let mut sketch_b = TupleSketchBuilder::new(DefaultUpdatePolicy::<i64>::default()).build();
    for key in 0..1000 {
        sketch_a.update(key, 1u64);
        sketch_b.update(key, -7i64);
    }

    let operator = TupleJaccardSimilarity::default();
    let jaccard = operator.compute(&sketch_a, &sketch_b).unwrap();
    assert_jaccard_exact(jaccard, 1.0);
    assert!(operator.exactly_equal(&sketch_a, &sketch_b).unwrap());

    let compact_a = sketch_a.compact(true);
    let compact_b = sketch_b.compact(true);
    let jaccard = operator.compute(&compact_a, &compact_b).unwrap();
    assert_jaccard_exact(jaccard, 1.0);
    assert!(operator.exactly_equal(&sketch_a, &compact_b).unwrap());
    assert!(operator.exactly_equal(&compact_a, &sketch_b).unwrap());
}

#[test]
fn test_half_overlap_estimation_mode() {
    let sketch_a = tuple_sketch_with_range(0, 10000);
    let sketch_b = tuple_sketch_with_range(5000, 10000);

    let operator = TupleJaccardSimilarity::default();
    let jaccard = operator.compute(&sketch_a, &sketch_b).unwrap();
    assert_jaccard_estimate(jaccard, 0.33);
    assert!(!operator.exactly_equal(&sketch_a, &sketch_b).unwrap());

    let jaccard = operator
        .compute(&sketch_a.compact(true), &sketch_b.compact(true))
        .unwrap();
    assert_jaccard_estimate(jaccard, 0.33);
}

#[test]
fn test_custom_seed_and_seed_mismatch() {
    let seed = 123;
    let empty = default_tuple_sketch_builder().build();
    let mut sketch_a = TupleSketchBuilder::new(DefaultUpdatePolicy::<u64>::default())
        .seed(seed)
        .build();
    let mut sketch_b = TupleSketchBuilder::new(DefaultUpdatePolicy::<u64>::default())
        .seed(seed)
        .build();
    for value in 0..1000 {
        sketch_a.update(value, 1u64);
        sketch_b.update(value, 2u64);
    }

    let operator = TupleJaccardSimilarity::with_seed(seed);
    let jaccard = operator.compute(&sketch_a, &sketch_b).unwrap();
    assert_jaccard_exact(jaccard, 1.0);
    assert!(operator.exactly_equal(&sketch_a, &sketch_b).unwrap());
    assert!(
        TupleJaccardSimilarity::default()
            .compute(&sketch_a, &sketch_b)
            .is_err()
    );
    assert!(
        TupleJaccardSimilarity::default()
            .exactly_equal(&sketch_a, &sketch_b)
            .is_err()
    );
    assert!(
        !TupleJaccardSimilarity::default()
            .exactly_equal(&empty, &sketch_a)
            .unwrap()
    );
}

#[test]
fn test_distinct_non_empty_sketches_with_no_retained_entries_are_uncertain() {
    let mut sketch_a = default_tuple_sketch_builder()
        .sampling_probability(1e-12)
        .build();
    let mut sketch_b = default_tuple_sketch_builder()
        .sampling_probability(1e-12)
        .build();
    sketch_a.update("apple", 1u64);
    sketch_b.update("banana", 1u64);

    assert!(!sketch_a.is_empty());
    assert!(!sketch_b.is_empty());
    assert_eq!(sketch_a.num_retained(), 0);
    assert_eq!(sketch_b.num_retained(), 0);

    let operator = TupleJaccardSimilarity::default();
    let jaccard = operator.compute(&sketch_a, &sketch_b).unwrap();
    assert_eq!(jaccard.lower_bound(), 0.0);
    assert_eq!(jaccard.estimate(), 0.5);
    assert_eq!(jaccard.upper_bound(), 1.0);

    assert!(operator.exactly_equal(&sketch_a, &sketch_b).unwrap());
}
