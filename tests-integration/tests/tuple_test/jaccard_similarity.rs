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
use datasketches::tuple::TupleSketch;
use datasketches::tuple::TupleSketchBuilder;
use googletest::assert_that;
use googletest::prelude::anything;
use googletest::prelude::err;
use googletest::prelude::near;

use crate::default_tuple_sketch_builder;
use crate::tuple_sketch_with_range;

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

fn non_empty_sketch_without_retained_entries(
    sampling_probability: f32,
    value: &str,
) -> TupleSketch<DefaultUpdatePolicy<u64>> {
    let mut sketch = default_tuple_sketch_builder()
        .sampling_probability(sampling_probability)
        .build()
        .unwrap();
    sketch.update(value, 1u64);

    assert!(!sketch.is_empty());
    assert_eq!(sketch.num_retained(), 0);
    sketch
}

#[test]
fn test_empty() {
    let sketch_a = default_tuple_sketch_builder().build().unwrap();
    let sketch_b = default_tuple_sketch_builder().build().unwrap();

    let operator = TupleJaccardSimilarity::default();
    let jaccard = operator.compute(&sketch_a, &sketch_b).unwrap();

    assert_jaccard_exact(jaccard, 1.0);
    assert!(operator.exactly_equal(&sketch_a, &sketch_b).unwrap());
}

#[test]
fn test_summary_values_and_types_do_not_affect_similarity() {
    let mut sketch_a = TupleSketchBuilder::new(DefaultUpdatePolicy::<u64>::default())
        .build()
        .unwrap();
    let mut sketch_b = TupleSketchBuilder::new(DefaultUpdatePolicy::<i64>::default())
        .build()
        .unwrap();
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
    let empty = default_tuple_sketch_builder().build().unwrap();
    let mut sketch_a = TupleSketchBuilder::new(DefaultUpdatePolicy::<u64>::default())
        .seed(seed)
        .build()
        .unwrap();
    let mut sketch_b = TupleSketchBuilder::new(DefaultUpdatePolicy::<u64>::default())
        .seed(seed)
        .build()
        .unwrap();
    for value in 0..1000 {
        sketch_a.update(value, 1u64);
        sketch_b.update(value, 2u64);
    }

    let operator = TupleJaccardSimilarity::with_seed(seed).unwrap();
    let jaccard = operator.compute(&sketch_a, &sketch_b).unwrap();
    assert_jaccard_exact(jaccard, 1.0);
    assert!(operator.exactly_equal(&sketch_a, &sketch_b).unwrap());
    assert_that!(
        TupleJaccardSimilarity::default().compute(&sketch_a, &sketch_b),
        err(anything())
    );
    assert_that!(
        TupleJaccardSimilarity::default().exactly_equal(&sketch_a, &sketch_b),
        err(anything())
    );
    assert!(
        !TupleJaccardSimilarity::default()
            .exactly_equal(&empty, &sketch_a)
            .unwrap()
    );
}

#[test]
fn test_equal_theta_non_empty_sketches_with_no_retained_entries_are_identical() {
    let sketch_a = non_empty_sketch_without_retained_entries(1e-12, "apple");
    let sketch_b = non_empty_sketch_without_retained_entries(1e-12, "banana");

    assert_eq!(sketch_a.theta64(), sketch_b.theta64());

    let operator = TupleJaccardSimilarity::default();
    assert_jaccard_exact(operator.compute(&sketch_a, &sketch_b).unwrap(), 1.0);
    assert_jaccard_exact(operator.compute(&sketch_a, &sketch_a).unwrap(), 1.0);
    assert!(operator.exactly_equal(&sketch_a, &sketch_b).unwrap());
}

#[test]
fn test_distinct_theta_non_empty_sketches_with_no_retained_entries_are_uncertain() {
    let sketch_a = non_empty_sketch_without_retained_entries(1e-12, "apple");
    let different_theta = non_empty_sketch_without_retained_entries(2e-12, "orange");

    assert_ne!(sketch_a.theta64(), different_theta.theta64());

    let operator = TupleJaccardSimilarity::default();
    let jaccard = operator.compute(&sketch_a, &different_theta).unwrap();
    assert_eq!(jaccard.lower_bound(), 0.0);
    assert_eq!(jaccard.estimate(), 0.5);
    assert_eq!(jaccard.upper_bound(), 1.0);

    assert!(!operator.exactly_equal(&sketch_a, &different_theta).unwrap());
}
