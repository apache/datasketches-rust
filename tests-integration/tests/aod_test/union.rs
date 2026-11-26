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

use datasketches::aod::ArrayOfDoublesSketchBuilder;
use datasketches::aod::ArrayOfDoublesUnionBuilder;
use datasketches::error::ErrorKind;
use tests_integration::MAX_THETA;
use tests_integration::ZERO_HASH_SEED;

use crate::sketch_with_range;

#[test]
fn builder_validates_configuration_at_build() {
    for error in [
        ArrayOfDoublesUnionBuilder::default()
            .num_values(0)
            .build()
            .unwrap_err(),
        ArrayOfDoublesUnionBuilder::default()
            .lg_k(27)
            .build()
            .unwrap_err(),
        ArrayOfDoublesUnionBuilder::default()
            .sampling_probability(-0.1)
            .build()
            .unwrap_err(),
        ArrayOfDoublesUnionBuilder::default()
            .seed(ZERO_HASH_SEED)
            .build()
            .unwrap_err(),
    ] {
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }
}

#[test]
fn union_adds_overlapping_values_element_by_element() {
    let mut a = ArrayOfDoublesSketchBuilder::default()
        .num_values(2)
        .build()
        .unwrap();
    a.update("shared", [1.0, 2.0]).unwrap();
    a.update("only_a", [3.0, 4.0]).unwrap();
    let mut b = ArrayOfDoublesSketchBuilder::default()
        .num_values(2)
        .build()
        .unwrap();
    b.update("shared", [10.0, 20.0]).unwrap();
    b.update("only_b", [30.0, 40.0]).unwrap();

    let mut union = ArrayOfDoublesUnionBuilder::default()
        .num_values(2)
        .build()
        .unwrap();
    union.update(&a).unwrap();
    union.update(&b.compact(true)).unwrap();
    let result = union.to_sketch(true);

    assert_eq!(result.num_retained(), 3);
    assert_eq!(result.num_values(), 2);
    let mut values: Vec<_> = result.iter().map(|entry| entry.values().to_vec()).collect();
    values.sort_by(|a, b| a[0].total_cmp(&b[0]));
    assert_eq!(values, [vec![3.0, 4.0], vec![11.0, 22.0], vec![30.0, 40.0]]);
}

#[test]
fn union_rejects_incompatible_inputs_without_mutation() {
    let input = sketch_with_range(0, 10, 2);
    let wrong_num_values = sketch_with_range(10, 10, 1);
    let mut wrong_seed = ArrayOfDoublesSketchBuilder::default()
        .num_values(2)
        .seed(2)
        .build()
        .unwrap();
    wrong_seed.update(100_u64, [1.0, 1.0]).unwrap();

    let mut union = ArrayOfDoublesUnionBuilder::default()
        .num_values(2)
        .build()
        .unwrap();
    union.update(&input).unwrap();
    let expected = union.to_sketch(true).serialize();

    let error = union.update(&wrong_num_values).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    assert!(error.message().contains("expected 2, got 1"));
    assert_eq!(union.to_sketch(true).serialize(), expected);

    let error = union.update(&wrong_seed).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    assert_eq!(union.to_sketch(true).serialize(), expected);
}

#[test]
fn reset_restores_the_initial_empty_state() {
    let input = sketch_with_range(0, 100, 1);
    let mut union = ArrayOfDoublesUnionBuilder::default()
        .sampling_probability(0.5)
        .build()
        .unwrap();
    union.update(&input).unwrap();
    assert!(!union.to_sketch(true).is_empty());

    union.reset();
    let result = union.to_sketch(false);
    assert!(result.is_empty());
    assert_eq!(result.num_values(), 1);
    assert_eq!(result.theta64(), MAX_THETA);
    assert_eq!(result.estimate(), 0.0);
}
