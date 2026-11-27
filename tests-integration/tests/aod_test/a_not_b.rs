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

use datasketches::aod::ArrayOfDoublesANotB;
use datasketches::aod::ArrayOfDoublesSketchBuilder;
use datasketches::error::ErrorKind;
use tests_integration::MAX_THETA;
use tests_integration::ZERO_HASH_SEED;

use crate::sketch_with_range;

#[test]
fn construction_validates_seed() {
    let error = ArrayOfDoublesANotB::with_seed(ZERO_HASH_SEED).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn difference_keeps_only_values_from_a() {
    let mut a = ArrayOfDoublesSketchBuilder::default()
        .num_values(2)
        .build()
        .unwrap();
    a.update("shared", [3.0, 4.0]).unwrap();
    a.update("only_a", [5.0, 6.0]).unwrap();
    let mut b = ArrayOfDoublesSketchBuilder::default()
        .num_values(2)
        .build()
        .unwrap();
    b.update("shared", [30.0, 40.0]).unwrap();
    b.update("only_b", [50.0, 60.0]).unwrap();

    let result = ArrayOfDoublesANotB::default()
        .compute(&a, &b.compact(true), true)
        .unwrap();

    assert_eq!(result.num_retained(), 1);
    assert_eq!(result.num_values(), 2);
    assert_eq!(result.iter().next().unwrap().values(), &[5.0, 6.0]);
}

#[test]
fn empty_inputs_use_the_operator_seed_and_preserve_num_values() {
    let empty_other_seed = ArrayOfDoublesSketchBuilder::default()
        .num_values(3)
        .seed(2)
        .build()
        .unwrap();
    let non_empty = sketch_with_range(0, 10, 3);
    let op = ArrayOfDoublesANotB::default();

    let result = op.compute(&empty_other_seed, &non_empty, true).unwrap();
    assert!(result.is_empty());
    assert_eq!(result.num_values(), 3);
    assert_eq!(result.seed_hash(), non_empty.seed_hash());
    assert_eq!(result.theta64(), MAX_THETA);

    let result = op.compute(&non_empty, &empty_other_seed, true).unwrap();
    assert_eq!(result.num_retained(), 10);
    assert_eq!(result.num_values(), 3);
}

#[test]
fn inputs_require_matching_num_values_even_when_empty() {
    let empty_two = ArrayOfDoublesSketchBuilder::default()
        .num_values(2)
        .build()
        .unwrap();
    let empty_one = ArrayOfDoublesSketchBuilder::default().build().unwrap();
    let error = ArrayOfDoublesANotB::default()
        .compute(&empty_two, &empty_one, true)
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn non_empty_inputs_require_the_operator_seed() {
    let mut other_seed = ArrayOfDoublesSketchBuilder::default()
        .seed(2)
        .build()
        .unwrap();
    other_seed.update("value", [1.0]).unwrap();
    let empty = ArrayOfDoublesSketchBuilder::default().build().unwrap();
    let good = sketch_with_range(0, 10, 1);
    let op = ArrayOfDoublesANotB::default();

    let error = op.compute(&other_seed, &empty, true).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    let error = op.compute(&good, &other_seed, true).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}
