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

use datasketches::aod::ArrayOfDoublesCombiner;
use datasketches::aod::ArrayOfDoublesIntersection;
use datasketches::aod::ArrayOfDoublesSketchBuilder;
use datasketches::error::ErrorKind;
use tests_integration::ZERO_HASH_SEED;

use crate::SumCombiner;
use crate::sketch_with_range;

#[derive(Debug, Clone, Copy)]
struct MaxCombiner;

impl ArrayOfDoublesCombiner for MaxCombiner {
    fn combine(&self, existing: &mut [f64], incoming: &[f64]) {
        for (existing, incoming) in existing.iter_mut().zip(incoming) {
            *existing = existing.max(*incoming);
        }
    }
}

#[test]
fn construction_validates_num_values_and_seed() {
    let error = ArrayOfDoublesIntersection::new(0, SumCombiner).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    let error = ArrayOfDoublesIntersection::with_seed(1, SumCombiner, ZERO_HASH_SEED).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn result_before_first_update_returns_none() {
    let intersection = ArrayOfDoublesIntersection::new(1, SumCombiner).unwrap();
    assert!(!intersection.has_result());
    assert!(intersection.to_sketch(true).is_none());
}

#[test]
fn custom_combiner_controls_shared_values() {
    let mut a = ArrayOfDoublesSketchBuilder::default()
        .num_values(2)
        .build()
        .unwrap();
    a.update("shared", [3.0, 9.0]).unwrap();
    a.update("only_a", [100.0, 100.0]).unwrap();
    let mut b = ArrayOfDoublesSketchBuilder::default()
        .num_values(2)
        .build()
        .unwrap();
    b.update("shared", [4.0, 8.0]).unwrap();
    b.update("only_b", [200.0, 200.0]).unwrap();

    let mut intersection = ArrayOfDoublesIntersection::new(2, MaxCombiner).unwrap();
    intersection.update(&a).unwrap();
    intersection.update(&b.compact(true)).unwrap();
    let result = intersection.to_sketch(true).unwrap();

    assert_eq!(result.num_retained(), 1);
    assert_eq!(result.iter().next().unwrap().values(), &[4.0, 9.0]);
}

#[test]
fn sum_combiner_accumulates_shared_values() {
    let mut a = ArrayOfDoublesSketchBuilder::default().build().unwrap();
    a.update("shared", [3.0]).unwrap();
    let mut b = ArrayOfDoublesSketchBuilder::default().build().unwrap();
    b.update("shared", [4.0]).unwrap();

    let mut intersection = ArrayOfDoublesIntersection::new(1, SumCombiner).unwrap();
    intersection.update(&a).unwrap();
    intersection.update(&b).unwrap();
    assert_eq!(
        intersection
            .to_sketch(true)
            .unwrap()
            .iter()
            .next()
            .unwrap()
            .values(),
        &[7.0]
    );
}

#[test]
fn disjoint_result_is_empty_and_later_inputs_still_require_compatibility() {
    let a = sketch_with_range(0, 100, 2);
    let b = sketch_with_range(100, 100, 2);
    let wrong_num_values = sketch_with_range(0, 1, 1);
    let mut wrong_seed = ArrayOfDoublesSketchBuilder::default()
        .num_values(2)
        .seed(2)
        .build()
        .unwrap();
    wrong_seed.update(0_u64, [1.0, 1.0]).unwrap();

    let mut intersection = ArrayOfDoublesIntersection::new(2, SumCombiner).unwrap();
    intersection.update(&a).unwrap();
    intersection.update(&b).unwrap();
    let result = intersection.to_sketch(true).unwrap();
    assert!(result.is_empty());

    let error = intersection.update(&wrong_num_values).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    let error = intersection.update(&wrong_seed).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}
