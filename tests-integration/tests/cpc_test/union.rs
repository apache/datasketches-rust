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

use datasketches::cpc::CpcSketch;
use datasketches::cpc::CpcUnion;
use datasketches::error::ErrorKind;
use googletest::assert_that;
use googletest::prelude::near;

const RELATIVE_ERROR_FOR_LG_K_11: f64 = 0.02;

#[test]
fn test_empty() {
    let union = CpcUnion::new(11).unwrap();
    let sketch = union.to_sketch();
    assert!(sketch.is_empty());
    assert_eq!(sketch.estimate(), 0.0);
}

#[test]
fn test_two_values() {
    let mut sketch = CpcSketch::new(11).unwrap();
    sketch.update(1);
    let mut union = CpcUnion::new(11).unwrap();
    union.update(&sketch).unwrap();

    let result = union.to_sketch();
    assert!(!result.is_empty());
    assert_eq!(result.estimate(), 1.0);

    sketch.update(2);
    union.update(&sketch).unwrap();
    let result = union.to_sketch();
    assert!(!result.is_empty());
    assert_that!(
        result.estimate(),
        near(2.0, RELATIVE_ERROR_FOR_LG_K_11 * 2.0)
    );
}

#[test]
fn test_custom_seed() {
    let mut sketch = CpcSketch::with_seed(11, 123).unwrap();
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);

    let mut union = CpcUnion::with_seed(11, 123).unwrap();
    union.update(&sketch).unwrap();
    let result = union.to_sketch();
    assert!(!result.is_empty());
    assert_that!(
        result.estimate(),
        near(3.0, RELATIVE_ERROR_FOR_LG_K_11 * 3.0)
    );
}

#[test]
fn test_custom_seed_mismatch() {
    let mut sketch = CpcSketch::with_seed(11, 123).unwrap();
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);

    let mut union = CpcUnion::with_seed(11, 234).unwrap();
    let error = union.update(&sketch).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn test_sliding_union_matches_single_sketch() {
    let mut key = 0;
    let mut sketch = CpcSketch::new(11).unwrap();
    let mut union = CpcUnion::new(11).unwrap();
    for _ in 0..32 {
        let mut tmp = CpcSketch::new(11).unwrap();
        for _ in 0..8192 {
            sketch.update(key);
            tmp.update(key);
            key += 1;
        }
        union.update(&tmp).unwrap();
    }
    let result = union.to_sketch();
    assert!(!result.is_empty());
    let estimate = sketch.estimate();
    assert_that!(
        result.estimate(),
        near(estimate, RELATIVE_ERROR_FOR_LG_K_11 * estimate)
    );
}

#[test]
fn union_folds_wider_coupon_rows_to_the_destination_precision() {
    // Exercise a Hybrid, Pinned, and Sliding source with four rows per destination row.
    for items in [256, 2_048, 32_768] {
        let mut wide = CpcSketch::new(10).unwrap();
        let mut narrow = CpcSketch::new(8).unwrap();
        for value in 0..items {
            wide.update(value);
            narrow.update(value);
        }

        let mut folded = CpcUnion::new(8).unwrap();
        folded.update(&wide).unwrap();
        let mut direct = CpcUnion::new(8).unwrap();
        direct.update(&narrow).unwrap();
        assert_eq!(folded.serialize(), direct.serialize());
    }
}

#[test]
fn test_reduce_k() {
    for (initial_lg_12, lg_11_count, expected_est) in [
        (0_u64, 10_000_u64, 10_000.0),
        (100, 1_000, 1_000.0),
        (500, 1_000, 1_000.0),
    ] {
        let mut union = CpcUnion::new(12).unwrap();
        if initial_lg_12 > 0 {
            let mut sketch12 = CpcSketch::new(12).unwrap();
            for i in 0..initial_lg_12 {
                sketch12.update(i);
            }
            union.update(&sketch12).unwrap();
        }
        let mut sketch11 = CpcSketch::new(11).unwrap();
        for i in 0..lg_11_count {
            sketch11.update(i);
        }
        union.update(&sketch11).unwrap();

        let result = union.to_sketch();
        assert_eq!(result.lg_k(), 11);
        assert_that!(
            result.estimate(),
            near(expected_est, RELATIVE_ERROR_FOR_LG_K_11 * expected_est)
        );
    }
}
