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
use googletest::assert_that;
use googletest::prelude::near;

const RELATIVE_ERROR_FOR_LG_K_11: f64 = 0.02;

#[test]
fn test_empty() {
    let union = CpcUnion::new(11);
    let sketch = union.get_result();
    assert!(sketch.is_empty());
    assert_eq!(sketch.estimate(), 0.0);
}

#[test]
fn test_two_values() {
    let mut sketch = CpcSketch::new(11);
    sketch.update(1);
    let mut union = CpcUnion::new(11);
    union.update(&sketch);

    let result = union.get_result();
    assert!(!result.is_empty());
    assert_eq!(result.estimate(), 1.0);

    sketch.update(2);
    union.update(&sketch);
    let result = union.get_result();
    assert!(!result.is_empty());
    assert_that!(
        sketch.estimate(),
        near(2.0, RELATIVE_ERROR_FOR_LG_K_11 * 2.0)
    );
}

#[test]
fn test_custom_seed() {
    let mut sketch = CpcSketch::with_seed(11, 123);
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);

    let mut union = CpcUnion::with_seed(11, 123);
    union.update(&sketch);
    let result = union.get_result();
    assert!(!result.is_empty());
    assert_that!(
        result.estimate(),
        near(3.0, RELATIVE_ERROR_FOR_LG_K_11 * 3.0)
    );
}

#[test]
#[should_panic]
fn test_custom_seed_mismatch() {
    let mut sketch = CpcSketch::with_seed(11, 123);
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);

    let mut union = CpcUnion::with_seed(11, 234);
    union.update(&sketch);
}

#[test]
fn test_large_values() {
    let mut key = 0;
    let mut sketch = CpcSketch::new(11);
    let mut union = CpcUnion::new(11);
    for _ in 0..1000 {
        let mut tmp = CpcSketch::new(11);
        for _ in 0..10000 {
            sketch.update(key);
            tmp.update(key);
            key += 1;
        }
        union.update(&tmp);
    }
    let result = union.get_result();
    assert!(!result.is_empty());
    assert_eq!(result.num_coupons(), union.num_coupons());
    let estimate = sketch.estimate();
    assert_that!(
        result.estimate(),
        near(estimate, RELATIVE_ERROR_FOR_LG_K_11 * estimate)
    );
}
