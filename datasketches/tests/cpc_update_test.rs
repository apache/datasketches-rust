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

use datasketches::common::NumStdDev;
use datasketches::cpc::CpcSketch;
use googletest::assert_that;
use googletest::prelude::ge;
use googletest::prelude::le;
use googletest::prelude::near;

const RELATIVE_ERROR_FOR_LG_K_11: f64 = 0.02;

fn assert_same_sketch_state(left: &CpcSketch, right: &CpcSketch) {
    assert_eq!(left.serialize(), right.serialize());
    assert_eq!(left.estimate(), right.estimate());
}

#[test]
fn test_empty() {
    let sketch = CpcSketch::new(11);
    assert!(sketch.is_empty());
    assert_eq!(sketch.estimate(), 0.0);
    assert_eq!(sketch.lower_bound(NumStdDev::One), 0.0);
    assert_eq!(sketch.upper_bound(NumStdDev::One), 0.0);
    assert!(sketch.validate());
}

#[test]
fn test_one_value() {
    let mut sketch = CpcSketch::new(11);
    sketch.update(1);
    assert!(!sketch.is_empty());
    assert_eq!(sketch.estimate(), 1.0);
    assert_that!(sketch.estimate(), ge(sketch.lower_bound(NumStdDev::One)));
    assert_that!(sketch.estimate(), le(sketch.upper_bound(NumStdDev::One)));
    assert!(sketch.validate());
}

#[test]
fn test_scalar_integer_inputs_are_canonicalized() {
    let mut i32_sketch = CpcSketch::new(11);
    i32_sketch.update(42i32);

    let mut i64_sketch = CpcSketch::new(11);
    i64_sketch.update(42i64);

    assert_same_sketch_state(&i32_sketch, &i64_sketch);
}

#[test]
fn test_unsigned_narrow_integer_inputs_follow_cpp_signed_path() {
    let mut u32_sketch = CpcSketch::new(11);
    u32_sketch.update(u32::MAX);

    let mut signed_sketch = CpcSketch::new(11);
    signed_sketch.update(-1i64);

    let mut u64_sketch = CpcSketch::new(11);
    u64_sketch.update(u32::MAX as u64);

    assert_same_sketch_state(&u32_sketch, &signed_sketch);
    assert_ne!(u32_sketch.serialize(), u64_sketch.serialize());
}

#[test]
fn test_string_hashes_as_raw_utf8_bytes() {
    let mut string_sketch = CpcSketch::new(11);
    string_sketch.update("hello");

    let mut bytes_sketch = CpcSketch::new(11);
    bytes_sketch.update("hello".as_bytes());

    assert_same_sketch_state(&string_sketch, &bytes_sketch);
}

#[test]
fn test_float_inputs_match_java_cpp_canonicalization_rules() {
    let mut f64_sketch = CpcSketch::new(11);
    f64_sketch.update(1.5f64);

    let mut f32_sketch = CpcSketch::new(11);
    f32_sketch.update(1.5f32);

    assert_same_sketch_state(&f64_sketch, &f32_sketch);
}

#[test]
fn test_many_values() {
    let mut sketch = CpcSketch::new(11);
    for i in 0..10000 {
        sketch.update(i);
    }
    assert!(!sketch.is_empty());
    assert_that!(
        sketch.estimate(),
        near(10000.0, RELATIVE_ERROR_FOR_LG_K_11 * 10000.0)
    );
    assert_that!(sketch.estimate(), ge(sketch.lower_bound(NumStdDev::One)));
    assert_that!(sketch.estimate(), le(sketch.upper_bound(NumStdDev::One)));
    assert!(sketch.validate());
}
