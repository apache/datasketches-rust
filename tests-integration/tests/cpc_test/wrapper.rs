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
use datasketches::cpc::CpcUnion;
use datasketches::cpc::CpcWrapper;
use datasketches::error::ErrorKind;
use googletest::assert_that;
use googletest::prelude::contains_substring;
use googletest::prelude::eq;

#[test]
fn test_cpc_wrapper() {
    let lg_k = 10;
    let mut sk1 = CpcSketch::new(lg_k).unwrap();
    let mut sk2 = CpcSketch::new(lg_k).unwrap();
    let mut sk_dst = CpcSketch::new(lg_k).unwrap();

    let n = 100000;
    for i in 0..n {
        sk1.update(i);
        sk2.update(i + n);
        sk_dst.update(i);
        sk_dst.update(i + n);
    }

    let dst_est = sk_dst.estimate();
    let dst_lb = sk_dst.lower_bound(NumStdDev::Two);
    let dst_ub = sk_dst.upper_bound(NumStdDev::Two);

    let concat_bytes = sk_dst.serialize();
    let concat_wrapper = CpcWrapper::new(&concat_bytes).unwrap();
    assert_that!(concat_wrapper.lg_k(), eq(lg_k));
    assert_that!(concat_wrapper.estimate(), eq(dst_est));
    assert_that!(concat_wrapper.lower_bound(NumStdDev::Two), eq(dst_lb));
    assert_that!(concat_wrapper.upper_bound(NumStdDev::Two), eq(dst_ub));

    let mut union = CpcUnion::new(lg_k).unwrap();
    union.update(&sk1).unwrap();
    union.update(&sk2).unwrap();
    let merged = union.to_sketch();
    let merged_est = merged.estimate();
    let merged_lb = merged.lower_bound(NumStdDev::Two);
    let merged_ub = merged.upper_bound(NumStdDev::Two);

    let merged_bytes = merged.serialize();
    let merged_wrapper = CpcWrapper::new(&merged_bytes).unwrap();
    assert_that!(merged_wrapper.lg_k(), eq(lg_k));
    assert_that!(merged_wrapper.estimate(), eq(merged_est));
    assert_that!(merged_wrapper.lower_bound(NumStdDev::Two), eq(merged_lb));
    assert_that!(merged_wrapper.upper_bound(NumStdDev::Two), eq(merged_ub));
}

#[test]
fn test_is_empty() {
    let empty_sketch = CpcSketch::new(10).unwrap();
    let empty_bytes = empty_sketch.serialize();
    let empty_wrapper = CpcWrapper::new(&empty_bytes).unwrap();
    assert_that!(empty_wrapper.is_empty(), eq(true));

    let mut non_empty_sketch = CpcSketch::new(10).unwrap();
    non_empty_sketch.update(1u64);
    let non_empty_bytes = non_empty_sketch.serialize();
    let non_empty_wrapper = CpcWrapper::new(&non_empty_bytes).unwrap();
    assert_that!(non_empty_wrapper.is_empty(), eq(false));
}

#[test]
fn test_is_compressed() {
    let sketch = CpcSketch::new(10).unwrap();
    let mut bytes = sketch.serialize();
    bytes[5] &= (-3i8) as u8; // clear compressed flag
    let err = CpcWrapper::new(&bytes).unwrap_err();
    assert_that!(
        err.message(),
        contains_substring("only compressed sketches are supported")
    );
}

#[test]
fn test_invalid_image_fields_are_invalid_data() {
    let original = CpcSketch::new(10).unwrap().serialize();

    for (index, value, field) in [
        (3, 3, "lg_k"),
        (3, 27, "lg_k"),
        (4, 64, "first_interesting_column"),
    ] {
        let mut bytes = original.clone();
        bytes[index] = value;

        let sketch_err = CpcSketch::deserialize(&bytes).unwrap_err();
        assert_that!(sketch_err.kind(), eq(ErrorKind::InvalidData));
        assert_that!(sketch_err.message(), contains_substring(field));

        let wrapper_err = CpcWrapper::new(&bytes).unwrap_err();
        assert_that!(wrapper_err.kind(), eq(ErrorKind::InvalidData));
        assert_that!(wrapper_err.message(), contains_substring(field));
    }
}
