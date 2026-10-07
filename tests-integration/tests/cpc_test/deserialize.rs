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

//! Regression tests for deserializing malformed CPC sketches.

use datasketches::cpc::CpcSketch;
use datasketches::error::ErrorKind;
use tests_integration::ZERO_HASH_SEED;

fn valid_bytes(lg_k: u8, n: u64) -> Vec<u8> {
    let mut sketch = CpcSketch::new(lg_k).unwrap();
    for i in 0..n {
        sketch.update(i);
    }
    sketch.serialize()
}

#[test]
fn truncated_compressed_streams_return_errors() {
    let bytes = valid_bytes(10, 8_000);
    for len in 0..bytes.len() {
        assert!(
            CpcSketch::deserialize(&bytes[..len]).is_err(),
            "accepted truncation at {len} bytes"
        );
    }
}

#[test]
fn oversized_coupon_count_is_rejected() {
    let mut bytes = valid_bytes(10, 8_000);
    bytes[11] = u8::MAX;
    assert!(CpcSketch::deserialize(&bytes).is_err());
}

#[test]
fn zero_seed_hash_uses_the_callers_error_kind() {
    let constructor_error = CpcSketch::with_seed(10, ZERO_HASH_SEED).unwrap_err();
    assert_eq!(constructor_error.kind(), ErrorKind::InvalidArgument);

    let bytes = valid_bytes(10, 0);
    let deserialization_error =
        CpcSketch::deserialize_with_seed(&bytes, ZERO_HASH_SEED).unwrap_err();
    assert_eq!(deserialization_error.kind(), ErrorKind::InvalidData);
}
