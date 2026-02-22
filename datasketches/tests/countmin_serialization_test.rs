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

mod common;

use std::fs;

use common::serialization_test_data;
use datasketches::countmin::CountMinSketch;
use googletest::assert_that;
use googletest::prelude::contains_substring;

// This test validates binary format compatibility (deserialize + byte round-trip) for
// C++ Count-Min snapshots. It intentionally does not assert estimate equivalence against
// original input keys because per-row hash seed derivation differs across implementations.
fn assert_cpp_snapshot(
    filename: &str,
    seed: u64,
    expected_num_hashes: u8,
    expected_num_buckets: u32,
    expected_total_weight: u64,
) {
    let path = serialization_test_data("cpp_generated_files", filename);
    let bytes = fs::read(&path).unwrap();

    let sketch = CountMinSketch::<u64>::deserialize_with_seed(&bytes, seed).unwrap();

    assert_eq!(sketch.num_hashes(), expected_num_hashes);
    assert_eq!(sketch.num_buckets(), expected_num_buckets);
    assert_eq!(sketch.seed(), seed);
    assert_eq!(sketch.total_weight(), expected_total_weight);
    assert_eq!(sketch.is_empty(), expected_total_weight == 0);

    let roundtrip = sketch.serialize();
    assert_eq!(roundtrip, bytes, "round-trip bytes differ for {filename}");
}

#[test]
fn test_deserialize_cpp_empty_snapshot() {
    assert_cpp_snapshot("countmin_empty_cpp.sk", 9001, 1, 5, 0);
}

#[test]
fn test_deserialize_cpp_non_empty_snapshot() {
    assert_cpp_snapshot("countmin_non_empty_cpp.sk", 9001, 3, 1024, 2850);
}

#[test]
fn test_deserialize_cpp_snapshot_with_wrong_seed() {
    let path = serialization_test_data("cpp_generated_files", "countmin_non_empty_cpp.sk");
    let bytes = fs::read(&path).unwrap();

    let err = CountMinSketch::<u64>::deserialize_with_seed(&bytes, 9000).unwrap_err();
    assert_that!(err.message(), contains_substring("incompatible seed hash"));
}
