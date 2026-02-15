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
use std::path::PathBuf;

use common::serialization_test_data;
use datasketches::theta::CompactThetaSketch;
use googletest::assert_that;
use googletest::prelude::near;

fn test_sketch_file(path: PathBuf, expected_cardinality: usize, use_compressed_round_trip: bool) {
    let expected = expected_cardinality as f64;

    let bytes = fs::read(&path).unwrap();
    let sketch1 = CompactThetaSketch::deserialize(&bytes).unwrap();
    let estimate1 = sketch1.estimate();
    assert_that!(estimate1, near(expected, expected * 0.03));

    // Serialize and deserialize again to test round-trip.
    let serialized_bytes = if use_compressed_round_trip {
        sketch1.serialize_compressed()
    } else {
        sketch1.serialize()
    };
    let sketch2 = CompactThetaSketch::deserialize(&serialized_bytes).unwrap_or_else(|err| {
        panic!(
            "Deserialization failed after round-trip for {}: {}",
            path.display(),
            err
        )
    });

    // Theta snapshots from Java/C++ are not required to match byte-for-byte output
    // from this implementation. Verify our own serialization is stable instead.
    let serialized_bytes2 = if use_compressed_round_trip {
        sketch2.serialize_compressed()
    } else {
        sketch2.serialize()
    };
    assert_eq!(
        serialized_bytes,
        serialized_bytes2,
        "Serialized bytes are unstable after round-trip for {}",
        path.display()
    );

    let estimate2 = sketch2.estimate();
    assert_eq!(
        estimate1,
        estimate2,
        "Estimates differ after round-trip for {}",
        path.display()
    );
}

#[test]
fn test_java_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000];

    for n in test_cases {
        let filename = format!("theta_n{}_java.sk", n);
        let path = serialization_test_data("java_generated_files", &filename);
        test_sketch_file(path, n, false);
    }

    let compressed_test_cases = [10, 100, 1000, 10_000, 100_000, 1_000_000];

    for n in compressed_test_cases {
        let filename = format!("theta_compressed_n{}_java.sk", n);
        let path = serialization_test_data("java_generated_files", &filename);
        test_sketch_file(path, n, true);
    }

    let path =
        serialization_test_data("java_generated_files", "theta_non_empty_no_entries_java.sk");
    test_sketch_file(path, 0, false);
}

#[test]
fn test_cpp_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000];

    for n in test_cases {
        let filename = format!("theta_n{}_cpp.sk", n);
        let path = serialization_test_data("cpp_generated_files", &filename);
        test_sketch_file(path, n, false);
    }

    let compressed_test_cases = [10, 100, 1000, 10_000, 100_000, 1_000_000];

    for n in compressed_test_cases {
        let filename = format!("theta_compressed_n{}_cpp.sk", n);
        let path = serialization_test_data("cpp_generated_files", &filename);
        test_sketch_file(path, n, true);
    }

    let path = serialization_test_data("cpp_generated_files", "theta_non_empty_no_entries_cpp.sk");
    test_sketch_file(path, 0, false);
}
