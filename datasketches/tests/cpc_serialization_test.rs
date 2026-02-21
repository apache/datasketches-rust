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
use datasketches::cpc::CpcSketch;
use googletest::assert_that;
use googletest::prelude::near;

fn test_sketch_file(path: PathBuf, expected_cardinality: usize) {
    let expected = expected_cardinality as f64;

    let bytes = fs::read(&path).unwrap();
    let sketch1 = CpcSketch::deserialize(&bytes).unwrap();
    let estimate1 = sketch1.estimate();
    assert_that!(estimate1, near(expected, expected * 0.02));

    // Serialize and deserialize again to test round-trip
    let serialized_bytes = sketch1.serialize();
    let sketch2 = CpcSketch::deserialize(&serialized_bytes).unwrap_or_else(|err| {
        panic!(
            "Deserialization failed after round-trip for {}: {}",
            path.display(),
            err
        )
    });

    // CpcSketch serialization should be stable
    assert_eq!(
        bytes,
        serialized_bytes,
        "Serialized bytes differ after round-trip for {}",
        path.display()
    );

    // Verify estimates match after round-trip
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
    let test_cases = [0, 100, 200, 2000, 20000];

    for n in test_cases {
        let filename = format!("cpc_n{}_java.sk", n);
        let path = serialization_test_data("java_generated_files", &filename);
        test_sketch_file(path, n);
    }
}

#[test]
fn test_cpp_compatibility() {
    let test_cases = [0, 100, 200, 2000, 20000];

    for n in test_cases {
        let filename = format!("cpc_n{}_cpp.sk", n);
        let path = serialization_test_data("cpp_generated_files", &filename);
        test_sketch_file(path, n);
    }
}
