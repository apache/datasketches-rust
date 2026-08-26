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

use std::fs;
use std::path::Path;

use datasketches::cpc::CpcSketch;
use googletest::assert_that;
use googletest::prelude::near;

use crate::serialization_test_data;

fn test_sketch_file(path: &Path, expected_cardinality: usize) -> CpcSketch {
    let expected = expected_cardinality as f64;

    let bytes = fs::read(path).unwrap();
    let sketch = CpcSketch::deserialize(&bytes).unwrap();
    assert_that!(sketch.estimate(), near(expected, expected * 0.02));

    let serialized_bytes = sketch.serialize();
    let round_trip = CpcSketch::deserialize(&serialized_bytes).unwrap_or_else(|err| {
        panic!(
            "Deserialization failed after round-trip for {}: {}",
            path.display(),
            err
        )
    });

    assert_eq!(
        bytes,
        serialized_bytes,
        "Serialized bytes differ after round-trip for {}",
        path.display()
    );

    assert_eq!(
        sketch.estimate(),
        round_trip.estimate(),
        "Estimates differ after round-trip for {}",
        path.display()
    );

    sketch
}

fn test_sketch_replay(path: &Path, sketch: CpcSketch, inputs: impl Iterator<Item = usize>) {
    let initial_estimate = sketch.estimate();
    let initial_num_coupons = sketch.num_coupons();

    let mut sketch = sketch;
    for value in inputs {
        sketch.update(value);
    }
    assert_eq!(
        initial_num_coupons,
        sketch.num_coupons(),
        "Coupon count changed after replaying input for {}",
        path.display()
    );
    assert_eq!(
        initial_estimate,
        sketch.estimate(),
        "Estimate changed after replaying input for {}",
        path.display()
    );
    assert!(
        sketch.validate(),
        "Sketch became invalid after replaying input for {}",
        path.display()
    );
}

#[test]
fn test_java_compatibility() {
    let test_cases = [0, 100, 200, 2000, 20000];

    for n in test_cases {
        let filename = format!("cpc_n{}_java.sk", n);
        let path = serialization_test_data("java_generated_files", &filename);
        let sketch = test_sketch_file(&path, n);
        test_sketch_replay(&path, sketch, 0..n);
    }
}

#[test]
fn test_cpp_compatibility() {
    let test_cases = [0, 100, 200, 2000, 20000];

    for n in test_cases {
        let filename = format!("cpc_n{}_cpp.sk", n);
        let path = serialization_test_data("cpp_generated_files", &filename);
        let sketch = test_sketch_file(&path, n);
        test_sketch_replay(&path, sketch, 1..=n);
    }
}

#[test]
fn test_go_compatibility() {
    let test_cases = [0, 100, 200, 2000, 20000];

    for n in test_cases {
        let filename = format!("cpc_n{n}_go.sk");
        let path = serialization_test_data("go_generated_files", &filename);
        let sketch = test_sketch_file(&path, n);
        test_sketch_replay(&path, sketch, 0..n);
    }
}
