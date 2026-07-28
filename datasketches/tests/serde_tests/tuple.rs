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
use std::path::PathBuf;

use datasketches::error::ErrorKind;
use datasketches::tuple::CompactTupleSketch;
use datasketches::tuple::DefaultUpdatePolicy;
use datasketches::tuple::TupleSketchBuilder;
use googletest::assert_that;
use googletest::prelude::near;

use crate::assert_truncated_inputs_rejected;
use crate::serialization_test_data;

fn test_sketch_file(path: PathBuf, expected_cardinality: usize) {
    let expected = expected_cardinality as f64;

    let bytes = fs::read(&path).unwrap();
    let sketch1 = CompactTupleSketch::<i32>::deserialize(&bytes)
        .unwrap_or_else(|err| panic!("Deserialization failed for {}: {}", path.display(), err));

    assert_eq!(
        sketch1.is_empty(),
        expected_cardinality == 0,
        "Unexpected is_empty for {}",
        path.display()
    );

    let estimate1 = sketch1.estimate();
    assert_that!(estimate1, near(expected, expected * 0.03));

    // Snapshots from Java/C++ are not required to match byte-for-byte output from this
    // implementation. Verify our own serialization is stable across a round-trip instead.
    let serialized_bytes = sketch1.serialize();
    let sketch2 = CompactTupleSketch::<i32>::deserialize(&serialized_bytes).unwrap_or_else(|err| {
        panic!(
            "Deserialization failed after round-trip for {}: {}",
            path.display(),
            err
        )
    });

    let serialized_bytes2 = sketch2.serialize();
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
        let filename = format!("tuple_int_n{}_java.sk", n);
        let path = serialization_test_data("java_generated_files", &filename);
        test_sketch_file(path, n);
    }
}

#[test]
fn test_cpp_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000];

    for n in test_cases {
        let filename = format!("tuple_int_n{}_cpp.sk", n);
        let path = serialization_test_data("cpp_generated_files", &filename);
        test_sketch_file(path, n);
    }
}

#[test]
fn malformed_input_is_rejected() {
    let mut sketch = TupleSketchBuilder::new(DefaultUpdatePolicy::<u64>::default())
        .lg_k(5)
        .build();
    for value in 0..100 {
        sketch.update(value, 1);
    }
    let bytes = sketch.compact(true).serialize();

    assert_truncated_inputs_rejected(&bytes, CompactTupleSketch::<u64>::deserialize);

    let err = CompactTupleSketch::<u64>::deserialize_with_seed(&bytes, 8).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);

    let mut wrong_family = bytes;
    wrong_family[2] = 0;
    let err = CompactTupleSketch::<u64>::deserialize(&wrong_family).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);
}
