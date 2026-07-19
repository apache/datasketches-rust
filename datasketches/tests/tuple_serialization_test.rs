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

//! Cross-language compatibility tests for Tuple sketch serialization.
//!
//! The fixtures are produced by the upstream Java and C++ generators (see
//! `tools/generate_serialization_test_data.py`):
//!
//! * Java: `TupleCrossLanguageTest.generateForCppIntegerSummary` writes `tuple_int_n{n}_java.sk`
//!   using its `IntegerSummary`.
//! * C++: `tuple_sketch_serialize_for_java.cpp` writes `tuple_int_n{n}_cpp.sk` using an `int`
//!   summary.
//!
//! Both build a tuple sketch with `update(i, i)` for `i` in `0..n`, so the summary is a 4-byte
//! little-endian signed integer — exactly what the `i32` [`TupleSummaryValue`] implementation
//! reads. The `aod_*`/`aos_*` fixtures use Array-of-Doubles / Array-of-Strings summaries, which
//! this crate does not implement, so they are intentionally not covered here.

#![cfg(feature = "tuple")]

mod common;

use std::fs;
use std::path::PathBuf;

use common::serialization_test_data;
use datasketches::tuple::CompactTupleSketch;
use googletest::assert_that;
use googletest::prelude::near;

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
