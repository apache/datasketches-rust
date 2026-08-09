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

use datasketches::codec::SketchBytes;
use datasketches::common::NumStdDev;
use datasketches::error::ErrorKind;
use datasketches::theta::CompactThetaSketch;
use datasketches::theta::ThetaSketchBuilder;
use googletest::assert_that;
use googletest::prelude::near;

use crate::serialization_test_data;

fn serialize_v2_exact(entries: &[u64]) -> Vec<u8> {
    let current = ThetaSketchBuilder::default().build().compact(true);
    let current_bytes = current.serialize();
    let mut bytes = SketchBytes::with_capacity((2 + entries.len()) * size_of::<u64>());
    bytes.write_u8(2); // preamble longs
    bytes.write_u8(2); // serialization version
    bytes.write_u8(current_bytes[2]); // theta family ID
    bytes.write_u8(0); // unused
    bytes.write_u16_le(0); // unused
    bytes.write_u16_le(current.seed_hash());
    bytes.write_u32_le(entries.len() as u32);
    bytes.write_u32_le(0); // unused
    for &entry in entries {
        bytes.write_u64_le(entry);
    }
    bytes.into_bytes()
}

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

    // Theta snapshots from other implementations are not required to match byte-for-byte output
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

#[test]
fn test_go_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000];

    for n in test_cases {
        let filename = format!("theta_n{n}_go.sk");
        let path = serialization_test_data("go_generated_files", &filename);
        test_sketch_file(path, n, false);
    }

    let compressed_test_cases = [10, 100, 1000, 10_000, 100_000, 1_000_000];

    for n in compressed_test_cases {
        let filename = format!("theta_compressed_n{n}_go.sk");
        let path = serialization_test_data("go_generated_files", &filename);
        test_sketch_file(path, n, true);
    }

    let path = serialization_test_data("go_generated_files", "theta_non_empty_no_entries_go.sk");
    test_sketch_file(path, 0, false);
}

#[test]
fn malformed_input_is_rejected() {
    let mut sketch = ThetaSketchBuilder::default().lg_k(5).build();
    for value in 0..5000 {
        sketch.update(value);
    }
    let bytes = sketch.compact(true).serialize();

    let truncated = &bytes[..bytes.len() - 1];
    let err = CompactThetaSketch::deserialize(truncated).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);

    let err = CompactThetaSketch::deserialize_with_seed(&bytes, 8).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);

    let mut wrong_family = bytes.clone();
    wrong_family[2] = 0;
    let err = CompactThetaSketch::deserialize(&wrong_family).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);

    let mut unsupported_version = bytes;
    unsupported_version[1] = 99;
    let err = CompactThetaSketch::deserialize(&unsupported_version).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);
}

#[test]
fn test_v2_exact_non_empty_compatibility() {
    let entries = [1, 7, 42];
    let sketch = CompactThetaSketch::deserialize(&serialize_v2_exact(&entries)).unwrap();

    assert!(!sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert!(sketch.is_ordered());
    assert_eq!(sketch.num_retained(), entries.len());
    assert_eq!(sketch.estimate(), entries.len() as f64);
    assert_eq!(sketch.lower_bound(NumStdDev::One), entries.len() as f64);
    assert_eq!(sketch.upper_bound(NumStdDev::One), entries.len() as f64);
    assert_eq!(
        sketch.iter().map(|entry| entry.hash()).collect::<Vec<_>>(),
        entries
    );

    let restored = CompactThetaSketch::deserialize(&sketch.serialize()).unwrap();
    assert!(!restored.is_empty());
    assert_eq!(restored.num_retained(), entries.len());
    assert_eq!(restored.estimate(), entries.len() as f64);
}

#[test]
fn test_v2_exact_zero_entries_remains_empty() {
    let sketch = CompactThetaSketch::deserialize(&serialize_v2_exact(&[])).unwrap();

    assert!(sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.num_retained(), 0);
    assert_eq!(sketch.estimate(), 0.0);
    assert_eq!(sketch.lower_bound(NumStdDev::One), 0.0);
    assert_eq!(sketch.upper_bound(NumStdDev::One), 0.0);
}
