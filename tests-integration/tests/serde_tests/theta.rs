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
use tests_integration::ZERO_HASH_SEED;

use crate::serialization_test_data;

fn serialize_v2_exact(entries: &[u64]) -> Vec<u8> {
    let current = ThetaSketchBuilder::default().build().unwrap().compact(true);
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
fn empty_images_use_canonical_seed_hash() {
    let expected = fs::read(serialization_test_data(
        "cpp_generated_files",
        "theta_n0_cpp.sk",
    ))
    .unwrap();

    for builder in [
        ThetaSketchBuilder::default(),
        ThetaSketchBuilder::default()
            .seed(123)
            .sampling_probability(0.5),
    ] {
        let sketch = builder.build().unwrap().compact(false);
        assert_eq!(sketch.serialize(), expected);
        assert_eq!(sketch.serialize_compressed(), expected);

        // Older Rust images stored the configured seed hash even when empty.
        let mut legacy = expected.clone();
        legacy[6..8].copy_from_slice(&sketch.seed_hash().to_le_bytes());
        for bytes in [&expected, &legacy] {
            let restored = CompactThetaSketch::deserialize_with_seed(bytes, 456).unwrap();
            assert!(restored.is_empty());
            assert_eq!(restored.serialize(), expected);
            assert_eq!(restored.serialize_compressed(), expected);
        }
    }
}

#[test]
fn non_empty_images_without_entries_preserve_seed_hash() {
    let mut sketch = ThetaSketchBuilder::default()
        .seed(123)
        .sampling_probability(1e-12)
        .build()
        .unwrap();
    sketch.update("apple");
    let compact = sketch.compact(true);
    assert!(!compact.is_empty());
    assert_eq!(compact.num_retained(), 0);

    for bytes in [compact.serialize(), compact.serialize_compressed()] {
        let restored = CompactThetaSketch::deserialize_with_seed(&bytes, 123).unwrap();
        assert!(!restored.is_empty());
        assert_eq!(restored.num_retained(), 0);
        assert_eq!(restored.theta64(), compact.theta64());
        assert_eq!(restored.seed_hash(), compact.seed_hash());
        let err = CompactThetaSketch::deserialize(&bytes).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }
}

#[test]
fn malformed_input_is_rejected() {
    let mut sketch = ThetaSketchBuilder::default().lg_k(5).build().unwrap();
    for value in 0..5000 {
        sketch.update(value);
    }
    let bytes = sketch.compact(true).serialize();

    let truncated = &bytes[..bytes.len() - 1];
    let err = CompactThetaSketch::deserialize(truncated).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);

    let err = CompactThetaSketch::deserialize_with_seed(&bytes, 8).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);

    let err = CompactThetaSketch::deserialize_with_seed(&bytes, ZERO_HASH_SEED).unwrap_err();
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
fn declared_entry_payload_is_checked_before_allocating() {
    let mut uncompressed = serialize_v2_exact(&[1]);
    uncompressed[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(CompactThetaSketch::deserialize(&uncompressed).is_err());

    let mut sketch = ThetaSketchBuilder::default().lg_k(5).build().unwrap();
    for value in 0..5000 {
        sketch.update(value);
    }
    let compressed = sketch.compact(true).serialize_compressed();

    let mut invalid_entry_width = compressed.clone();
    invalid_entry_width[3] = 64;
    assert!(CompactThetaSketch::deserialize(&invalid_entry_width).is_err());

    let mut oversized_entry_count = compressed;
    let count_offset = usize::from(oversized_entry_count[0]) * size_of::<u64>();
    oversized_entry_count[4] = 4;
    oversized_entry_count[count_offset..count_offset + size_of::<u32>()].fill(u8::MAX);
    assert!(CompactThetaSketch::deserialize(&oversized_entry_count).is_err());
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
fn test_v2_empty_images_ignore_seed_hash() {
    let reference = ThetaSketchBuilder::default()
        .seed(123)
        .build()
        .unwrap()
        .compact(true);
    let expected = reference.serialize();

    // Java accepts empty v2 images with one, two, or three preamble longs.
    for pre_longs in [1, 2, 3] {
        let mut bytes = serialize_v2_exact(&[]);
        bytes[0] = pre_longs;
        if pre_longs == 1 {
            bytes.truncate(8);
        } else if pre_longs == 3 {
            bytes.extend_from_slice(&(i64::MAX as u64).to_le_bytes());
        }

        for seed_hash in [0, reference.seed_hash()] {
            bytes[6..8].copy_from_slice(&seed_hash.to_le_bytes());
            for seed in [123, 456] {
                let sketch = CompactThetaSketch::deserialize_with_seed(&bytes, seed).unwrap();
                assert!(sketch.is_empty());
                assert!(!sketch.is_estimation_mode());
                assert_eq!(sketch.num_retained(), 0);
                assert_eq!(sketch.estimate(), 0.0);
                assert_eq!(sketch.lower_bound(NumStdDev::One), 0.0);
                assert_eq!(sketch.upper_bound(NumStdDev::One), 0.0);
                assert_eq!(sketch.seed_hash(), 0);
                assert_eq!(sketch.serialize(), expected);

                let restored =
                    CompactThetaSketch::deserialize_with_seed(&sketch.serialize(), seed).unwrap();
                assert!(restored.is_empty());
                assert_eq!(restored.seed_hash(), 0);
            }
        }
    }
}

#[test]
fn test_v2_non_empty_images_validate_seed_hash() {
    let exact = serialize_v2_exact(&[1]);
    let mut sampled = serialize_v2_exact(&[]);
    sampled[0] = 3;
    sampled.extend_from_slice(&((i64::MAX as u64) / 2).to_le_bytes());

    // Zero retained entries with theta < 1 still describe a non-empty sketch.
    for mut bytes in [exact, sampled] {
        let sketch = CompactThetaSketch::deserialize(&bytes).unwrap();
        assert!(!sketch.is_empty());

        let err = CompactThetaSketch::deserialize_with_seed(&bytes, 456).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);

        bytes[6..8].fill(0);
        let err = CompactThetaSketch::deserialize(&bytes).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }
}
