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

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use datasketches::aod::ArrayOfDoublesSketchBuilder;
use datasketches::aod::ArrayOfNumbersSketchBuilder;
use datasketches::aod::CompactArrayOfDoublesSketch;
use datasketches::aod::CompactArrayOfNumbersSketch;
use datasketches::error::ErrorKind;
use tests_integration::MAX_THETA;
use tests_integration::ZERO_HASH_SEED;

use crate::serialization_test_data;

const N_STD: [u64; 8] = [0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000];

fn test_sketch_file(path: PathBuf, expected_cardinality: u64, num_values: u8) {
    let bytes = fs::read(&path).unwrap();
    let sketch = CompactArrayOfDoublesSketch::deserialize(&bytes)
        .unwrap_or_else(|err| panic!("deserialization failed for {}: {err}", path.display()));

    assert_eq!(sketch.num_values(), num_values, "{}", path.display());
    assert_eq!(
        sketch.is_empty(),
        expected_cardinality == 0,
        "{}",
        path.display()
    );
    let expected = expected_cardinality as f64;
    let tolerance = expected * 0.03;
    assert!(
        (sketch.estimate() - expected).abs() <= tolerance,
        "estimate {} differs from {expected} for {}",
        sketch.estimate(),
        path.display()
    );
    for entry in sketch.iter() {
        assert_eq!(entry.values().len(), usize::from(num_values));
    }

    let serialized = sketch.serialize();
    let restored = CompactArrayOfDoublesSketch::deserialize(&serialized).unwrap();
    assert_eq!(restored.serialize(), serialized, "{}", path.display());
    assert_eq!(restored.estimate(), sketch.estimate());
    for (expected, actual) in sketch.iter().zip(restored.iter()) {
        assert_eq!(actual.hash(), expected.hash());
        assert_eq!(actual.values(), expected.values());
    }
}

#[test]
fn java_compatibility() {
    for num_values in [1, 3] {
        for n in N_STD {
            let filename = format!("aod_{num_values}_n{n}_java.sk");
            test_sketch_file(
                serialization_test_data("java_generated_files", &filename),
                n,
                num_values,
            );
        }
    }
}

#[test]
fn cpp_compatibility() {
    for num_values in [1, 3] {
        for n in N_STD {
            let filename = format!("aod_{num_values}_n{n}_cpp.sk");
            test_sketch_file(
                serialization_test_data("cpp_generated_files", &filename),
                n,
                num_values,
            );
        }
    }
}

#[test]
fn go_compatibility() {
    for num_values in [1, 3] {
        for n in N_STD {
            let filename = format!("aod_{num_values}_n{n}_go.sk");
            test_sketch_file(
                serialization_test_data("go_generated_files", &filename),
                n,
                num_values,
            );
        }
    }
}

#[test]
fn rust_compatibility() {
    for num_values in [1, 3] {
        for n in N_STD {
            let filename = format!("aod_{num_values}_n{n}_rust.sk");
            let path = serialization_test_data("rust_generated_files", &filename);
            let bytes = fs::read(&path).unwrap();
            let sketch = CompactArrayOfDoublesSketch::deserialize(&bytes).unwrap();
            assert!(sketch.is_ordered(), "{filename}");
            assert_eq!(sketch.serialize(), bytes, "roundtrip: {filename}");
            test_sketch_file(path, n, num_values);
        }
    }
}

fn entries(path: &Path) -> BTreeMap<u64, Vec<f64>> {
    let bytes = fs::read(path).unwrap();
    CompactArrayOfDoublesSketch::deserialize(&bytes)
        .unwrap()
        .iter()
        .map(|entry| (entry.hash(), entry.values().to_vec()))
        .collect()
}

#[test]
fn rust_serialization_matches_cpp() {
    for num_values in [1, 3] {
        for n in N_STD {
            let name = format!("aod_{num_values}_n{n}");
            let rust = fs::read(serialization_test_data(
                "rust_generated_files",
                &format!("{name}_rust.sk"),
            ))
            .unwrap();
            let cpp = fs::read(serialization_test_data(
                "cpp_generated_files",
                &format!("{name}_cpp.sk"),
            ))
            .unwrap();
            assert_eq!(rust, cpp, "{name}");
        }
    }

    let rust = fs::read(serialization_test_data(
        "rust_generated_files",
        "aod_1_non_empty_no_entries_rust.sk",
    ))
    .unwrap();
    let cpp = fs::read(serialization_test_data(
        "cpp_generated_files",
        "aod_1_non_empty_no_entries_cpp.sk",
    ))
    .unwrap();
    assert_eq!(rust, cpp, "aod_1_non_empty_no_entries");
}

#[test]
fn cross_language_summaries_match_by_hash() {
    let cpp = entries(&serialization_test_data(
        "cpp_generated_files",
        "aod_3_n10_cpp.sk",
    ));
    let java = entries(&serialization_test_data(
        "java_generated_files",
        "aod_3_n10_java.sk",
    ));
    let rust = entries(&serialization_test_data(
        "rust_generated_files",
        "aod_3_n10_rust.sk",
    ));
    let go = entries(&serialization_test_data(
        "go_generated_files",
        "aod_3_n10_go.sk",
    ));

    assert_eq!(cpp.len(), 10);
    assert_eq!(java, cpp);
    assert_eq!(rust, cpp);
    assert_eq!(go, cpp);
}

#[test]
fn non_empty_images_without_entries_preserve_state_and_require_seed() {
    for (directory, suffix) in [
        ("cpp_generated_files", "cpp"),
        ("go_generated_files", "go"),
        ("java_generated_files", "java"),
        ("rust_generated_files", "rust"),
    ] {
        let filename = format!("aod_1_non_empty_no_entries_{suffix}.sk");
        let path = serialization_test_data(directory, &filename);
        let bytes = fs::read(&path).unwrap();
        let sketch = CompactArrayOfDoublesSketch::deserialize(&bytes).unwrap();

        assert!(!sketch.is_empty(), "{filename}");
        assert_eq!(sketch.num_retained(), 0, "{filename}");
        assert!(sketch.theta64() < MAX_THETA, "{filename}");
        assert_eq!(sketch.num_values(), 1, "{filename}");
        let serialized = sketch.serialize();
        assert_eq!(
            CompactArrayOfDoublesSketch::deserialize(&serialized)
                .unwrap()
                .serialize(),
            serialized
        );
        let error = CompactArrayOfDoublesSketch::deserialize_with_seed(&bytes, 8).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData, "{filename}");
    }
}

#[test]
fn empty_serialization_matches_the_cpp_canonical_image() {
    let expected = fs::read(serialization_test_data(
        "cpp_generated_files",
        "aod_1_n0_cpp.sk",
    ))
    .unwrap();
    let sketch = ArrayOfDoublesSketchBuilder::default()
        .seed(123)
        .build()
        .unwrap()
        .compact(true);
    assert_eq!(sketch.serialize(), expected);
}

#[test]
fn empty_deserialization_uses_the_supplied_seed() {
    let expected = fs::read(serialization_test_data(
        "java_generated_files",
        "aod_1_n0_java.sk",
    ))
    .unwrap();
    let other_seed_hash = ArrayOfDoublesSketchBuilder::default()
        .build()
        .unwrap()
        .seed_hash();
    let expected_seed_hash = ArrayOfDoublesSketchBuilder::default()
        .seed(123)
        .build()
        .unwrap()
        .seed_hash();

    let go_bytes = fs::read(serialization_test_data(
        "go_generated_files",
        "aod_1_n0_go.sk",
    ))
    .unwrap();
    assert_ne!(u16::from_le_bytes(go_bytes[6..8].try_into().unwrap()), 0);
    let go_restored = CompactArrayOfDoublesSketch::deserialize_with_seed(&go_bytes, 123).unwrap();
    assert!(go_restored.is_empty());
    assert_eq!(go_restored.seed_hash(), expected_seed_hash);

    for stored_seed_hash in [0, other_seed_hash] {
        let mut bytes = expected.clone();
        bytes[6..8].copy_from_slice(&stored_seed_hash.to_le_bytes());
        let restored = CompactArrayOfDoublesSketch::deserialize_with_seed(&bytes, 123).unwrap();
        assert!(restored.is_empty());
        assert_eq!(restored.seed_hash(), expected_seed_hash);
    }

    let error =
        CompactArrayOfDoublesSketch::deserialize_with_seed(&expected, ZERO_HASH_SEED).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
}

fn assert_invalid_data(bytes: &[u8]) {
    let error = CompactArrayOfDoublesSketch::deserialize(bytes).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
}

#[test]
fn truncated_images_are_rejected() {
    let bytes = fs::read(serialization_test_data(
        "rust_generated_files",
        "aod_3_n10_rust.sk",
    ))
    .unwrap();
    for end in [0, 1, 7, 15, 16, 23, 24, bytes.len() - 1] {
        assert_invalid_data(&bytes[..end]);
    }
}

#[test]
fn invalid_header_fields_are_rejected() {
    let bytes = fs::read(serialization_test_data(
        "rust_generated_files",
        "aod_3_n10_rust.sk",
    ))
    .unwrap();

    for (index, value) in [(0, 2), (1, 2), (2, 3), (3, 1), (5, 0)] {
        let mut corrupted = bytes.clone();
        corrupted[index] = value;
        assert_invalid_data(&corrupted);
    }

    let mut empty_with_entries = bytes.clone();
    empty_with_entries[4] |= 1 << 2;
    assert_invalid_data(&empty_with_entries);

    let mut zero_hash_sketch = ArrayOfDoublesSketchBuilder::default()
        .seed(0)
        .num_values(2)
        .build()
        .unwrap();
    zero_hash_sketch.update((), [1.0, 2.0]).unwrap();
    assert!(!zero_hash_sketch.is_empty());
    assert_eq!(zero_hash_sketch.num_retained(), 0);
    assert_eq!(zero_hash_sketch.theta64(), MAX_THETA);
    let zero_hash_bytes = zero_hash_sketch.compact(true).serialize();
    let zero_hash_restored =
        CompactArrayOfDoublesSketch::deserialize_with_seed(&zero_hash_bytes, 0).unwrap();
    assert!(!zero_hash_restored.is_empty());
    assert_eq!(zero_hash_restored.num_retained(), 0);
    assert_eq!(zero_hash_restored.theta64(), MAX_THETA);

    let mut zero_theta = bytes.clone();
    zero_theta[8..16].fill(0);
    assert_invalid_data(&zero_theta);

    let mut excessive_num_values = bytes.clone();
    excessive_num_values[5] = 4;
    assert_invalid_data(&excessive_num_values);

    let error = CompactArrayOfDoublesSketch::deserialize_with_seed(&bytes, 8).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
}

#[test]
fn invalid_entry_counts_are_rejected_before_allocation() {
    let bytes = fs::read(serialization_test_data(
        "rust_generated_files",
        "aod_3_n10_rust.sk",
    ))
    .unwrap();

    let mut zero_entries = bytes.clone();
    zero_entries[16..20].fill(0);
    assert_invalid_data(&zero_entries);

    let mut excessive_entries = bytes.clone();
    excessive_entries[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_invalid_data(&excessive_entries);
}

#[test]
fn invalid_hashes_are_rejected() {
    let bytes = fs::read(serialization_test_data(
        "rust_generated_files",
        "aod_3_n10_rust.sk",
    ))
    .unwrap();

    let mut zero_hash = bytes.clone();
    zero_hash[24..32].fill(0);
    assert_invalid_data(&zero_hash);

    let mut hash_above_theta = bytes.clone();
    hash_above_theta[24..32].copy_from_slice(&MAX_THETA.to_le_bytes());
    assert_invalid_data(&hash_above_theta);

    let mut unordered_hashes = bytes.clone();
    let first_hash: [u8; 8] = unordered_hashes[24..32].try_into().unwrap();
    let second_hash: [u8; 8] = unordered_hashes[32..40].try_into().unwrap();
    unordered_hashes[24..32].copy_from_slice(&second_hash);
    unordered_hashes[32..40].copy_from_slice(&first_hash);
    assert_invalid_data(&unordered_hashes);
}

#[test]
fn duplicate_hashes_are_rejected_in_unordered_images() {
    let mut sketch = ArrayOfDoublesSketchBuilder::default()
        .num_values(3)
        .build()
        .unwrap();
    for key in 0..10 {
        sketch.update(key, vec![key as f64; 3]).unwrap();
    }
    let mut bytes = sketch.compact(false).serialize();
    assert_eq!(bytes[4] & (1 << 4), 0);
    let first_hash: [u8; 8] = bytes[24..32].try_into().unwrap();
    bytes[32..40].copy_from_slice(&first_hash);

    assert_invalid_data(&bytes);
}

#[test]
fn direct_union_serialization_matches_compact_sketch_path() {
    use datasketches::aod::ArrayOfDoublesUnionBuilder;

    let empty = ArrayOfDoublesUnionBuilder::default()
        .num_values(2)
        .build()
        .unwrap();
    for ordered in [true, false] {
        assert_eq!(
            empty.serialize(ordered),
            empty.to_sketch(ordered).serialize()
        );
    }

    let seed = 123;
    for (lg_k, union_p, input_p, count) in [
        (5, 1.0, 1.0, 0_u64),
        (5, 1.0, 1.0, 1),
        (5, 1.0, 1.0, 20),
        (5, 1.0, 1.0, 5_000),
        (10, 1.0, 1.0, 5_000),
        (8, 0.5, 1.0, 5_000),
        (5, 1e-12, 1.0, 10),
        (5, 1.0, 0.1, 200),
    ] {
        let mut a = ArrayOfDoublesSketchBuilder::default()
            .num_values(2)
            .seed(seed)
            .build()
            .unwrap();
        let mut b = ArrayOfDoublesSketchBuilder::default()
            .num_values(2)
            .seed(seed)
            .sampling_probability(input_p)
            .build()
            .unwrap();
        for key in 0..count {
            a.update(key, [1.0, key as f64]).unwrap();
            b.update(key + count / 2, [2.0, 10.0]).unwrap();
        }

        let mut union = ArrayOfDoublesUnionBuilder::default()
            .num_values(2)
            .seed(seed)
            .lg_k(lg_k)
            .sampling_probability(union_p)
            .build()
            .unwrap();
        union.update(&a).unwrap();
        union.update(&b).unwrap();

        for ordered in [true, false] {
            let direct = union.serialize(ordered);
            let via_sketch = union.to_sketch(ordered).serialize();
            if ordered {
                assert_eq!(direct, via_sketch);
            }
            let decoded_direct =
                CompactArrayOfDoublesSketch::deserialize_with_seed(&direct, seed).unwrap();
            let decoded_sketch =
                CompactArrayOfDoublesSketch::deserialize_with_seed(&via_sketch, seed).unwrap();
            assert_eq!(decoded_direct.is_empty(), decoded_sketch.is_empty());
            assert_eq!(decoded_direct.is_ordered(), decoded_sketch.is_ordered());
            assert_eq!(decoded_direct.theta64(), decoded_sketch.theta64());
            assert_eq!(decoded_direct.seed_hash(), decoded_sketch.seed_hash());
            let map_direct: BTreeMap<_, _> = decoded_direct
                .iter()
                .map(|e| (e.hash(), e.values().to_vec()))
                .collect();
            let map_sketch: BTreeMap<_, _> = decoded_sketch
                .iter()
                .map(|e| (e.hash(), e.values().to_vec()))
                .collect();
            assert_eq!(map_direct, map_sketch);
        }
    }
}

// Generated by datasketches-go 3055e847084bd72c7738a1dfd85a5714f25934e1 using
// `NewArrayOfNumbersUpdateSketch[float32](3)` with keys and values 0 through 9.
const GO_F32_N10_HEX: &str = concat!(
    "010109031803cc93ffffffffffffff7f0a0000000000000015f97dcbbd86a105",
    "40de2ee1c9db3d08698bb991b8685708fe162113fb98bc10bd3273724691cc14",
    "c397fc1281709d1ee56b61eec88044201ad1300b998c2f22ba40b3c1da06695d",
    "e0f48bea9983c37c0000803f0000803f0000803f000080400000804000008040",
    "0000004100000041000000410000c0400000c0400000c0400000a0400000a040",
    "0000a04000000040000000400000004000000000000000000000000000001041",
    "00001041000010410000404000004040000040400000e0400000e0400000e040",
);

fn hex_bytes(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0);
    (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).unwrap())
        .collect()
}

#[test]
fn f32_serialization_matches_go() {
    let expected = hex_bytes(GO_F32_N10_HEX);
    assert_eq!(expected.len(), 224);

    let mut sketch = ArrayOfNumbersSketchBuilder::<f32>::default()
        .num_values(3)
        .build()
        .unwrap();
    for key in 0..10 {
        sketch.update(key as i64, [key as f32; 3]).unwrap();
    }
    let compact = sketch.compact(true);
    assert_eq!(compact.serialize(), expected);

    let restored = CompactArrayOfNumbersSketch::<f32>::deserialize(&expected).unwrap();
    assert_eq!(restored.serialize(), expected);
    let expected_entries: BTreeMap<_, _> = compact
        .iter()
        .map(|entry| (entry.hash(), entry.values().to_vec()))
        .collect();
    let actual_entries: BTreeMap<_, _> = restored
        .iter()
        .map(|entry| (entry.hash(), entry.values().to_vec()))
        .collect();
    assert_eq!(actual_entries, expected_entries);
}

#[test]
fn compact_round_trip_preserves_values() {
    let mut sketch = ArrayOfDoublesSketchBuilder::default()
        .num_values(3)
        .build()
        .unwrap();
    for i in 0..100 {
        sketch
            .update(i, [i as f64, (i * 2) as f64, (i * 3) as f64])
            .unwrap();
    }
    for ordered in [false, true] {
        let bytes = sketch.compact(ordered).serialize();
        let restored = CompactArrayOfDoublesSketch::deserialize(&bytes).unwrap();
        assert_eq!(restored.is_ordered(), ordered);
        assert_eq!(restored.serialize(), bytes);

        let mut expected: Vec<_> = sketch
            .iter()
            .map(|entry| (entry.hash(), entry.values().to_vec()))
            .collect();
        expected.sort_unstable_by_key(|entry| entry.0);
        let mut actual: Vec<_> = restored
            .iter()
            .map(|entry| (entry.hash(), entry.values().to_vec()))
            .collect();
        actual.sort_unstable_by_key(|entry| entry.0);
        assert_eq!(actual, expected);
    }
}
