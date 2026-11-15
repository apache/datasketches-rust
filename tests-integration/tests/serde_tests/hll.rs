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

use datasketches::common::NumStdDev;
use datasketches::hash::value::natural_extend;
use datasketches::hll::HllSketch;
use datasketches::hll::HllType;
use datasketches::hll::HllUnion;
use googletest::assert_that;
use googletest::prelude::all;
use googletest::prelude::ge;
use googletest::prelude::le;
use googletest::prelude::lt;
use googletest::prelude::near;

use crate::serialization_test_data;

#[test]
fn union_serialization_preserves_result_and_subsequent_updates() {
    for hll_type in [HllType::Hll4, HllType::Hll6, HllType::Hll8] {
        for count in [0, 4, 100, 10_000_u64] {
            let mut source = HllSketch::new(12, hll_type).unwrap();
            for value in 0..count {
                source.update(value);
            }
            for lg_max_k in [8, 12, 14] {
                let mut union = HllUnion::new(lg_max_k).unwrap();
                union.update(&source);
                let bytes = union.serialize(HllType::Hll8);
                assert_eq!(bytes, union.to_sketch(HllType::Hll8).serialize());

                let mut restored = HllSketch::deserialize(&bytes).unwrap();
                assert_eq!(restored.target_type(), HllType::Hll8);
                assert_eq!(restored.lg_config_k(), union.lg_config_k());
                assert_eq!(restored.estimate(), union.estimate());

                for value in count..count + 1_000 {
                    union.update_value(value);
                    restored.update(value);
                }
                assert_eq!(union.to_sketch(HllType::Hll8), restored);

                union.reset();
                for target_type in [HllType::Hll4, HllType::Hll6, HllType::Hll8] {
                    let empty = HllSketch::deserialize(&union.serialize(target_type)).unwrap();
                    assert!(empty.is_empty());
                    assert_eq!(empty.target_type(), target_type);
                    assert_eq!(empty.lg_config_k(), lg_max_k);
                }
            }
        }
    }
}

fn assert_union_matches_sketch(
    union: &HllUnion,
    target_type: HllType,
    check_bytes_when_single_aux: bool,
) {
    let bytes = union.serialize(target_type);
    let expected = union.to_sketch(target_type);
    if check_bytes_when_single_aux {
        let dense_offset = 40 + (1usize << (union.lg_config_k() - 1));
        let single_or_no_aux =
            target_type != HllType::Hll4 || bytes.len() <= dense_offset + size_of::<u32>();
        if single_or_no_aux {
            assert_eq!(
                bytes,
                expected.serialize(),
                "byte mismatch for target_type={target_type:?}, lg_k={}",
                union.lg_config_k()
            );
        }
    }

    let restored = HllSketch::deserialize(&bytes).unwrap();
    assert_eq!(restored.target_type(), target_type);
    assert_eq!(restored.lg_config_k(), expected.lg_config_k());
    assert_eq!(restored.is_empty(), expected.is_empty());
    assert_eq!(restored, expected);
    assert_eq!(restored.estimate(), expected.estimate());
    assert_eq!(restored.composite_estimate(), expected.composite_estimate());
    assert_eq!(restored.estimate(), union.estimate());
    assert_eq!(restored.composite_estimate(), union.composite_estimate());
    for num_std_dev in [NumStdDev::One, NumStdDev::Two, NumStdDev::Three] {
        assert_eq!(
            restored.lower_bound(num_std_dev),
            expected.lower_bound(num_std_dev)
        );
        assert_eq!(
            restored.upper_bound(num_std_dev),
            expected.upper_bound(num_std_dev)
        );
        assert_eq!(
            restored.lower_bound(num_std_dev),
            union.lower_bound(num_std_dev)
        );
        assert_eq!(
            restored.upper_bound(num_std_dev),
            union.upper_bound(num_std_dev)
        );
    }
}

#[test]
fn union_serialization_handles_all_modes_and_target_types() {
    for count in [0, 4, 100, 10_000_u64] {
        let mut union = HllUnion::new(12).unwrap();
        for value in 0..count {
            union.update_value(value);
        }
        for target_type in [HllType::Hll4, HllType::Hll6, HllType::Hll8] {
            assert_union_matches_sketch(&union, target_type, true);
        }
    }
}

#[test]
fn union_serialization_cold_and_warm_composite_states_match() {
    let build_composite_union = || {
        let mut union = HllUnion::new(12).unwrap();
        for (start, end) in [(0_u64, 3_000), (3_000, 6_000)] {
            let mut sketch = HllSketch::new(12, HllType::Hll8).unwrap();
            for value in start..end {
                sketch.update(value);
            }
            union.update(&sketch);
        }
        union
    };

    for target_type in [HllType::Hll4, HllType::Hll6, HllType::Hll8] {
        // Cold: serialize before calling any estimate or to_sketch on this union.
        let cold_union = build_composite_union();
        let cold_bytes = cold_union.serialize(target_type);

        // Warm: populate lazy estimator cache before serializing.
        let warm_union = build_composite_union();
        let _ = warm_union.composite_estimate();
        let warm_bytes = warm_union.serialize(target_type);

        assert_eq!(cold_bytes, warm_bytes);
        assert_union_matches_sketch(&cold_union, target_type, true);
    }
}

#[test]
fn union_serialization_handles_downsampled_unions() {
    // Single-sketch downsample keeps HIP mode.
    let mut wide = HllSketch::new(14, HllType::Hll8).unwrap();
    for value in 0..10_000_u64 {
        wide.update(value);
    }
    let mut hip_downsampled = HllUnion::new(10).unwrap();
    hip_downsampled.update(&wide);
    assert_eq!(hip_downsampled.lg_config_k(), 10);
    for target_type in [HllType::Hll4, HllType::Hll6, HllType::Hll8] {
        assert_union_matches_sketch(&hip_downsampled, target_type, true);
    }

    // Multi-sketch downsample merges into dense OOO Array8 at the smaller lg_k.
    let mut narrow = HllSketch::new(8, HllType::Hll4).unwrap();
    for value in 10_000..15_000_u64 {
        narrow.update(value);
    }
    let mut ooo_downsampled = HllUnion::new(12).unwrap();
    ooo_downsampled.update(&wide);
    ooo_downsampled.update(&narrow);
    assert_eq!(ooo_downsampled.lg_config_k(), 8);
    for target_type in [HllType::Hll4, HllType::Hll6, HllType::Hll8] {
        assert_union_matches_sketch(&ooo_downsampled, target_type, true);
    }
}

#[test]
fn union_serialization_hll4_handles_shifted_cur_min_and_multi_aux() {
    // Keys verified at lg_k = 4 to hit distinct slots with register value >= 22.
    const HIGH_VALUE_KEYS: [u64; 4] = [503_824, 2_048_382, 2_144_141, 9_410_086];

    let mut direct_union = HllUnion::new(4).unwrap();
    for value in 0..200_u64 {
        direct_union.update_value(value);
    }
    for value in HIGH_VALUE_KEYS {
        direct_union.update_value(value);
    }

    let mut merged_left = HllSketch::new(4, HllType::Hll8).unwrap();
    for value in 0..200_u64 {
        merged_left.update(value);
    }
    let mut merged_right = HllSketch::new(4, HllType::Hll8).unwrap();
    for value in HIGH_VALUE_KEYS {
        merged_right.update(value);
    }
    let mut merged_union = HllUnion::new(4).unwrap();
    merged_union.update(&merged_left);
    merged_union.update(&merged_right);

    for union in [&direct_union, &merged_union] {
        let hll4_bytes = union.serialize(HllType::Hll4);
        let expected_hll4_bytes = union.to_sketch(HllType::Hll4).serialize();
        let dense_end = 40 + (1usize << (4 - 1));
        assert!(hll4_bytes[6] > 0, "expected shifted cur_min > 0");
        assert!(
            (hll4_bytes.len() - dense_end) / size_of::<u32>() >= 2,
            "expected at least 2 aux exceptions"
        );
        // Preamble and packed nibbles must match bit-for-bit; only aux coupon order may differ.
        assert_eq!(hll4_bytes[..dense_end], expected_hll4_bytes[..dense_end]);
        assert_eq!(hll4_bytes.len(), expected_hll4_bytes.len());

        for target_type in [HllType::Hll4, HllType::Hll6, HllType::Hll8] {
            assert_union_matches_sketch(union, target_type, target_type != HllType::Hll4);
        }
    }
}

fn test_sketch_file(path: PathBuf, expected_cardinality: usize, expected_lg_k: u8) {
    let expected = expected_cardinality as f64;

    let bytes = fs::read(&path).unwrap();
    let sketch1 = HllSketch::deserialize(&bytes).unwrap();
    let estimate1 = sketch1.estimate();

    assert_eq!(
        sketch1.lg_config_k(),
        expected_lg_k,
        "Wrong lg_config_k in {}",
        path.display()
    );

    // Check cardinality estimate with error bounds
    // For lg_k=12, theoretical RSE ≈ 1.625%, but we use 2% margin to account for:
    // * Small sample sizes (especially n < 100)
    // * Out-of-order mode (composite estimator)
    // * Variation across implementations
    if expected > 0.0 {
        let error_margin = 0.02; // 2% error margin
        let lower_bound = expected * (1.0 - error_margin);
        let upper_bound = expected * (1.0 + error_margin);

        assert_that!(
            estimate1,
            all!(ge(lower_bound), le(upper_bound)),
            "path: {}",
            path.display()
        );
    } else {
        // For n=0, estimate should be very close to 0
        assert_that!(estimate1, lt(1.0), "path: {}", path.display());
    }

    // Serialize and deserialize again to test round-trip
    let serialized_bytes = sketch1.serialize();
    let sketch2 = HllSketch::deserialize(&serialized_bytes).unwrap_or_else(|err| {
        panic!(
            "Deserialization failed after round-trip for {}: {}",
            path.display(),
            err
        )
    });

    // Check that both sketches are functionally equivalent
    assert_eq!(
        sketch1.lg_config_k(),
        sketch2.lg_config_k(),
        "lg_config_k mismatch after round-trip for {}",
        path.display()
    );

    // Check that the sketches are functionally equal
    assert_eq!(
        sketch1,
        sketch2,
        "Sketches are not equal after round-trip for {}",
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

/// Reproducer for https://github.com/apache/datasketches-rust/issues/115
///
/// A compact-serialized List has no trailing COUPON_EMPTY (0) sentinels.
/// Before the fix, update() would scan the fully-packed array, find no
/// empty slot, and silently drop the new value.
#[test]
fn test_update_after_deserialize_list_mode() {
    const LG_K: u8 = 11;
    for hll_type in [HllType::Hll4, HllType::Hll6, HllType::Hll8] {
        let mut sketch = HllSketch::new(LG_K, hll_type).unwrap();
        sketch.update(1u64);

        // Round-trip through serialization (compact format, List mode)
        let bytes = sketch.serialize();
        let mut sketch = HllSketch::deserialize(&bytes).unwrap();

        // This update was silently dropped before the fix
        sketch.update(2u64);

        let est = sketch.estimate();
        assert_that!(est, near(2.0, 0.1), "hll_type: {hll_type:?}");
    }
}

#[test]
fn coupon_mode_sizes_are_validated_before_allocating() {
    let mut list = HllSketch::new(12, HllType::Hll8).unwrap();
    list.update(1_u64);
    let mut invalid_list_size = list.serialize();
    invalid_list_size[4] = u8::MAX;
    assert!(HllSketch::deserialize(&invalid_list_size).is_err());

    let mut invalid_list_count = list.serialize();
    invalid_list_count[6] = u8::MAX;
    assert!(HllSketch::deserialize(&invalid_list_count).is_err());

    let mut set = HllSketch::new(12, HllType::Hll8).unwrap();
    for value in 0..10 {
        set.update(value);
    }
    let mut invalid_set_size = set.serialize();
    invalid_set_size[4] = u8::MAX;
    assert!(HllSketch::deserialize(&invalid_set_size).is_err());

    let mut invalid_set_count = set.serialize();
    invalid_set_count[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(HllSketch::deserialize(&invalid_set_count).is_err());
}

#[test]
fn hll_mode_round_trip_preserves_registers_and_rejects_truncation() {
    for hll_type in [HllType::Hll4, HllType::Hll6, HllType::Hll8] {
        let mut sketch = HllSketch::new(12, hll_type).unwrap();
        for value in 0..10_000 {
            sketch.update(value);
        }

        let bytes = sketch.serialize();
        let restored = HllSketch::deserialize(&bytes).unwrap();
        assert_eq!(restored, sketch, "hll_type: {hll_type:?}");
        assert!(HllSketch::deserialize(&bytes[..bytes.len() - 1]).is_err());
    }
}

#[test]
fn hll4_updatable_aux_table_matches_compact_image() {
    let path = serialization_test_data("java_generated_files", "hll4_n100000_java.sk");
    let compact = fs::read(path).unwrap();
    let lg_k = compact[3];
    let lg_arr = compact[4];
    let aux_count = u32::from_le_bytes(compact[36..40].try_into().unwrap()) as usize;
    let aux_start = 40 + (1usize << lg_k) / 2;
    assert!(aux_count > 0);

    let mut aux_table = vec![0_u32; 1usize << lg_arr];
    let table_mask = aux_table.len() as u32 - 1;
    for bytes in compact[aux_start..aux_start + aux_count * size_of::<u32>()].chunks_exact(4) {
        let coupon = u32::from_le_bytes(bytes.try_into().unwrap());
        let slot = coupon & ((1_u32 << 26) - 1);
        let stride = (slot >> lg_arr) | 1;
        let mut probe = slot & table_mask;
        while aux_table[probe as usize] != 0 {
            probe = (probe + stride) & table_mask;
        }
        aux_table[probe as usize] = coupon;
    }

    let mut updatable = compact[..aux_start].to_vec();
    updatable[5] &= !8; // clear the compact flag
    for coupon in aux_table {
        updatable.extend_from_slice(&coupon.to_le_bytes());
    }

    assert_eq!(
        HllSketch::deserialize(&updatable).unwrap(),
        HllSketch::deserialize(&compact).unwrap()
    );
}

#[test]
fn test_serialized_bytes_match_reference_files_for_coupon_modes() {
    fn serialized_mode_name(bytes: &[u8]) -> &'static str {
        // The HLL preamble stores current mode in the low two bits of byte 7.
        match bytes[7] & 0x3 {
            0 => "List",
            1 => "Set",
            2 => "HLL",
            _ => "unknown",
        }
    }

    for (hll_type, type_name) in [
        (HllType::Hll4, "hll4"),
        (HllType::Hll6, "hll6"),
        (HllType::Hll8, "hll8"),
    ] {
        for (n, mode) in [(0_u32, "List"), (1, "List"), (10, "Set"), (100, "Set")] {
            // Fixture generators use lg_k 12 and update the sketch with 0..n.
            let mut sketch = HllSketch::new(12, hll_type).unwrap();
            for value in 0..n {
                sketch.update(natural_extend::from_u32(value));
            }

            let bytes = sketch.serialize();
            assert_eq!(
                serialized_mode_name(&bytes),
                mode,
                "Rust {type_name} n{n} should serialize in {mode} mode"
            );

            for (dir, suffix) in [
                ("java_generated_files", "java"),
                ("cpp_generated_files", "cpp"),
                ("go_generated_files", "go"),
            ] {
                let filename = format!("{type_name}_n{n}_{suffix}.sk");
                let path = serialization_test_data(dir, &filename);
                let expected = fs::read(&path).unwrap();
                assert_eq!(
                    serialized_mode_name(&expected),
                    mode,
                    "{} should be a {mode} mode fixture",
                    path.display()
                );
                assert_eq!(
                    bytes,
                    expected,
                    "Rust {type_name} n{n} {mode} bytes must match {}",
                    path.display()
                );
            }
        }
    }
}

#[test]
fn test_java_hll4_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10000, 100000, 1000000];

    for n in test_cases {
        let filename = format!("hll4_n{}_java.sk", n);
        let path = serialization_test_data("java_generated_files", &filename);
        test_sketch_file(path, n, 12);
    }
}

#[test]
fn test_java_hll6_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10000, 100000, 1000000];

    for n in test_cases {
        let filename = format!("hll6_n{}_java.sk", n);
        let path = serialization_test_data("java_generated_files", &filename);
        test_sketch_file(path, n, 12);
    }
}

#[test]
fn test_java_hll8_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10000, 100000, 1000000];

    for n in test_cases {
        let filename = format!("hll8_n{}_java.sk", n);
        let path = serialization_test_data("java_generated_files", &filename);
        test_sketch_file(path, n, 12);
    }
}

#[test]
fn test_cpp_hll4_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10000, 100000, 1000000];

    for n in test_cases {
        let filename = format!("hll4_n{}_cpp.sk", n);
        let path = serialization_test_data("cpp_generated_files", &filename);
        test_sketch_file(path, n, 12);
    }
}

#[test]
fn test_cpp_hll6_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10000, 100000, 1000000];

    for n in test_cases {
        let filename = format!("hll6_n{}_cpp.sk", n);
        let path = serialization_test_data("cpp_generated_files", &filename);
        test_sketch_file(path, n, 12);
    }
}

#[test]
fn test_cpp_hll8_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10000, 100000, 1000000];

    for n in test_cases {
        let filename = format!("hll8_n{}_cpp.sk", n);
        let path = serialization_test_data("cpp_generated_files", &filename);
        test_sketch_file(path, n, 12);
    }
}

#[test]
fn test_go_hll4_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10000, 100000, 1000000];

    for n in test_cases {
        let filename = format!("hll4_n{n}_go.sk");
        let path = serialization_test_data("go_generated_files", &filename);
        test_sketch_file(path, n, 12);
    }
}

#[test]
fn test_go_hll6_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10000, 100000, 1000000];

    for n in test_cases {
        let filename = format!("hll6_n{n}_go.sk");
        let path = serialization_test_data("go_generated_files", &filename);
        test_sketch_file(path, n, 12);
    }
}

#[test]
fn test_go_hll8_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10000, 100000, 1000000];

    for n in test_cases {
        let filename = format!("hll8_n{n}_go.sk");
        let path = serialization_test_data("go_generated_files", &filename);
        test_sketch_file(path, n, 12);
    }
}

#[test]
fn test_estimate_accuracy() {
    // This test verifies and prints actual estimates to show accuracy
    let test_cases = [
        ("java_generated_files", "hll8_n1000_java.sk", 1000),
        ("java_generated_files", "hll8_n10000_java.sk", 10000),
        ("java_generated_files", "hll8_n100000_java.sk", 100000),
        ("java_generated_files", "hll8_n1000000_java.sk", 1000000),
    ];

    println!("\nCardinality Estimation Accuracy:");
    println!("{:<12} {:<12} {:<10}", "Expected", "Estimate", "Error %");
    println!("{:-<40}", "");

    for (dir, file, expected) in test_cases {
        let path = serialization_test_data(dir, file);
        let bytes = fs::read(&path).unwrap();
        let sketch = HllSketch::deserialize(&bytes).unwrap();
        let estimate = sketch.estimate();
        let error_pct = ((estimate - expected as f64).abs() / expected as f64) * 100.;

        println!("{:<12} {:<12.0} {:<10.3}", expected, estimate, error_pct,);

        // All estimates should be within 2% error
        assert_that!(error_pct, lt(2.));
    }
}
