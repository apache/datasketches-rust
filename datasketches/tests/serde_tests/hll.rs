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

use datasketches::hash::value::natural_extend;
use datasketches::hll::HllSketch;
use datasketches::hll::HllType;
use googletest::assert_that;
use googletest::prelude::all;
use googletest::prelude::ge;
use googletest::prelude::le;
use googletest::prelude::lt;
use googletest::prelude::near;

use crate::serialization_test_data;

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
        let mut sketch = HllSketch::new(LG_K, hll_type);
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
    let mut list = HllSketch::new(12, HllType::Hll8);
    list.update(1_u64);
    let mut invalid_list_size = list.serialize();
    invalid_list_size[4] = u8::MAX;
    assert!(HllSketch::deserialize(&invalid_list_size).is_err());

    let mut invalid_list_count = list.serialize();
    invalid_list_count[6] = u8::MAX;
    assert!(HllSketch::deserialize(&invalid_list_count).is_err());

    let mut set = HllSketch::new(12, HllType::Hll8);
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
        let mut sketch = HllSketch::new(12, hll_type);
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
            let mut sketch = HllSketch::new(12, hll_type);
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
