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

//! HyperLogLog Union Integration Tests
//!
//! These tests verify the public API behavior of HllUnion, focusing on:
//! * Basic union operations
//! * Mode transitions and mixed-mode unions
//! * Different HLL types and lg_k values
//! * Bounds and statistical properties
//! * Mathematical properties (commutativity, associativity, idempotency)
//! * Reset and reuse patterns
//!
//! This mirrors the testing strategy used in hll_update_test.rs

use datasketches::common::NumStdDev;
use datasketches::hll::HllSketch;
use datasketches::hll::HllType;
use datasketches::hll::HllUnion;
use googletest::assert_that;
use googletest::prelude::all;
use googletest::prelude::ge;
use googletest::prelude::gt;
use googletest::prelude::le;
use googletest::prelude::lt;
use googletest::prelude::near;

const HLL_TYPES: [HllType; 3] = [HllType::Hll4, HllType::Hll6, HllType::Hll8];

fn make_hll_sketch(hll_type: HllType, lg_config_k: u8, start: u64, end: u64) -> HllSketch {
    let mut sketch = HllSketch::new(lg_config_k, hll_type);
    for value in start..end {
        sketch.update(value);
    }
    sketch
}

fn assert_estimate_within(estimate: f64, expected: f64, relative_error: f64) {
    let actual_relative_error = (estimate - expected).abs() / expected;
    assert_that!(
        actual_relative_error,
        le(relative_error),
        "expected={expected}, estimate={estimate}"
    );
}

fn serialize_flat_union(first: &HllSketch, second: &HllSketch, third: &HllSketch) -> Vec<u8> {
    let mut union = HllUnion::new(8);
    union.update(first);
    union.update(second);
    union.update(third);
    union.to_sketch(HllType::Hll8).serialize()
}

fn serialize_nested_union(first: &HllSketch, second: &HllSketch, third: &HllSketch) -> Vec<u8> {
    let mut prefix = HllUnion::new(8);
    prefix.update(first);
    prefix.update(second);

    let mut union = HllUnion::new(8);
    union.update(&prefix.to_sketch(HllType::Hll8));
    union.update(third);
    union.to_sketch(HllType::Hll8).serialize()
}

#[test]
fn test_union_basic_operations() {
    let mut union = HllUnion::new(12);

    // Empty union
    assert!(union.is_empty());
    assert_eq!(union.estimate(), 0.0);

    // Update with empty sketch should not change state
    let empty_sketch = HllSketch::new(12, HllType::Hll8);
    union.update(&empty_sketch);
    assert!(union.is_empty());

    // Create sketches with overlapping values
    let mut sketch1 = HllSketch::new(12, HllType::Hll8);
    for i in 0..500 {
        sketch1.update(i);
    }

    let mut sketch2 = HllSketch::new(12, HllType::Hll8);
    for i in 400..900 {
        sketch2.update(i);
    }

    union.update(&sketch1);
    union.update(&sketch2);

    // Should estimate ~900 unique values (0-899)
    let estimate = union.estimate();
    assert_that!(estimate, all!(gt(800.0), lt(1000.0)));
    assert!(!union.is_empty());

    // Adding empty sketch should not affect estimate
    let estimate_before = union.estimate();
    union.update(&empty_sketch);
    assert_eq!(estimate_before, union.estimate());

    // Test update_value with different types
    union.update_value("hello");
    union.update_value(42i32);
    union.update_value(vec![1, 2, 3]);
    assert_that!(union.estimate(), gt(estimate_before));

    // Test duplicate handling - same sketch added multiple times
    let mut dup_union = HllUnion::new(12);
    let mut sketch = HllSketch::new(12, HllType::Hll8);
    for i in 0..100 {
        sketch.update(i);
    }
    for _ in 0..5 {
        dup_union.update(&sketch);
    }
    let dup_estimate = dup_union.estimate();
    assert_that!(dup_estimate, near(100.0, 20.0));
}

#[test]
fn test_union_mode_transitions() {
    let mut union = HllUnion::new(12);

    // Start with List mode (small cardinality)
    let mut sketch1 = HllSketch::new(12, HllType::Hll8);
    for i in 0..10 {
        sketch1.update(i);
    }

    let mut sketch2 = HllSketch::new(12, HllType::Hll8);
    for i in 5..15 {
        sketch2.update(i);
    }

    union.update(&sketch1);
    union.update(&sketch2);

    let estimate = union.estimate();
    assert_that!(estimate, near(15.0, 5.0));

    // Trigger Set mode promotion
    let mut sketch3 = HllSketch::new(12, HllType::Hll8);
    for i in 0..600 {
        sketch3.update(i);
    }
    union.update(&sketch3);

    let estimate = union.estimate();
    assert_that!(estimate, near(600.0, 100.0));

    // Trigger HLL mode promotion
    let mut sketch4 = HllSketch::new(12, HllType::Hll8);
    for i in 500..10_000 {
        sketch4.update(i);
    }
    union.update(&sketch4);

    let estimate = union.estimate();
    assert_that!(estimate, all!(gt(9_000.0), lt(11_000.0)));
}

#[test]
fn test_union_mixed_modes() {
    let mut union = HllUnion::new(12);

    // Small sketch (List mode)
    let mut sketch1 = HllSketch::new(12, HllType::Hll8);
    sketch1.update("a");
    sketch1.update("b");
    sketch1.update("c");

    // Large sketch (Array mode)
    let mut sketch2 = HllSketch::new(12, HllType::Hll8);
    for i in 0..10_000 {
        sketch2.update(i);
    }

    union.update(&sketch1);
    union.update(&sketch2);

    let result = union.to_sketch(HllType::Hll8);
    let estimate = result.estimate();

    // Should estimate ~10,003 unique values
    assert_that!(estimate, all!(gt(9_500.0), lt(10_500.0)));
}

#[test]
fn test_union_mixed_hll_types() {
    let mut union = HllUnion::new(12);

    // Mix Hll4, Hll6, and Hll8 sketches
    let mut sketch1 = HllSketch::new(12, HllType::Hll4);
    for i in 0..3_000 {
        sketch1.update(i);
    }

    let mut sketch2 = HllSketch::new(12, HllType::Hll6);
    for i in 2_000..5_000 {
        sketch2.update(i);
    }

    let mut sketch3 = HllSketch::new(12, HllType::Hll8);
    for i in 4_000..7_000 {
        sketch3.update(i);
    }

    union.update(&sketch1);
    union.update(&sketch2);
    union.update(&sketch3);

    // Test getting result in different types
    let result4 = union.to_sketch(HllType::Hll4);
    let result6 = union.to_sketch(HllType::Hll6);
    let result8 = union.to_sketch(HllType::Hll8);

    assert_eq!(result4.target_type(), HllType::Hll4);
    assert_eq!(result6.target_type(), HllType::Hll6);
    assert_eq!(result8.target_type(), HllType::Hll8);

    // Should estimate ~7,000 unique values (0-6,999)
    for (result, type_name) in [
        (result4.estimate(), "Hll4"),
        (result6.estimate(), "Hll6"),
        (result8.estimate(), "Hll8"),
    ] {
        assert_that!(
            result,
            all!(gt(6_000.0), lt(8_000.0)),
            "hll_type: {type_name}"
        );
    }
}

#[test]
fn test_union_lg_k_handling() {
    // Test multiple downsizing operations: 12 → 10 → 8
    let mut union = HllUnion::new(12);

    // Start with lg_k=12
    let mut sketch1 = HllSketch::new(12, HllType::Hll8);
    for i in 0..5_000 {
        sketch1.update(i);
    }
    union.update(&sketch1);
    assert_eq!(union.lg_config_k(), 12);

    // Add sketch with lg_k=10 (triggers downsizing)
    let mut sketch2 = HllSketch::new(10, HllType::Hll8);
    for i in 4_000..8_000 {
        sketch2.update(i);
    }
    union.update(&sketch2);
    assert_eq!(union.lg_config_k(), 10, "Gadget should downsize to lg_k=10");

    // Add sketch with lg_k=8 (triggers another downsizing)
    let mut sketch3 = HllSketch::new(8, HllType::Hll8);
    for i in 7_000..10_000 {
        sketch3.update(i);
    }
    union.update(&sketch3);
    assert_eq!(union.lg_config_k(), 8, "Gadget should downsize to lg_k=8");

    let result = union.to_sketch(HllType::Hll8);
    let estimate = result.estimate();

    // Should estimate ~10,000 unique values (0-9,999)
    // Lower precision means higher error tolerance
    assert_that!(estimate, all!(gt(8_000.0), lt(12_000.0)));

    // Test downsampling: union at lower precision than sketch
    let mut union2 = HllUnion::new(10);
    let mut sketch_high_precision = HllSketch::new(12, HllType::Hll8);
    for i in 0..5_000 {
        sketch_high_precision.update(i);
    }

    union2.update(&sketch_high_precision);
    let result2 = union2.to_sketch(HllType::Hll8);
    assert_eq!(result2.lg_config_k(), 10, "Result should be at lg_k=10");

    let estimate2 = result2.estimate();
    assert_that!(estimate2, all!(gt(4_000.0), lt(6_000.0)));
}

// Regression coverage for https://github.com/apache/datasketches-cpp/pull/512.
#[test]
fn test_union_downsampling_merge_is_not_empty() {
    for hll_type in HLL_TYPES {
        let sketch = make_hll_sketch(hll_type, 15, 0, 100_000);
        let mut union = HllUnion::new(8);
        union.update(&sketch);

        assert!(!union.is_empty(), "{hll_type:?} union should not be empty");
    }
}

#[test]
fn test_union_mixed_lg_k_estimate_is_merge_order_independent() {
    const N: u64 = 100_000;

    for hll_type in HLL_TYPES {
        let a = make_hll_sketch(hll_type, 15, 0, N);
        let b = make_hll_sketch(hll_type, 8, N, 2 * N);
        let expected = 2.0 * N as f64;

        let mut larger_first = HllUnion::new(8);
        larger_first.update(&a);
        larger_first.update(&b);
        let larger_first_estimate = larger_first.estimate();
        assert_estimate_within(larger_first_estimate, expected, 0.1);

        let mut smaller_first = HllUnion::new(8);
        smaller_first.update(&b);
        smaller_first.update(&a);
        let smaller_first_estimate = smaller_first.estimate();
        assert_estimate_within(smaller_first_estimate, expected, 0.1);
        assert_eq!(
            larger_first_estimate, smaller_first_estimate,
            "{hll_type:?} estimate should be merge-order independent",
        );
    }
}

#[test]
fn test_union_scalar_update_after_downsampling_merge() {
    const N: u64 = 100_000;

    for hll_type in HLL_TYPES {
        let sketch = make_hll_sketch(hll_type, 15, 0, N);
        let mut union = HllUnion::new(8);
        union.update(&sketch);
        for value in N..2 * N {
            union.update_value(value);
        }

        assert_estimate_within(union.estimate(), 2.0 * N as f64, 0.1);
    }
}

#[test]
fn test_union_serialization_is_grouping_independent() {
    const N: u64 = 100_000;

    for hll_type in HLL_TYPES {
        let a = make_hll_sketch(hll_type, 15, 0, N);
        let b = make_hll_sketch(hll_type, 8, N, 2 * N);
        let c = make_hll_sketch(hll_type, 11, N / 2, N + N / 2);

        for (first, second, third) in [(&b, &c, &a), (&a, &c, &b), (&a, &b, &c)] {
            assert_eq!(
                serialize_nested_union(first, second, third),
                serialize_flat_union(first, second, third),
                "{hll_type:?} serialization should be grouping independent",
            );
        }
    }
}

#[test]
fn test_union_bounds() {
    let mut union = HllUnion::new(12);

    // Empty union
    assert_eq!(union.estimate(), 0.0);
    let empty_lower = union.lower_bound(NumStdDev::Two);
    let empty_upper = union.upper_bound(NumStdDev::Two);
    assert_that!(empty_lower, ge(0.0));
    assert_that!(empty_upper, ge(0.0));
    assert_that!(empty_lower, le(empty_upper));

    // Add sketches
    let mut sketch1 = HllSketch::new(12, HllType::Hll8);
    for i in 0..500 {
        sketch1.update(i);
    }

    let mut sketch2 = HllSketch::new(12, HllType::Hll8);
    for i in 400..900 {
        sketch2.update(i);
    }

    union.update(&sketch1);
    union.update(&sketch2);

    let estimate = union.estimate();
    let upper1 = union.upper_bound(NumStdDev::One);
    let lower1 = union.lower_bound(NumStdDev::One);
    let upper2 = union.upper_bound(NumStdDev::Two);
    let lower2 = union.lower_bound(NumStdDev::Two);
    let upper3 = union.upper_bound(NumStdDev::Three);
    let lower3 = union.lower_bound(NumStdDev::Three);

    // Basic sanity checks
    assert_that!(estimate, ge(lower1));
    assert_that!(estimate, le(upper1));

    // Bounds should widen with more standard deviations
    assert_that!(lower2, le(lower1));
    assert_that!(upper1, le(upper2));
    assert_that!(lower3, le(lower2));
    assert_that!(upper2, le(upper3));

    // Bounds should be reasonable
    assert_that!(lower3, gt(estimate * 0.5));
    assert_that!(upper3, lt(estimate * 1.5));

    // Test that smaller lg_k has wider bounds (higher RSE)
    let mut union_small = HllUnion::new(8);
    let mut union_large = HllUnion::new(14);

    let mut sketch_small = HllSketch::new(8, HllType::Hll8);
    let mut sketch_large = HllSketch::new(14, HllType::Hll8);

    for i in 0..1000 {
        sketch_small.update(i);
        sketch_large.update(i);
    }

    union_small.update(&sketch_small);
    union_large.update(&sketch_large);

    let est_small = union_small.estimate();
    let est_large = union_large.estimate();

    let width_small = (union_small.upper_bound(NumStdDev::Two)
        - union_small.lower_bound(NumStdDev::Two))
        / est_small;
    let width_large = (union_large.upper_bound(NumStdDev::Two)
        - union_large.lower_bound(NumStdDev::Two))
        / est_large;

    assert_that!(width_small, gt(width_large));
}

#[test]
fn test_union_reset() {
    let mut union = HllUnion::new(12);

    let mut sketch = HllSketch::new(12, HllType::Hll8);
    for i in 0..1000 {
        sketch.update(i);
    }

    union.update(&sketch);
    assert!(!union.is_empty());
    assert_that!(union.estimate(), gt(900.0));

    // Reset should clear all state
    union.reset();
    assert!(union.is_empty());
    assert_eq!(union.estimate(), 0.0);
    assert_eq!(union.lg_config_k(), 12, "lg_max_k should be preserved");

    // Reuse after reset - multiple iterations
    for iteration in 0..3 {
        let mut sketch = HllSketch::new(12, HllType::Hll8);
        for i in (iteration * 100)..((iteration + 1) * 100) {
            sketch.update(i);
        }

        union.update(&sketch);
        assert!(!union.is_empty());

        union.reset();
        assert!(union.is_empty());
    }
}

#[test]
fn test_union_commutativity() {
    // Verify A∪B = B∪A
    let mut sketch_a = HllSketch::new(12, HllType::Hll8);
    for i in 0..1000 {
        sketch_a.update(i);
    }

    let mut sketch_b = HllSketch::new(12, HllType::Hll8);
    for i in 500..1500 {
        sketch_b.update(i);
    }

    // A∪B
    let mut union1 = HllUnion::new(12);
    union1.update(&sketch_a);
    union1.update(&sketch_b);

    // B∪A
    let mut union2 = HllUnion::new(12);
    union2.update(&sketch_b);
    union2.update(&sketch_a);

    assert_eq!(union1.estimate(), union2.estimate());
}

#[test]
fn test_union_associativity() {
    // Verify (A∪B)∪C = A∪(B∪C)
    let mut sketch_a = HllSketch::new(12, HllType::Hll8);
    let mut sketch_b = HllSketch::new(12, HllType::Hll8);
    let mut sketch_c = HllSketch::new(12, HllType::Hll8);

    for i in 0..1000 {
        sketch_a.update(i);
    }
    for i in 500..1500 {
        sketch_b.update(i);
    }
    for i in 1000..2000 {
        sketch_c.update(i);
    }

    // Compute (A∪B)∪C
    let mut union1 = HllUnion::new(12);
    union1.update(&sketch_a);
    union1.update(&sketch_b);
    let ab_sketch = union1.to_sketch(HllType::Hll8);

    let mut union2 = HllUnion::new(12);
    union2.update(&ab_sketch);
    union2.update(&sketch_c);
    let est1 = union2.estimate();

    // Compute A∪(B∪C)
    let mut union3 = HllUnion::new(12);
    union3.update(&sketch_b);
    union3.update(&sketch_c);
    let bc_sketch = union3.to_sketch(HllType::Hll8);

    let mut union4 = HllUnion::new(12);
    union4.update(&sketch_a);
    union4.update(&bc_sketch);
    let est2 = union4.estimate();

    assert_eq!(est1, est2);
}

#[test]
fn test_union_idempotency() {
    // Verify A∪A = A
    let mut sketch = HllSketch::new(12, HllType::Hll8);
    for i in 0..1000 {
        sketch.update(i);
    }

    let mut union = HllUnion::new(12);
    union.update(&sketch);
    let est1 = union.estimate();

    // Union with itself
    union.update(&sketch);
    let est2 = union.estimate();

    assert_eq!(est1, est2);
}

#[test]
fn test_union_merge_order_regression() {
    // Large fractional powers of two reproduce the reference implementation's merge-order case.
    let points_per_octave = 1 << 17;

    fn power_series_sketch(start: i64, points_per_octave: i32, limit: i64) -> HllSketch {
        fn next_power_series_point(points_per_octave: i32, current: i64) -> i64 {
            let current = current.max(1);
            let mut generating_index =
                ((current as f64).log2() * f64::from(points_per_octave)).round() as i32;
            loop {
                generating_index += 1;
                let next = 2_f64
                    .powf(f64::from(generating_index) / f64::from(points_per_octave))
                    .round() as i64;
                if next > current {
                    return next;
                }
            }
        }

        let mut sketch = HllSketch::new(11, HllType::Hll8);
        let mut value = start;
        while value < limit {
            sketch.update(value);
            value = next_power_series_point(points_per_octave, value);
        }
        sketch
    }

    let a = power_series_sketch(1_i64 << 59, points_per_octave, 1_i64 << 60);
    let b = power_series_sketch(1_i64 << 60, points_per_octave, 1_i64 << 61);
    let c = power_series_sketch(1_i64 << 61, points_per_octave, 1_i64 << 62);
    let sketches = [&a, &b, &c];

    fn merge_estimate(sketches: &[&HllSketch; 3], order: [usize; 3]) -> f64 {
        let mut union = HllUnion::new(11);
        for index in order {
            union.update(sketches[index]);
        }
        union.estimate()
    }

    let estimates = [
        merge_estimate(&sketches, [0, 1, 2]),
        merge_estimate(&sketches, [0, 2, 1]),
        merge_estimate(&sketches, [1, 0, 2]),
        merge_estimate(&sketches, [1, 2, 0]),
        merge_estimate(&sketches, [2, 0, 1]),
        merge_estimate(&sketches, [2, 1, 0]),
    ];

    for estimate in estimates.iter().skip(1) {
        assert_eq!(*estimate, estimates[0]);
    }
}

#[test]
fn test_union_large_cardinality() {
    let mut union = HllUnion::new(14);

    // Create three large sketches with overlap
    let mut sketch1 = HllSketch::new(14, HllType::Hll8);
    for i in 0..100_000 {
        sketch1.update(i);
    }

    let mut sketch2 = HllSketch::new(14, HllType::Hll8);
    for i in 50_000..150_000 {
        sketch2.update(i);
    }

    let mut sketch3 = HllSketch::new(14, HllType::Hll8);
    for i in 100_000..200_000 {
        sketch3.update(i);
    }

    union.update(&sketch1);
    union.update(&sketch2);
    union.update(&sketch3);

    let estimate = union.estimate();
    let relative_error = (estimate - 200_000.0).abs() / 200_000.0;

    // For lg_k=14, relative error should be ~1.04%
    assert_that!(relative_error, lt(0.05));
}

#[test]
#[should_panic(expected = "lg_max_k must be in [4, 21]")]
fn test_union_invalid_lg_k_low() {
    HllUnion::new(3);
}

#[test]
#[should_panic(expected = "lg_max_k must be in [4, 21]")]
fn test_union_invalid_lg_k_high() {
    HllUnion::new(22);
}

#[test]
fn test_union_validation() {
    // Test valid boundaries
    let union_min = HllUnion::new(4);
    let union_max = HllUnion::new(21);

    assert_eq!(union_min.lg_max_k(), 4);
    assert_eq!(union_max.lg_max_k(), 21);
    assert!(union_min.is_empty());
    assert!(union_max.is_empty());

    // Test lg_max_k is preserved
    let mut union = HllUnion::new(15);
    let mut sketch = HllSketch::new(12, HllType::Hll8);
    for i in 0..1000 {
        sketch.update(i);
    }

    union.update(&sketch);
    assert_eq!(union.lg_max_k(), 15, "lg_max_k should not change");

    union.reset();
    assert_eq!(union.lg_max_k(), 15, "lg_max_k should persist after reset");
}

#[test]
fn test_union_estimated_size() {
    let mut union = HllUnion::new(10);
    assert_eq!(union.estimated_size(), 128);

    let mut sketch = HllSketch::new(10, HllType::Hll8);
    for i in 0..1000 {
        sketch.update(i);
    }
    union.update(&sketch);
    assert_eq!(union.estimated_size(), 1120);
}
