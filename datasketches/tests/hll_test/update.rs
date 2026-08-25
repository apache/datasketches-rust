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

use datasketches::common::NumStdDev;
use datasketches::hll::HllSketch;
use datasketches::hll::HllType;
use googletest::assert_that;
use googletest::prelude::all;
use googletest::prelude::ge;
use googletest::prelude::gt;
use googletest::prelude::le;
use googletest::prelude::lt;
use googletest::prelude::near;

#[test]
fn test_basic_update() {
    let mut sketch = HllSketch::new(12, HllType::Hll8);

    // Initially empty
    assert_eq!(sketch.estimate(), 0.0);

    // Update with some values
    for i in 0..100 {
        sketch.update(i);
    }

    let estimate = sketch.estimate();
    assert_that!(estimate, gt(0.0));
    assert_that!(estimate, near(100.0, 20.0));
}

#[test]
fn test_list_to_set_promotion() {
    // Use lg_k=12, which has promotion threshold ~512 for List→Set
    let mut sketch = HllSketch::new(12, HllType::Hll8);

    // Add enough unique values to trigger promotion
    for i in 0..600 {
        sketch.update(i);
    }

    let estimate = sketch.estimate();
    assert_that!(estimate, near(600.0, 100.0));
}

#[test]
fn test_set_to_hll_promotion() {
    // Use lg_k=10 (K=1024), set promotes at 75% = 768
    let mut sketch = HllSketch::new(10, HllType::Hll8);

    // Add enough values to trigger List→Set→HLL promotions
    for i in 0..1000 {
        sketch.update(i);
    }

    let estimate = sketch.estimate();
    assert_that!(estimate, near(1000.0, 150.0));
}

#[test]
fn test_duplicate_handling() {
    let mut sketch = HllSketch::new(12, HllType::Hll8);

    // Add same values multiple times
    for _ in 0..10 {
        for i in 0..100 {
            sketch.update(i);
        }
    }

    // Estimate should reflect ~100 unique values, not 1000
    let estimate = sketch.estimate();
    assert_that!(estimate, near(100.0, 20.0));
}

#[test]
fn test_different_types() {
    let mut sketch = HllSketch::new(10, HllType::Hll8);

    // Mix different types
    sketch.update(42i32);
    sketch.update("hello");
    sketch.update(100u64);
    sketch.update(true);
    sketch.update(vec![1, 2, 3]);

    let estimate = sketch.estimate();
    assert_that!(estimate, ge(5.0));
}

#[test]
fn test_hll4_type() {
    let mut sketch = HllSketch::new(12, HllType::Hll4);

    for i in 0..1000 {
        sketch.update(i);
    }

    let estimate = sketch.estimate();
    assert_that!(estimate, near(1000.0, 200.0));
}

#[test]
fn test_hll6_type() {
    let mut sketch = HllSketch::new(12, HllType::Hll6);

    for i in 0..1000 {
        sketch.update(i);
    }

    let estimate = sketch.estimate();
    assert_that!(estimate, near(1000.0, 200.0));
}

#[test]
fn test_serialization_roundtrip_after_updates() {
    let mut sketch1 = HllSketch::new(12, HllType::Hll8);

    // Add values and promote through all modes
    for i in 0..2000 {
        sketch1.update(i);
    }

    let estimate1 = sketch1.estimate();

    // Serialize and deserialize
    let bytes = sketch1.serialize();
    let sketch2 = HllSketch::deserialize(&bytes).unwrap();

    let estimate2 = sketch2.estimate();

    // Estimates should match after round-trip (allow some numerical error)
    let relative_error = (estimate1 - estimate2).abs() / estimate1;
    assert_that!(
        relative_error,
        lt(0.05),
        "estimate1={estimate1}, estimate2={estimate2}"
    );
}

#[test]
fn test_large_cardinality() {
    let mut sketch = HllSketch::new(14, HllType::Hll8);

    // Add 100K unique values
    for i in 0..100_000 {
        sketch.update(i);
    }

    let estimate = sketch.estimate();
    let relative_error = (estimate - 100_000.0).abs() / 100_000.0;

    // For lg_k=14, relative error should be ~1.04%
    assert_that!(relative_error, lt(0.05));
}

#[test]
fn test_equals_method() {
    let mut sketch1 = HllSketch::new(10, HllType::Hll8);
    let mut sketch2 = HllSketch::new(10, HllType::Hll8);

    // Both start equal (empty)
    assert!(sketch1.eq(&sketch2));

    // Add same values to both
    for i in 0..100 {
        sketch1.update(i);
        sketch2.update(i);
    }

    // Should still be equal
    assert!(sketch1.eq(&sketch2));

    // Add different value to sketch2
    sketch2.update(999);

    // Now they're different
    assert!(!sketch1.eq(&sketch2));
}

#[test]
#[should_panic(expected = "lg_config_k must be in [4, 21]")]
fn test_invalid_lg_k_low() {
    HllSketch::new(3, HllType::Hll8);
}

#[test]
#[should_panic(expected = "lg_config_k must be in [4, 21]")]
fn test_invalid_lg_k_high() {
    HllSketch::new(22, HllType::Hll8);
}

#[test]
fn test_bounds_basic() {
    let mut sketch = HllSketch::new(12, HllType::Hll8);

    // Add 1000 unique values
    for i in 0..1000 {
        sketch.update(i);
    }

    let estimate = sketch.estimate();
    let upper1 = sketch.upper_bound(NumStdDev::One);
    let lower1 = sketch.lower_bound(NumStdDev::One);
    let upper2 = sketch.upper_bound(NumStdDev::Two);
    let lower2 = sketch.lower_bound(NumStdDev::Two);
    let upper3 = sketch.upper_bound(NumStdDev::Three);
    let lower3 = sketch.lower_bound(NumStdDev::Three);

    // Basic sanity checks
    assert_that!(estimate, ge(lower1));
    assert_that!(estimate, le(upper1));

    // Bounds should widen with more standard deviations
    assert_that!(lower2, le(lower1));
    assert_that!(upper1, le(upper2));
    assert_that!(lower3, le(lower2));
    assert_that!(upper2, le(upper3));

    // Bounds should be reasonable (within 50% for 3-sigma)
    assert_that!(lower3, gt(estimate * 0.5));
    assert_that!(upper3, lt(estimate * 1.5));
}

#[test]
fn test_bounds_all_modes() {
    // Test List mode (small cardinality)
    let mut sketch = HllSketch::new(12, HllType::Hll8);
    for i in 0..10 {
        sketch.update(i);
    }
    let estimate = sketch.estimate();
    let upper = sketch.upper_bound(NumStdDev::Two);
    let lower = sketch.lower_bound(NumStdDev::Two);
    assert_that!(estimate, all!(ge(lower), le(upper)), "mode: LIST");

    // Test Set mode (medium cardinality)
    for i in 10..100 {
        sketch.update(i);
    }
    let estimate = sketch.estimate();
    let upper = sketch.upper_bound(NumStdDev::Two);
    let lower = sketch.lower_bound(NumStdDev::Two);
    assert_that!(estimate, all!(ge(lower), le(upper)), "mode: SET");

    // Test HLL mode (large cardinality)
    for i in 100..5000 {
        sketch.update(i);
    }
    let estimate = sketch.estimate();
    let upper = sketch.upper_bound(NumStdDev::Two);
    let lower = sketch.lower_bound(NumStdDev::Two);
    assert_that!(estimate, all!(ge(lower), le(upper)), "mode: HLL");
}

#[test]
fn test_bounds_different_lg_k() {
    // Smaller lg_k should have wider bounds (higher RSE)
    let mut sketch_small = HllSketch::new(8, HllType::Hll8); // lg_k=8, k=256
    let mut sketch_large = HllSketch::new(14, HllType::Hll8); // lg_k=14, k=16384

    for i in 0..1000 {
        sketch_small.update(i);
        sketch_large.update(i);
    }

    let est_small = sketch_small.estimate();
    let est_large = sketch_large.estimate();

    let upper_small = sketch_small.upper_bound(NumStdDev::Two);
    let lower_small = sketch_small.lower_bound(NumStdDev::Two);
    let upper_large = sketch_large.upper_bound(NumStdDev::Two);
    let lower_large = sketch_large.lower_bound(NumStdDev::Two);

    // Calculate relative width of confidence intervals
    let width_small = (upper_small - lower_small) / est_small;
    let width_large = (upper_large - lower_large) / est_large;

    // Smaller sketch should have wider relative confidence interval
    assert_that!(width_small, gt(width_large));
}

#[test]
fn test_bounds_empty_sketch() {
    let sketch = HllSketch::new(12, HllType::Hll8);

    let estimate = sketch.estimate();
    let upper = sketch.upper_bound(NumStdDev::Two);
    let lower = sketch.lower_bound(NumStdDev::Two);

    assert_eq!(estimate, 0.0, "Empty sketch should have 0 estimate");
    assert_that!(lower, ge(0.0));
    assert_that!(upper, ge(0.0));
    assert_that!(lower, le(upper));
}
