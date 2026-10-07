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

use datasketches::error::ErrorKind;
use datasketches::hll::HllSketch;
use datasketches::hll::HllType;
use googletest::assert_that;
use googletest::prelude::ge;
use googletest::prelude::gt;
use googletest::prelude::lt;
use googletest::prelude::near;

#[test]
fn test_basic_update() {
    let mut sketch = HllSketch::new(12, HllType::Hll8).unwrap();

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
    let mut sketch = HllSketch::new(12, HllType::Hll8).unwrap();

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
    let mut sketch = HllSketch::new(10, HllType::Hll8).unwrap();

    // Add enough values to trigger List→Set→HLL promotions
    for i in 0..1000 {
        sketch.update(i);
    }

    let estimate = sketch.estimate();
    assert_that!(estimate, near(1000.0, 150.0));
}

#[test]
fn test_duplicate_handling() {
    let mut sketch = HllSketch::new(12, HllType::Hll8).unwrap();

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
    let mut sketch = HllSketch::new(10, HllType::Hll8).unwrap();

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
    let mut sketch = HllSketch::new(12, HllType::Hll4).unwrap();

    for i in 0..1000 {
        sketch.update(i);
    }

    let estimate = sketch.estimate();
    assert_that!(estimate, near(1000.0, 200.0));
}

#[test]
fn test_hll6_type() {
    let mut sketch = HllSketch::new(12, HllType::Hll6).unwrap();

    for i in 0..1000 {
        sketch.update(i);
    }

    let estimate = sketch.estimate();
    assert_that!(estimate, near(1000.0, 200.0));
}

#[test]
fn test_serialization_roundtrip_after_updates() {
    let mut sketch1 = HllSketch::new(12, HllType::Hll8).unwrap();

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
    let mut sketch = HllSketch::new(14, HllType::Hll8).unwrap();

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
    let mut sketch1 = HllSketch::new(10, HllType::Hll8).unwrap();
    let mut sketch2 = HllSketch::new(10, HllType::Hll8).unwrap();

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
fn test_invalid_lg_k_low_returns_error() {
    let error = HllSketch::new(3, HllType::Hll8).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn test_invalid_lg_k_high_returns_error() {
    let error = HllSketch::new(22, HllType::Hll8).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}
