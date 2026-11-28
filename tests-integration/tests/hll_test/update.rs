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
use datasketches::hll::Coupon;
use datasketches::hll::HllSketch;
use datasketches::hll::HllType;
use datasketches::hll::HllUnion;
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
fn ordered_coupon_batch_matches_scalar_updates() {
    // At lg_k 4, the list promotes directly to HLL and the HLL4 minimum rises during the batch.
    // At lg_k 8, the batch promotes through a set.
    for (lg_k, num_values) in [(4, 100), (8, 128)] {
        for hll_type in [HllType::Hll4, HllType::Hll6, HllType::Hll8] {
            let mut scalar = HllSketch::new(lg_k, hll_type).unwrap();
            let mut batch = scalar.clone();
            let coupons = (0..num_values).chain(0..64).map(Coupon::from_value);
            for coupon in coupons.clone() {
                scalar.update_with_coupon(coupon);
            }
            batch.update_with_coupons(coupons);
            assert_eq!(batch.serialize(), scalar.serialize());
        }
    }
}

#[test]
fn ordered_coupon_batch_preserves_composite_state_after_union() {
    let mut union = HllUnion::new(4).unwrap();
    for start in [0, 32] {
        let mut part = HllSketch::new(4, HllType::Hll8).unwrap();
        for value in start..start + 32 {
            part.update(value);
        }
        union.update(&part);
    }
    let mut scalar = union.to_sketch(HllType::Hll8);
    let mut batch = scalar.clone();
    for value in 64..256 {
        scalar.update(value);
    }
    batch.update_with_coupons((64..256).map(Coupon::from_value));
    assert_eq!(batch.serialize(), scalar.serialize());
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
fn test_hll4_and_hll6_types() {
    for hll_type in [HllType::Hll4, HllType::Hll6] {
        let mut sketch = HllSketch::new(12, hll_type).unwrap();
        for i in 0..1000 {
            sketch.update(i);
        }
        assert_that!(sketch.estimate(), near(1000.0, 200.0));
    }
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
fn test_invalid_lg_k_returns_error() {
    for lg_k in [3, 22] {
        let error = HllSketch::new(lg_k, HllType::Hll8).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }
}

#[test]
fn list_storage_reports_no_heap() {
    let mut sketch = HllSketch::new(14, HllType::Hll8).unwrap();
    assert_eq!(sketch.estimated_size(), 0);
    for value in 0..7_u64 {
        sketch.update(value);
    }
    assert_eq!(sketch.estimated_size(), 0);

    let mut decoded = HllSketch::deserialize(&sketch.serialize()).unwrap();
    assert_eq!(decoded.estimated_size(), 0);
    decoded.update(7_u64);
    assert!(decoded.estimated_size() > 0);
}
