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
use datasketches::hll::HllUnion;
use googletest::assert_that;
use googletest::prelude::all;
use googletest::prelude::ge;
use googletest::prelude::gt;
use googletest::prelude::le;
use googletest::prelude::lt;
use googletest::prelude::near;

fn hll_mode_sketch(lg_k: u8, hll_type: HllType, n: u64) -> HllSketch {
    let mut sketch = HllSketch::new(lg_k, hll_type).unwrap();
    for value in 0..n {
        sketch.update(value);
    }
    sketch
}

fn hll_mode_union(lg_k: u8, hll_type: HllType, n: u64) -> HllSketch {
    let mut even = HllSketch::new(lg_k, hll_type).unwrap();
    let mut odd = HllSketch::new(lg_k, hll_type).unwrap();
    for value in 0..n {
        if value % 2 == 0 {
            even.update(value);
        } else {
            odd.update(value);
        }
    }
    let mut union = HllUnion::new(lg_k).unwrap();
    union.update(&even);
    union.update(&odd);
    union.to_sketch(hll_type)
}

#[test]
fn hll_mode_lower_bound_matches_cross_language_register_floor() {
    // C++ reports a floor of 8 for this stream in all three target representations.
    for hll_type in [HllType::Hll4, HllType::Hll6, HllType::Hll8] {
        let sketch = hll_mode_sketch(4, hll_type, 12);
        assert_eq!(
            sketch.lower_bound(NumStdDev::Three),
            8.0,
            "type={hll_type:?}"
        );
    }

    // The one-standard-deviation bound remains above the register floor.
    let sketch = hll_mode_sketch(4, HllType::Hll8, 12);
    assert_that!(
        sketch.lower_bound(NumStdDev::One),
        near(9.946968965192236, 1e-9)
    );

    // The same floor applies when a union selects the composite estimator.
    let sketch = hll_mode_union(7, HllType::Hll4, 40);
    assert_eq!(sketch.lower_bound(NumStdDev::Three), 34.0);
}

#[test]
fn test_bounds_basic() {
    let mut sketch = HllSketch::new(12, HllType::Hll8).unwrap();

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
    let mut sketch = HllSketch::new(12, HllType::Hll8).unwrap();
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
    let mut sketch_small = HllSketch::new(8, HllType::Hll8).unwrap(); // lg_k=8, k=256
    let mut sketch_large = HllSketch::new(14, HllType::Hll8).unwrap(); // lg_k=14, k=16384

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
    let sketch = HllSketch::new(12, HllType::Hll8).unwrap();

    let estimate = sketch.estimate();
    let upper = sketch.upper_bound(NumStdDev::Two);
    let lower = sketch.lower_bound(NumStdDev::Two);

    assert_eq!(estimate, 0.0, "Empty sketch should have 0 estimate");
    assert_that!(lower, ge(0.0));
    assert_that!(upper, ge(0.0));
    assert_that!(lower, le(upper));
}
