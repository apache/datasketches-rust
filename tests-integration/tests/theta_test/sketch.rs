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
use datasketches::error::ErrorKind;
use datasketches::hash::value::canonical_float;
use datasketches::theta::CompactThetaSketch;
use datasketches::theta::ThetaSketchBuilder;
use googletest::assert_that;
use googletest::prelude::ge;
use googletest::prelude::gt;
use googletest::prelude::le;
use googletest::prelude::lt;
use googletest::prelude::near;
use tests_integration::MAX_THETA;
use tests_integration::ZERO_HASH_SEED;

#[test]
fn builder_validates_configuration_at_build() {
    let error = ThetaSketchBuilder::default().lg_k(4).build().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);

    let error = ThetaSketchBuilder::default()
        .sampling_probability(f32::NAN)
        .build()
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);

    let error = ThetaSketchBuilder::default()
        .seed(ZERO_HASH_SEED)
        .build()
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn test_basic_update() {
    let mut sketch = ThetaSketchBuilder::default().lg_k(12).build().unwrap();
    assert!(sketch.is_empty());
    assert_eq!(sketch.estimate(), 0.0);

    sketch.update("value1");
    assert!(!sketch.is_empty());
    assert_eq!(sketch.estimate(), 1.0);

    sketch.update("value2");
    assert_eq!(sketch.estimate(), 2.0);
}

#[test]
fn test_update_various_types() {
    let mut sketch = ThetaSketchBuilder::default().lg_k(12).build().unwrap();

    sketch.update("string");
    sketch.update(42i64);
    sketch.update(42u64);
    // where floating-point numbers have different representations
    sketch.update(canonical_float::from_f64(3.15));
    sketch.update(canonical_float::from_f64(3.15));
    sketch.update(canonical_float::from_f32(3.15));
    sketch.update(canonical_float::from_f32(3.15));
    sketch.update([1u8, 2, 3]);

    assert!(!sketch.is_empty());
    assert_eq!(sketch.estimate(), 5.0);

    let mut sketch = ThetaSketchBuilder::default().lg_k(12).build().unwrap();

    sketch.update("string");
    sketch.update(42i64);
    sketch.update(42u64);
    // where floating-point numbers have the same representation
    sketch.update(canonical_float::from_f64(5.0));
    sketch.update(canonical_float::from_f64(5.0));
    sketch.update(canonical_float::from_f32(5.0));
    sketch.update(canonical_float::from_f32(5.0));
    sketch.update([1u8, 2, 3]);

    assert!(!sketch.is_empty());
    assert_eq!(sketch.estimate(), 4.0);
}

#[test]
fn test_duplicate_updates() {
    let mut sketch = ThetaSketchBuilder::default().lg_k(12).build().unwrap();

    for _ in 0..100 {
        sketch.update("same_value");
    }

    assert_eq!(sketch.estimate(), 1.0);
}

#[test]
fn test_theta_reduction() {
    let mut sketch = ThetaSketchBuilder::default().lg_k(5).build().unwrap(); // Small k to trigger theta reduction
    assert!(!sketch.is_estimation_mode()); // Should be in estimation mode

    // Insert many values to trigger theta reduction
    for i in 0..1000 {
        sketch.update(format!("value_{}", i));
    }

    assert!(sketch.is_estimation_mode()); // Should be in estimation mode
    assert_that!(sketch.theta(), lt(1.0));
}

#[test]
fn test_trim() {
    let mut exact = ThetaSketchBuilder::default().lg_k(12).build().unwrap();
    let mut sketch = ThetaSketchBuilder::default().lg_k(5).build().unwrap();

    for i in 0..1000 {
        exact.update(i);
        sketch.update(i);
    }

    let mut expected_hashes: Vec<_> = exact.iter().map(|entry| entry.hash()).collect();
    expected_hashes.sort_unstable();

    let before_trim = sketch.num_retained();
    sketch.trim();

    let mut retained_hashes: Vec<_> = sketch.iter().map(|entry| entry.hash()).collect();
    retained_hashes.sort_unstable();

    assert_that!(sketch.num_retained(), le(before_trim));
    assert_eq!(sketch.num_retained(), 32);
    assert_eq!(retained_hashes, expected_hashes[..32]);
    assert_eq!(sketch.theta64(), expected_hashes[32]);
}

#[test]
fn test_reset() {
    let mut sketch = ThetaSketchBuilder::default().lg_k(5).build().unwrap();

    // Insert many values
    for i in 0..1000 {
        sketch.update(format!("value_{}", i));
    }
    assert!(!sketch.is_empty());
    assert!(sketch.is_estimation_mode());
    assert_that!(sketch.num_retained(), gt(32));
    assert_that!(sketch.theta(), lt(1.0));

    sketch.reset();
    assert!(sketch.is_empty());
    assert_eq!(sketch.estimate(), 0.0);
    assert_eq!(sketch.theta(), 1.0);
    assert_eq!(sketch.num_retained(), 0);
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.lower_bound(NumStdDev::One), 0.0);
    assert_eq!(sketch.upper_bound(NumStdDev::One), 0.0);
}

#[test]
fn test_iterator() {
    let mut sketch = ThetaSketchBuilder::default().lg_k(12).build().unwrap();

    sketch.update("value1");
    sketch.update("value2");
    sketch.update("value3");

    let count: usize = sketch.iter().count();
    assert_eq!(count, sketch.num_retained());
}

#[test]
fn test_bounds_empty_sketch() {
    let sketch = ThetaSketchBuilder::default().lg_k(12).build().unwrap();
    assert!(sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.theta(), 1.0);
    assert_eq!(sketch.estimate(), 0.0);
    assert_eq!(sketch.lower_bound(NumStdDev::One), 0.0);
    assert_eq!(sketch.upper_bound(NumStdDev::One), 0.0);
    assert_eq!(sketch.lower_bound(NumStdDev::Two), 0.0);
    assert_eq!(sketch.upper_bound(NumStdDev::Two), 0.0);
    assert_eq!(sketch.lower_bound(NumStdDev::Three), 0.0);
    assert_eq!(sketch.upper_bound(NumStdDev::Three), 0.0);
}

#[test]
fn test_bounds_exact_mode() {
    let mut sketch = ThetaSketchBuilder::default().lg_k(12).build().unwrap();
    for i in 0..2000 {
        sketch.update(i);
    }
    assert!(!sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.theta(), 1.0);
    assert_eq!(sketch.estimate(), 2000.0);
    assert_eq!(sketch.lower_bound(NumStdDev::One), 2000.0);
    assert_eq!(sketch.upper_bound(NumStdDev::One), 2000.0);
}

#[test]
fn test_bounds_estimation_mode() {
    let mut sketch = ThetaSketchBuilder::default().lg_k(12).build().unwrap();
    let n = 10000;
    for i in 0..n {
        sketch.update(i);
    }
    assert!(!sketch.is_empty());
    assert!(sketch.is_estimation_mode());
    assert_that!(sketch.theta(), lt(1.0));

    let estimate = sketch.estimate();
    let lower_bound_1 = sketch.lower_bound(NumStdDev::One);
    let upper_bound_1 = sketch.upper_bound(NumStdDev::One);
    let lower_bound_2 = sketch.lower_bound(NumStdDev::Two);
    let upper_bound_2 = sketch.upper_bound(NumStdDev::Two);
    let lower_bound_3 = sketch.lower_bound(NumStdDev::Three);
    let upper_bound_3 = sketch.upper_bound(NumStdDev::Three);

    // Check estimate is within reasonable margin (2% to be safe)
    assert_that!(estimate, near(n as f64, n as f64 * 0.02));

    // Check bounds are in correct order
    assert_that!(estimate, gt(lower_bound_1));
    assert_that!(estimate, lt(upper_bound_1));
    assert_that!(estimate, gt(lower_bound_2));
    assert_that!(estimate, lt(upper_bound_2));
    assert_that!(estimate, gt(lower_bound_3));
    assert_that!(estimate, lt(upper_bound_3));

    // Check that wider confidence intervals are indeed wider
    assert_that!(lower_bound_3, lt(lower_bound_2));
    assert_that!(lower_bound_2, lt(lower_bound_1));
    assert_that!(upper_bound_1, lt(upper_bound_2));
    assert_that!(upper_bound_2, lt(upper_bound_3));
}

#[test]
fn test_bounds_with_sampling() {
    let mut sketch = ThetaSketchBuilder::default()
        .lg_k(12)
        .sampling_probability(0.5)
        .build()
        .unwrap();

    for i in 0..1000 {
        sketch.update(i);
    }

    assert!(!sketch.is_empty());
    assert!(sketch.is_estimation_mode());
    assert_that!(sketch.theta(), lt(1.0));

    let estimate = sketch.estimate();
    let lower_bound = sketch.lower_bound(NumStdDev::Two);
    let upper_bound = sketch.upper_bound(NumStdDev::Two);

    assert_that!(estimate, ge(lower_bound));
    assert_that!(estimate, le(upper_bound));
}

#[test]
fn test_bounds_all_num_std_devs() {
    let mut sketch = ThetaSketchBuilder::default().lg_k(12).build().unwrap();
    for i in 0..10000 {
        sketch.update(i);
    }

    // Test all valid NumStdDev values work correctly
    // These no longer return Result, so we just verify they return valid values
    let lb1 = sketch.lower_bound(NumStdDev::One);
    let lb2 = sketch.lower_bound(NumStdDev::Two);
    let lb3 = sketch.lower_bound(NumStdDev::Three);
    let ub1 = sketch.upper_bound(NumStdDev::One);
    let ub2 = sketch.upper_bound(NumStdDev::Two);
    let ub3 = sketch.upper_bound(NumStdDev::Three);

    // Verify the bounds are properly ordered
    assert_that!(lb3, le(lb2));
    assert_that!(lb2, le(lb1));
    assert_that!(ub1, le(ub2));
    assert_that!(ub2, le(ub3));
}

#[test]
fn test_bounds_empty_with_sampling() {
    let sketch = ThetaSketchBuilder::default()
        .lg_k(12)
        .sampling_probability(0.1)
        .build()
        .unwrap();

    assert!(sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.estimate(), 0.0);
    assert_eq!(sketch.lower_bound(NumStdDev::One), 0.0);
    assert_eq!(sketch.upper_bound(NumStdDev::One), 0.0);
}

#[test]
fn test_sampling_state_transitions_through_compaction_and_reset() {
    let screened_value = (0u64..)
        .find(|candidate| {
            let mut sketch = ThetaSketchBuilder::default()
                .lg_k(12)
                .sampling_probability(0.5)
                .build()
                .unwrap();
            sketch.update(*candidate);
            !sketch.is_empty() && sketch.num_retained() == 0
        })
        .expect("failed to find a value screened out by the sampling theta");

    let mut sketch = ThetaSketchBuilder::default()
        .lg_k(12)
        .sampling_probability(0.5)
        .build()
        .unwrap();

    assert!(sketch.is_empty());
    assert_eq!(sketch.theta64(), MAX_THETA);
    assert!(!sketch.is_estimation_mode());
    let empty_compact = sketch.compact(false);
    assert!(empty_compact.is_empty());
    assert!(empty_compact.is_ordered());
    let bytes = empty_compact.serialize();
    assert_eq!(
        CompactThetaSketch::deserialize(&bytes).unwrap().serialize(),
        bytes
    );

    sketch.update(screened_value);

    assert!(!sketch.is_empty());
    assert_eq!(sketch.num_retained(), 0);
    assert!(sketch.is_estimation_mode());
    assert_that!(sketch.theta64(), lt(MAX_THETA));

    let compact = sketch.compact(false);
    assert!(!compact.is_empty());
    assert_eq!(compact.num_retained(), 0);
    assert_eq!(compact.theta64(), sketch.theta64());

    sketch.reset();
    assert!(sketch.is_empty());
    assert_eq!(sketch.theta64(), MAX_THETA);
    assert!(!sketch.is_estimation_mode());
}
