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

//! Behavioral tests for the Tuple sketch, mirroring `theta_sketch_test.rs`.
//!
//! Updates carry a `u64` summary combined with the default (additive) policy, so the distinct-count
//! behavior matches the Theta sketch while the summaries accumulate alongside each key.

#![cfg(feature = "tuple")]

use datasketches::common::NumStdDev;
use datasketches::hash_value;
use datasketches::tuple::TupleSketchBuilder;

#[test]
fn test_basic_update() {
    let mut sketch = TupleSketchBuilder::default().lg_k(12).build::<u64>();
    assert!(sketch.is_empty());
    assert_eq!(sketch.estimate(), 0.0);

    sketch.update("value1", 1u64);
    assert!(!sketch.is_empty());
    assert_eq!(sketch.estimate(), 1.0);

    sketch.update("value2", 1u64);
    assert_eq!(sketch.estimate(), 2.0);
}

#[test]
fn test_summary_accumulates_per_key() {
    let mut sketch = TupleSketchBuilder::default().lg_k(12).build::<u64>();
    for _ in 0..5 {
        sketch.update("same_key", 2u64);
    }
    assert_eq!(sketch.estimate(), 1.0);
    assert_eq!(sketch.num_retained(), 1);
    // The default policy folds each update into the retained summary: 5 * 2 == 10.
    assert_eq!(sketch.iter().next().unwrap().1, &10);
}

#[test]
fn test_update_various_types() {
    let mut sketch = TupleSketchBuilder::default().lg_k(12).build::<u64>();

    sketch.update("string", 1u64);
    sketch.update(42i64, 1u64);
    sketch.update(42u64, 1u64);
    // where floating-point numbers have different representations
    sketch.update(hash_value::canonical_float::from_f64(3.15), 1u64);
    sketch.update(hash_value::canonical_float::from_f64(3.15), 1u64);
    sketch.update(hash_value::canonical_float::from_f32(3.15), 1u64);
    sketch.update(hash_value::canonical_float::from_f32(3.15), 1u64);
    sketch.update([1u8, 2, 3], 1u64);

    assert!(!sketch.is_empty());
    assert_eq!(sketch.estimate(), 5.0);

    let mut sketch = TupleSketchBuilder::default().lg_k(12).build::<u64>();

    sketch.update("string", 1u64);
    sketch.update(42i64, 1u64);
    sketch.update(42u64, 1u64);
    // where floating-point numbers have the same representation
    sketch.update(hash_value::canonical_float::from_f64(5.0), 1u64);
    sketch.update(hash_value::canonical_float::from_f64(5.0), 1u64);
    sketch.update(hash_value::canonical_float::from_f32(5.0), 1u64);
    sketch.update(hash_value::canonical_float::from_f32(5.0), 1u64);
    sketch.update([1u8, 2, 3], 1u64);

    assert!(!sketch.is_empty());
    assert_eq!(sketch.estimate(), 4.0);
}

#[test]
fn test_duplicate_updates() {
    let mut sketch = TupleSketchBuilder::default().lg_k(12).build::<u64>();

    for _ in 0..100 {
        sketch.update("same_value", 1u64);
    }

    assert_eq!(sketch.estimate(), 1.0);
}

#[test]
fn test_theta_reduction() {
    let mut sketch = TupleSketchBuilder::default().lg_k(5).build::<u64>(); // Small k to trigger theta reduction
    assert!(!sketch.is_estimation_mode());

    // Insert many values to trigger theta reduction
    for i in 0..1000 {
        sketch.update(format!("value_{}", i), 1u64);
    }

    assert!(sketch.is_estimation_mode());
    assert!(sketch.theta() < 1.0);
}

#[test]
fn test_trim() {
    let mut sketch = TupleSketchBuilder::default().lg_k(5).build::<u64>();

    // Insert many values
    for i in 0..1000 {
        sketch.update(format!("value_{}", i), 1u64);
    }

    let before_trim = sketch.num_retained();
    sketch.trim();
    let after_trim = sketch.num_retained();

    // After trim, should have approximately k entries
    assert!(after_trim <= before_trim);
    assert_eq!(sketch.num_retained(), 32);
}

#[test]
fn test_reset() {
    let mut sketch = TupleSketchBuilder::default().lg_k(5).build::<u64>();

    // Insert many values
    for i in 0..1000 {
        sketch.update(format!("value_{}", i), 1u64);
    }
    assert!(!sketch.is_empty());
    assert!(sketch.is_estimation_mode());
    assert!(sketch.num_retained() > 32);
    assert!(sketch.theta() < 1.0);

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
    let mut sketch = TupleSketchBuilder::default().lg_k(12).build::<u64>();

    sketch.update("value1", 1u64);
    sketch.update("value2", 1u64);
    sketch.update("value3", 1u64);

    let count: usize = sketch.iter().count();
    assert_eq!(count, sketch.num_retained());
}

#[test]
fn test_bounds_empty_sketch() {
    let sketch = TupleSketchBuilder::default().lg_k(12).build::<u64>();
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
    let mut sketch = TupleSketchBuilder::default().lg_k(12).build::<u64>();
    for i in 0..2000 {
        sketch.update(i, 1u64);
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
    let mut sketch = TupleSketchBuilder::default().lg_k(12).build::<u64>();
    let n = 10000;
    for i in 0..n {
        sketch.update(i, 1u64);
    }
    assert!(!sketch.is_empty());
    assert!(sketch.is_estimation_mode());
    assert!(sketch.theta() < 1.0);

    let estimate = sketch.estimate();
    let lower_bound_1 = sketch.lower_bound(NumStdDev::One);
    let upper_bound_1 = sketch.upper_bound(NumStdDev::One);
    let lower_bound_2 = sketch.lower_bound(NumStdDev::Two);
    let upper_bound_2 = sketch.upper_bound(NumStdDev::Two);
    let lower_bound_3 = sketch.lower_bound(NumStdDev::Three);
    let upper_bound_3 = sketch.upper_bound(NumStdDev::Three);

    // Check estimate is within reasonable margin (2% to be safe)
    assert!(
        (estimate - n as f64).abs() < n as f64 * 0.02,
        "estimate {} is not within 2% of {}",
        estimate,
        n
    );

    // Check bounds are in correct order
    assert!(lower_bound_1 < estimate);
    assert!(estimate < upper_bound_1);
    assert!(lower_bound_2 < estimate);
    assert!(estimate < upper_bound_2);
    assert!(lower_bound_3 < estimate);
    assert!(estimate < upper_bound_3);

    // Check that wider confidence intervals are indeed wider
    assert!(lower_bound_3 < lower_bound_2);
    assert!(lower_bound_2 < lower_bound_1);
    assert!(upper_bound_1 < upper_bound_2);
    assert!(upper_bound_2 < upper_bound_3);
}

#[test]
fn test_bounds_with_sampling() {
    let mut sketch = TupleSketchBuilder::default()
        .lg_k(12)
        .sampling_probability(0.5)
        .build::<u64>();

    for i in 0..1000 {
        sketch.update(i, 1u64);
    }

    assert!(!sketch.is_empty());
    assert!(sketch.is_estimation_mode());
    assert!(sketch.theta() < 1.0);

    let estimate = sketch.estimate();
    let lower_bound = sketch.lower_bound(NumStdDev::Two);
    let upper_bound = sketch.upper_bound(NumStdDev::Two);

    assert!(lower_bound <= estimate);
    assert!(estimate <= upper_bound);
}

#[test]
fn test_bounds_all_num_std_devs() {
    let mut sketch = TupleSketchBuilder::default().lg_k(12).build::<u64>();
    for i in 0..10000 {
        sketch.update(i, 1u64);
    }

    let lb1 = sketch.lower_bound(NumStdDev::One);
    let lb2 = sketch.lower_bound(NumStdDev::Two);
    let lb3 = sketch.lower_bound(NumStdDev::Three);
    let ub1 = sketch.upper_bound(NumStdDev::One);
    let ub2 = sketch.upper_bound(NumStdDev::Two);
    let ub3 = sketch.upper_bound(NumStdDev::Three);

    // Verify the bounds are properly ordered
    assert!(lb3 <= lb2);
    assert!(lb2 <= lb1);
    assert!(ub1 <= ub2);
    assert!(ub2 <= ub3);
}

#[test]
fn test_bounds_empty_estimation_mode() {
    // Create a sketch with sampling probability < 1.0 to force estimation mode
    let sketch = TupleSketchBuilder::default()
        .lg_k(12)
        .sampling_probability(0.1)
        .build::<u64>();

    // The sketch is empty but theta < 1.0, so it's in estimation mode.
    // When empty, both bounds should return 0.0 (matching the Java/Theta behavior).
    assert!(sketch.is_empty());
    assert!(sketch.is_estimation_mode());
    assert_eq!(sketch.estimate(), 0.0);
    assert_eq!(sketch.lower_bound(NumStdDev::One), 0.0);
    assert_eq!(sketch.upper_bound(NumStdDev::One), 0.0);
}

#[test]
fn test_compact_preserves_logical_non_empty_after_screened_update() {
    let screened_value = (0u64..)
        .find(|candidate| {
            let mut sketch = TupleSketchBuilder::default()
                .lg_k(12)
                .sampling_probability(0.5)
                .build::<u64>();
            sketch.update(*candidate, 1u64);
            !sketch.is_empty() && sketch.num_retained() == 0
        })
        .expect("failed to find a value screened out by the sampling theta");

    let mut sketch = TupleSketchBuilder::default()
        .lg_k(12)
        .sampling_probability(0.5)
        .build::<u64>();
    sketch.update(screened_value, 1u64);

    assert!(!sketch.is_empty());
    assert_eq!(sketch.num_retained(), 0);

    let compact = sketch.compact(false);
    assert!(!compact.is_empty());
    assert_eq!(compact.num_retained(), 0);
    assert_eq!(compact.theta64(), sketch.theta64());
}
