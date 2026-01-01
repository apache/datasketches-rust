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

use datasketches::theta::ThetaSketch;

#[test]
fn test_basic_update() {
    let mut sketch = ThetaSketch::builder().lg_k(12).build();
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
    let mut sketch = ThetaSketch::builder().lg_k(12).build();

    sketch.update("string");
    sketch.update(42i64);
    sketch.update(42u64);
    sketch.update_f64(3.15);
    sketch.update_f64(3.15);
    sketch.update_f32(3.15);
    sketch.update_f32(3.15);
    sketch.update([1u8, 2, 3]);

    assert!(!sketch.is_empty());
    assert_eq!(sketch.estimate(), 5.0);
}

#[test]
fn test_duplicate_updates() {
    let mut sketch = ThetaSketch::builder().lg_k(12).build();

    for _ in 0..100 {
        sketch.update("same_value");
    }

    assert_eq!(sketch.estimate(), 1.0);
}

#[test]
fn test_theta_reduction() {
    let mut sketch = ThetaSketch::builder().lg_k(5).build(); // Small k to trigger theta reduction
    assert!(!sketch.is_estimation_mode()); // Should be in estimation mode

    // Insert many values to trigger theta reduction
    for i in 0..1000 {
        sketch.update(format!("value_{}", i));
    }

    assert!(sketch.is_estimation_mode()); // Should be in estimation mode
    assert!(sketch.theta() < 1.0);
}

#[test]
fn test_trim() {
    let mut sketch = ThetaSketch::builder().lg_k(5).build();

    // Insert many values
    for i in 0..1000 {
        sketch.update(format!("value_{}", i));
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
    let mut sketch = ThetaSketch::builder().lg_k(5).build();

    // Insert many values
    for i in 0..1000 {
        sketch.update(format!("value_{}", i));
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
    assert_eq!(sketch.lower_bound(1).unwrap(), 0.0);
    assert_eq!(sketch.upper_bound(1).unwrap(), 0.0);
}

#[test]
fn test_iterator() {
    let mut sketch = ThetaSketch::builder().lg_k(12).build();

    sketch.update("value1");
    sketch.update("value2");
    sketch.update("value3");

    let count: usize = sketch.iter().count();
    assert_eq!(count, sketch.num_retained());
}

#[test]
fn test_bounds_empty_sketch() {
    let sketch = ThetaSketch::builder().lg_k(12).build();
    assert!(sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.theta(), 1.0);
    assert_eq!(sketch.estimate(), 0.0);
    assert_eq!(sketch.lower_bound(1).unwrap(), 0.0);
    assert_eq!(sketch.upper_bound(1).unwrap(), 0.0);
    assert_eq!(sketch.lower_bound(2).unwrap(), 0.0);
    assert_eq!(sketch.upper_bound(2).unwrap(), 0.0);
    assert_eq!(sketch.lower_bound(3).unwrap(), 0.0);
    assert_eq!(sketch.upper_bound(3).unwrap(), 0.0);
}

#[test]
fn test_bounds_exact_mode() {
    let mut sketch = ThetaSketch::builder().lg_k(12).build();
    for i in 0..2000 {
        sketch.update(i);
    }
    assert!(!sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.theta(), 1.0);
    assert_eq!(sketch.estimate(), 2000.0);
    assert_eq!(sketch.lower_bound(1).unwrap(), 2000.0);
    assert_eq!(sketch.upper_bound(1).unwrap(), 2000.0);
}

#[test]
fn test_bounds_estimation_mode() {
    let mut sketch = ThetaSketch::builder().lg_k(12).build();
    let n = 10000;
    for i in 0..n {
        sketch.update(i);
    }
    assert!(!sketch.is_empty());
    assert!(sketch.is_estimation_mode());
    assert!(sketch.theta() < 1.0);

    let estimate = sketch.estimate();
    let lower_bound_1 = sketch.lower_bound(1).unwrap();
    let upper_bound_1 = sketch.upper_bound(1).unwrap();
    let lower_bound_2 = sketch.lower_bound(2).unwrap();
    let upper_bound_2 = sketch.upper_bound(2).unwrap();
    let lower_bound_3 = sketch.lower_bound(3).unwrap();
    let upper_bound_3 = sketch.upper_bound(3).unwrap();

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
    let mut sketch = ThetaSketch::builder()
        .lg_k(12)
        .sampling_probability(0.5)
        .build();

    for i in 0..1000 {
        sketch.update(i);
    }

    assert!(!sketch.is_empty());
    assert!(sketch.is_estimation_mode());
    assert!(sketch.theta() < 1.0);

    let estimate = sketch.estimate();
    let lower_bound = sketch.lower_bound(2).unwrap();
    let upper_bound = sketch.upper_bound(2).unwrap();

    assert!(lower_bound <= estimate);
    assert!(estimate <= upper_bound);
}

#[test]
fn test_bounds_invalid_num_std_devs() {
    let mut sketch = ThetaSketch::builder().lg_k(12).build();
    for i in 0..10000 {
        sketch.update(i);
    }

    // Test invalid values
    assert!(sketch.lower_bound(0).is_err());
    assert!(sketch.lower_bound(4).is_err());
    assert!(sketch.upper_bound(0).is_err());
    assert!(sketch.upper_bound(4).is_err());

    // Test valid values
    assert!(sketch.lower_bound(1).is_ok());
    assert!(sketch.lower_bound(2).is_ok());
    assert!(sketch.lower_bound(3).is_ok());
    assert!(sketch.upper_bound(1).is_ok());
    assert!(sketch.upper_bound(2).is_ok());
    assert!(sketch.upper_bound(3).is_ok());
}

#[test]
fn test_bounds_empty_estimation_mode() {
    // Create a sketch with sampling probability < 1.0 to force estimation mode
    let sketch = ThetaSketch::builder()
        .lg_k(12)
        .sampling_probability(0.1)
        .build();

    // The sketch is empty but theta < 1.0, so it's in estimation mode
    // However, when empty, both bounds should return 0.0 per Java implementation
    assert!(sketch.is_empty());
    assert!(sketch.is_estimation_mode());
    assert_eq!(sketch.estimate(), 0.0);
    assert_eq!(sketch.lower_bound(1).unwrap(), 0.0);
    assert_eq!(sketch.upper_bound(1).unwrap(), 0.0);
}
