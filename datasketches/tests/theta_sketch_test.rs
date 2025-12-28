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
    let mut sketch = ThetaSketch::builder().set_lg_k(12).build();
    assert!(sketch.is_empty());
    assert_eq!(sketch.get_estimate(), 0.0);

    sketch.update("value1");
    assert!(!sketch.is_empty());
    assert_eq!(sketch.get_estimate(), 1.0);

    sketch.update("value2");
    assert_eq!(sketch.get_estimate(), 2.0);
}

#[test]
fn test_update_various_types() {
    let mut sketch = ThetaSketch::builder().set_lg_k(12).build();

    sketch.update("string");
    sketch.update(42i64);
    sketch.update(42u64);
    sketch.update_f64(3.15);
    sketch.update_f64(3.15);
    sketch.update_f32(3.15);
    sketch.update_f32(3.15);
    sketch.update([1u8, 2, 3]);

    assert!(!sketch.is_empty());
    assert_eq!(sketch.get_estimate(), 5.0);
}

#[test]
fn test_duplicate_updates() {
    let mut sketch = ThetaSketch::builder().set_lg_k(12).build();

    for _ in 0..100 {
        sketch.update("same_value");
    }

    assert_eq!(sketch.get_estimate(), 1.0);
}

#[test]
fn test_theta_reduction() {
    let mut sketch = ThetaSketch::builder().set_lg_k(5).build(); // Small k to trigger theta reduction
    assert!(!sketch.is_estimation_mode()); // Should be in estimation mode

    // Insert many values to trigger theta reduction
    for i in 0..1000 {
        sketch.update(format!("value_{}", i));
    }

    assert!(sketch.is_estimation_mode()); // Should be in estimation mode
    assert!(sketch.get_theta() < 1.0);
}

#[test]
fn test_trim() {
    let mut sketch = ThetaSketch::builder().set_lg_k(5).build();

    // Insert many values
    for i in 0..1000 {
        sketch.update(format!("value_{}", i));
    }

    let before_trim = sketch.get_num_retained();
    sketch.trim();
    let after_trim = sketch.get_num_retained();

    // After trim, should have approximately k entries
    assert!(after_trim <= before_trim);
    assert_eq!(sketch.get_num_retained(), 32);
}

#[test]
fn test_reset() {
    let mut sketch = ThetaSketch::builder().set_lg_k(5).build();

    // Insert many values
    for i in 0..1000 {
        sketch.update(format!("value_{}", i));
    }
    assert!(!sketch.is_empty());
    assert!(sketch.is_estimation_mode());
    assert!(sketch.get_num_retained() > 32);
    assert!(sketch.get_theta() < 1.0);

    sketch.reset();
    assert!(sketch.is_empty());
    assert_eq!(sketch.get_estimate(), 0.0);
    assert_eq!(sketch.get_theta(), 1.0);
    assert_eq!(sketch.get_num_retained(), 0);
    assert!(!sketch.is_estimation_mode());
}

#[test]
fn test_iterator() {
    let mut sketch = ThetaSketch::builder().set_lg_k(12).build();

    sketch.update("value1");
    sketch.update("value2");
    sketch.update("value3");

    let count: usize = sketch.iter().count();
    assert_eq!(count, sketch.get_num_retained());
}
