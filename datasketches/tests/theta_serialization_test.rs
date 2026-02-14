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

//! Theta Sketch Serialization Tests
//!
//! These tests verify binary serialization/deserialization for Theta sketches,
//! including cross-language compatibility with Java and C++ implementations.

mod common;

use std::fs;

use common::serialization_test_data;
use datasketches::common::NumStdDev;
use datasketches::theta::CompactThetaSketch;
use datasketches::theta::ThetaSketch;

#[test]
fn test_serialize_deserialize_empty_sketch() {
    let sketch = ThetaSketch::builder().lg_k(12).build();
    assert!(sketch.is_empty());

    let bytes = sketch.serialize();
    let restored = ThetaSketch::deserialize(&bytes).unwrap();

    assert!(restored.is_empty());
    assert_eq!(sketch.estimate(), restored.estimate());
    assert_eq!(sketch.theta64(), restored.theta64());
}

#[test]
fn test_serialize_deserialize_single_value() {
    let mut sketch = ThetaSketch::builder().lg_k(12).build();
    sketch.update("single_value");

    let bytes = sketch.serialize();
    let restored = ThetaSketch::deserialize(&bytes).unwrap();

    assert!(!restored.is_empty());
    assert_eq!(sketch.estimate(), restored.estimate());
    assert_eq!(sketch.num_retained(), restored.num_retained());
}

#[test]
fn test_serialize_deserialize_multiple_values() {
    let mut sketch = ThetaSketch::builder().lg_k(12).build();
    for i in 0..100 {
        sketch.update(format!("value_{}", i));
    }

    let bytes = sketch.serialize();
    let restored = ThetaSketch::deserialize(&bytes).unwrap();

    assert_eq!(sketch.num_retained(), restored.num_retained());
    assert_eq!(sketch.estimate(), restored.estimate());
    assert!(!restored.is_estimation_mode()); // 100 values shouldn't trigger estimation mode with lg_k=12
}

#[test]
fn test_serialize_deserialize_estimation_mode() {
    let mut sketch = ThetaSketch::builder().lg_k(5).build(); // Small k to trigger estimation mode

    // Insert enough values to trigger estimation mode
    for i in 0..1000 {
        sketch.update(format!("value_{}", i));
    }

    assert!(sketch.is_estimation_mode());

    let bytes = sketch.serialize();
    let restored = ThetaSketch::deserialize(&bytes).unwrap();

    assert!(restored.is_estimation_mode());
    assert_eq!(sketch.theta64(), restored.theta64());
    assert_eq!(sketch.num_retained(), restored.num_retained());

    // Estimates should be equal
    let sketch_estimate = sketch.estimate();
    let restored_estimate = restored.estimate();
    assert!(
        (sketch_estimate - restored_estimate).abs() < 0.001,
        "Estimates differ: {} vs {}",
        sketch_estimate,
        restored_estimate
    );
}

#[test]
fn test_serialize_deserialize_with_custom_seed() {
    let custom_seed = 12345u64;
    let mut sketch = ThetaSketch::builder().lg_k(12).seed(custom_seed).build();

    for i in 0..50 {
        sketch.update(i);
    }

    let bytes = sketch.serialize();

    // Should fail with wrong seed
    let result = ThetaSketch::deserialize(&bytes);
    assert!(result.is_err(), "Should fail with default seed");

    // Should succeed with correct seed
    let restored = ThetaSketch::deserialize_with_seed(&bytes, custom_seed).unwrap();
    assert_eq!(sketch.estimate(), restored.estimate());
}

#[test]
fn test_round_trip_preserves_entries() {
    let mut sketch = ThetaSketch::builder().lg_k(12).build();
    for i in 0..50 {
        sketch.update(format!("value_{}", i));
    }

    let original_entries: Vec<u64> = sketch.iter().collect();

    let bytes = sketch.serialize();
    let restored = ThetaSketch::deserialize(&bytes).unwrap();

    let restored_entries: Vec<u64> = restored.iter().collect();

    // Since compact sketches store sorted entries, compare sorted
    let mut original_sorted = original_entries.clone();
    original_sorted.sort_unstable();

    assert_eq!(original_sorted, restored_entries);
}

#[test]
fn test_compact_preserves_functionality() {
    let mut sketch = ThetaSketch::builder().lg_k(10).build();
    for i in 0..500 {
        sketch.update(i);
    }

    let compact = sketch.compact();

    // All functionality should work on compact sketch
    assert_eq!(sketch.estimate(), compact.estimate());
    assert_eq!(sketch.theta(), compact.theta());
    assert_eq!(sketch.theta64(), compact.theta64());
    assert_eq!(sketch.is_empty(), compact.is_empty());
    assert_eq!(sketch.is_estimation_mode(), compact.is_estimation_mode());
    assert_eq!(sketch.num_retained(), compact.num_retained());

    // Bounds should also match
    assert_eq!(
        sketch.lower_bound(NumStdDev::One),
        compact.lower_bound(NumStdDev::One)
    );
    assert_eq!(
        sketch.upper_bound(NumStdDev::One),
        compact.upper_bound(NumStdDev::One)
    );
    assert_eq!(
        sketch.lower_bound(NumStdDev::Two),
        compact.lower_bound(NumStdDev::Two)
    );
    assert_eq!(
        sketch.upper_bound(NumStdDev::Two),
        compact.upper_bound(NumStdDev::Two)
    );
}

#[test]
fn test_serialization_size() {
    // Empty sketch should be minimal
    let empty_sketch = ThetaSketch::builder().build();
    let empty_bytes = empty_sketch.serialize();
    assert_eq!(empty_bytes.len(), 8); // 1 preamble long

    // Non-empty sketch in exact mode
    let mut exact_sketch = ThetaSketch::builder().lg_k(12).build();
    for i in 0..10 {
        exact_sketch.update(i);
    }
    let exact_bytes = exact_sketch.serialize();
    // 2 preamble longs (16 bytes) + 10 hash values (80 bytes) = 96 bytes
    assert_eq!(exact_bytes.len(), 16 + 10 * 8);

    // Sketch in estimation mode
    let mut estimation_sketch = ThetaSketch::builder().lg_k(5).build();
    for i in 0..1000 {
        estimation_sketch.update(i);
    }
    let estimation_bytes = estimation_sketch.serialize();
    // 3 preamble longs (24 bytes) + entries * 8 bytes
    let expected_size = 24 + estimation_sketch.num_retained() * 8;
    assert_eq!(estimation_bytes.len(), expected_size);
}

#[test]
fn test_deserialize_truncated_data() {
    let mut sketch = ThetaSketch::builder().build();
    sketch.update("test");
    let bytes = sketch.serialize();

    // Try to deserialize truncated data
    for len in 0..bytes.len() - 1 {
        let truncated = &bytes[..len];
        let result = CompactThetaSketch::deserialize(truncated);
        assert!(result.is_err(), "Should fail with {} bytes", len);
    }
}

#[test]
fn test_multiple_serialization_round_trips() {
    let mut sketch = ThetaSketch::builder().lg_k(10).build();
    for i in 0..100 {
        sketch.update(i);
    }

    let original_estimate = sketch.estimate();

    // Multiple round trips should preserve data
    let mut bytes = sketch.serialize();
    for _ in 0..5 {
        let restored = CompactThetaSketch::deserialize(&bytes).unwrap();
        assert_eq!(original_estimate, restored.estimate());
        bytes = restored.serialize();
    }

    let final_sketch = CompactThetaSketch::deserialize(&bytes).unwrap();
    assert_eq!(original_estimate, final_sketch.estimate());
}

#[test]
fn test_different_lg_k_values() {
    for lg_k in [5, 8, 10, 12, 16, 20] {
        let mut sketch = ThetaSketch::builder().lg_k(lg_k).build();
        for i in 0..100 {
            sketch.update(i);
        }

        let bytes = sketch.serialize();
        let restored = ThetaSketch::deserialize(&bytes).unwrap();

        assert_eq!(
            sketch.estimate(),
            restored.estimate(),
            "Failed for lg_k={}",
            lg_k
        );
    }
}

#[test]
fn test_sampling_probability_serialization() {
    let mut sketch = ThetaSketch::builder()
        .lg_k(12)
        .sampling_probability(0.5)
        .build();

    for i in 0..1000 {
        sketch.update(i);
    }

    // Should be in estimation mode due to sampling
    assert!(sketch.is_estimation_mode());

    let bytes = sketch.serialize();
    let restored = ThetaSketch::deserialize(&bytes).unwrap();

    assert!(restored.is_estimation_mode());
    assert_eq!(sketch.theta64(), restored.theta64());
}

// =============================================================================
// Cross-language compatibility tests (Java)
// =============================================================================

#[test]
fn test_java_theta_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10000, 100000, 1000000];
    for n in test_cases {
        let filename = format!("theta_n{}_java.sk", n);
        let path = serialization_test_data("java_generated_files", &filename);
        let bytes = fs::read(&path).unwrap();
        let sketch = CompactThetaSketch::deserialize(&bytes).unwrap();

        if n == 0 {
            assert!(sketch.is_empty(), "Sketch should be empty for n=0");
        } else {
            assert!(!sketch.is_empty(), "Sketch should not be empty for n={}", n);
            let estimate = sketch.estimate();
            let error = (estimate - n as f64).abs() / n as f64;
            assert!(
                error <= 0.03,
                "Estimate {} too far from expected {} (error: {:.2}%)",
                estimate,
                n,
                error * 100.0
            );
        }
    }
}

#[test]
fn test_java_theta_non_empty_no_entries() {
    let path =
        serialization_test_data("java_generated_files", "theta_non_empty_no_entries_java.sk");
    let bytes = fs::read(&path).unwrap();
    let sketch = CompactThetaSketch::deserialize(&bytes).unwrap();

    assert!(!sketch.is_empty());
    assert_eq!(sketch.num_retained(), 0);
}
