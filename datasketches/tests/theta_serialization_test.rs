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

//! Serialization tests for ThetaSketch

use datasketches::theta::ThetaSketch;

#[test]
fn test_serialize_empty() {
    let sketch = ThetaSketch::builder().lg_k(12).build();
    let bytes = sketch.serialize();

    // Empty sketch should be 8 bytes (1 preamble long)
    assert_eq!(bytes.len(), 8, "Empty sketch should be 8 bytes");

    // Verify preamble
    assert_eq!(bytes[0], 1, "PreLongs should be 1 for empty");
    assert_eq!(bytes[1], 3, "SerVer should be 3");
    assert_eq!(bytes[2], 3, "FamilyID should be 3 (Theta)");
    assert_eq!(bytes[3], 12, "lg_k should be 12");

    // Round-trip
    let restored = ThetaSketch::deserialize(&bytes).unwrap();
    assert!(restored.is_empty());
    assert_eq!(restored.estimate(), 0.0);
    assert_eq!(restored.lg_k(), 12);
}

#[test]
fn test_serialize_single_item() {
    let mut sketch = ThetaSketch::builder().lg_k(12).build();
    sketch.update("apple");

    let bytes = sketch.serialize();

    // Single item in exact mode: 16 bytes preamble + 8 bytes data = 24 bytes
    assert_eq!(bytes.len(), 24, "Single item sketch should be 24 bytes");

    // Verify preamble
    assert_eq!(bytes[0], 2, "PreLongs should be 2 for exact mode");

    // Round-trip
    let restored = ThetaSketch::deserialize(&bytes).unwrap();
    assert!(!restored.is_empty());
    assert_eq!(restored.estimate(), 1.0);
    assert_eq!(restored.num_retained(), 1);
}

#[test]
fn test_serialize_exact_mode() {
    let mut sketch = ThetaSketch::builder().lg_k(12).build();
    for i in 0..100 {
        sketch.update(format!("item_{}", i));
    }

    assert!(!sketch.is_estimation_mode(), "Should be in exact mode");

    let bytes = sketch.serialize();

    // Exact mode: 16 bytes preamble + 100 * 8 bytes data
    let expected_size = 16 + 100 * 8;
    assert_eq!(bytes.len(), expected_size);

    // Verify preamble
    assert_eq!(bytes[0], 2, "PreLongs should be 2 for exact mode");

    // Round-trip
    let restored = ThetaSketch::deserialize(&bytes).unwrap();
    assert_eq!(restored.estimate(), sketch.estimate());
    assert_eq!(restored.num_retained(), sketch.num_retained());
    assert!(!restored.is_estimation_mode());
}

#[test]
fn test_serialize_estimation_mode() {
    let mut sketch = ThetaSketch::builder().lg_k(10).build(); // Small k to trigger estimation
    for i in 0..10000 {
        sketch.update(i);
    }

    assert!(sketch.is_estimation_mode(), "Should be in estimation mode");

    let bytes = sketch.serialize();

    // Estimation mode: 24 bytes preamble + entries * 8 bytes
    let num_entries = sketch.num_retained();
    let expected_size = 24 + num_entries * 8;
    assert_eq!(bytes.len(), expected_size);

    // Verify preamble
    assert_eq!(bytes[0], 3, "PreLongs should be 3 for estimation mode");

    // Round-trip
    let restored = ThetaSketch::deserialize(&bytes).unwrap();

    // Estimates should be close (accounting for floating point)
    let diff = (restored.estimate() - sketch.estimate()).abs();
    assert!(
        diff < 1.0,
        "Estimates should match: {} vs {}",
        restored.estimate(),
        sketch.estimate()
    );

    assert_eq!(restored.num_retained(), sketch.num_retained());
    assert!(restored.is_estimation_mode());
    assert!(restored.theta() < 1.0);
}

#[test]
fn test_round_trip_various_lg_k() {
    for lg_k in [5, 8, 10, 12, 14, 16] {
        let mut sketch = ThetaSketch::builder().lg_k(lg_k).build();
        for i in 0..500 {
            sketch.update(format!("lg_k_{}_item_{}", lg_k, i));
        }

        let bytes = sketch.serialize();
        let restored = ThetaSketch::deserialize(&bytes).unwrap();

        assert_eq!(restored.lg_k(), lg_k, "lg_k mismatch for lg_k={}", lg_k);
        assert_eq!(
            restored.estimate(),
            sketch.estimate(),
            "Estimate mismatch for lg_k={}",
            lg_k
        );
        assert_eq!(
            restored.num_retained(),
            sketch.num_retained(),
            "Retained count mismatch for lg_k={}",
            lg_k
        );
    }
}

#[test]
fn test_deserialize_invalid_data() {
    // Too short
    let result = ThetaSketch::deserialize(&[1, 2, 3]);
    assert!(result.is_err());

    // Wrong family ID
    let bad_family = vec![1, 3, 99, 12, 12, 0, 0, 0]; // family = 99
    let result = ThetaSketch::deserialize(&bad_family);
    assert!(result.is_err());
}

#[test]
fn test_serialize_with_custom_seed() {
    let custom_seed = 12345u64;
    let mut sketch = ThetaSketch::builder().lg_k(10).seed(custom_seed).build();
    sketch.update("test");

    let bytes = sketch.serialize();

    // Should fail with default seed
    let result = ThetaSketch::deserialize(&bytes);
    assert!(result.is_err(), "Should fail with wrong seed");

    // Should succeed with correct seed
    let restored = ThetaSketch::deserialize_with_seed(&bytes, custom_seed).unwrap();
    assert_eq!(restored.estimate(), sketch.estimate());
}

#[test]
fn test_serialized_entries_are_sorted() {
    let mut sketch = ThetaSketch::builder().lg_k(12).build();
    for i in 0..10 {
        sketch.update(i);
    }

    let bytes = sketch.serialize();

    // Read hash entries from serialized data (after 16-byte preamble)
    let data_start = 16;
    let mut entries: Vec<u64> = Vec::new();
    let mut offset = data_start;
    while offset + 8 <= bytes.len() {
        let entry = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        entries.push(entry);
        offset += 8;
    }

    // Verify entries are sorted
    let mut sorted = entries.clone();
    sorted.sort();
    assert_eq!(entries, sorted, "Serialized entries should be sorted");
}
