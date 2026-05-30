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

#![cfg(feature = "req")]

//! Serialization round-trip and cross-language compatibility tests for ReqSketch.

use std::fs;
use std::path::PathBuf;

use crate::serialization_test_data;
use datasketches::req::RankAccuracy;
use datasketches::req::ReqSketch;
use datasketches::req::ReqValue;
use datasketches::req::SearchCriteria;

// ---------- Rust ↔ Rust round-trip ----------

fn round_trip_one<T>(k: u16, ra: RankAccuracy, n: u64, make_item: impl Fn(u64) -> T)
where
    T: ReqValue + std::fmt::Debug + PartialEq,
{
    let mut a: ReqSketch<T> = ReqSketch::try_new(k, ra).unwrap();
    for i in 0..n {
        a.update(make_item(i));
    }
    let bytes = a.serialize();
    let b: ReqSketch<T> = ReqSketch::deserialize(&bytes).unwrap();
    assert_eq!(a.n(), b.n());
    assert_eq!(a.k(), b.k());
    assert_eq!(a.rank_accuracy(), b.rank_accuracy());
    assert_eq!(a.min_item(), b.min_item());
    assert_eq!(a.max_item(), b.max_item());
    assert_eq!(bytes, b.serialize(), "non-stable serialization");
}

#[test]
fn round_trip_f64_matrix() {
    for &k in &[4u16, 12, 1024] {
        for &ra in &[RankAccuracy::HighRank, RankAccuracy::LowRank] {
            for &n in &[0u64, 1, 4, 5, 100, 10_000] {
                round_trip_one::<f64>(k, ra, n, |i| i as f64);
            }
        }
    }
}

#[test]
fn round_trip_f32_basic() {
    for &n in &[0u64, 1, 4, 5, 1000] {
        round_trip_one::<f32>(12, RankAccuracy::HighRank, n, |i| i as f32);
    }
}

#[test]
fn round_trip_i64_basic() {
    for &n in &[0u64, 1, 4, 5, 1000] {
        round_trip_one::<i64>(12, RankAccuracy::HighRank, n, |i| i as i64);
    }
}

// ---------- Deserialize error paths ----------
//
// Each test crafts a malformed byte sequence and asserts that deserialize returns
// Err, exercising the validation guards in ReqSketch::deserialize.

use datasketches::error::ErrorKind;

#[test]
fn deserialize_truncated_preamble() {
    // Less than 8 bytes — can't even read the fixed preamble.
    for n in 0..8usize {
        let bytes = vec![0u8; n];
        let result = ReqSketch::<f32>::deserialize(&bytes);
        assert!(
            result.is_err(),
            "deserialize succeeded with {} bytes (expected error)",
            n
        );
    }
}

#[test]
fn deserialize_wrong_family_id() {
    // Valid preamble structure but family != 17.
    // Flags=4 (IS_EMPTY), k=12 (little-endian: 12, 0).
    let bytes = [
        2u8,  // preamble_ints (PREAMBLE_INTS_EXACT)
        1u8,  // serial_version
        99u8, // family — wrong (REQ is 17)
        4u8,  // flags (IS_EMPTY)
        12u8, 0u8, // k = 12
        0u8, // num_levels
        0u8, // num_raw_items
    ];
    let result = ReqSketch::<f32>::deserialize(&bytes);
    assert!(result.is_err(), "deserialize accepted wrong family id");
    let err = result.unwrap_err();
    assert_eq!(
        err.kind(),
        ErrorKind::InvalidData,
        "wrong error kind: {:?}",
        err.kind()
    );
}

#[test]
fn deserialize_wrong_serial_version() {
    // Serial version != 1 should be rejected.
    let bytes = [
        2u8, 99u8, // serial_version — wrong (REQ uses 1)
        17u8, 4u8, // IS_EMPTY
        12u8, 0u8, 0u8, 0u8,
    ];
    let result = ReqSketch::<f32>::deserialize(&bytes);
    assert!(result.is_err(), "deserialize accepted wrong serial version");
}

#[test]
fn deserialize_invalid_preamble_ints() {
    // preamble_ints must be 2 (exact) or 4 (estimation). Try 3.
    let bytes = [3u8, 1, 17, 4, 12, 0, 0, 0];
    let result = ReqSketch::<f32>::deserialize(&bytes);
    assert!(
        result.is_err(),
        "deserialize accepted invalid preamble_ints=3"
    );
}

#[test]
fn deserialize_rejects_non_empty_zero_levels() {
    // Non-empty flags with num_levels=0 used to create a sketch with n=1 but
    // no level-0 compactor, causing the next update to panic.
    let bytes = [
        2u8, // PREAMBLE_INTS_EXACT
        1, 17, 8u8, // IS_HIGH_RANK only: not empty, not raw
        12, 0,   // k
        0u8, // num_levels=0 is invalid for non-empty sketches
        0u8,
    ];
    let result = ReqSketch::<f32>::deserialize(&bytes);
    assert!(
        result.is_err(),
        "deserialize accepted non-empty sketch with zero levels"
    );
}

#[test]
fn deserialize_rejects_inconsistent_raw_items_header() {
    // RAW_ITEMS is only valid for one non-empty level with 1..=4 raw items.
    let raw_with_no_items = [
        2u8, 1, 17, 24u8, // IS_HIGH_RANK | RAW_ITEMS
        12, 0, 1u8, // num_levels
        0u8, // invalid raw item count
    ];
    assert!(
        ReqSketch::<f32>::deserialize(&raw_with_no_items).is_err(),
        "deserialize accepted raw-items sketch with no raw items"
    );

    let raw_with_two_levels = [
        4u8, 1, 17, 24u8, // IS_HIGH_RANK | RAW_ITEMS
        12, 0, 2u8, // invalid for raw-items sketches
        1u8,
    ];
    assert!(
        ReqSketch::<f32>::deserialize(&raw_with_two_levels).is_err(),
        "deserialize accepted raw-items sketch with multiple levels"
    );
}

#[test]
fn deserialize_odd_k() {
    // k must be even. Try k=11.
    let bytes = [
        2u8, 1, 17, 4u8, // IS_EMPTY
        11u8, 0u8, // k=11 (odd)
        0u8, 0u8,
    ];
    let result = ReqSketch::<f32>::deserialize(&bytes);
    assert!(result.is_err(), "deserialize accepted odd k=11");
}

#[test]
fn deserialize_k_out_of_range() {
    // k must be in [4, 1024]. Try k=2 (too small).
    let bytes_small = [2u8, 1, 17, 4, 2, 0, 0, 0];
    assert!(
        ReqSketch::<f32>::deserialize(&bytes_small).is_err(),
        "accepted k=2"
    );

    // k=2048 (too large): little-endian 2048 = [0x00, 0x08]
    let bytes_big = [2u8, 1, 17, 4, 0, 8, 0, 0];
    assert!(
        ReqSketch::<f32>::deserialize(&bytes_big).is_err(),
        "accepted k=2048"
    );
}

#[test]
fn deserialize_truncated_estimation_mode() {
    // preamble_ints=4, num_levels=2 (multi-level), not empty — code will try to read
    // n (u64) + min_f32 + max_f32 + compactor preambles, but we provide nothing beyond
    // the 8-byte preamble.
    // flags=8 (IS_HIGH_RANK only — not empty, not raw).
    let bytes = [
        4u8, // PREAMBLE_INTS_ESTIMATION
        1, 17, 8u8, // IS_HIGH_RANK only (not empty, not raw)
        12, 0,   // k
        2u8, // num_levels = 2 (triggers n/min/max read)
        0u8, /* num_raw_items
              * no payload — truncated */
    ];
    let result = ReqSketch::<f32>::deserialize(&bytes);
    assert!(
        result.is_err(),
        "deserialize accepted truncated estimation-mode bytes"
    );
}

#[test]
fn deserialize_truncated_raw_items() {
    // raw_items=true (FLAG_RAW_ITEMS=0x10), num_raw_items=3, but only 1 f32 follows.
    // flags = IS_HIGH_RANK | RAW_ITEMS = 8 | 16 = 24, num_levels=1
    let bytes = [
        2u8, 1, 17, 24u8, // IS_HIGH_RANK | RAW_ITEMS
        12, 0, 1u8, // num_levels=1
        3u8, // num_raw_items=3 (but only 1 f32 supplied)
        0u8, 0, 0x80, 0x3f, // 1.0_f32 (only 1 of the 3 promised items)
    ];
    let result = ReqSketch::<f32>::deserialize(&bytes);
    assert!(result.is_err(), "deserialize accepted truncated raw_items");
}

// ---------- Deserialize hardening: malformed compactor fields ----------
//
// A non-empty, non-raw, single-level sketch carries a full 20-byte compactor
// preamble whose `section_size_raw`, `lg_weight`, and `num_items` fields are read
// straight off the wire. Without bounds checks these crafted values either panic
// (arithmetic overflow) or trigger an unbounded allocation in `Compactor::deserialize`.

/// Builds a non-empty, non-raw, single-level (`num_levels = 1`) REQ sketch image
/// with a fully specified compactor preamble, so an individual field can be made
/// malformed in isolation. With valid inputs the result deserializes successfully
/// (see `single_level_image_is_valid_baseline`).
fn single_level_image(
    section_size_raw: f32,
    lg_weight: u8,
    num_sections: u8,
    num_items: u32,
    items: &[f32],
) -> Vec<u8> {
    // Preamble (8 bytes): preamble_ints = 2 (EXACT, since num_levels == 1),
    // serial_version = 1, family = 17 (REQ), flags = 8 (IS_HIGH_RANK: not empty,
    // not raw), k = 12 (u16 LE), num_levels = 1, num_raw_items = 0.
    let mut b = vec![2u8, 1, 17, 8, 12, 0, 1, 0];
    // Compactor preamble (20 bytes).
    b.extend_from_slice(&0u64.to_le_bytes()); // state
    b.extend_from_slice(&section_size_raw.to_le_bytes());
    b.push(lg_weight);
    b.push(num_sections);
    b.extend_from_slice(&0u16.to_le_bytes()); // padding
    b.extend_from_slice(&num_items.to_le_bytes());
    for &item in items {
        b.extend_from_slice(&item.to_le_bytes());
    }
    b
}

#[test]
fn single_level_image_is_valid_baseline() {
    // Control: the builder with well-formed fields round-trips, so the malformed
    // variants below isolate exactly one bad field.
    let bytes = single_level_image(12.0, 0, 3, 1, &[1.0]);
    assert!(
        ReqSketch::<f32>::deserialize(&bytes).is_ok(),
        "baseline single-level image should deserialize"
    );
}

#[test]
fn deserialize_rejects_out_of_range_section_size() {
    // A garbage section_size_raw drives the `nominal_capacity` arithmetic to overflow.
    let bytes = single_level_image(1e30, 0, 3, 1, &[1.0]);
    assert!(
        ReqSketch::<f32>::deserialize(&bytes).is_err(),
        "deserialize accepted out-of-range section_size_raw"
    );
}

#[test]
fn deserialize_rejects_oversized_lg_weight() {
    // lg_weight >= 64 makes the per-item weight `1u64 << lg_weight` overflow.
    let bytes = single_level_image(12.0, 64, 3, 1, &[1.0]);
    assert!(
        ReqSketch::<f32>::deserialize(&bytes).is_err(),
        "deserialize accepted lg_weight = 64"
    );
}

#[test]
fn deserialize_rejects_oversized_compactor_num_items() {
    // num_items claims billions of items while only one is supplied: deserialize
    // must fail gracefully without attempting a multi-gigabyte allocation.
    let bytes = single_level_image(12.0, 0, 3, u32::MAX, &[1.0]);
    assert!(
        ReqSketch::<f32>::deserialize(&bytes).is_err(),
        "deserialize accepted oversized num_items"
    );
}

// ---------- Cross-language compatibility ----------
//
// Requires fixtures generated by `tools/generate_serialization_test_data.py`.
// If `tests/serde_tests/{cpp,java}_generated_files/` is missing, the
// `serialization_test_data` helper panics with regeneration instructions.

fn validate_cross_language_fixture(path: PathBuf, expected_n: u64) {
    let bytes =
        fs::read(&path).unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    let sketch = ReqSketch::<f32>::deserialize(&bytes)
        .unwrap_or_else(|e| panic!("deserialize failed for {}: {e}", path.display()));

    assert_eq!(sketch.n(), expected_n, "n mismatch on {}", path.display());
    assert_eq!(sketch.k(), 12, "k mismatch on {}", path.display());
    assert_eq!(sketch.rank_accuracy(), RankAccuracy::HighRank);

    if expected_n > 0 {
        assert_eq!(sketch.min_item().copied(), Some(1.0_f32));
        assert_eq!(sketch.max_item().copied(), Some(expected_n as f32));
        let _ = sketch.quantile(0.5, SearchCriteria::Inclusive).unwrap();
    }

    let serialized = sketch.serialize();
    assert_eq!(
        bytes,
        serialized,
        "byte mismatch on {} — wire format diverges from C++/Java",
        path.display()
    );
}

#[test]
fn cpp_compatibility() {
    for n in [0u64, 1, 10, 100, 1000, 10000, 100000, 1000000] {
        let path =
            serialization_test_data("cpp_generated_files", &format!("req_float_n{n}_cpp.sk"));
        validate_cross_language_fixture(path, n);
    }
}

#[test]
fn java_compatibility() {
    for n in [0u64, 1, 10, 100, 1000, 10000, 100000, 1000000] {
        let path =
            serialization_test_data("java_generated_files", &format!("req_float_n{n}_java.sk"));
        validate_cross_language_fixture(path, n);
    }
}
