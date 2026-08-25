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

//! Serialization round-trip and cross-language compatibility tests for ReqSketch.

use std::fs;
use std::path::PathBuf;

use datasketches::req::RankAccuracy;
use datasketches::req::ReqSketch;
use datasketches::req::ReqValue;
use datasketches::req::SearchCriteria;
use googletest::assert_that;
use googletest::prelude::anything;
use googletest::prelude::err;
use googletest::prelude::ok;

use crate::serialization_test_data;

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
    for &k in &[4u16, 6, 10, 12, 1024] {
        for &ra in &[RankAccuracy::HighRank, RankAccuracy::LowRank] {
            for &n in &[0u64, 1, 4, 5, 100, 1_250, 2_562, 10_000, 100_000] {
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
        assert_that!(result, err(anything()), "preamble length: {n}");
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
    assert_that!(result, err(anything()));
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
    assert_that!(result, err(anything()));
}

#[test]
fn deserialize_invalid_preamble_ints() {
    // preamble_ints must be 2 (exact) or 4 (estimation). Try 3.
    let bytes = [3u8, 1, 17, 4, 12, 0, 0, 0];
    let result = ReqSketch::<f32>::deserialize(&bytes);
    assert_that!(result, err(anything()));
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
    assert_that!(result, err(anything()));
}

#[test]
fn deserialize_rejects_inconsistent_raw_items_header() {
    // RAW_ITEMS is only valid for one non-empty level with 1..=4 raw items.
    let raw_with_no_items = [
        2u8, 1, 17, 24u8, // IS_HIGH_RANK | RAW_ITEMS
        12, 0, 1u8, // num_levels
        0u8, // invalid raw item count
    ];
    assert_that!(
        ReqSketch::<f32>::deserialize(&raw_with_no_items),
        err(anything())
    );

    let raw_with_two_levels = [
        4u8, 1, 17, 24u8, // IS_HIGH_RANK | RAW_ITEMS
        12, 0, 2u8, // invalid for raw-items sketches
        1u8,
    ];
    assert_that!(
        ReqSketch::<f32>::deserialize(&raw_with_two_levels),
        err(anything())
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
    assert_that!(result, err(anything()));
}

#[test]
fn deserialize_k_out_of_range() {
    // k must be in [4, 1024]. Try k=2 (too small).
    let bytes_small = [2u8, 1, 17, 4, 2, 0, 0, 0];
    assert_that!(ReqSketch::<f32>::deserialize(&bytes_small), err(anything()));

    // k=2048 (too large): little-endian 2048 = [0x00, 0x08]
    let bytes_big = [2u8, 1, 17, 4, 0, 8, 0, 0];
    assert_that!(ReqSketch::<f32>::deserialize(&bytes_big), err(anything()));
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
    assert_that!(result, err(anything()));
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
    assert_that!(result, err(anything()));
}

#[test]
fn merge_preserves_order_across_serde_round_trip() {
    let mut high = ReqSketch::<f64>::new();
    let mut low = ReqSketch::<f64>::new();

    for value in 1000..=1072 {
        high.update(value as f64);
    }
    for value in 0..=72 {
        low.update(value as f64);
    }

    high.merge(&low).unwrap();
    let restored = ReqSketch::<f64>::deserialize(&high.serialize()).unwrap();
    let view = restored.sorted_view();

    for value in 0..=1072 {
        let value = value as f64;
        assert_eq!(
            restored.rank(&value, SearchCriteria::Inclusive).unwrap(),
            view.rank(&value, SearchCriteria::Inclusive).unwrap(),
        );
    }
}

// ---------- Deserialize hardening: malformed compactor fields ----------
//
const EXACT_COMPACTOR_OFFSET: usize = 8;
const ESTIMATION_COMPACTOR_OFFSET: usize = 24;
const STATE_OFFSET: usize = 0;
const SECTION_SIZE_RAW_OFFSET: usize = 8;
const LG_WEIGHT_OFFSET: usize = 12;
const NUM_SECTIONS_OFFSET: usize = 13;
const NUM_ITEMS_OFFSET: usize = 16;
const FLAG_LEVEL_ZERO_SORTED: u8 = 1 << 5;

fn exact_image(k: u16, items: &[f32]) -> Vec<u8> {
    let mut bytes = vec![2u8, 1, 17, 8];
    bytes.extend_from_slice(&k.to_le_bytes());
    bytes.extend_from_slice(&[1, 0]);
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&(k as f32).to_le_bytes());
    bytes.extend_from_slice(&[0, 3, 0, 0]);
    bytes.extend_from_slice(&(items.len() as u32).to_le_bytes());
    for item in items {
        bytes.extend_from_slice(&item.to_le_bytes());
    }
    bytes
}

fn estimation_image(k: u16, n: u64) -> Vec<u8> {
    let mut sketch = ReqSketch::<f32>::try_new(k, RankAccuracy::HighRank).unwrap();
    for item in 1..=n {
        sketch.update(item as f32);
    }
    let bytes = sketch.serialize();
    assert!(bytes[6] > 1);
    bytes
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn assert_invalid_data(bytes: &[u8]) {
    let error = ReqSketch::<f32>::deserialize(bytes).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
}

#[test]
fn canonical_exact_image_is_valid() {
    let bytes = exact_image(12, &[1.0, 2.0, 3.0, 4.0, 5.0]);
    assert_that!(ReqSketch::<f32>::deserialize(&bytes), ok(anything()));
}

#[test]
fn deserialize_rejects_issue_218_states() {
    let mut zero_sections = exact_image(12, &[1.0, 2.0, 3.0, 4.0, 5.0]);
    zero_sections[EXACT_COMPACTOR_OFFSET + NUM_SECTIONS_OFFSET] = 0;
    assert_invalid_data(&zero_sections);

    let mut wrong_weight = exact_image(12, &[1.0, 2.0, 3.0, 4.0, 5.0]);
    wrong_weight[EXACT_COMPACTOR_OFFSET + LG_WEIGHT_OFFSET] = 63;
    assert_invalid_data(&wrong_weight);
}

#[test]
fn deserialize_rejects_inconsistent_weighted_count() {
    let mut bytes = estimation_image(12, 1_000);
    bytes[8..16].copy_from_slice(&1_001u64.to_le_bytes());
    assert_invalid_data(&bytes);
}

#[test]
fn deserialize_rejects_unreachable_section_configuration() {
    let mut invalid_raw = exact_image(12, &[1.0, 2.0, 3.0, 4.0, 5.0]);
    invalid_raw[EXACT_COMPACTOR_OFFSET + SECTION_SIZE_RAW_OFFSET
        ..EXACT_COMPACTOR_OFFSET + SECTION_SIZE_RAW_OFFSET + 4]
        .copy_from_slice(&0.0f32.to_le_bytes());
    assert_invalid_data(&invalid_raw);

    let mut invalid_sections = exact_image(12, &[1.0, 2.0, 3.0, 4.0, 5.0]);
    invalid_sections[EXACT_COMPACTOR_OFFSET + NUM_SECTIONS_OFFSET] = 6;
    assert_invalid_data(&invalid_sections);
}

#[test]
fn deserialize_accepts_java_minimum_section_schedule() {
    let mut bytes = estimation_image(6, 1_250);
    let compactor = ESTIMATION_COMPACTOR_OFFSET;
    assert_eq!(read_u64(&bytes, compactor + STATE_OFFSET), 32);

    let java_raw = (6.0 / std::f64::consts::SQRT_2) as f32;
    bytes[compactor + SECTION_SIZE_RAW_OFFSET..compactor + SECTION_SIZE_RAW_OFFSET + 4]
        .copy_from_slice(&java_raw.to_le_bytes());
    bytes[compactor + NUM_SECTIONS_OFFSET] = 6;

    let mut sketch = ReqSketch::<f32>::deserialize(&bytes).unwrap();
    for item in 1_251..=2_500 {
        sketch.update(item as f32);
    }
    let continued = sketch.serialize();
    assert_that!(ReqSketch::<f32>::deserialize(&continued), ok(anything()));
}

#[test]
fn deserialize_rejects_capacity_changing_float_drift() {
    let mut bytes = estimation_image(10, 2_562);
    let raw_offset = ESTIMATION_COMPACTOR_OFFSET + SECTION_SIZE_RAW_OFFSET;
    let raw_bits = u32::from_le_bytes(bytes[raw_offset..raw_offset + 4].try_into().unwrap());
    assert_eq!(read_u64(&bytes, ESTIMATION_COMPACTOR_OFFSET), 32);
    assert_eq!(f32::from_bits(raw_bits), 5.0);
    assert_eq!(bytes[ESTIMATION_COMPACTOR_OFFSET + NUM_SECTIONS_OFFSET], 12);

    // One ULP below 5.0 rounds to a section size of 4 rather than 6.
    bytes[raw_offset..raw_offset + 4].copy_from_slice(&(raw_bits - 1).to_le_bytes());
    assert_invalid_data(&bytes);
}

#[test]
fn deserialize_rejects_state_that_exceeds_n() {
    let mut bytes = estimation_image(12, 1_000);
    let compactor = ESTIMATION_COMPACTOR_OFFSET;
    bytes[compactor + STATE_OFFSET..compactor + STATE_OFFSET + 8]
        .copy_from_slice(&u64::MAX.to_le_bytes());
    let mut raw = 12.0f32;
    for _ in 0..4 {
        raw /= std::f32::consts::SQRT_2;
    }
    bytes[compactor + SECTION_SIZE_RAW_OFFSET..compactor + SECTION_SIZE_RAW_OFFSET + 4]
        .copy_from_slice(&raw.to_le_bytes());
    bytes[compactor + NUM_SECTIONS_OFFSET] = 48;
    assert_invalid_data(&bytes);
}

#[test]
fn deserialize_rejects_false_sorted_claim_and_nan() {
    let mut unsorted = exact_image(12, &[3.0, 4.0, 5.0, 1.0, 2.0]);
    unsorted[3] |= FLAG_LEVEL_ZERO_SORTED;
    assert_invalid_data(&unsorted);

    let nan = exact_image(12, &[1.0, 2.0, f32::NAN, 4.0, 5.0]);
    assert_invalid_data(&nan);
}

#[test]
fn deserialize_rejects_invalid_extrema_and_raw_nan() {
    let mut nan_min = estimation_image(12, 1_000);
    nan_min[16..20].copy_from_slice(&f32::NAN.to_le_bytes());
    assert_invalid_data(&nan_min);

    let mut reversed = estimation_image(12, 1_000);
    reversed[16..20].copy_from_slice(&2.0f32.to_le_bytes());
    reversed[20..24].copy_from_slice(&1.0f32.to_le_bytes());
    assert_invalid_data(&reversed);

    let mut raw_nan = vec![2u8, 1, 17, 8 | 16, 12, 0, 1, 1];
    raw_nan.extend_from_slice(&f32::NAN.to_le_bytes());
    assert_invalid_data(&raw_nan);
}

#[test]
fn deserialize_rejects_noncanonical_exact_mode_and_capacity() {
    assert_invalid_data(&exact_image(12, &[1.0]));

    let items: Vec<f32> = (0..72).map(|item| item as f32).collect();
    assert_invalid_data(&exact_image(12, &items));
}

#[test]
fn deserialize_rejects_oversized_compactor_num_items() {
    let mut bytes = exact_image(12, &[1.0, 2.0, 3.0, 4.0, 5.0]);
    let offset = EXACT_COMPACTOR_OFFSET + NUM_ITEMS_OFFSET;
    bytes[offset..offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_invalid_data(&bytes);
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
