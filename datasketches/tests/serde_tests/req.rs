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
// A non-empty, non-raw, single-level sketch carries a full 20-byte compactor
// preamble whose `section_size_raw`, `lg_weight`, and `num_items` fields are read
// straight off the wire. Serializers use RAW_ITEMS for one-level sketches with
// n ≤ 4, so the canonical non-raw baseline uses five items and mutates exactly
// one invariant in each malformed case.

const FIVE_ITEMS: [f32; 5] = [1.0, 2.0, 3.0, 4.0, 5.0];
const FLAG_HRA: u8 = 8;
const FLAG_RAW_ITEMS: u8 = 16;
const FLAG_LEVEL_ZERO_SORTED: u8 = 32;

fn assert_invalid_data(bytes: &[u8]) {
    let err = match ReqSketch::<f32>::deserialize(bytes) {
        Ok(_) => panic!("expected InvalidData, deserialize succeeded"),
        Err(err) => err,
    };
    assert_eq!(
        err.kind(),
        ErrorKind::InvalidData,
        "wrong error kind: {:?}",
        err.kind()
    );
}

fn assert_invalid_data_containing(bytes: &[u8], needle: &str) {
    let err = match ReqSketch::<f32>::deserialize(bytes) {
        Ok(_) => panic!("expected InvalidData, deserialize succeeded"),
        Err(err) => err,
    };
    assert_eq!(
        err.kind(),
        ErrorKind::InvalidData,
        "wrong error kind: {:?}",
        err.kind()
    );
    assert!(
        err.message().contains(needle),
        "expected message to contain {needle:?}, got {:?}",
        err.message()
    );
}

/// Builds a non-empty, non-raw, single-level (`num_levels = 1`) REQ sketch image
/// with a fully specified compactor preamble, so an individual field can be made
/// malformed in isolation. With valid inputs the result deserializes successfully
/// (see `single_level_image_is_valid_baseline`).
fn single_level_image(
    k: u16,
    flags: u8,
    state: u64,
    section_size_raw: f32,
    lg_weight: u8,
    num_sections: u8,
    num_items: u32,
    items: &[f32],
) -> Vec<u8> {
    // Preamble (8 bytes): preamble_ints = 2 (EXACT, since num_levels == 1),
    // serial_version = 1, family = 17 (REQ).
    let mut b = vec![2u8, 1, 17, flags];
    b.extend_from_slice(&k.to_le_bytes());
    b.push(1); // num_levels
    b.push(0); // num_raw_items
    b.extend_from_slice(&state.to_le_bytes());
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

fn canonical_five_item_image() -> Vec<u8> {
    single_level_image(12, FLAG_HRA, 0, 12.0, 0, 3, 5, &FIVE_ITEMS)
}

fn write_compactor(buf: &mut Vec<u8>, lg_weight: u8, items: &[f32]) {
    buf.extend_from_slice(&0u64.to_le_bytes());
    buf.extend_from_slice(&12.0f32.to_le_bytes());
    buf.push(lg_weight);
    buf.push(3);
    buf.extend_from_slice(&0u16.to_le_bytes());
    buf.extend_from_slice(&(items.len() as u32).to_le_bytes());
    for &item in items {
        buf.extend_from_slice(&item.to_le_bytes());
    }
}

#[test]
fn single_level_image_is_valid_baseline() {
    // Control: five non-raw items is the canonical one-level image. Serializers
    // use RAW_ITEMS for n ≤ 4, so a one-item non-raw image is not a valid baseline.
    assert_that!(
        ReqSketch::<f32>::deserialize(&canonical_five_item_image()),
        ok(anything())
    );
    let k4 = single_level_image(4, FLAG_HRA, 0, 4.0, 0, 3, 5, &FIVE_ITEMS);
    assert_that!(ReqSketch::<f32>::deserialize(&k4), ok(anything()));
    let doubled = single_level_image(
        12,
        FLAG_HRA,
        4,
        12.0 / std::f32::consts::SQRT_2,
        0,
        6,
        5,
        &FIVE_ITEMS,
    );
    assert_that!(ReqSketch::<f32>::deserialize(&doubled), ok(anything()));
    let sorted_claim = single_level_image(
        12,
        FLAG_HRA | FLAG_LEVEL_ZERO_SORTED,
        0,
        12.0,
        0,
        3,
        5,
        &FIVE_ITEMS,
    );
    assert_that!(ReqSketch::<f32>::deserialize(&sorted_claim), ok(anything()));
    let unsorted_no_claim =
        single_level_image(12, FLAG_HRA, 0, 12.0, 0, 3, 5, &[3.0, 4.0, 5.0, 1.0, 2.0]);
    assert_that!(
        ReqSketch::<f32>::deserialize(&unsorted_no_claim),
        ok(anything())
    );
}

#[test]
fn deserialize_rejects_out_of_range_section_size() {
    // A garbage section_size_raw is not reachable from k under the doubling schedule.
    let bytes = single_level_image(12, FLAG_HRA, 0, 1e30, 0, 3, 5, &FIVE_ITEMS);
    assert_invalid_data_containing(&bytes, "not reachable");
}

#[test]
fn deserialize_rejects_undersized_section_size() {
    // `section_size_raw = 0.0` is in `0..=MAX_K` but is not produced by the schedule.
    let bytes = single_level_image(12, FLAG_HRA, 0, 0.0, 0, 3, 5, &FIVE_ITEMS);
    assert_invalid_data_containing(&bytes, "not reachable");
}

#[test]
fn deserialize_rejects_k_section_size_mismatch() {
    // k=4 with section_size_raw=1024 used to be accepted because each field was
    // checked only against global MAX_K, yielding capacity 6144 instead of 24.
    let bytes = single_level_image(4, FLAG_HRA, 0, 1024.0, 0, 3, 5, &FIVE_ITEMS);
    assert_invalid_data_containing(&bytes, "not reachable");
}

#[test]
fn deserialize_rejects_oversized_lg_weight() {
    // lg_weight must equal the enclosing level index (0 here).
    let bytes = single_level_image(12, FLAG_HRA, 0, 12.0, 64, 3, 5, &FIVE_ITEMS);
    assert_invalid_data_containing(&bytes, "does not match level");
}

#[test]
fn deserialize_rejects_oversized_compactor_num_items() {
    // num_items claims billions of items while only five are supplied: deserialize
    // must fail gracefully without attempting a multi-gigabyte allocation.
    let bytes = single_level_image(12, FLAG_HRA, 0, 12.0, 0, 3, u32::MAX, &FIVE_ITEMS);
    assert_invalid_data(&bytes);
}

#[test]
fn deserialize_rejects_zero_num_sections() {
    // A single-level image with num_sections = 0 used to deserialize, then
    // ReqSketch::merge panicked at `1u64 << (self.num_sections - 1)`.
    let bytes = single_level_image(12, FLAG_HRA, 0, 12.0, 0, 0, 5, &FIVE_ITEMS);
    assert_invalid_data_containing(&bytes, "not reachable");
}

#[test]
fn deserialize_rejects_nonzero_invalid_num_sections() {
    // num_sections = 1 is on neither the initial value (3) nor the doubling schedule.
    let bytes = single_level_image(12, FLAG_HRA, 0, 12.0, 0, 1, 5, &FIVE_ITEMS);
    assert_invalid_data_containing(&bytes, "not reachable");
}

#[test]
fn deserialize_rejects_level0_lg_weight_mismatch() {
    // A level-0 compactor with lg_weight = 63 used to deserialize, then rank()
    // returned ~9.22e18 instead of a value in [0.0, 1.0].
    let bytes = single_level_image(12, FLAG_HRA, 0, 12.0, 63, 3, 5, &FIVE_ITEMS);
    assert_invalid_data_containing(&bytes, "does not match level");
}

#[test]
fn deserialize_rejects_false_sorted_claim() {
    // IS_LEVEL_ZERO_SORTED with unsorted items makes rank() disagree with sorted_view().
    let mut bytes = vec![2u8, 1, 17, FLAG_HRA | FLAG_LEVEL_ZERO_SORTED, 12, 0, 1, 0];
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&12.0f32.to_le_bytes());
    bytes.extend_from_slice(&[0, 3, 0, 0]);
    bytes.extend_from_slice(&5u32.to_le_bytes());
    for item in [3.0f32, 4.0, 5.0, 1.0, 2.0] {
        bytes.extend_from_slice(&item.to_le_bytes());
    }
    assert_invalid_data_containing(&bytes, "claimed sorted");
}

#[test]
fn deserialize_rejects_false_sorted_claim_raw_items() {
    let mut bytes = vec![
        2u8,
        1,
        17,
        FLAG_HRA | FLAG_RAW_ITEMS | FLAG_LEVEL_ZERO_SORTED,
        12,
        0,
        1,
        2,
    ];
    for item in [2.0f32, 1.0] {
        bytes.extend_from_slice(&item.to_le_bytes());
    }
    assert_invalid_data_containing(&bytes, "claimed sorted");
}

#[test]
fn deserialize_rejects_retained_at_or_above_capacity() {
    // Level-0 capacity for k=12, num_sections=3 is 2 * 12 * 3 = 72.
    // `update()` compresses when num_retained meets max_nom_size. An image that
    // already sits at or above capacity would skip compact after the next
    // equality-only update. Reject both boundaries at deserialize.
    let at_capacity: Vec<f32> = (0..72).map(|i| i as f32).collect();
    let bytes = single_level_image(12, FLAG_HRA, 0, 12.0, 0, 3, 72, &at_capacity);
    assert_invalid_data_containing(&bytes, "not below total nominal capacity");

    let over_capacity: Vec<f32> = (0..73).map(|i| i as f32).collect();
    let bytes = single_level_image(12, FLAG_HRA, 0, 12.0, 0, 3, 73, &over_capacity);
    assert_invalid_data_containing(&bytes, "not below total nominal capacity");
}

#[test]
fn deserialize_rejects_weighted_count_mismatch() {
    // Two levels: 3 items at weight 1 plus 1 item at weight 2 → weighted count 5.
    // Lying n isolates the mismatch branch.
    let mut bytes = vec![4u8, 1, 17, FLAG_HRA, 12, 0, 2, 0];
    bytes.extend_from_slice(&99u64.to_le_bytes());
    bytes.extend_from_slice(&1.0f32.to_le_bytes());
    bytes.extend_from_slice(&4.0f32.to_le_bytes());
    write_compactor(&mut bytes, 0, &[1.0, 2.0, 3.0]);
    write_compactor(&mut bytes, 1, &[4.0]);
    assert_invalid_data_containing(&bytes, "does not match n");
}

#[test]
fn deserialize_accepts_matching_two_level_weighted_count() {
    let mut bytes = vec![4u8, 1, 17, FLAG_HRA, 12, 0, 2, 0];
    bytes.extend_from_slice(&5u64.to_le_bytes());
    bytes.extend_from_slice(&1.0f32.to_le_bytes());
    bytes.extend_from_slice(&4.0f32.to_le_bytes());
    write_compactor(&mut bytes, 0, &[1.0, 2.0, 3.0]);
    write_compactor(&mut bytes, 1, &[4.0]);
    assert_that!(ReqSketch::<f32>::deserialize(&bytes), ok(anything()));
}

#[test]
fn deserialize_rejects_weighted_count_overflow() {
    // 64 compactors with lg_weight equal to the level index. Two items at
    // level 63 make `num_items * 2^lg_weight` overflow u64.
    let mut bytes = vec![4u8, 1, 17, FLAG_HRA, 12, 0, 64, 0];
    bytes.extend_from_slice(&2u64.to_le_bytes());
    bytes.extend_from_slice(&1.0f32.to_le_bytes());
    bytes.extend_from_slice(&2.0f32.to_le_bytes());
    for level in 0u8..64 {
        let items: &[f32] = if level == 63 { &[1.0, 2.0] } else { &[] };
        write_compactor(&mut bytes, level, items);
    }
    assert_invalid_data_containing(&bytes, "weighted count overflow");
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
