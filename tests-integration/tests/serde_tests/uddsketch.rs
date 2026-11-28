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

use std::fs;

use datasketches::error::ErrorKind;
use datasketches::uddsketch::UddSketch;

use crate::serialization_test_data;

fn assert_invalid_data(bytes: &[u8]) {
    let err = UddSketch::deserialize(bytes).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);
}

#[test]
fn rust_generated_snapshots_round_trip_and_match_fresh_builds() {
    for n in [0_u64, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000] {
        let filename = format!("uddsketch_n{n}_rust.sk");
        let path = serialization_test_data("rust_generated_files", &filename);
        let bytes = fs::read(&path).unwrap();

        let decoded = UddSketch::deserialize(&bytes).unwrap();
        assert_eq!(decoded.max_buckets(), 256);
        assert_eq!(decoded.initial_alpha(), 0.01);
        assert_eq!(decoded.total_weight(), n);
        assert_eq!(decoded.serialize(), bytes, "{filename}");

        let mut expected = UddSketch::new(256, 0.01).unwrap();
        for i in 0..n {
            let value = match i % 5 {
                0 => (i + 1) as f64,
                1 => -((i + 1) as f64),
                2 => 0.0,
                3 => 2f64.powi(((i % 64) as i32) - 32),
                _ => -2f64.powi(((i % 64) as i32) - 32),
            };
            expected.update(value);
        }
        assert_eq!(expected.serialize(), bytes, "{filename}");
    }
}

#[test]
fn truncated_and_trailing_images_are_rejected() {
    let empty_bytes = UddSketch::new(128, 0.02).unwrap().serialize();
    for end in 0..empty_bytes.len() {
        assert_invalid_data(&empty_bytes[..end]);
    }
    let mut empty_trailing = empty_bytes.clone();
    empty_trailing.push(0);
    assert_invalid_data(&empty_trailing);

    let mut populated = UddSketch::new(64, 0.01).unwrap();
    for v in [-100.0, -1.0, 0.0, 2.0, 500.0] {
        populated.update(v);
    }
    let bytes = populated.serialize();
    for end in 0..bytes.len() {
        assert_invalid_data(&bytes[..end]);
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert_invalid_data(&trailing);
}

#[test]
fn corrupted_headers_and_bucket_invariants_are_rejected() {
    let mut sketch = UddSketch::new(16, 0.01).unwrap();
    sketch.update(-20.0);
    sketch.update(-2.0);
    sketch.update(0.0);
    sketch.update(3.0);
    sketch.update(30.0);
    let valid = sketch.serialize();
    assert!(UddSketch::deserialize(&valid).is_ok());

    // Header field corruptions.
    for (offset, bad_byte) in [
        (0usize, 2u8), // wrong preamble_longs for non-empty
        (1, 2),        // unsupported serial_version
        (2, 20),       // wrong family_id (TDIGEST)
        (3, 0x80),     // reserved flag bit 7
        (3, 0x7e),     // FLAGS_IS_EMPTY combined with non-empty flags
        (3, 0x00),     // non-empty with no occupied buckets
        (3, 0x22),     // FLAG_NEG_UNIT_WEIGHT without FLAG_HAS_NEG
        (3, 0x42),     // FLAG_POS_UNIT_WEIGHT without FLAG_HAS_POS
    ] {
        let mut bad = valid.clone();
        bad[offset] = bad_byte;
        assert_invalid_data(&bad);
    }

    let empty_valid = UddSketch::new(16, 0.01).unwrap().serialize();
    for (offset, bad_byte) in [(0usize, 4u8), (3, 0x05), (3, 0x84)] {
        let mut bad = empty_valid.clone();
        bad[offset] = bad_byte;
        assert_invalid_data(&bad);
    }

    // Out-of-range max_buckets (< 4 or > 2^26).
    for max_buckets in [0u32, 3, (1 << 26) + 1, u32::MAX] {
        let mut bad_max_buckets = valid.clone();
        bad_max_buckets[4..8].copy_from_slice(&max_buckets.to_le_bytes());
        assert_invalid_data(&bad_max_buckets);
    }

    // Out-of-range initial_alpha.
    for alpha in [0.0f64, 1.0, f64::NAN] {
        let mut bad_alpha = valid.clone();
        bad_alpha[8..16].copy_from_slice(&alpha.to_le_bytes());
        assert_invalid_data(&bad_alpha);
    }

    // Retained buckets exceeding max_buckets: set max_buckets = 4 with 5 retained buckets.
    let mut over_budget = UddSketch::new(8, 0.01).unwrap();
    for v in [1.0, 2.0, 4.0, 8.0, 16.0] {
        over_budget.update(v);
    }
    let mut over_budget_bytes = over_budget.serialize();
    over_budget_bytes[4..8].copy_from_slice(&4u32.to_le_bytes());
    assert_invalid_data(&over_budget_bytes);

    // Negative zero in min_value (16..24) or max_value (24..32).
    let mut zero_only = UddSketch::new(8, 0.01).unwrap();
    zero_only.update(0.0);
    let zero_bytes = zero_only.serialize();
    for range in [16..24, 24..32] {
        let mut bad = zero_bytes.clone();
        bad[range].copy_from_slice(&(-0.0f64).to_le_bytes());
        assert_invalid_data(&bad);
    }

    // Zero zero_weight when FLAG_HAS_ZERO is set.
    let mut zero_zero_weight = zero_bytes.clone();
    zero_zero_weight[32] = 0;
    assert_invalid_data(&zero_zero_weight);

    // Non-zero num_collapses validation (`FLAG_HAS_COLLAPSES = 1 << 0`, byte inserted at 32).
    let with_collapses = |bytes: &[u8], level: u8| -> Vec<u8> {
        let mut out = Vec::with_capacity(bytes.len() + 1);
        out.extend_from_slice(&bytes[..32]);
        out[3] |= 1 << 0;
        out.push(level);
        out.extend_from_slice(&bytes[32..]);
        out
    };
    assert_invalid_data(&with_collapses(&valid, 0)); // 0 invalid when FLAG_HAS_COLLAPSES set
    assert_invalid_data(&with_collapses(&valid, 63)); // > MAX_COLLAPSES (62)

    let mut unit_three = UddSketch::new(4, 0.01).unwrap();
    for v in [-1.0, 1.0, 1.01] {
        unit_three.update(v);
    }
    let unit_three_bytes = unit_three.serialize();

    let mut weighted_three = UddSketch::new(4, 0.01).unwrap();
    for v in [-1.0, 1.0, 1.01] {
        weighted_three.update_with_weight(v, 2);
    }
    let weighted_three_bytes = weighted_three.serialize();

    for forged_level in [1u8, 32, 62] {
        assert_invalid_data(&with_collapses(&zero_bytes, forged_level));
        assert_invalid_data(&with_collapses(&valid, forged_level));
        // 3 unit-weight buckets with max_buckets = 4 pass `total_buckets >= max_buckets / 2 + 1`
        // (`3 >= 3`), yet could only come from at most 3 pre-collapse buckets (`<= 4`).
        assert_invalid_data(&with_collapses(&unit_three_bytes, forged_level));
    }
    // With weight 2 per bucket, `num_collapses = 32` is still unreachable because `key_bounds(31)`
    // collapses to `(0, 1)`, leaving at most 3 reachable pre-collapse child buckets (`<= 4`).
    assert_invalid_data(&with_collapses(&weighted_three_bytes, 32));

    // Extrema-bounded pre-collapse check: 4 buckets with `max_buckets = 4` where outer buckets
    // have weight 2, yet recorded `(min_value, max_value)` keep `hi_child` out of reach at
    // `prev_level = 9` so at most 4 pre-collapse buckets could have existed (`<= 4`).
    let mut extrema_bounded = UddSketch::new(4, 0.01).unwrap();
    extrema_bounded.update_with_weight(-1.01, 2);
    extrema_bounded.update(-1.0);
    extrema_bounded.update(1.0);
    extrema_bounded.update_with_weight(1.01, 2);
    assert_invalid_data(&with_collapses(&extrema_bounded.serialize(), 10));

    // Same extrema-bounded check for single-sign (positive-only / negative-only) with inner
    // bucket weight 2 where `lo_child` is below the recorded inner extremum.
    for pairs in [
        [(1.0, 2u64), (1.01, 1), (1.03, 1)],
        [(-1.03, 1u64), (-1.01, 1), (-1.0, 2)],
    ] {
        let mut single_sign = UddSketch::new(4, 0.01).unwrap();
        for (v, w) in pairs {
            single_sign.update_with_weight(v, w);
        }
        assert_invalid_data(&with_collapses(&single_sign.serialize(), 10));
    }

    // Reversed or bucket-mismatched extrema.
    let mut bad_min = valid.clone();
    bad_min[16..24].copy_from_slice(&(-1.0f64).to_le_bytes()); // wrong bucket for min_value
    assert_invalid_data(&bad_min);

    let mut bad_max = valid.clone();
    bad_max[24..32].copy_from_slice(&1000.0f64.to_le_bytes()); // wrong bucket for max_value
    assert_invalid_data(&bad_max);

    // Zero run length when FLAG_HAS_NEG is set, and DoS run length > remaining bytes.
    let mut neg_only = UddSketch::new(16_384, 0.01).unwrap();
    neg_only.update(-2.0);
    let neg_only_bytes = neg_only.serialize();
    let mut zero_neg_len = neg_only_bytes.clone();
    zero_neg_len[32] = 0;
    assert_invalid_data(&zero_neg_len);

    let mut dos_neg_len = neg_only_bytes[..32].to_vec();
    // Claim 10_000 buckets (`0x90, 0x4e`) with only 2 trailing bytes (`<= max_buckets = 16_384`).
    dos_neg_len.extend_from_slice(&[0x90, 0x4e, 0x00, 0x00]);
    assert_invalid_data(&dos_neg_len);

    // Out-of-range first bucket key (`i64::MAX` ZigZag -> 9-byte varint) and delta overflow.
    let mut pos_two = UddSketch::new(16, 0.01).unwrap();
    pos_two.update(2.0);
    pos_two.update(3.0);
    let pos_two_bytes = pos_two.serialize();
    let mut out_of_range_first = pos_two_bytes[..33].to_vec(); // 32B header + num_pos=2
    out_of_range_first.extend_from_slice(&[0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
    out_of_range_first.push(0x00);
    assert_invalid_data(&out_of_range_first);

    let mut out_of_range_delta = pos_two_bytes[..32].to_vec();
    out_of_range_delta.push(0x02); // num_pos = 2
    out_of_range_delta.push(0x00); // first_key = 0
    out_of_range_delta.extend_from_slice(&[0xff; 9]); // delta_m1 = u64::MAX > max_key - 0
    assert_invalid_data(&out_of_range_delta);

    // Bucket count overflow (`count - 1 = u64::MAX`) and total weight overflow across runs.
    let mut weighted_one = UddSketch::new(16, 0.01).unwrap();
    weighted_one.update_with_weight(2.0, 2);
    let weighted_one_bytes = weighted_one.serialize();
    let mut count_overflow = weighted_one_bytes[..33].to_vec(); // 32B header + num_pos=1
    count_overflow.push(0x00); // first_key = 0
    count_overflow.extend_from_slice(&[0xff; 9]); // count - 1 = u64::MAX
    assert_invalid_data(&count_overflow);

    // Total weight overflow (`zero_weight = u64::MAX` + positive bucket weight).
    let mut weight_overflow = valid[..32].to_vec();
    weight_overflow.extend_from_slice(&[0xff; 9]); // zero_weight = u64::MAX
    weight_overflow.extend_from_slice(&valid[33..]);
    assert_invalid_data(&weight_overflow);
}

#[test]
fn v1_wire_format_golden_fixture_is_pinned() {
    // Frozen `SERIAL_VERSION = 1` (`Family::UDDSKETCH` = 24) image not touched by `datagen`,
    // guarding compact header fields, unit-weight flags, and Q52 delta-varint bucket coordinates.
    #[rustfmt::skip]
    const GOLDEN_V1: [u8; 49] = [
        0x04, 0x01, 0x18, 0x7a, 0x00, 0x01, 0x00, 0x00,
        0x7b, 0x14, 0xae, 0x47, 0xe1, 0x7a, 0x84, 0x3f,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1c, 0xc0,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40,
        0x02, 0x04, 0x93, 0x0f, 0xac, 0x01, 0xbf, 0x06,
        0x3e, 0x04, 0xd9, 0x0f, 0xad, 0x01, 0xbe, 0x06,
        0x59,
    ];

    let decoded = UddSketch::deserialize(&GOLDEN_V1).unwrap();
    assert_eq!(decoded.max_buckets(), 256);
    assert_eq!(decoded.initial_alpha(), 0.01);
    assert_eq!(decoded.num_collapses(), 0);
    assert_eq!(decoded.total_weight(), 10);
    assert_eq!(decoded.zero_weight(), 2);
    assert_eq!(decoded.num_retained(), 8);
    assert_eq!(decoded.min_value(), Some(-7.0));
    assert_eq!(decoded.max_value(), Some(6.0));
    assert_eq!(decoded.serialize(), GOLDEN_V1);

    let mut fresh = UddSketch::new(256, 0.01).unwrap();
    for i in 0..10_u64 {
        let value = match i % 5 {
            0 => (i + 1) as f64,
            1 => -((i + 1) as f64),
            2 => 0.0,
            3 => 2f64.powi(((i % 64) as i32) - 32),
            _ => -2f64.powi(((i % 64) as i32) - 32),
        };
        fresh.update(value);
    }
    assert_eq!(fresh.serialize(), GOLDEN_V1);
}
