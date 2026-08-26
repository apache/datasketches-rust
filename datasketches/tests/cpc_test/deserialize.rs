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

//! Regression tests for deserializing malformed CPC sketches.

use datasketches::cpc::CpcSketch;

/// Builds a valid serialized sketch that exercises a particular CPC flavor.
fn valid_bytes(lg_k: u8, n: u64) -> Vec<u8> {
    let mut sketch = CpcSketch::new(lg_k);
    for i in 0..n {
        sketch.update(i);
    }
    sketch.serialize()
}

/// Each non-empty entry lands in a different CPC flavor.
const CASES: &[(u8, u64)] = &[
    (4, 0),
    (4, 3),
    (6, 20),
    (8, 200),
    (8, 2000),
    (10, 800),
    (10, 8000),
];

#[test]
fn truncated_compressed_streams_return_errors() {
    let bytes = valid_bytes(10, 8_000);
    for len in 0..bytes.len() {
        assert!(
            CpcSketch::deserialize(&bytes[..len]).is_err(),
            "accepted truncation at {len} bytes"
        );
    }
}

#[test]
fn targeted_corruptions_return_err() {
    // A sliding-flavor sketch drives the pair/window decoders and the offset/permutation logic.
    let base = valid_bytes(10, 8000);

    // Layout: [preamble_ints, serial_version, family, lg_k, first_interesting_column, flags,
    //          seed_hash(2), num_coupons(4), ...]. Corrupting the num_coupons field makes the
    //          decoders read past the compressed buffer / compute an out-of-range window offset.
    let mut num_coupons_hi = base.clone();
    num_coupons_hi[11] = 0xff; // enormous num_coupons
    assert!(CpcSketch::deserialize(&num_coupons_hi).is_err());

    // Flipping the flags byte makes the declared flavor inconsistent with the stored data.
    let mut bad_flags = base.clone();
    bad_flags[5] ^= 0xff;
    assert!(CpcSketch::deserialize(&bad_flags).is_err());

    // These payload edits previously reached panicking decoder and pair-table paths.
    let mut bad_payload = base.clone();
    let last = bad_payload.len() - 1;
    bad_payload[last] = bad_payload[last].wrapping_add(1);
    bad_payload[last - 3] ^= 0xa5;
    let _ = CpcSketch::deserialize(&bad_payload);

    // A sparse sketch whose declared entry count exceeds its data words must be rejected up front.
    let sparse = valid_bytes(8, 200);
    let mut inflated = sparse.clone();
    // num_coupons is a u32 at offset 8; inflate it far beyond the coupon space.
    inflated[10] = 0xff;
    inflated[11] = 0xff;
    assert!(CpcSketch::deserialize(&inflated).is_err());
}

#[test]
fn valid_sketches_round_trip_unchanged() {
    for &(lg_k, n) in CASES {
        let mut sketch = CpcSketch::new(lg_k);
        for i in 0..n {
            sketch.update(i);
        }
        let bytes = sketch.serialize();

        let restored = CpcSketch::deserialize(&bytes).unwrap_or_else(|e| {
            panic!("valid sketch (lg_k={lg_k}, n={n}) failed to deserialize: {e}")
        });

        assert_eq!(
            sketch.estimate(),
            restored.estimate(),
            "estimate changed after round-trip (lg_k={lg_k}, n={n})"
        );
        assert_eq!(
            sketch.num_coupons(),
            restored.num_coupons(),
            "num_coupons changed after round-trip (lg_k={lg_k}, n={n})"
        );
        assert_eq!(
            bytes,
            restored.serialize(),
            "re-serialized bytes changed after round-trip (lg_k={lg_k}, n={n})"
        );
    }
}
