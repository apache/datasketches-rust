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

//! Regression tests for deserializing malformed/corrupt CPC sketches.
//!
//! `CpcSketch::deserialize` returns a `Result`, so it must reject corrupt bytes with an error
//! instead of panicking (index-out-of-bounds, failed asserts, or arithmetic overflow) while
//! decompressing the untrusted stream. See the deserialize hardening added alongside these tests.

use datasketches::cpc::CpcSketch;

/// Builds a valid serialized sketch that exercises a particular CPC flavor.
fn valid_bytes(lg_k: u8, n: u64) -> Vec<u8> {
    let mut sketch = CpcSketch::new(lg_k);
    for i in 0..n {
        sketch.update(i);
    }
    sketch.serialize()
}

/// A small deterministic xorshift generator so the fuzz body is reproducible.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// `lg_k` values kept at or below 10 so the (pre-existing, unrelated) 32-bit threshold arithmetic
/// in `determine_pseudo_phase` cannot overflow in debug builds. Each entry lands in a different
/// flavor: empty, sparse, hybrid, pinned, and sliding.
const CASES: &[(u8, u64)] = &[
    (4, 0),
    (4, 3),
    (6, 20),
    (8, 200),
    (8, 2000),
    (10, 800),
    (10, 8000),
];

/// Corrupting a valid sketch in any way must never panic: deserialize returns `Ok` or `Err`.
///
/// Before the deserialize hardening this test panicked (e.g. index out of bounds in
/// `maybe_fill_bitbuf`, or a failed assert in `PairTable::lookup`).
#[test]
fn corrupt_sketches_never_panic() {
    let mut rng = Rng(0x1234_5678_9abc_def1);
    let mut checked = 0u64;

    for &(lg_k, n) in CASES {
        let base = valid_bytes(lg_k, n);

        // Exhaustive single-byte edits.
        for pos in 0..base.len() {
            for delta in [1u8, 2, 4, 8, 16, 32, 64, 128, 255] {
                let mut bytes = base.clone();
                bytes[pos] = bytes[pos].wrapping_add(delta);
                // Must return without panicking; the value itself is unimportant.
                let _ = CpcSketch::deserialize(&bytes);
                checked += 1;
            }
        }

        // Truncations of every length.
        for len in 0..base.len() {
            let _ = CpcSketch::deserialize(&base[..len]);
            checked += 1;
        }

        // Random multi-byte edits.
        for _ in 0..20_000 {
            let mut bytes = base.clone();
            let flips = 1 + (rng.next() % 6) as usize;
            for _ in 0..flips {
                if bytes.is_empty() {
                    break;
                }
                let pos = (rng.next() as usize) % bytes.len();
                bytes[pos] = (rng.next() & 0xff) as u8;
            }
            let _ = CpcSketch::deserialize(&bytes);
            checked += 1;
        }
    }

    // Wholly random buffers.
    for _ in 0..50_000 {
        let len = (rng.next() % 200) as usize;
        let bytes: Vec<u8> = (0..len).map(|_| (rng.next() & 0xff) as u8).collect();
        let _ = CpcSketch::deserialize(&bytes);
        checked += 1;
    }

    assert!(checked > 100_000, "expected a meaningful number of trials");
}

/// Hand-picked corruptions that each used to reach a distinct panic site now return an error.
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

    // Corrupting a byte deep in the compressed payload yields garbage decoded pairs.
    let mut bad_payload = base.clone();
    let last = bad_payload.len() - 1;
    bad_payload[last] = bad_payload[last].wrapping_add(1);
    bad_payload[last - 3] ^= 0xa5;
    // May be Ok or Err depending on the bytes, but must not panic.
    let _ = CpcSketch::deserialize(&bad_payload);

    // A sparse sketch whose declared entry count exceeds its data words must be rejected up front.
    let sparse = valid_bytes(8, 200);
    let mut inflated = sparse.clone();
    // num_coupons is a u32 at offset 8; inflate it far beyond the coupon space.
    inflated[10] = 0xff;
    inflated[11] = 0xff;
    assert!(CpcSketch::deserialize(&inflated).is_err());
}

/// The hardening must not change behavior for any valid sketch.
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
