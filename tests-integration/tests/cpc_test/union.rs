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

use datasketches::cpc::CpcSketch;
use datasketches::cpc::CpcUnion;
use datasketches::error::ErrorKind;
use googletest::assert_that;
use googletest::prelude::near;

const RELATIVE_ERROR_FOR_LG_K_11: f64 = 0.02;

#[test]
fn test_empty() {
    let union = CpcUnion::new(11).unwrap();
    let sketch = union.to_sketch();
    assert!(sketch.is_empty());
    assert_eq!(sketch.estimate(), 0.0);
}

#[test]
fn test_two_values() {
    let mut sketch = CpcSketch::new(11).unwrap();
    sketch.update(1);
    let mut union = CpcUnion::new(11).unwrap();
    union.update(&sketch).unwrap();

    let result = union.to_sketch();
    assert!(!result.is_empty());
    assert_eq!(result.estimate(), 1.0);

    sketch.update(2);
    union.update(&sketch).unwrap();
    let result = union.to_sketch();
    assert!(!result.is_empty());
    assert_that!(
        result.estimate(),
        near(2.0, RELATIVE_ERROR_FOR_LG_K_11 * 2.0)
    );
}

#[test]
fn test_custom_seed() {
    let mut sketch = CpcSketch::with_seed(11, 123).unwrap();
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);

    let mut union = CpcUnion::with_seed(11, 123).unwrap();
    union.update(&sketch).unwrap();
    let result = union.to_sketch();
    assert!(!result.is_empty());
    assert_that!(
        result.estimate(),
        near(3.0, RELATIVE_ERROR_FOR_LG_K_11 * 3.0)
    );
}

#[test]
fn test_custom_seed_mismatch() {
    let mut sketch = CpcSketch::with_seed(11, 123).unwrap();
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);

    let mut union = CpcUnion::with_seed(11, 234).unwrap();
    let error = union.update(&sketch).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn test_sliding_union_matches_single_sketch() {
    let mut key = 0;
    let mut sketch = CpcSketch::new(11).unwrap();
    let mut union = CpcUnion::new(11).unwrap();
    for _ in 0..32 {
        let mut tmp = CpcSketch::new(11).unwrap();
        for _ in 0..8192 {
            sketch.update(key);
            tmp.update(key);
            key += 1;
        }
        union.update(&tmp).unwrap();
    }
    let result = union.to_sketch();
    assert!(!result.is_empty());
    let estimate = sketch.estimate();
    assert_that!(
        result.estimate(),
        near(estimate, RELATIVE_ERROR_FOR_LG_K_11 * estimate)
    );
}

#[test]
fn test_reduce_k_empty() {
    let mut sketch = CpcSketch::new(11).unwrap();
    for i in 0..10000 {
        sketch.update(i);
    }
    let mut union = CpcUnion::new(12).unwrap();
    union.update(&sketch).unwrap();
    let result = union.to_sketch();
    assert_eq!(result.lg_k(), 11);
    assert_that!(
        result.estimate(),
        near(10000.0, RELATIVE_ERROR_FOR_LG_K_11 * 10000.0)
    );
}

#[test]
fn test_reduce_k_sparse() {
    let mut union = CpcUnion::new(12).unwrap();

    let mut sketch12 = CpcSketch::new(12).unwrap();
    for i in 0..100 {
        sketch12.update(i);
    }
    union.update(&sketch12).unwrap();

    let mut sketch11 = CpcSketch::new(11).unwrap();
    for i in 0..1000 {
        sketch11.update(i);
    }
    union.update(&sketch11).unwrap();

    let result = union.to_sketch();
    assert_eq!(result.lg_k(), 11);
    assert_that!(
        result.estimate(),
        near(1000.0, RELATIVE_ERROR_FOR_LG_K_11 * 1000.0)
    );
}

#[test]
fn test_reduce_k_window() {
    let mut union = CpcUnion::new(12).unwrap();

    let mut sketch12 = CpcSketch::new(12).unwrap();
    for i in 0..500 {
        sketch12.update(i);
    }
    union.update(&sketch12).unwrap();

    let mut sketch11 = CpcSketch::new(11).unwrap();
    for i in 0..1000 {
        sketch11.update(i);
    }
    union.update(&sketch11).unwrap();

    let result = union.to_sketch();
    assert_eq!(result.lg_k(), 11);
    assert_that!(
        result.estimate(),
        near(1000.0, RELATIVE_ERROR_FOR_LG_K_11 * 1000.0)
    );
}

#[test]
fn test_lg_k_too_small_returns_error() {
    let error = CpcSketch::new(3).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn test_lg_k_too_large_returns_error() {
    let error = CpcSketch::new(27).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn test_union_serialize_all_flavors_and_subsequent_merges() {
    const SEED: u64 = 0x5eed_cafe;

    // Covers Empty, Sparse, Hybrid, Pinned (without and with post-window surprises), and Sliding
    // across MIN_LG_K = 4 and lg_k = 8, spanning multiple window offsets and permutation phases.
    let cases: &[(u8, &[u64])] = &[
        (4, &[0, 1, 3, 12, 40, 120, 500]),
        (8, &[0, 10, 60, 180, 1_500, 3_000, 12_000, 50_000]),
    ];

    let mut saw_window_only = false;
    let mut saw_window_and_table = false;
    for &(lg_k, counts) in cases {
        for &num_values in counts {
            let mut union = CpcUnion::with_seed(lg_k, SEED).unwrap();
            if num_values > 0 {
                let split = num_values / 2;
                let mut left = CpcSketch::with_seed(lg_k, SEED).unwrap();
                for value in 0..split {
                    left.update(value);
                }
                let mut right = CpcSketch::with_seed(lg_k, SEED).unwrap();
                for value in split..num_values {
                    right.update(value);
                }
                union.update(&left).unwrap();
                union.update(&right).unwrap();
            }

            let bytes = union.serialize();
            let expected_sketch = union.to_sketch();
            assert_eq!(
                bytes,
                expected_sketch.serialize(),
                "mismatch at lg_k={lg_k}, n={num_values}"
            );

            let flags = bytes[5];
            let has_table = flags & (1 << 3) != 0;
            let has_window = flags & (1 << 4) != 0;
            if has_window && !has_table {
                saw_window_only = true;
            }
            if has_window && has_table {
                saw_window_and_table = true;
            }

            assert_eq!(
                CpcSketch::deserialize(&bytes).unwrap_err().kind(),
                ErrorKind::InvalidData
            );
            let decoded = CpcSketch::deserialize_with_seed(&bytes, SEED).unwrap();
            assert_eq!(decoded.lg_k(), expected_sketch.lg_k());
            assert_eq!(decoded.is_empty(), expected_sketch.is_empty());
            assert_eq!(decoded.estimate(), expected_sketch.estimate());

            // Decoded union image must merge cleanly into downstream unions.
            let mut extra = CpcSketch::with_seed(lg_k, SEED).unwrap();
            for value in num_values..(num_values + 64) {
                extra.update(value);
            }

            let mut downstream_from_bytes = CpcUnion::with_seed(lg_k, SEED).unwrap();
            downstream_from_bytes.update(&decoded).unwrap();
            downstream_from_bytes.update(&extra).unwrap();

            let mut downstream_from_sketch = CpcUnion::with_seed(lg_k, SEED).unwrap();
            downstream_from_sketch.update(&expected_sketch).unwrap();
            downstream_from_sketch.update(&extra).unwrap();

            assert_eq!(
                downstream_from_bytes.serialize(),
                downstream_from_sketch.to_sketch().serialize()
            );
        }
    }

    assert!(
        saw_window_only,
        "expected a pinned bit-matrix case without surprise table"
    );
    assert!(
        saw_window_and_table,
        "expected windowed bit-matrix cases with surprise table"
    );
}

#[test]
fn test_union_serialize_mixed_lg_k_downsampling_across_bit_matrix_flavors() {
    let mut union = CpcUnion::new(11).unwrap();

    // Progress through Sparse -> Hybrid -> Pinned -> Sliding while mixing wider and narrower lg_k
    // inputs so both source folding and bit-matrix downsampling are exercised before serializing.
    for (lg_k, range) in [
        (12_u8, 0_u64..80),
        (10, 40..450),
        (11, 300..900),
        (9, 500..2_200),
        (10, 1_800..4_500),
        (8, 2_000..15_000),
    ] {
        let mut sketch = CpcSketch::new(lg_k).unwrap();
        for value in range {
            sketch.update(value);
        }
        union.update(&sketch).unwrap();

        let bytes = union.serialize();
        let expected = union.to_sketch();
        assert_eq!(bytes, expected.serialize());

        let decoded = CpcSketch::deserialize(&bytes).unwrap();
        assert_eq!(decoded.lg_k(), union.lg_k());
        assert_eq!(decoded.estimate(), expected.estimate());
    }
}
