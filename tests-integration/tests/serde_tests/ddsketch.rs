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

use datasketches::ddsketch::DdSketch;
use datasketches::ddsketch::DdSketchBuilder;
use datasketches::ddsketch::MappingKind;
use datasketches::ddsketch::StoreKind;
use datasketches::error::ErrorKind;

use crate::serialization_test_data;

const N_STD: [u64; 8] = [0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000];

#[test]
fn rust_snapshots_round_trip_and_preserve_queries() {
    for n in N_STD {
        let filename = format!("ddsketch_cubic_n{n}_rust.sk");
        let bytes = fs::read(serialization_test_data("rust_generated_files", &filename)).unwrap();
        let sketch = DdSketch::deserialize(&bytes).unwrap();
        assert_eq!(sketch.mapping_kind(), MappingKind::Cubic, "{filename}");
        assert_eq!(sketch.store_kind(), StoreKind::default(), "{filename}");
        assert_eq!(sketch.total_weight(), n, "{filename}");
        assert_eq!(sketch.serialize(), bytes, "{filename}");

        if n > 0 {
            let p50 = sketch.quantile(0.5).unwrap().unwrap();
            let expected = (((n - 1) / 2) + 1) as f64;
            assert!(
                (p50 - expected).abs() <= 0.01 * expected + 1e-9,
                "{filename}: p50={p50}, expected={expected}"
            );
        }
    }

    for n in N_STD {
        let filename = format!("ddsketch_signed_n{n}_rust.sk");
        let bytes = fs::read(serialization_test_data("rust_generated_files", &filename)).unwrap();
        let sketch = DdSketch::deserialize(&bytes).unwrap();
        assert_eq!(
            sketch.mapping_kind(),
            MappingKind::Logarithmic,
            "{filename}"
        );
        assert_eq!(
            sketch.store_kind(),
            StoreKind::CollapsingLowestDense { max_num_bins: 512 },
            "{filename}"
        );
        assert_eq!(sketch.total_weight(), n, "{filename}");
        assert_eq!(sketch.serialize(), bytes, "{filename}");
        if n > 0 {
            let expected_zeros = (n - 1) / 5 + 1;
            assert_eq!(sketch.zero_weight(), expected_zeros, "{filename}");
        }
    }
}

#[test]
fn native_round_trips_across_mappings_stores_collapse_states_and_large_weights() {
    let mappings = [
        MappingKind::Cubic,
        MappingKind::Linear,
        MappingKind::Logarithmic,
    ];
    let stores = [
        StoreKind::CollapsingLowestDense { max_num_bins: 2048 },
        StoreKind::CollapsingHighestDense { max_num_bins: 256 },
        StoreKind::UnboundedDense,
        StoreKind::Sparse,
    ];

    for mapping in mappings {
        for src_store in stores {
            let empty = DdSketchBuilder::new(0.01)
                .mapping(mapping)
                .store(src_store)
                .build()
                .unwrap();
            let empty_bytes = empty.serialize();
            assert_eq!(empty_bytes.len(), 24);

            let mut unit_sparse = empty.clone();
            for v in [-1e6, -1.0, 0.0, 1.0, 1e6] {
                unit_sparse.update(v);
            }

            let mut dense_contiguous = empty.clone();
            for i in 0..40 {
                let v = 10.0 * 1.02_f64.powi(i);
                dense_contiguous.update_with_weight(v, (i as u64 % 5) + 1);
            }

            // Include weights above 2^53 that cannot be represented exactly in IEEE 754 `f64`.
            let w_neg = (1_u64 << 60) + 7;
            let w_zero = (1_u64 << 58) + 11;
            let w_pos_lo = (1_u64 << 59) + 13;
            let w_pos_hi = (1_u64 << 57) + 19;
            let mut gapped_large_weights = empty.clone();
            gapped_large_weights.update_with_weight(-500.0, w_neg);
            gapped_large_weights.update_with_weight(0.0, w_zero);
            gapped_large_weights.update_with_weight(0.05, w_pos_lo);
            gapped_large_weights.update_with_weight(50_000.0, w_pos_hi);

            for sketch in [
                &empty,
                &unit_sparse,
                &dense_contiguous,
                &gapped_large_weights,
            ] {
                let bytes = sketch.serialize();
                let native_decoded = DdSketch::deserialize(&bytes).unwrap();
                assert_eq!(native_decoded.mapping_kind(), mapping);
                assert_eq!(native_decoded.store_kind(), src_store);
                assert_eq!(native_decoded.total_weight(), sketch.total_weight());
                assert_eq!(native_decoded.zero_weight(), sketch.zero_weight());
                assert_eq!(
                    native_decoded.iter().collect::<Vec<_>>(),
                    sketch.iter().collect::<Vec<_>>()
                );
                assert_eq!(
                    native_decoded
                        .quantiles(&[0.0, 0.25, 0.5, 0.75, 1.0])
                        .unwrap(),
                    sketch.quantiles(&[0.0, 0.25, 0.5, 0.75, 1.0]).unwrap()
                );

                for dst_store in stores {
                    let overridden = DdSketch::deserialize_with_store(&bytes, dst_store).unwrap();
                    assert_eq!(overridden.mapping_kind(), mapping);
                    assert_eq!(overridden.store_kind(), dst_store);
                    assert_eq!(overridden.total_weight(), sketch.total_weight());
                    assert_eq!(overridden.zero_weight(), sketch.zero_weight());
                }
            }
        }
    }

    // Verify that collapsed state survives round-trip and continues collapsing subsequent updates
    // identically to the original in-memory sketch.
    for store in [
        StoreKind::CollapsingLowestDense { max_num_bins: 64 },
        StoreKind::CollapsingHighestDense { max_num_bins: 64 },
    ] {
        let mut original = DdSketchBuilder::new(0.01).store(store).build().unwrap();
        for v in [-1e12, -1.0, 1.0, 1e12] {
            original.update(v);
        }
        let mut round_tripped = DdSketch::deserialize(&original.serialize()).unwrap();
        assert_eq!(round_tripped.store_kind(), store);

        for follow_up in [-1e-9, -1e15, 1e-9, 1e15, -25.0, 25.0] {
            original.update(follow_up);
            round_tripped.update(follow_up);
        }
        assert_eq!(
            round_tripped.iter().collect::<Vec<_>>(),
            original.iter().collect::<Vec<_>>()
        );
    }
}

#[test]
fn native_strict_prefixes_and_trailing_bytes_are_rejected() {
    let empty = DdSketch::default();

    let mut unit_deltas = DdSketch::new(0.01).unwrap();
    unit_deltas.update(-100.0);
    unit_deltas.update(0.0);
    unit_deltas.update(100.0);

    let mut contiguous = DdSketch::new(0.01).unwrap();
    for i in 0..16 {
        contiguous.update_with_weight(1.02_f64.powi(i), 3);
    }

    let mut gapped = DdSketch::new(0.01).unwrap();
    gapped.update_with_weight(1.0, 5);
    gapped.update_with_weight(1e9, 7);

    for sketch in [empty, unit_deltas, contiguous, gapped] {
        let bytes = sketch.serialize();
        for end in 0..bytes.len() {
            assert_eq!(
                DdSketch::deserialize(&bytes[..end]).unwrap_err().kind(),
                ErrorKind::InvalidData,
                "truncated prefix {end}/{}",
                bytes.len()
            );
        }
        let mut with_trailing = bytes.clone();
        with_trailing.push(0x00);
        assert_eq!(
            DdSketch::deserialize(&with_trailing).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }
}

#[test]
fn native_malformed_headers_and_body_invariants_are_rejected() {
    let empty_bytes = DdSketch::default().serialize();

    // Wrong preamble_longs, serial_version, family_id, reserved flag bit, and mapping_tag = 3.
    for (offset, bad_byte) in [(0, 1_u8), (1, 2), (2, 20), (3, 0x90), (3, 0x13)] {
        let mut img = empty_bytes.clone();
        img[offset] = bad_byte;
        assert_eq!(
            DdSketch::deserialize(&img).unwrap_err().kind(),
            ErrorKind::InvalidData,
            "header mutation at byte {offset} = {bad_byte:#04x}"
        );
    }

    // Collapsing store with max_num_bins = 0 or > MAX_DENSE_BINS (67_108_864).
    for bad_max_bins in [0_u32, (1 << 26) + 1] {
        let mut img = empty_bytes.clone();
        img[4..8].copy_from_slice(&bad_max_bins.to_le_bytes());
        assert_eq!(
            DdSketch::deserialize(&img).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }

    // UnboundedDense (store_tag = 2) with non-zero max_num_bins.
    let mut bad_unbounded = DdSketchBuilder::new(0.01)
        .store(StoreKind::UnboundedDense)
        .build()
        .unwrap()
        .serialize();
    bad_unbounded[4..8].copy_from_slice(&128_u32.to_le_bytes());
    assert_eq!(
        DdSketch::deserialize(&bad_unbounded).unwrap_err().kind(),
        ErrorKind::InvalidData
    );

    // Collapsed flag on empty sketch, on Sparse sketch, or on under-full collapsing sign store.
    let mut empty_collapsed = empty_bytes.clone();
    empty_collapsed[3] |= 1 << 6;
    assert_eq!(
        DdSketch::deserialize(&empty_collapsed).unwrap_err().kind(),
        ErrorKind::InvalidData
    );

    let mut single_pos = DdSketch::new(0.01).unwrap();
    single_pos.update(10.0);
    let mut forged_collapsed = single_pos.serialize();
    forged_collapsed[3] |= 1 << 6; // FLAG_POS_COLLAPSED on 1-bin store (span = 1 != 2048)
    assert_eq!(
        DdSketch::deserialize(&forged_collapsed).unwrap_err().kind(),
        ErrorKind::InvalidData
    );

    let mut sparse_one = DdSketchBuilder::new(0.01)
        .store(StoreKind::Sparse)
        .build()
        .unwrap();
    sparse_one.update(10.0);
    let mut sparse_collapsed = sparse_one.serialize();
    sparse_collapsed[3] |= 1 << 6;
    assert_eq!(
        DdSketch::deserialize(&sparse_collapsed).unwrap_err().kind(),
        ErrorKind::InvalidData
    );

    // Non-empty header (FLAG_IS_EMPTY cleared) with zero_weight = 0 and both stores empty.
    let mut zero_total = empty_bytes.clone();
    zero_total[3] &= !(1 << 4);
    zero_total.extend_from_slice(&[0x00, 0x00, 0x00]);
    assert_eq!(
        DdSketch::deserialize(&zero_total).unwrap_err().kind(),
        ErrorKind::InvalidData
    );

    // Non-increasing delta in NATIVE_STORE_INDEX_DELTAS (2 bins: idx = 5, delta = 0).
    let mut dup_delta = empty_bytes.clone();
    dup_delta[3] &= !(1 << 4);
    dup_delta.extend_from_slice(&[
        0x00, // zero_weight = 0
        0x00, // neg = Empty
        0x02, // pos = IndexDeltas
        0x02, // num_bins = 2
        0x0a, // +5
        0x00, // +0 (duplicate index!)
    ]);
    assert_eq!(
        DdSketch::deserialize(&dup_delta).unwrap_err().kind(),
        ErrorKind::InvalidData
    );

    // Zero weight in NATIVE_STORE_DELTAS_AND_COUNTS.
    let mut zero_bin = empty_bytes.clone();
    zero_bin[3] &= !(1 << 4);
    zero_bin.extend_from_slice(&[
        0x00, // zero_weight = 0
        0x00, // neg = Empty
        0x03, // pos = DeltasAndCounts
        0x01, // num_bins = 1
        0x0a, // +5
        0x00, // weight = 0 (invalid!)
    ]);
    assert_eq!(
        DdSketch::deserialize(&zero_bin).unwrap_err().kind(),
        ErrorKind::InvalidData
    );

    // Wide index span (80_000_001 > MAX_DENSE_BINS = 67_108_864) encoded on a Sparse native sketch:
    // rejected when overridden to UnboundedDense, accepted by Sparse and CollapsingLowestDense.
    let mut wide_sparse = DdSketchBuilder::new(0.01)
        .mapping(MappingKind::Logarithmic)
        .store(StoreKind::Sparse)
        .build()
        .unwrap()
        .serialize();
    wide_sparse[3] &= !(1 << 4);
    wide_sparse.push(0x00); // zero_weight = 0
    wide_sparse.push(0x00); // neg = Empty
    wide_sparse.push(0x02); // pos = IndexDeltas
    wide_sparse.push(0x02); // 2 bins
    encode_test_varint(&mut wide_sparse, -40_000_000);
    encode_test_varint(&mut wide_sparse, 80_000_000);

    assert_eq!(
        DdSketch::deserialize_with_store(&wide_sparse, StoreKind::UnboundedDense)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
    let sparse_ok = DdSketch::deserialize(&wide_sparse).unwrap();
    assert_eq!(sparse_ok.store_kind(), StoreKind::Sparse);
    assert_eq!(sparse_ok.total_weight(), 2);
    assert_eq!(sparse_ok.num_retained(), 2);

    let collapsing_ok = DdSketch::deserialize_with_store(
        &wide_sparse,
        StoreKind::CollapsingLowestDense { max_num_bins: 2048 },
    )
    .unwrap();
    assert_eq!(collapsing_ok.total_weight(), 2);
}

#[test]
fn auto_detects_datadog_reference_format_and_rejects_malformed_reference_images() {
    let gamma = (1.0 + 0.01_f64) / (1.0 - 0.01_f64);

    // Empty reference images across Logarithmic (0x02), Linear (0x06), and Cubic (0x0e).
    for (flag, expected_kind) in [
        (0x02_u8, MappingKind::Logarithmic),
        (0x06, MappingKind::Linear),
        (0x0e, MappingKind::Cubic),
    ] {
        let mut empty_ref = vec![flag];
        empty_ref.extend_from_slice(&gamma.to_le_bytes());
        empty_ref.extend_from_slice(&0.0_f64.to_le_bytes());
        let decoded = DdSketch::deserialize(&empty_ref).unwrap();
        assert!(decoded.is_empty());
        assert_eq!(decoded.mapping_kind(), expected_kind);
        assert_eq!(decoded.store_kind(), StoreKind::default());
    }

    // Reordered reference blocks with summary-statistics features and all three bin encodings:
    // 1. FlagCount (0xa0) + varfloat64(9.0 = [0x86, 0x40])
    // 2. FlagSum   (0x84) + f64 LE
    // 3. FlagMin   (0x88) + f64 LE
    // 4. FlagMax   (0x8c) + f64 LE
    // 5. ZeroCount (0x04) + varfloat64(2.0 = [0x03])
    // 6. NegativeStore IndexDeltas (0x0b): 1 bin at index 10 (varint 0x14)
    // 7. NegativeStore DeltasAndCounts (0x07): 1 bin at index 12 (varint 0x18), count 3.0 (0x04)
    // 8. Logarithmic mapping (0x02): gamma, offset = 0.0
    // 9. PositiveStore ContiguousCounts (0x0d): 2 bins from index 5, step 2, counts [1.0, 2.0]
    let mut wire = Vec::new();
    wire.push(0xa0);
    wire.extend_from_slice(&[0x86, 0x40]);
    wire.push(0x84);
    wire.extend_from_slice(&12.5_f64.to_le_bytes());
    wire.push(0x88);
    wire.extend_from_slice(&(-1.5_f64).to_le_bytes());
    wire.push(0x8c);
    wire.extend_from_slice(&20.0_f64.to_le_bytes());
    wire.push(0x04);
    wire.push(0x03); // zero_count = 2
    wire.push(0x0b);
    wire.push(0x01);
    wire.push(0x14); // neg index 10, weight 1
    wire.push(0x07);
    wire.push(0x01);
    wire.push(0x18); // neg index 12
    wire.push(0x04); // weight 3
    wire.push(0x02);
    wire.extend_from_slice(&gamma.to_le_bytes());
    wire.extend_from_slice(&0.0_f64.to_le_bytes());
    wire.push(0x0d);
    wire.push(0x02); // 2 bins
    wire.push(0x0a); // first_index = +5
    wire.push(0x04); // index_delta = +2
    wire.push(0x02); // count[0] = 1
    wire.push(0x03); // count[1] = 2

    let decoded = DdSketch::deserialize(&wire).unwrap();
    assert_eq!(decoded.mapping_kind(), MappingKind::Logarithmic);
    assert_eq!(decoded.store_kind(), StoreKind::default());
    assert_eq!(decoded.total_weight(), 9);
    assert_eq!(decoded.zero_weight(), 2);
    assert_eq!(decoded.num_retained(), 5);

    let decoded_sparse = DdSketch::deserialize_with_store(&wire, StoreKind::Sparse).unwrap();
    assert_eq!(decoded_sparse.store_kind(), StoreKind::Sparse);
    assert_eq!(
        decoded_sparse.iter().collect::<Vec<_>>(),
        decoded.iter().collect::<Vec<_>>()
    );

    // Missing index mapping (only ZeroCount = 1).
    assert_eq!(
        DdSketch::deserialize(&[0x04, 0x02]).unwrap_err().kind(),
        ErrorKind::InvalidData
    );

    // Unsupported reference Quadratic mapping (0x0a) and conflicting duplicate mappings.
    let mut quadratic = vec![0x0a];
    quadratic.extend_from_slice(&gamma.to_le_bytes());
    quadratic.extend_from_slice(&0.0_f64.to_le_bytes());
    assert_eq!(
        DdSketch::deserialize(&quadratic).unwrap_err().kind(),
        ErrorKind::InvalidData
    );

    let mut base_ref = vec![0x02];
    base_ref.extend_from_slice(&gamma.to_le_bytes());
    base_ref.extend_from_slice(&0.0_f64.to_le_bytes());

    let mut conflicting = base_ref.clone();
    conflicting.push(0x02);
    conflicting.extend_from_slice(&((1.0 + 0.05_f64) / (1.0 - 0.05_f64)).to_le_bytes());
    conflicting.extend_from_slice(&0.0_f64.to_le_bytes());
    assert_eq!(
        DdSketch::deserialize(&conflicting).unwrap_err().kind(),
        ErrorKind::InvalidData
    );

    // Invalid gamma values in reference header.
    for bad_gamma in [0.5_f64, 1.0, 1.0 + 1e-12, f64::NAN, f64::INFINITY] {
        let mut img = vec![0x02];
        img.extend_from_slice(&bad_gamma.to_le_bytes());
        img.extend_from_slice(&0.0_f64.to_le_bytes());
        assert_eq!(
            DdSketch::deserialize(&img).unwrap_err().kind(),
            ErrorKind::InvalidData,
            "bad_gamma={bad_gamma}"
        );
    }

    // Negative (-1.0) and fractional (-0.5) varfloat64 counts in reference image.
    for bad_varfloat in [
        &[0x82_u8, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x30][..],
        &[0xfe_u8, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x3f][..],
    ] {
        let mut img = base_ref.clone();
        img.push(0x04);
        img.extend_from_slice(bad_varfloat);
        assert_eq!(
            DdSketch::deserialize(&img).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }

    // Index overflowing i32::MAX in reference IndexDeltas.
    let mut i32_overflow = base_ref.clone();
    i32_overflow.extend_from_slice(&[0x09, 0x01, 0x80, 0x80, 0x80, 0x80, 0x10]);
    assert_eq!(
        DdSketch::deserialize(&i32_overflow).unwrap_err().kind(),
        ErrorKind::InvalidData
    );

    // Wide index span in reference image rejected by UnboundedDense, accepted by Sparse.
    let mut wide_ref = base_ref;
    wide_ref.push(0x09);
    wide_ref.push(0x02);
    encode_test_varint(&mut wide_ref, -40_000_000);
    encode_test_varint(&mut wide_ref, 80_000_000);
    assert_eq!(
        DdSketch::deserialize_with_store(&wide_ref, StoreKind::UnboundedDense)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
    assert_eq!(
        DdSketch::deserialize_with_store(&wide_ref, StoreKind::Sparse)
            .unwrap()
            .total_weight(),
        2
    );

    // Invalid StoreKind passed to deserialize_with_store returns InvalidArgument even on bad bytes.
    assert_eq!(
        DdSketch::deserialize_with_store(&[], StoreKind::CollapsingLowestDense { max_num_bins: 0 })
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
}

fn encode_test_varint(out: &mut Vec<u8>, v: i64) {
    let mut u = ((v << 1) ^ (v >> 63)) as u64;
    for _ in 0..8 {
        if u < 0x80 {
            break;
        }
        out.push((u as u8) | 0x80);
        u >>= 7;
    }
    out.push(u as u8);
}
