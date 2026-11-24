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

            // Include weights above 2^53 that cannot be represented without rounding in `f64`.
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
                let decoded = DdSketch::deserialize(&bytes).unwrap();
                assert_eq!(decoded.mapping_kind(), mapping);
                assert_eq!(decoded.store_kind(), src_store);
                assert_eq!(decoded.total_weight(), sketch.total_weight());
                assert_eq!(decoded.zero_weight(), sketch.zero_weight());
                assert_eq!(decoded.min_value(), sketch.min_value());
                assert_eq!(decoded.max_value(), sketch.max_value());
                assert_eq!(decoded.sum(), sketch.sum());
                assert_eq!(
                    decoded.iter().collect::<Vec<_>>(),
                    sketch.iter().collect::<Vec<_>>()
                );
                assert_eq!(
                    decoded.quantiles(&[0.0, 0.25, 0.5, 0.75, 1.0]).unwrap(),
                    sketch.quantiles(&[0.0, 0.25, 0.5, 0.75, 1.0]).unwrap()
                );

                // Native images are rejected by `deserialize_compat` without heuristic fallback.
                assert_eq!(
                    DdSketch::deserialize_compat(&bytes, StoreKind::default())
                        .unwrap_err()
                        .kind(),
                    ErrorKind::InvalidData
                );
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

    // Wide UnboundedDense sketch (> 65,536 bins) uses contiguous native encoding and round-trips.
    let mut wide_dense = DdSketchBuilder::new(1e-5)
        .mapping(MappingKind::Logarithmic)
        .store(StoreKind::UnboundedDense)
        .build()
        .unwrap();
    wide_dense.update(1.0);
    wide_dense.update(10.0);
    let wide_dense_bytes = wide_dense.serialize();
    assert!(wide_dense_bytes.len() > 65_536);
    let wide_dense_decoded = DdSketch::deserialize(&wide_dense_bytes).unwrap();
    assert_eq!(wide_dense_decoded.store_kind(), StoreKind::UnboundedDense);
    assert_eq!(
        wide_dense_decoded.iter().collect::<Vec<_>>(),
        wide_dense.iter().collect::<Vec<_>>()
    );

    // Out-of-domain bucket indices (below `zero_threshold`, underflowing to 0.0, or exceeding
    // f64::MAX under alpha = 0.01) are rejected even for Sparse stores.
    for bad_idx in [-100_000_i64, -36_000, 100_000] {
        let mut out_of_domain = DdSketchBuilder::new(0.01)
            .mapping(MappingKind::Logarithmic)
            .store(StoreKind::Sparse)
            .build()
            .unwrap()
            .serialize();
        out_of_domain[3] &= !(1 << 4);
        out_of_domain.extend_from_slice(&[0x00, 0x00, 0x02, 0x01]);
        encode_test_varint(&mut out_of_domain, bad_idx);
        assert_eq!(
            DdSketch::deserialize(&out_of_domain).unwrap_err().kind(),
            ErrorKind::InvalidData,
            "bad_idx={bad_idx}"
        );
    }

    // Forged small delta-encoded image claiming > 65,536 span on a dense store is rejected before
    // allocating, while a Sparse store at `alpha = 1e-6` accepts legitimate wide-span indices.
    let mut forged_wide_dense = DdSketchBuilder::new(1e-6)
        .mapping(MappingKind::Logarithmic)
        .store(StoreKind::UnboundedDense)
        .build()
        .unwrap()
        .serialize();
    forged_wide_dense[3] &= !(1 << 4);
    forged_wide_dense.extend_from_slice(&[0x00, 0x00, 0x02, 0x02]);
    encode_test_varint(&mut forged_wide_dense, -40_000_000);
    encode_test_varint(&mut forged_wide_dense, 80_000_000);
    assert_eq!(
        DdSketch::deserialize(&forged_wide_dense)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    let mut valid_wide_sparse = forged_wide_dense;
    valid_wide_sparse[3] = (valid_wide_sparse[3] & !(0b11 << 2)) | (3 << 2);
    let sparse_ok = DdSketch::deserialize(&valid_wide_sparse).unwrap();
    assert_eq!(sparse_ok.store_kind(), StoreKind::Sparse);
    assert_eq!(sparse_ok.total_weight(), 2);
    assert_eq!(sparse_ok.num_retained(), 2);
    assert_eq!(sparse_ok.min_value(), None);
    assert!(sparse_ok.quantile(0.0).unwrap().unwrap() > 0.0);

    // Inverted, non-finite, or sign-contradictory exact summary bounds in native image
    // (`FLAG_HAS_EXACT_SUMMARY` = 1 << 7) are rejected.
    let mut bad_native_summary = single_pos.serialize();
    bad_native_summary[24..32].copy_from_slice(&50.0_f64.to_le_bytes()); // min = 50.0 > max = 10.0
    assert_eq!(
        DdSketch::deserialize(&bad_native_summary)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    let mut sign_contradictory_native = single_pos.serialize();
    sign_contradictory_native[24..32].copy_from_slice(&(-10.0_f64).to_le_bytes());
    sign_contradictory_native[32..40].copy_from_slice(&(-10.0_f64).to_le_bytes());
    sign_contradictory_native[40..48].copy_from_slice(&(-10.0_f64).to_le_bytes());
    assert_eq!(
        DdSketch::deserialize(&sign_contradictory_native)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    // Forged native collapsing store claiming `num_bins > max_num_bins` (contiguous or delta) is
    // rejected upfront.
    let narrow_header = DdSketchBuilder::new(0.01)
        .mapping(MappingKind::Logarithmic)
        .store(StoreKind::CollapsingLowestDense { max_num_bins: 16 })
        .build()
        .unwrap()
        .serialize();
    let mut forged_contig_over_max = narrow_header.clone();
    forged_contig_over_max[3] &= !(1 << 4);
    forged_contig_over_max.extend_from_slice(&[0x00, 0x00, 0x01, 32, 0x00]);
    forged_contig_over_max.extend(std::iter::repeat_n(0x01, 32));
    assert_eq!(
        DdSketch::deserialize(&forged_contig_over_max)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    let mut forged_delta_over_max = narrow_header;
    forged_delta_over_max[3] &= !(1 << 4);
    forged_delta_over_max.extend_from_slice(&[0x00, 0x00, 0x02, 0x02]);
    encode_test_varint(&mut forged_delta_over_max, 0);
    encode_test_varint(&mut forged_delta_over_max, 32);
    assert_eq!(
        DdSketch::deserialize(&forged_delta_over_max)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    // Out-of-domain native ContiguousCounts block is rejected before allocating, including when a
    // trailing zero-weight slot extends one bin past `max_indexable_index`.
    for (first_idx, weights) in [
        (100_000_i64, &[0x01_u8, 0x01, 0x01, 0x01][..]),
        (35_489, &[0x01_u8, 0x00][..]),
    ] {
        let mut forged_contig_out_of_domain = DdSketchBuilder::new(0.01)
            .mapping(MappingKind::Logarithmic)
            .store(StoreKind::UnboundedDense)
            .build()
            .unwrap()
            .serialize();
        forged_contig_out_of_domain[3] &= !(1 << 4);
        forged_contig_out_of_domain.extend_from_slice(&[0x00, 0x00, 0x01, weights.len() as u8]);
        encode_test_varint(&mut forged_contig_out_of_domain, first_idx);
        forged_contig_out_of_domain.extend_from_slice(weights);
        assert_eq!(
            DdSketch::deserialize(&forged_contig_out_of_domain)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );
    }
}

#[test]
fn compat_round_trips_across_mappings_stores_and_weight_boundaries() {
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
            let empty_compat = empty.serialize_compat().unwrap();
            assert_eq!(empty_compat.len(), 17);

            let mut unit_sparse = empty.clone();
            for v in [-1e6, -1.0, 0.0, 1.0, 1e6] {
                unit_sparse.update(v);
            }

            let mut dense_contiguous = empty.clone();
            for i in 0..40 {
                let v = 10.0 * 1.02_f64.powi(i);
                dense_contiguous.update_with_weight(v, (i as u64 % 5) + 1);
            }

            // Exact `2^53 - 1` total weight is representable losslessly in compat `varfloat64`
            // (which biases counts by `+ 1.0` in `f64`).
            let mut boundary_weighted = empty.clone();
            boundary_weighted.update_with_weight(-500.0, (1_u64 << 52) - 5);
            boundary_weighted.update_with_weight(0.0, 1_u64 << 51);
            boundary_weighted.update_with_weight(0.05, 4);
            boundary_weighted.update_with_weight(50_000.0, 1_u64 << 51);
            assert_eq!(boundary_weighted.total_weight(), (1_u64 << 53) - 1);

            for sketch in [&empty, &unit_sparse, &dense_contiguous, &boundary_weighted] {
                let compat_bytes = sketch.serialize_compat().unwrap();
                // Native `deserialize` rejects compat images without heuristic auto-detection.
                assert_eq!(
                    DdSketch::deserialize(&compat_bytes).unwrap_err().kind(),
                    ErrorKind::InvalidData
                );

                for dst_store in stores {
                    let decoded = DdSketch::deserialize_compat(&compat_bytes, dst_store).unwrap();
                    assert_eq!(decoded.mapping_kind(), mapping);
                    assert_eq!(decoded.store_kind(), dst_store);
                    assert_eq!(decoded.total_weight(), sketch.total_weight());
                    assert_eq!(decoded.zero_weight(), sketch.zero_weight());
                    assert_eq!(decoded.min_value(), sketch.min_value());
                    assert_eq!(decoded.max_value(), sketch.max_value());
                    assert_eq!(decoded.sum(), sketch.sum());
                    // When dst_store does not collapse `sketch`'s retained span, buckets match.
                    if matches!(dst_store, StoreKind::UnboundedDense | StoreKind::Sparse)
                        || dst_store == src_store
                    {
                        assert_eq!(
                            decoded.iter().collect::<Vec<_>>(),
                            sketch.iter().collect::<Vec<_>>()
                        );
                    }
                }
            }
        }
    }

    // Weights at or above `2^53` (where `weight + 1.0` rounds in IEEE 754 `f64`) are rejected by
    // `serialize_compat` with `InvalidArgument`, including when individual buckets stay below
    // `2^53` while `total_weight` (encoded in `FlagCount`) reaches `2^53`.
    let mut over_zero = DdSketch::new(0.01).unwrap();
    over_zero.update_with_weight(0.0, 1_u64 << 53);
    assert_eq!(
        over_zero.serialize_compat().unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );

    let mut over_bucket = DdSketch::new(0.01).unwrap();
    over_bucket.update_with_weight(10.0, 1_u64 << 53);
    assert_eq!(
        over_bucket.serialize_compat().unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );

    let mut over_total = DdSketch::new(0.01).unwrap();
    over_total.update_with_weight(10.0, 1_u64 << 52);
    over_total.update_with_weight(20.0, 1_u64 << 52);
    assert_eq!(
        over_total.serialize_compat().unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
}

#[test]
fn compat_decodes_reordered_summary_blocks_and_rejects_malformed_images() {
    let gamma = (1.0 + 0.01_f64) / (1.0 - 0.01_f64);

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

    let decoded = DdSketch::deserialize_compat(&wire, StoreKind::default()).unwrap();
    assert_eq!(decoded.mapping_kind(), MappingKind::Logarithmic);
    assert_eq!(decoded.store_kind(), StoreKind::default());
    assert_eq!(decoded.total_weight(), 9);
    assert_eq!(decoded.zero_weight(), 2);
    assert_eq!(decoded.num_retained(), 5);
    assert_eq!(decoded.min_value(), Some(-1.5));
    assert_eq!(decoded.max_value(), Some(20.0));
    assert_eq!(decoded.sum(), Some(12.5));
    assert_eq!(decoded.quantile(0.0).unwrap(), Some(-1.5));
    assert_eq!(decoded.quantile(1.0).unwrap(), Some(20.0));

    let reencoded =
        DdSketch::deserialize_compat(&decoded.serialize_compat().unwrap(), StoreKind::default())
            .unwrap();
    assert_eq!(reencoded.min_value(), Some(-1.5));
    assert_eq!(reencoded.max_value(), Some(20.0));
    assert_eq!(reencoded.sum(), Some(12.5));

    let decoded_sparse = DdSketch::deserialize_compat(&wire, StoreKind::Sparse).unwrap();
    assert_eq!(decoded_sparse.store_kind(), StoreKind::Sparse);
    assert_eq!(
        decoded_sparse.iter().collect::<Vec<_>>(),
        decoded.iter().collect::<Vec<_>>()
    );

    // `sketches-go` derives Linear `offset = 1 / log2(gamma)` and Cubic `gamma` with `math.Pow`,
    // differing in the last bits from `DdSketchBuilder::new(0.01)`; both must merge cleanly under
    // the bucket-drift mapping tolerance. Bare compat images without summary flags leave
    // `min_value`/`max_value`/`sum` as `None` and re-encode without summary blocks.
    let go_linear_gamma = ((1.0 + 0.01_f64) / (1.0 - 0.01_f64)).powf(std::f64::consts::LN_2);
    let go_linear_offset = 49.998333288886464_f64;
    let mut go_linear_wire = vec![0x06];
    go_linear_wire.extend_from_slice(&go_linear_gamma.to_le_bytes());
    go_linear_wire.extend_from_slice(&go_linear_offset.to_le_bytes());
    go_linear_wire.extend_from_slice(&[0x09, 0x01, 0x64]); // 1 pos bin at index 50
    let go_linear = DdSketch::deserialize_compat(&go_linear_wire, StoreKind::default()).unwrap();
    assert_eq!(go_linear.min_value(), None);
    assert_eq!(go_linear.max_value(), None);
    assert_eq!(go_linear.sum(), None);
    assert_eq!(go_linear.serialize_compat().unwrap(), go_linear_wire);
    let mut rust_linear = DdSketchBuilder::new(0.01)
        .mapping(MappingKind::Linear)
        .build()
        .unwrap();
    rust_linear.update(10.0);
    assert_eq!(rust_linear.min_value(), Some(10.0));
    rust_linear.merge(&go_linear).unwrap();
    assert_eq!(rust_linear.total_weight(), 2);
    assert_eq!(rust_linear.min_value(), None);
    assert_eq!(rust_linear.sum(), None);

    // Malformed summary blocks in compat format (count mismatch, missing max, min > max, NaN min,
    // or non-zero summary on an empty sketch) are rejected with InvalidData.
    let mut count_mismatch = wire.clone();
    count_mismatch[1..3].copy_from_slice(&[0x82, 0x40]); // FlagCount = 8 != total_weight 9
    assert_eq!(
        DdSketch::deserialize_compat(&count_mismatch, StoreKind::default())
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    let mut inverted_bounds = wire.clone();
    inverted_bounds[13..21].copy_from_slice(&50.0_f64.to_le_bytes()); // min = 50 > max = 20
    assert_eq!(
        DdSketch::deserialize_compat(&inverted_bounds, StoreKind::default())
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    let mut nan_min = wire.clone();
    nan_min[13..21].copy_from_slice(&f64::NAN.to_le_bytes());
    assert_eq!(
        DdSketch::deserialize_compat(&nan_min, StoreKind::default())
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    let mut pos_only = DdSketch::new(0.01).unwrap();
    pos_only.update(10.0);
    let mut sign_contradictory_compat = pos_only.serialize_compat().unwrap();
    let neg_ten = (-10.0_f64).to_le_bytes();
    sign_contradictory_compat[3..11].copy_from_slice(&neg_ten); // FlagMin = -10.0
    sign_contradictory_compat[12..20].copy_from_slice(&neg_ten); // FlagMax = -10.0 with pos bin!
    assert_eq!(
        DdSketch::deserialize_compat(&sign_contradictory_compat, StoreKind::default())
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    let go_cubic_gamma = 1.0200022828058628_f64;
    let mut go_cubic_wire = vec![0x0e];
    go_cubic_wire.extend_from_slice(&go_cubic_gamma.to_le_bytes());
    go_cubic_wire.extend_from_slice(&0.0_f64.to_le_bytes());
    go_cubic_wire.extend_from_slice(&[0x09, 0x01, 0x64]); // 1 pos bin at index 50
    let go_cubic = DdSketch::deserialize_compat(&go_cubic_wire, StoreKind::default()).unwrap();
    let mut rust_cubic = DdSketchBuilder::new(0.01)
        .mapping(MappingKind::Cubic)
        .build()
        .unwrap();
    rust_cubic.update(10.0);
    rust_cubic.merge(&go_cubic).unwrap();
    assert_eq!(rust_cubic.total_weight(), 2);

    // At `alpha = 1e-6`, `sketches-go`'s `Linear` `offset = 1 / log2(gamma)` (~499999.999976)
    // differs from Rust's `ln_1p` offset (~499999.9999998) by ~2.4e-5 bins (4.7e-11 relative);
    // both merge directions, native round-trips, and concatenated compat streams in both orders
    // must succeed even with `f64::MAX` populated.
    let go_tight_gamma = ((1.0 + 1e-6_f64) / (1.0 - 1e-6_f64)).powf(std::f64::consts::LN_2);
    let go_tight_offset = 1.0 / go_tight_gamma.log2();
    let mut go_tight_header = vec![0x06];
    go_tight_header.extend_from_slice(&go_tight_gamma.to_le_bytes());
    go_tight_header.extend_from_slice(&go_tight_offset.to_le_bytes());
    let mut go_tight = DdSketch::deserialize_compat(&go_tight_header, StoreKind::Sparse).unwrap();
    go_tight.update(1.0);
    go_tight.update(f64::MAX);

    let mut rust_tight = DdSketchBuilder::new(1e-6)
        .mapping(MappingKind::Linear)
        .store(StoreKind::Sparse)
        .build()
        .unwrap();
    let rust_tight_empty_compat = rust_tight.serialize_compat().unwrap();
    let go_tight_compat = go_tight.serialize_compat().unwrap();

    for concat in [
        [
            rust_tight_empty_compat.as_slice(),
            go_tight_compat.as_slice(),
        ]
        .concat(),
        [
            go_tight_compat.as_slice(),
            rust_tight_empty_compat.as_slice(),
        ]
        .concat(),
    ] {
        let joined = DdSketch::deserialize_compat(&concat, StoreKind::Sparse).unwrap();
        assert_eq!(joined.total_weight(), 2);
    }

    rust_tight.merge(&go_tight).unwrap();
    assert_eq!(
        DdSketch::deserialize(&rust_tight.serialize())
            .unwrap()
            .total_weight(),
        2
    );

    // Missing index mapping (only ZeroCount = 1).
    assert_eq!(
        DdSketch::deserialize_compat(&[0x04, 0x02], StoreKind::default())
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    // Unsupported reference Quadratic mapping (0x0a) and conflicting duplicate mappings.
    let mut quadratic = vec![0x0a];
    quadratic.extend_from_slice(&gamma.to_le_bytes());
    quadratic.extend_from_slice(&0.0_f64.to_le_bytes());
    assert_eq!(
        DdSketch::deserialize_compat(&quadratic, StoreKind::default())
            .unwrap_err()
            .kind(),
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
        DdSketch::deserialize_compat(&conflicting, StoreKind::default())
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    // Strict prefixes of an empty 17-byte compat image fail with InvalidData.
    for end in 0..base_ref.len() {
        assert_eq!(
            DdSketch::deserialize_compat(&base_ref[..end], StoreKind::default())
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );
    }

    // Invalid gamma values in reference header.
    for bad_gamma in [0.5_f64, 1.0, 1.0 + 1e-12, f64::NAN, f64::INFINITY] {
        let mut img = vec![0x02];
        img.extend_from_slice(&bad_gamma.to_le_bytes());
        img.extend_from_slice(&0.0_f64.to_le_bytes());
        assert_eq!(
            DdSketch::deserialize_compat(&img, StoreKind::default())
                .unwrap_err()
                .kind(),
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
            DdSketch::deserialize_compat(&img, StoreKind::default())
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );
    }

    // Index overflowing i32::MAX in reference IndexDeltas.
    let mut i32_overflow = base_ref.clone();
    i32_overflow.extend_from_slice(&[0x09, 0x01, 0x80, 0x80, 0x80, 0x80, 0x10]);
    assert_eq!(
        DdSketch::deserialize_compat(&i32_overflow, StoreKind::default())
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    // Out-of-domain bucket index (including sub-`zero_threshold` index -36_000) under `alpha =
    // 0.01` is rejected in compat format too.
    for bad_idx in [-100_000_i64, -36_000, 100_000] {
        let mut out_of_domain_ref = base_ref.clone();
        out_of_domain_ref.extend_from_slice(&[0x09, 0x01]);
        encode_test_varint(&mut out_of_domain_ref, bad_idx);
        assert_eq!(
            DdSketch::deserialize_compat(&out_of_domain_ref, StoreKind::Sparse)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );
    }

    // Duplicate contiguous blocks whose weights sum to `2^64` return InvalidData without panicking
    // across both collapsing and sparse stores (`0x7e` encodes `2^63` in `varfloat64`).
    let mut dup_contig_overflow = base_ref;
    dup_contig_overflow.extend_from_slice(&[0x0d, 0x01, 0x0a, 0x02, 0x7e]);
    dup_contig_overflow.extend_from_slice(&[0x0d, 0x01, 0x0a, 0x02, 0x7e]);
    for store in [StoreKind::default(), StoreKind::Sparse] {
        assert_eq!(
            DdSketch::deserialize_compat(&dup_contig_overflow, store)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );
    }

    // Valid wide index span at `alpha = 1e-6` in reference image is rejected by UnboundedDense,
    // while accepted without collapsing by Sparse.
    let tight_gamma = (1.0 + 1e-6_f64) / (1.0 - 1e-6_f64);
    let mut wide_ref = vec![0x02];
    wide_ref.extend_from_slice(&tight_gamma.to_le_bytes());
    wide_ref.extend_from_slice(&0.0_f64.to_le_bytes());
    wide_ref.push(0x09);
    wide_ref.push(0x02);
    encode_test_varint(&mut wide_ref, -40_000_000);
    encode_test_varint(&mut wide_ref, 80_000_000);
    assert_eq!(
        DdSketch::deserialize_compat(&wide_ref, StoreKind::UnboundedDense)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
    let sparse_wide = DdSketch::deserialize_compat(&wide_ref, StoreKind::Sparse).unwrap();
    assert_eq!(sparse_wide.total_weight(), 2);
    assert_eq!(sparse_wide.num_retained(), 2);
    assert_eq!(sparse_wide.min_value(), None);
    assert!(sparse_wide.quantile(0.0).unwrap().unwrap() > 0.0);

    // Combined negative + positive span exceeding `2^26` is also rejected for UnboundedDense even
    // when each sign individually stays below `2^26`, while Sparse accepts it.
    let mut combined_wide_ref = vec![0x02];
    combined_wide_ref.extend_from_slice(&tight_gamma.to_le_bytes());
    combined_wide_ref.extend_from_slice(&0.0_f64.to_le_bytes());
    combined_wide_ref.extend_from_slice(&[0x0b, 0x02]);
    encode_test_varint(&mut combined_wide_ref, -20_000_000);
    encode_test_varint(&mut combined_wide_ref, 40_000_000);
    combined_wide_ref.extend_from_slice(&[0x09, 0x02]);
    encode_test_varint(&mut combined_wide_ref, -20_000_000);
    encode_test_varint(&mut combined_wide_ref, 40_000_000);
    assert_eq!(
        DdSketch::deserialize_compat(&combined_wide_ref, StoreKind::UnboundedDense)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
    let sparse_combined =
        DdSketch::deserialize_compat(&combined_wide_ref, StoreKind::Sparse).unwrap();
    assert_eq!(sparse_combined.total_weight(), 4);
    assert_eq!(sparse_combined.num_retained(), 4);

    // Invalid StoreKind passed to deserialize_compat returns InvalidArgument even on empty bytes.
    assert_eq!(
        DdSketch::deserialize_compat(&[], StoreKind::CollapsingLowestDense { max_num_bins: 0 })
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );

    // Store block appearing before a missing or out-of-domain mapping is rejected during the
    // zero-allocation prescan across all target store kinds.
    let mut bins_without_mapping = vec![0x09, 0x02];
    encode_test_varint(&mut bins_without_mapping, -1_000_000);
    encode_test_varint(&mut bins_without_mapping, 2_000_000);
    for store in [StoreKind::UnboundedDense, StoreKind::Sparse] {
        assert_eq!(
            DdSketch::deserialize_compat(&bins_without_mapping, store)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );
    }

    let mut bins_before_incompatible_mapping = bins_without_mapping;
    bins_before_incompatible_mapping.push(0x02);
    bins_before_incompatible_mapping.extend_from_slice(&gamma.to_le_bytes());
    bins_before_incompatible_mapping.extend_from_slice(&0.0_f64.to_le_bytes());
    for store in [StoreKind::UnboundedDense, StoreKind::Sparse] {
        assert_eq!(
            DdSketch::deserialize_compat(&bins_before_incompatible_mapping, store)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );
    }

    // Wide contiguous compat block (`num_bins = 2_000`) decoded into narrow collapsing stores
    // matches incremental updates bit-for-bit, including subsequent updates.
    let mut wide_unbounded = DdSketchBuilder::new(0.005)
        .mapping(MappingKind::Cubic)
        .store(StoreKind::UnboundedDense)
        .build()
        .unwrap();
    for i in 0..2_000 {
        let v = 1.005_f64.powi(i - 1_000);
        wide_unbounded.update_with_weight(-v, (i as u64 % 3) + 1);
        wide_unbounded.update_with_weight(v, (i as u64 % 5) + 1);
    }
    let wide_compat_bytes = wide_unbounded.serialize_compat().unwrap();

    for narrow_store in [
        StoreKind::CollapsingLowestDense { max_num_bins: 16 },
        StoreKind::CollapsingHighestDense { max_num_bins: 16 },
    ] {
        let mut expected = DdSketchBuilder::new(0.005)
            .mapping(MappingKind::Cubic)
            .store(narrow_store)
            .build()
            .unwrap();
        for i in 0..2_000 {
            let v = 1.005_f64.powi(i - 1_000);
            expected.update_with_weight(-v, (i as u64 % 3) + 1);
            expected.update_with_weight(v, (i as u64 % 5) + 1);
        }

        let mut decoded_narrow =
            DdSketch::deserialize_compat(&wide_compat_bytes, narrow_store).unwrap();
        assert_eq!(
            decoded_narrow.iter().collect::<Vec<_>>(),
            expected.iter().collect::<Vec<_>>()
        );

        for follow_up in [-1e-12, -1e12, 1e-12, 1e12] {
            expected.update(follow_up);
            decoded_narrow.update(follow_up);
        }
        assert_eq!(
            decoded_narrow.iter().collect::<Vec<_>>(),
            expected.iter().collect::<Vec<_>>()
        );
    }
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
