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
                assert_eq!(
                    decoded.iter().collect::<Vec<_>>(),
                    sketch.iter().collect::<Vec<_>>()
                );
                assert_eq!(
                    decoded.quantiles(&[0.0, 0.25, 0.5, 0.75, 1.0]).unwrap(),
                    sketch.quantiles(&[0.0, 0.25, 0.5, 0.75, 1.0]).unwrap()
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
    assert!(sparse_ok.min_value().unwrap() > 0.0);

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
