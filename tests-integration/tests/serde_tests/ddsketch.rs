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
        assert_eq!(sketch.total_weight(), n, "{filename}");
        assert_eq!(sketch.serialize(), bytes, "{filename}");
        if n > 0 {
            let expected_zeros = (n - 1) / 5 + 1;
            assert_eq!(sketch.zero_weight(), expected_zeros, "{filename}");
        }
    }
}

#[test]
fn round_trips_across_mappings_stores_and_bin_encodings() {
    let mappings = [
        MappingKind::Cubic,
        MappingKind::Linear,
        MappingKind::Logarithmic,
    ];
    let stores = [
        StoreKind::CollapsingLowestDense { max_num_bins: 2048 },
        StoreKind::CollapsingHighestDense { max_num_bins: 2048 },
        StoreKind::UnboundedDense,
        StoreKind::Sparse,
    ];

    for mapping in mappings {
        for src_store in stores {
            // Exercise empty, unit-weight sparse deltas, dense contiguous counts, and gapped
            // multi-weight counts.
            let mut empty = DdSketchBuilder::new(0.01)
                .mapping(mapping)
                .store(src_store)
                .build()
                .unwrap();
            assert_eq!(empty.serialize().len(), 17);

            let mut unit_sparse = empty.clone();
            for v in [-1e6, -1.0, 0.0, 1.0, 1e6] {
                unit_sparse.update(v);
            }

            let mut dense_contiguous = empty.clone();
            for i in 0..40 {
                let v = 10.0 * 1.02_f64.powi(i);
                dense_contiguous.update_with_weight(v, (i as u64 % 5) + 1);
            }

            let mut gapped_weighted = empty.clone();
            gapped_weighted.update_with_weight(-500.0, 3);
            gapped_weighted.update_with_weight(0.0, 9);
            gapped_weighted.update_with_weight(0.05, 4);
            gapped_weighted.update_with_weight(50_000.0, 12);

            for sketch in [&empty, &unit_sparse, &dense_contiguous, &gapped_weighted] {
                let bytes = sketch.serialize();
                for dst_store in stores {
                    let decoded = DdSketch::deserialize_with_store(&bytes, dst_store).unwrap();
                    assert_eq!(decoded.mapping_kind(), mapping);
                    assert_eq!(decoded.store_kind(), dst_store);
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

            empty.update(1.0);
        }
    }
}

#[test]
fn accepts_datadog_reordered_blocks_and_exact_summary_statistic_blocks() {
    // Construct a canonical Datadog wire image with:
    // 1. FlagCount (0xa0) + varfloat64(6.0 = [0x85, 0x40])
    // 2. FlagSum   (0x84) + f64 LE
    // 3. FlagMin   (0x88) + f64 LE
    // 4. FlagMax   (0x8c) + f64 LE
    // 5. ZeroCount (0x04) + varfloat64(2.0 = [0x03])
    // 6. NegativeStore IndexDeltas (0x0b = (2 << 2) | 0b11): 1 bin at index 10 (varint 0x14)
    // 7. Logarithmic mapping (0x02): gamma = 1.0202020202020203, offset = 0.0
    // 8. PositiveStore ContiguousCounts (0x0d = (3 << 2) | 0b01): 2 bins from index 5, step 1,
    //    counts [1.0 (0x02), 2.0 (0x03)]
    let gamma = (1.0 + 0.01_f64) / (1.0 - 0.01_f64);
    let mut wire = Vec::new();
    wire.push(0xa0);
    wire.extend_from_slice(&[0x85, 0x40]); // count = 6
    wire.push(0x84);
    wire.extend_from_slice(&12.5_f64.to_le_bytes());
    wire.push(0x88);
    wire.extend_from_slice(&(-1.5_f64).to_le_bytes());
    wire.push(0x8c);
    wire.extend_from_slice(&20.0_f64.to_le_bytes());
    wire.push(0x04);
    wire.push(0x03); // zero_count = 2
    wire.push(0x0b);
    wire.push(0x01); // 1 negative bin
    wire.push(0x14); // zig-zag(+10) = 20 = 0x14
    wire.push(0x02);
    wire.extend_from_slice(&gamma.to_le_bytes());
    wire.extend_from_slice(&0.0_f64.to_le_bytes());
    wire.push(0x0d);
    wire.push(0x02); // 2 contiguous positive bins
    wire.push(0x0a); // first_index = +5 -> zig-zag 10
    wire.push(0x02); // index_delta = +1 -> zig-zag 2
    wire.push(0x02); // count[0] = 1
    wire.push(0x03); // count[1] = 2

    let decoded = DdSketch::deserialize(&wire).unwrap();
    assert_eq!(decoded.mapping_kind(), MappingKind::Logarithmic);
    assert_eq!(decoded.total_weight(), 6);
    assert_eq!(decoded.zero_weight(), 2);
    assert_eq!(decoded.num_retained(), 4);
}

#[test]
fn truncated_mid_block_prefixes_are_rejected() {
    // Empty sketch is a single 17-byte IndexMapping block: every strict prefix 0..17 is incomplete.
    let empty_bytes = DdSketch::default().serialize();
    assert_eq!(empty_bytes.len(), 17);
    for end in 0..empty_bytes.len() {
        let err = DdSketch::deserialize(&empty_bytes[..end]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData, "empty prefix len {end}");
    }

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

    // Non-empty images consist of self-delimiting blocks starting with the 17-byte IndexMapping.
    // Cutting before byte 17 or inside the final block must fail; cutting on an earlier block
    // boundary decodes a strict subset of the recorded weight.
    for sketch in [unit_deltas, contiguous, gapped] {
        let bytes = sketch.serialize();
        for end in 0..17 {
            assert_eq!(
                DdSketch::deserialize(&bytes[..end]).unwrap_err().kind(),
                ErrorKind::InvalidData,
                "mapping prefix len {end}"
            );
        }
        // Final byte removed always truncates the last block mid-field.
        assert_eq!(
            DdSketch::deserialize(&bytes[..(bytes.len() - 1)])
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );
        for end in 17..bytes.len() {
            match DdSketch::deserialize(&bytes[..end]) {
                Ok(partial) => assert!(partial.total_weight() < sketch.total_weight()),
                Err(err) => assert_eq!(err.kind(), ErrorKind::InvalidData),
            }
        }
    }
}

#[test]
fn malformed_and_adversarial_images_are_rejected() {
    let base_mapping = {
        let s = DdSketchBuilder::new(0.01)
            .mapping(MappingKind::Logarithmic)
            .build()
            .unwrap();
        s.serialize()
    };

    // Missing index mapping (only zero_count = 1).
    assert_eq!(
        DdSketch::deserialize(&[0x04, 0x02]).unwrap_err().kind(),
        ErrorKind::InvalidData
    );

    // Conflicting duplicate index mappings.
    let mut conflicting = base_mapping.clone();
    let other_mapping = DdSketchBuilder::new(0.05)
        .mapping(MappingKind::Logarithmic)
        .build()
        .unwrap()
        .serialize();
    conflicting.extend_from_slice(&other_mapping);
    assert_eq!(
        DdSketch::deserialize(&conflicting).unwrap_err().kind(),
        ErrorKind::InvalidData
    );

    // Invalid gamma values (<= 1.0, NaN, infinity, and i32-overflowing near-1.0 gamma).
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

    // Unknown flags across feature, mapping, and store namespaces.
    for unknown_flag in [0x00_u8, 0x12, 0x01, 0x11] {
        let mut img = base_mapping.clone();
        img.push(unknown_flag);
        img.extend_from_slice(&[0; 16]);
        assert_eq!(
            DdSketch::deserialize(&img).unwrap_err().kind(),
            ErrorKind::InvalidData,
            "flag={unknown_flag:#04x}"
        );
    }

    // Negative (-1.0) and fractional (-0.5) varfloat64 counts.
    for bad_varfloat in [
        &[0x82_u8, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x30][..],
        &[0xfe_u8, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x3f][..],
    ] {
        let mut img = base_mapping.clone();
        img.push(0x04); // ZeroCount
        img.extend_from_slice(bad_varfloat);
        assert_eq!(
            DdSketch::deserialize(&img).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }

    // Index overflowing i32::MAX in IndexDeltas (zig-zag(i64::from(i32::MAX) + 1) = 0x1_0000_0000).
    let mut i32_overflow = base_mapping.clone();
    i32_overflow.extend_from_slice(&[0x09, 0x01, 0x80, 0x80, 0x80, 0x80, 0x10]);
    assert_eq!(
        DdSketch::deserialize(&i32_overflow).unwrap_err().kind(),
        ErrorKind::InvalidData
    );

    // Wide index span (80_000_001 > MAX_DENSE_BINS = 67_108_864) with 2 sparse unit bins:
    // rejected for UnboundedDense before allocating a multi-hundred-MB slice, while accepted by
    // Sparse and CollapsingLowestDense.
    let mut wide_span = base_mapping.clone();
    // PositiveStore IndexDeltas (0x09), 2 bins: first = -40_000_000, delta = +80_000_000.
    wide_span.push(0x09);
    wide_span.push(0x02);
    encode_test_varint(&mut wide_span, -40_000_000);
    encode_test_varint(&mut wide_span, 80_000_000);

    assert_eq!(
        DdSketch::deserialize_with_store(&wide_span, StoreKind::UnboundedDense)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
    let sparse_ok = DdSketch::deserialize_with_store(&wide_span, StoreKind::Sparse).unwrap();
    assert_eq!(sparse_ok.total_weight(), 2);
    assert_eq!(sparse_ok.num_retained(), 2);
    let collapsing_ok = DdSketch::deserialize(&wide_span).unwrap();
    assert_eq!(collapsing_ok.total_weight(), 2);

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
