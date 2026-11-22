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

use datasketches::density::DensitySketch;
use datasketches::density::DensityValue;
use datasketches::density::GaussianKernel;
use datasketches::density::Kernel;
use datasketches::error::ErrorKind;

use crate::serialization_test_data;

fn rust_snapshot(value_type: &str, n: u64) -> Vec<u8> {
    let filename = format!("density_{value_type}_n{n}_rust.sk");
    fs::read(serialization_test_data("rust_generated_files", &filename)).unwrap()
}

fn assert_invalid_f64(bytes: &[u8]) {
    let err = DensitySketch::<f64>::deserialize(bytes).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);
}

fn assert_invalid_f32(bytes: &[u8]) {
    let err = DensitySketch::<f32>::deserialize(bytes).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);
}

#[derive(Debug, Clone, PartialEq)]
struct CustomScaleKernel {
    scale: f64,
}

impl Kernel<f64> for CustomScaleKernel {
    fn evaluate(&self, a: &[f64], b: &[f64]) -> f64 {
        assert_eq!(a.len(), b.len(), "point dimension mismatch");
        let dist_sq: f64 = a.iter().zip(b).map(|(&x, &y)| (x - y) * (x - y)).sum();
        (-self.scale * dist_sq).exp()
    }
}

#[test]
fn round_trips_for_rust_constructed_sketches() {
    // Empty round-trips for f64 and f32.
    let empty_f64 = DensitySketch::<f64>::new(12, 4).unwrap();
    let bytes = empty_f64.serialize();
    assert_eq!(bytes[2], 23);
    let decoded = DensitySketch::<f64>::deserialize(&bytes).unwrap();
    assert!(decoded.is_empty());
    assert_eq!(decoded.k(), 12);
    assert_eq!(decoded.dim(), 4);
    assert_eq!(decoded.serialize(), bytes);

    let empty_f32 = DensitySketch::<f32>::new(2, 1).unwrap();
    let bytes = empty_f32.serialize();
    let decoded = DensitySketch::<f32>::deserialize(&bytes).unwrap();
    assert!(decoded.is_empty());
    assert_eq!(decoded.k(), 2);
    assert_eq!(decoded.dim(), 1);
    assert_eq!(decoded.serialize(), bytes);

    // Exact and estimation round-trips for f64 and f32.
    let mut est_f64 = DensitySketch::<f64>::new(10, 2).unwrap();
    for i in 0..10 {
        est_f64.update(&[i as f64, -(i as f64)]).unwrap();
    }
    let exact_bytes = est_f64.serialize();
    let exact_decoded = DensitySketch::<f64>::deserialize(&exact_bytes).unwrap();
    assert!(!exact_decoded.is_estimation_mode());
    assert_eq!(exact_decoded.serialize(), exact_bytes);
    assert_eq!(
        exact_decoded.estimate(&[5.0, -5.0]).unwrap(),
        est_f64.estimate(&[5.0, -5.0]).unwrap()
    );

    for i in 10..200 {
        est_f64.update(&[i as f64, -(i as f64)]).unwrap();
    }
    let est_bytes = est_f64.serialize();
    let est_decoded = DensitySketch::<f64>::deserialize(&est_bytes).unwrap();
    assert!(est_decoded.is_estimation_mode());
    assert_eq!(est_decoded.serialize(), est_bytes);
    assert_eq!(
        est_decoded.estimate(&[100.0, -100.0]).unwrap(),
        est_f64.estimate(&[100.0, -100.0]).unwrap()
    );

    let mut est_f32 = DensitySketch::<f32>::new(8, 2).unwrap();
    for i in 0..120 {
        est_f32
            .update(&[i as f32 * 0.25, -(i as f32 * 0.25)])
            .unwrap();
    }
    let bytes_f32 = est_f32.serialize();
    let decoded_f32 = DensitySketch::<f32>::deserialize(&bytes_f32).unwrap();
    assert!(decoded_f32.is_estimation_mode());
    assert_eq!(decoded_f32.serialize(), bytes_f32);
    assert_eq!(
        decoded_f32.estimate(&[10.0, -10.0]).unwrap(),
        est_f32.estimate(&[10.0, -10.0]).unwrap()
    );

    // Reset, round-trip empty, reuse, and round-trip again.
    est_f64.reset();
    let reset_empty_bytes = est_f64.serialize();
    let mut reset_decoded = DensitySketch::<f64>::deserialize(&reset_empty_bytes).unwrap();
    assert!(reset_decoded.is_empty());
    assert_eq!(reset_decoded.k(), 10);
    assert_eq!(reset_decoded.dim(), 2);

    reset_decoded.update(&[3.0, 4.0]).unwrap();
    let reused_bytes = reset_decoded.serialize();
    let reused_decoded = DensitySketch::<f64>::deserialize(&reused_bytes).unwrap();
    assert_eq!(reused_decoded.total_weight(), 1);
    assert_eq!(reused_decoded.num_retained(), 1);
    assert_eq!(reused_decoded.estimate(&[3.0, 4.0]).unwrap(), Some(1.0));
    assert_eq!(reused_decoded.serialize(), reused_bytes);

    // Custom kernel round-trip via deserialize_with_kernel.
    let kernel = CustomScaleKernel { scale: 0.5 };
    let mut custom = DensitySketch::<f64, _>::with_kernel(8, 2, kernel.clone()).unwrap();
    for i in 0..40 {
        custom.update(&[i as f64 * 0.1, -(i as f64 * 0.1)]).unwrap();
    }
    let custom_bytes = custom.serialize();
    let custom_restored =
        DensitySketch::<f64, _>::deserialize_with_kernel(&custom_bytes, kernel.clone()).unwrap();
    assert_eq!(custom_restored.kernel(), &kernel);
    assert_eq!(custom_restored.serialize(), custom_bytes);
    assert_eq!(
        custom_restored.estimate(&[1.0, -1.0]).unwrap(),
        custom.estimate(&[1.0, -1.0]).unwrap()
    );
}

#[test]
fn every_truncated_prefix_is_rejected() {
    for n in [0, 10, 100] {
        let bytes = rust_snapshot("double", n);
        for end in 0..bytes.len() {
            assert_invalid_f64(&bytes[..end]);
        }
        assert!(DensitySketch::<f64>::deserialize(&bytes).is_ok());
    }

    for n in [0, 10, 100] {
        let bytes = rust_snapshot("float", n);
        for end in 0..bytes.len() {
            assert_invalid_f32(&bytes[..end]);
        }
        assert!(DensitySketch::<f32>::deserialize(&bytes).is_ok());
    }
}

#[test]
fn malformed_headers_and_parameters_are_rejected() {
    let empty = rust_snapshot("double", 0);
    let exact = rust_snapshot("double", 10);
    let est = rust_snapshot("double", 100);

    // Wrong family ID (offset 2).
    for base in [&empty, &exact, &est] {
        for bad_family in [0_u8, 18, 19, 20, 255] {
            let mut corrupted = base.clone();
            corrupted[2] = bad_family;
            assert_invalid_f64(&corrupted);
        }
    }

    // Wrong serial version (offset 1).
    for base in [&empty, &exact, &est] {
        for bad_ver in [0_u8, 2, 255] {
            let mut corrupted = base.clone();
            corrupted[1] = bad_ver;
            assert_invalid_f64(&corrupted);
        }
    }

    // Unknown flag bits (offset 3).
    for bad_empty_flags in [0x00_u8, 0x05, 0x06, 0x0c, 0x84, 0xff] {
        let mut corrupted = empty.clone();
        corrupted[3] = bad_empty_flags;
        assert_invalid_f64(&corrupted);
    }
    for base in [&exact, &est] {
        for bad_non_empty_flags in [0x01_u8, 0x02, 0x04, 0x08, 0x80, 0xff] {
            let mut corrupted = base.clone();
            corrupted[3] = bad_non_empty_flags;
            assert_invalid_f64(&corrupted);
        }
    }

    // Preamble ints mismatch (offset 0).
    for bad_empty_pre in [0_u8, 2, 4, 6] {
        let mut corrupted = empty.clone();
        corrupted[0] = bad_empty_pre;
        assert_invalid_f64(&corrupted);
    }
    for base in [&exact, &est] {
        for bad_non_empty_pre in [0_u8, 2, 3, 5, 7] {
            let mut corrupted = base.clone();
            corrupted[0] = bad_non_empty_pre;
            assert_invalid_f64(&corrupted);
        }
    }

    // k < 2 (offsets 4..6).
    for base in [&empty, &exact] {
        for bad_k in [0_u16, 1] {
            let mut corrupted = base.clone();
            corrupted[4..6].copy_from_slice(&bad_k.to_le_bytes());
            assert_invalid_f64(&corrupted);
        }
    }

    // dim == 0 (offsets 8..12) and huge dim (u32::MAX) on non-empty.
    for base in [&empty, &exact] {
        let mut corrupted = base.clone();
        corrupted[8..12].copy_from_slice(&0_u32.to_le_bytes());
        assert_invalid_f64(&corrupted);
    }
    {
        let mut huge_dim = exact.clone();
        huge_dim[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_invalid_f64(&huge_dim);
    }
}

#[test]
fn malformed_counts_levels_payloads_and_trailing_bytes_are_rejected() {
    let empty = rust_snapshot("double", 0);
    let single = rust_snapshot("double", 1);
    let exact = rust_snapshot("double", 10);
    let exact_f32 = rust_snapshot("float", 10);
    let est = rust_snapshot("double", 100);

    // Non-empty with n == 0 (offsets 16..24).
    {
        let mut zero_n = single.clone();
        zero_n[16..24].copy_from_slice(&0_u64.to_le_bytes());
        assert_invalid_f64(&zero_n);
    }

    // Non-empty with num_retained == 0 (offsets 12..16), both with n > 0 and n == 0.
    {
        let mut zero_retained = single.clone();
        zero_retained[12..16].copy_from_slice(&0_u32.to_le_bytes());
        assert_invalid_f64(&zero_retained);

        zero_retained[16..24].copy_from_slice(&0_u64.to_le_bytes());
        assert_invalid_f64(&zero_retained);
    }

    // num_retained > n.
    {
        let mut more_retained_than_n = exact.clone();
        more_retained_than_n[16..24].copy_from_slice(&9_u64.to_le_bytes());
        assert_invalid_f64(&more_retained_than_n);

        let mut huge_retained = exact.clone();
        huge_retained[12..16].copy_from_slice(&u32::MAX.to_le_bytes());
        huge_retained[16..24].copy_from_slice(&u64::MAX.to_le_bytes());
        assert_invalid_f64(&huge_retained);
    }

    // levels > 64: 64 empty levels followed by 1 non-empty level (65 levels total).
    {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[6, 1, 23, 0]);
        bytes.extend_from_slice(&10_u16.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes()); // dim = 1
        bytes.extend_from_slice(&1_u32.to_le_bytes()); // num_retained = 1
        bytes.extend_from_slice(&2_u64.to_le_bytes()); // n = 2
        for _ in 0..64 {
            bytes.extend_from_slice(&0_u32.to_le_bytes());
        }
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&1.0_f64.to_le_bytes());
        assert_invalid_f64(&bytes);
    }

    // level_len > remaining (declared num_retained=2, level_len=3 with 3 points present).
    {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[6, 1, 23, 0]);
        bytes.extend_from_slice(&10_u16.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes()); // dim = 1
        bytes.extend_from_slice(&2_u32.to_le_bytes()); // num_retained = 2
        bytes.extend_from_slice(&4_u64.to_le_bytes()); // n = 4
        bytes.extend_from_slice(&3_u32.to_le_bytes()); // level_len = 3 > remaining(2)
        bytes.extend_from_slice(&1.0_f64.to_le_bytes());
        bytes.extend_from_slice(&2.0_f64.to_le_bytes());
        bytes.extend_from_slice(&3.0_f64.to_le_bytes());
        assert_invalid_f64(&bytes);
    }

    // Truncated/short payload for declared num_retained or level_len (including u32::MAX).
    {
        let mut short_for_retained = single.clone();
        short_for_retained[12..16].copy_from_slice(&100_u32.to_le_bytes());
        short_for_retained[16..24].copy_from_slice(&100_u64.to_le_bytes());
        assert_invalid_f64(&short_for_retained);

        let mut short_for_level_len = single.clone();
        short_for_level_len[24..28].copy_from_slice(&50_u32.to_le_bytes());
        assert_invalid_f64(&short_for_level_len);

        let mut huge_level_len = single.clone();
        huge_level_len[24..28].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_invalid_f64(&huge_level_len);
    }

    // Non-finite coordinates (NaN, +inf, -inf) for f64 and f32.
    for bad_f64 in [
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::from_bits(0x7ff8_0000_0000_0001),
    ] {
        let mut corrupted = single.clone();
        // Second coordinate of the first point in single starts at 28 + 8 = 36.
        corrupted[36..44].copy_from_slice(&bad_f64.to_le_bytes());
        assert_invalid_f64(&corrupted);
    }
    for bad_f32 in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
        let mut corrupted = exact_f32.clone();
        // First coordinate of the first point starts at 28.
        corrupted[28..32].copy_from_slice(&bad_f32.to_le_bytes());
        assert_invalid_f32(&corrupted);
    }

    // Trailing extra bytes (empty and non-empty).
    for base in [&empty, &single, &exact, &est] {
        for trailing_len in 1..=3 {
            let mut unaligned = base.clone();
            unaligned.resize(unaligned.len() + trailing_len, 0x00);
            assert_invalid_f64(&unaligned);
        }

        let mut with_many = base.clone();
        with_many.extend_from_slice(&[0xaa; 8]);
        assert_invalid_f64(&with_many);
    }

    // Non-empty trailing non-zero u32 (level_len = 1 after all retained points are consumed).
    {
        let mut nonzero_trailing_u32 = est.clone();
        nonzero_trailing_u32.extend_from_slice(&1_u32.to_le_bytes());
        assert_invalid_f64(&nonzero_trailing_u32);
    }

    // Trailing empty levels pushing total levels past 64 must be rejected.
    {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[6, 1, 23, 0]);
        bytes.extend_from_slice(&2_u16.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes()); // dim = 1
        bytes.extend_from_slice(&1_u32.to_le_bytes()); // num_retained = 1
        bytes.extend_from_slice(&3_u64.to_le_bytes()); // n = 3
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&50.0_f64.to_le_bytes());
        for _ in 0..64 {
            bytes.extend_from_slice(&0_u32.to_le_bytes());
        }
        assert_invalid_f64(&bytes);
    }
}

#[test]
fn compacted_images_with_and_without_a_trailing_empty_level_round_trip() {
    fn images<T: DensityValue>(value: T) -> [Vec<u8>; 2]
    where
        GaussianKernel: Kernel<T>,
    {
        let mut sketch = DensitySketch::new(2, 1).unwrap();
        sketch.update(&[value]).unwrap();
        let mut single_level = sketch.serialize();
        // Compaction can leave a single retained point from three inputs.
        single_level[16..24].copy_from_slice(&3_u64.to_le_bytes());
        let mut empty_top_level = single_level.clone();
        empty_top_level.extend_from_slice(&0_u32.to_le_bytes());
        [single_level, empty_top_level]
    }

    for bytes in images(80.0_f64) {
        let mut sketch = DensitySketch::<f64>::deserialize(&bytes).unwrap();
        assert!(!sketch.is_empty());
        assert!(sketch.is_estimation_mode());
        assert_eq!(sketch.k(), 2);
        assert_eq!(sketch.dim(), 1);
        assert_eq!(sketch.total_weight(), 3);
        assert_eq!(sketch.num_retained(), 1);
        let retained: Vec<(&[f64], u64)> = sketch.iter().collect();
        assert_eq!(retained, vec![(&[80.0][..], 1)]);
        assert_eq!(sketch.estimate(&[80.0]).unwrap(), Some(1.0 / 3.0));
        assert_eq!(sketch.serialize(), bytes);

        // Further updates and merges preserve estimation mode and invariants.
        sketch.update(&[120.0]).unwrap();
        assert!(sketch.is_estimation_mode());
        assert_eq!(sketch.total_weight(), 4);
        let mut other = DensitySketch::<f64>::new(2, 1).unwrap();
        other.update(&[160.0]).unwrap();
        sketch.merge(&other).unwrap();
        assert!(sketch.is_estimation_mode());
        assert_eq!(sketch.total_weight(), 5);
        assert!(sketch.num_retained() > 0);
    }

    for bytes in images(80.0_f32) {
        let sketch = DensitySketch::<f32>::deserialize(&bytes).unwrap();
        assert!(!sketch.is_empty());
        assert!(sketch.is_estimation_mode());
        assert_eq!(sketch.k(), 2);
        assert_eq!(sketch.dim(), 1);
        assert_eq!(sketch.total_weight(), 3);
        assert_eq!(sketch.num_retained(), 1);
        let retained: Vec<(&[f32], u64)> = sketch.iter().collect();
        assert_eq!(retained, vec![(&[80.0_f32][..], 1)]);
        assert_eq!(sketch.estimate(&[80.0]).unwrap(), Some(1.0 / 3.0));
        assert_eq!(sketch.serialize(), bytes);
    }
}

#[test]
fn rust_double_snapshots_round_trip() {
    for n in [0_u64, 1, 10, 100, 1000, 10_000] {
        let filename = format!("density_double_n{n}_rust.sk");
        let path = serialization_test_data("rust_generated_files", &filename);
        let bytes = fs::read(&path).unwrap();
        let sketch = DensitySketch::<f64>::deserialize(&bytes).unwrap();

        assert_eq!(sketch.k(), 10, "{filename}");
        assert_eq!(sketch.dim(), 3, "{filename}");
        assert_eq!(sketch.total_weight(), n, "{filename}");
        assert_eq!(sketch.is_empty(), n == 0, "{filename}");
        assert_eq!(sketch.is_estimation_mode(), n >= 30, "{filename}");
        let retained_weight: u64 = sketch.iter().map(|(_, weight)| weight).sum();
        if n < 30 {
            assert_eq!(retained_weight, n, "{filename}");
        } else {
            assert!(retained_weight > 0, "{filename}");
        }

        if n == 0 {
            assert_eq!(sketch.num_retained(), 0, "{filename}");
            assert_eq!(sketch.estimate(&[0.0, 0.0, 0.0]).unwrap(), None);
        } else {
            assert!(sketch.num_retained() > 0, "{filename}");
            let (first_pt, _) = sketch.iter().next().unwrap();
            let (last_pt, _) = sketch.iter().last().unwrap();
            for pt in [first_pt, last_pt] {
                let est = sketch.estimate(pt).unwrap().unwrap();
                assert!(
                    est.is_finite() && est > 0.0 && est <= 1.0,
                    "{filename}: invalid density estimate {est} at retained point {pt:?}"
                );
            }
            let last_x = (n - 1) as f64;
            for sample in [[0.0, 0.0, 0.0], [last_x, last_x.sqrt(), -last_x]] {
                let est = sketch.estimate(&sample).unwrap().unwrap();
                assert!(
                    est.is_finite() && (0.0..=1.0).contains(&est),
                    "{filename}: invalid density estimate {est} at {sample:?}"
                );
                if n <= 10 {
                    assert!(est > 0.0, "{filename}: expected positive exact estimate");
                }
            }
        }

        assert_eq!(sketch.serialize(), bytes, "roundtrip: {filename}");
    }
}

#[test]
fn rust_float_snapshots_round_trip() {
    for n in [0_u64, 1, 10, 100, 1000, 10_000] {
        let filename = format!("density_float_n{n}_rust.sk");
        let path = serialization_test_data("rust_generated_files", &filename);
        let bytes = fs::read(&path).unwrap();
        let sketch = DensitySketch::<f32>::deserialize(&bytes).unwrap();

        assert_eq!(sketch.k(), 10, "{filename}");
        assert_eq!(sketch.dim(), 3, "{filename}");
        assert_eq!(sketch.total_weight(), n, "{filename}");
        assert_eq!(sketch.is_empty(), n == 0, "{filename}");
        assert_eq!(sketch.is_estimation_mode(), n >= 30, "{filename}");
        let retained_weight: u64 = sketch.iter().map(|(_, weight)| weight).sum();
        if n < 30 {
            assert_eq!(retained_weight, n, "{filename}");
        } else {
            assert!(retained_weight > 0, "{filename}");
        }

        if n == 0 {
            assert_eq!(sketch.num_retained(), 0, "{filename}");
            assert_eq!(sketch.estimate(&[0.0, 0.0, 0.0]).unwrap(), None);
        } else {
            assert!(sketch.num_retained() > 0, "{filename}");
            let (first_pt, _) = sketch.iter().next().unwrap();
            let (last_pt, _) = sketch.iter().last().unwrap();
            for pt in [first_pt, last_pt] {
                let est = sketch.estimate(pt).unwrap().unwrap();
                assert!(
                    est.is_finite() && est > 0.0 && est <= 1.0,
                    "{filename}: invalid density estimate {est} at retained point {pt:?}"
                );
            }
            let last_x = (n - 1) as f32;
            for sample in [[0.0, 0.0, 0.0], [last_x, last_x.sqrt(), -last_x]] {
                let est = sketch.estimate(&sample).unwrap().unwrap();
                assert!(
                    est.is_finite() && (0.0..=1.0).contains(&est),
                    "{filename}: invalid density estimate {est} at {sample:?}"
                );
                if n <= 10 {
                    assert!(est > 0.0, "{filename}: expected positive exact estimate");
                }
            }
        }

        assert_eq!(sketch.serialize(), bytes, "roundtrip: {filename}");
    }
}
