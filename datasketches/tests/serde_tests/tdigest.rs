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
use std::mem::size_of_val;
use std::path::PathBuf;

use datasketches::tdigest::TDigestMut;
use googletest::assert_that;
use googletest::prelude::eq;
use googletest::prelude::near;

use crate::serialization_test_data;

fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

fn patterned_digest(k: u16, len: usize, salt: usize) -> TDigestMut {
    let mut tdigest = TDigestMut::new(k);
    for index in 0..len {
        let value = (((index * 37 + salt * 17) % 101) as f64) - 50.0;
        tdigest.update(value);
    }
    tdigest
}

fn native_image(
    k: u16,
    reverse_merge: bool,
    min: f64,
    max: f64,
    centroids: &[(f64, u64)],
    buffered: &[f64],
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(32 + size_of_val(centroids) + size_of_val(buffered));
    bytes.extend_from_slice(&[2, 1, 20]); // preamble longs, serial version, family
    bytes.extend_from_slice(&k.to_le_bytes());
    bytes.extend_from_slice(&[if reverse_merge { 4 } else { 0 }, 0, 0]);
    bytes.extend_from_slice(&(centroids.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(buffered.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&min.to_le_bytes());
    bytes.extend_from_slice(&max.to_le_bytes());
    for &(mean, weight) in centroids {
        bytes.extend_from_slice(&mean.to_le_bytes());
        bytes.extend_from_slice(&weight.to_le_bytes());
    }
    for value in buffered {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn test_sketch_file(path: PathBuf, n: u64, with_buffer: bool, is_f32: bool) {
    let bytes = fs::read(&path).unwrap();
    let td = TDigestMut::deserialize(&bytes, is_f32).unwrap();
    let td = td.freeze();

    let path = path.display();
    if n == 0 {
        assert!(td.is_empty(), "filepath: {path}");
        assert_eq!(td.total_weight(), 0, "filepath: {path}");
    } else {
        assert!(!td.is_empty(), "filepath: {path}");
        assert_eq!(td.total_weight(), n, "filepath: {path}");
        assert_eq!(td.min_value(), Some(1.0), "filepath: {path}");
        assert_eq!(td.max_value(), Some(n as f64), "filepath: {path}");
        assert_eq!(td.rank(0.0), Some(0.0), "filepath: {path}");
        assert_eq!(td.rank((n + 1) as f64), Some(1.0), "filepath: {path}");
        if n == 1 {
            assert_eq!(td.rank(n as f64), Some(0.5), "filepath: {path}");
        } else {
            assert_that!(
                td.rank(n as f64 / 2.).unwrap(),
                near(0.5, 0.05),
                "filepath: {path}",
            );
        }
    }

    if !with_buffer && !is_f32 {
        let mut td = td.unfreeze();
        let roundtrip_bytes = td.serialize();
        assert_eq!(bytes, roundtrip_bytes, "filepath: {path}");
    }
}

#[test]
fn test_deserialize_from_cpp_snapshots() {
    let ns = [0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000];
    for n in ns {
        let filename = format!("tdigest_double_n{}_cpp.sk", n);
        let path = serialization_test_data("cpp_generated_files", &filename);
        test_sketch_file(path, n, false, false);
    }
    for n in ns {
        let filename = format!("tdigest_double_buf_n{}_cpp.sk", n);
        let path = serialization_test_data("cpp_generated_files", &filename);
        test_sketch_file(path, n, true, false);
    }
    for n in ns {
        let filename = format!("tdigest_float_n{}_cpp.sk", n);
        let path = serialization_test_data("cpp_generated_files", &filename);
        test_sketch_file(path, n, false, true);
    }
    for n in ns {
        let filename = format!("tdigest_float_buf_n{}_cpp.sk", n);
        let path = serialization_test_data("cpp_generated_files", &filename);
        test_sketch_file(path, n, true, true);
    }
}

#[test]
fn test_deserialize_from_reference_implementation() {
    for filename in [
        "tdigest_ref_k100_n10000_double.sk",
        "tdigest_ref_k100_n10000_float.sk",
    ] {
        let path = serialization_test_data("reference_files", filename);
        let bytes = fs::read(&path).unwrap();
        let td = TDigestMut::deserialize(&bytes, false).unwrap();
        let td = td.freeze();

        let n = 10000;
        let path = path.display();
        assert_eq!(td.k(), 100, "filepath: {path}");
        assert_eq!(td.total_weight(), n, "filepath: {path}");
        assert_eq!(td.min_value(), Some(0.0), "filepath: {path}");
        assert_eq!(td.max_value(), Some((n - 1) as f64), "filepath: {path}");
        assert_that!(td.rank(0.0).unwrap(), near(0.0, 0.0001), "filepath: {path}");
        assert_that!(
            td.rank(n as f64 / 4.).unwrap(),
            near(0.25, 0.0001),
            "filepath: {path}"
        );
        assert_that!(
            td.rank(n as f64 / 2.).unwrap(),
            near(0.5, 0.0001),
            "filepath: {path}"
        );
        assert_that!(
            td.rank((n * 3) as f64 / 4.).unwrap(),
            near(0.75, 0.0001),
            "filepath: {path}"
        );
        assert_that!(td.rank(n as f64).unwrap(), eq(1.0), "filepath: {path}");
    }
}

#[test]
fn test_deserialize_from_java_snapshots() {
    let ns = [0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000];
    for n in ns {
        let filename = format!("tdigest_double_n{}_java.sk", n);
        let path = serialization_test_data("java_generated_files", &filename);
        test_sketch_file(path, n, false, false);
    }
}

#[test]
fn test_deserialize_from_go_snapshots() {
    let ns = [0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000];
    for n in ns {
        let filename = format!("tdigest_double_n{n}_go.sk");
        let path = serialization_test_data("go_generated_files", &filename);
        test_sketch_file(path, n, false, false);
    }
    for n in ns {
        let filename = format!("tdigest_double_buf_n{n}_go.sk");
        let path = serialization_test_data("go_generated_files", &filename);
        test_sketch_file(path, n, true, false);
    }
}

#[test]
fn test_empty() {
    let mut td = TDigestMut::new(100);
    assert!(td.is_empty());

    let bytes = td.serialize();
    assert_eq!(bytes.len(), 8);
    let td = td.freeze();

    let deserialized_td = TDigestMut::deserialize(&bytes, false).unwrap();
    let deserialized_td = deserialized_td.freeze();
    assert_eq!(td.k(), deserialized_td.k());
    assert_eq!(td.total_weight(), deserialized_td.total_weight());
    assert!(td.is_empty());
    assert!(deserialized_td.is_empty());
}

#[test]
fn test_single_value() {
    let mut td = TDigestMut::default();
    td.update(123.0);

    let bytes = td.serialize();
    assert_eq!(bytes.len(), 16);

    let deserialized_td = TDigestMut::deserialize(&bytes, false).unwrap();
    let deserialized_td = deserialized_td.freeze();
    assert_eq!(deserialized_td.k(), 200);
    assert_eq!(deserialized_td.total_weight(), 1);
    assert!(!deserialized_td.is_empty());
    assert_eq!(deserialized_td.min_value(), Some(123.0));
    assert_eq!(deserialized_td.max_value(), Some(123.0));
}

#[test]
fn test_many_values() {
    let mut td = TDigestMut::new(100);
    for i in 0..1000 {
        td.update(i as f64);
    }

    let bytes = td.serialize();
    assert_eq!(bytes.len(), 1584);
    let td = td.freeze();

    let deserialized_td = TDigestMut::deserialize(&bytes, false).unwrap();
    let deserialized_td = deserialized_td.freeze();
    assert_eq!(td.k(), deserialized_td.k());
    assert_eq!(td.total_weight(), deserialized_td.total_weight());
    assert_eq!(td.is_empty(), deserialized_td.is_empty());
    assert_eq!(td.min_value(), deserialized_td.min_value());
    assert_eq!(td.max_value(), deserialized_td.max_value());
    assert_eq!(td.rank(500.0), deserialized_td.rank(500.0));
    assert_eq!(td.quantile(0.5), deserialized_td.quantile(0.5));
}

#[test]
fn test_serialized_bytes_stable_for_full_and_merged_digests() {
    let mut full_buffer = patterned_digest(200, 1_641, 0);
    let bytes = full_buffer.serialize();
    assert_eq!(bytes.len(), 2_864);
    assert_eq!(fnv1a(&bytes), 0x5c01_c50d_d1c8_fdbb);

    let mut left = patterned_digest(10, 201, 2);
    let mut right = patterned_digest(10, 199, 3);
    right.rank(0.0);
    left.merge(&right);
    let bytes = left.serialize();
    assert_eq!(bytes.len(), 272);
    assert_eq!(fnv1a(&bytes), 0x7d2e_a927_9b9e_f559);

    for &(left_len, right_len, expected_len, expected_hash) in &[
        (8, 201, 272, 0x8522_1f3f_152f_24e5),
        (201, 8, 256, 0xe60d_1f6f_f4b0_73e0),
    ] {
        let mut left = patterned_digest(10, left_len, 2);
        let right = patterned_digest(10, right_len, 3);
        left.merge(&right);
        let bytes = left.serialize();
        assert_eq!(bytes.len(), expected_len);
        assert_eq!(fnv1a(&bytes), expected_hash);
    }
}

#[test]
fn test_update_preserves_overfull_deserialized_buffer_lifecycle() {
    // k=10 normally merges at 200 unmerged values. Older implementations nevertheless accept an
    // image with 201 buffered values, so the next update must retain the established lifecycle:
    // append once, then perform exactly one merge when serialization requests a compact image.
    let values = (0..201).map(f64::from).collect::<Vec<_>>();
    let bytes = native_image(10, false, 0.0, 200.0, &[], &values);
    let mut tdigest = TDigestMut::deserialize(&bytes, false).unwrap();
    tdigest.update(201.0);

    let serialized = tdigest.serialize();
    assert_eq!(tdigest.total_weight(), 202);
    assert_eq!(serialized.len(), 240);
    assert_eq!(serialized[5] & 4, 4);
    assert_eq!(fnv1a(&serialized), 0x7c04_c480_a60f_ff29);
}

#[test]
fn test_deserialize_rejects_truncated_large_payload_before_allocation() {
    let mut bytes = native_image(10, false, 0.0, 1.0, &[], &[]);
    bytes[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    bytes[12..16].copy_from_slice(&u32::MAX.to_le_bytes());

    assert!(TDigestMut::deserialize(&bytes, false).is_err());
}

#[test]
fn test_mixed_state_merge_preserves_stable_order() {
    for &(left_reverse, right_reverse, expected_flags, expected_hash) in &[
        (false, true, 4, 0x3865_ad26_04fc_aa2d),
        (true, false, 0, 0x52ab_9379_2c44_0a6e),
    ] {
        let left_image = native_image(
            10,
            left_reverse,
            -1.0,
            2.0,
            &[(0.0, 2), (1.0, 3), (2.0, 2)],
            &[1.0, 1.0, -1.0],
        );
        let right_image = native_image(
            10,
            right_reverse,
            0.0,
            4.0,
            &[(0.0, 5), (1.0, 2), (3.0, 4)],
            &[1.0, 0.0, 4.0],
        );
        let mut left = TDigestMut::deserialize(&left_image, false).unwrap();
        let right = TDigestMut::deserialize(&right_image, false).unwrap();
        let mut right_before = right.clone();
        let right_before = right_before.serialize();

        left.merge(&right);
        let serialized = left.serialize();
        let mut right_after = right.clone();

        assert_eq!(left.total_weight(), 24);
        assert_eq!(serialized.len(), 160);
        assert_eq!(serialized[5], expected_flags);
        assert_eq!(fnv1a(&serialized), expected_hash);
        assert_eq!(right_after.serialize(), right_before);
    }
}

#[test]
fn test_large_weights_produce_finite_extreme_quantile() {
    let lower = f64::from_bits(f64::MAX.to_bits() - 1);
    let mut tdigest = TDigestMut::default();
    tdigest.update(lower);
    tdigest.update(f64::MAX);
    let mut bytes = tdigest.serialize();

    // These valid weights retain a positive lower contribution even though its normalized ratio
    // rounds away when interpolating between the two extreme values.
    bytes[40..48].copy_from_slice(&((1_u64 << 52) - 1).to_le_bytes());
    bytes[56..64].copy_from_slice(&(1_u64 << 52).to_le_bytes());

    let mut tdigest = TDigestMut::deserialize(&bytes, false).unwrap();
    let quantile = tdigest.quantile(0.25).unwrap();
    assert!(
        quantile.is_finite() && (lower..=f64::MAX).contains(&quantile),
        "quantile must remain within its finite interpolation bounds, got {quantile}"
    );
}
