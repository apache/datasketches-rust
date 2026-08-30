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
use std::path::PathBuf;

use datasketches::tdigest::TDigest;
use datasketches::tdigest::TDigestMut;
use googletest::assert_that;
use googletest::prelude::all;
use googletest::prelude::eq;
use googletest::prelude::ge;
use googletest::prelude::is_finite;
use googletest::prelude::le;
use googletest::prelude::near;

use crate::serialization_test_data;

fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

fn patterned_digest(k: u16, len: usize, salt: usize) -> TDigestMut {
    let mut tdigest = TDigestMut::new(k).unwrap();
    for index in 0..len {
        let value = (((index * 37 + salt * 17) % 101) as f64) - 50.0;
        tdigest.update(value);
    }
    tdigest
}

fn test_sketch_file(path: PathBuf, n: u64, with_buffer: bool, is_f32: bool) {
    let bytes = fs::read(&path).unwrap();
    let td = if is_f32 {
        TDigestMut::deserialize_f32(&bytes)
    } else {
        TDigestMut::deserialize(&bytes)
    }
    .unwrap();
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
        let td = TDigestMut::deserialize(&bytes).unwrap();
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
    let mut td = TDigestMut::new(100).unwrap();
    assert!(td.is_empty());

    let bytes = td.serialize();
    assert_eq!(bytes.len(), 8);
    let td = td.freeze();

    let deserialized_td = TDigestMut::deserialize(&bytes).unwrap();
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

    let deserialized_td = TDigestMut::deserialize(&bytes).unwrap();
    let deserialized_td = deserialized_td.freeze();
    assert_eq!(deserialized_td.k(), 200);
    assert_eq!(deserialized_td.total_weight(), 1);
    assert!(!deserialized_td.is_empty());
    assert_eq!(deserialized_td.min_value(), Some(123.0));
    assert_eq!(deserialized_td.max_value(), Some(123.0));
}

#[test]
fn test_many_values() {
    let mut td = TDigestMut::new(100).unwrap();
    for i in 0..1000 {
        td.update(i as f64);
    }

    let bytes = td.serialize();
    assert_eq!(bytes.len(), 1584);
    let td = td.freeze();

    let deserialized_td = TDigestMut::deserialize(&bytes).unwrap();
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
fn test_frozen_roundtrip() {
    let tdigest = patterned_digest(100, 1000, 7);
    let expected = tdigest.freeze();

    let bytes = expected.serialize();
    let actual = TDigest::deserialize(&bytes).unwrap();

    assert_eq!(actual.k(), expected.k());
    assert_eq!(actual.total_weight(), expected.total_weight());
    assert_eq!(actual.min_value(), expected.min_value());
    assert_eq!(actual.max_value(), expected.max_value());
    assert_eq!(actual.quantile(0.5), expected.quantile(0.5));
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
        (201, 401, 288, 0x4cb8_4037_5e68_ca4b),
        (401, 201, 288, 0x6f6d_e965_77a7_a53f),
    ] {
        let mut left = patterned_digest(10, left_len, 2);
        let right = patterned_digest(10, right_len, 3);
        left.merge(&right);
        let bytes = left.serialize();
        assert_eq!(bytes.len(), expected_len);
        assert_eq!(fnv1a(&bytes), expected_hash);
    }

    let mut left = patterned_digest(10, 199, 2);
    let left = left.serialize();
    let mut left = TDigestMut::deserialize(&left).unwrap();
    let mut right = patterned_digest(10, 199, 3);
    let right = right.serialize();
    let right = TDigestMut::deserialize(&right).unwrap();
    left.merge(&right);
    let bytes = left.serialize();
    assert_eq!(bytes.len(), 272);
    assert_eq!(fnv1a(&bytes), 0x5759_0428_c175_88ab);
}

#[test]
fn test_updates_normalize_overfull_deserialized_buffer_without_centroids() {
    let path = serialization_test_data("cpp_generated_files", "tdigest_double_buf_n10_cpp.sk");
    let mut bytes = fs::read(path).unwrap();
    assert_eq!(&bytes[8..12], &0_u32.to_le_bytes()); // num centroids
    assert_eq!(&bytes[12..16], &10_u32.to_le_bytes()); // num buffered

    // k=100 normally compresses at 840 buffered values. Extend a real C++ image without centroids
    // just past that producer threshold while keeping every value within the recorded min/max.
    bytes[12..16].copy_from_slice(&841_u32.to_le_bytes());
    for _ in 0..831 {
        bytes.extend_from_slice(&10_f64.to_le_bytes());
    }

    let mut tdigest = TDigestMut::deserialize(&bytes).unwrap();
    for _ in 0..10_000 {
        tdigest.update(10.0);
    }

    assert_eq!(tdigest.total_weight(), 10_841);
    assert_eq!(tdigest.min_value(), Some(1.0));
    assert_eq!(tdigest.max_value(), Some(10.0));
    // The overfull image must not disable future compression and let the buffered tail grow with
    // every subsequent value.
    assert!(tdigest.estimated_size() < 32_768);
    let serialized = tdigest.serialize();
    assert_eq!(&serialized[12..16], &0_u32.to_le_bytes());

    let roundtrip = TDigestMut::deserialize(&serialized).unwrap();
    assert_eq!(roundtrip.total_weight(), 10_841);
    assert_eq!(roundtrip.min_value(), Some(1.0));
    assert_eq!(roundtrip.max_value(), Some(10.0));
}

#[test]
fn test_updates_normalize_overfull_deserialized_mixed_buffer() {
    let path = serialization_test_data("cpp_generated_files", "tdigest_double_buf_n1000_cpp.sk");
    let mut bytes = fs::read(path).unwrap();
    assert_eq!(&bytes[8..12], &89_u32.to_le_bytes()); // num centroids
    assert_eq!(&bytes[12..16], &160_u32.to_le_bytes()); // num buffered

    // Extend the buffered tail of a real mixed C++ image just past the k=100 producer threshold.
    bytes[12..16].copy_from_slice(&841_u32.to_le_bytes());
    for _ in 0..681 {
        bytes.extend_from_slice(&1_000_f64.to_le_bytes());
    }

    let mut tdigest = TDigestMut::deserialize(&bytes).unwrap();
    for _ in 0..10_000 {
        tdigest.update(1_000.0);
    }

    assert_eq!(tdigest.total_weight(), 11_681);
    assert_eq!(tdigest.min_value(), Some(1.0));
    assert_eq!(tdigest.max_value(), Some(1_000.0));
    // The overfull image must not disable future compression and let the centroid tail grow with
    // every subsequent value.
    assert!(tdigest.estimated_size() < 32_768);
    let serialized = tdigest.serialize();
    assert_eq!(&serialized[12..16], &0_u32.to_le_bytes());

    let roundtrip = TDigestMut::deserialize(&serialized).unwrap();
    assert_eq!(roundtrip.total_weight(), 11_681);
    assert_eq!(roundtrip.min_value(), Some(1.0));
    assert_eq!(roundtrip.max_value(), Some(1_000.0));
}

#[test]
fn test_deserialize_rejects_truncated_large_payload_before_allocation() {
    let mut tdigest = TDigestMut::new(10).unwrap();
    tdigest.update(0.0);
    tdigest.update(1.0);
    let mut bytes = tdigest.serialize();
    bytes[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    bytes[12..16].copy_from_slice(&u32::MAX.to_le_bytes());

    assert!(TDigestMut::deserialize(&bytes).is_err());
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

    let mut tdigest = TDigestMut::deserialize(&bytes).unwrap();
    let quantile = tdigest.quantile(0.25).unwrap();
    assert_that!(quantile, all!(is_finite(), ge(lower), le(f64::MAX)));
}
