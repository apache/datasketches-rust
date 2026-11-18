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

use datasketches::bloom::BloomFilter;
use datasketches::bloom::BloomFilterBuilder;
use datasketches::error::ErrorKind;
use datasketches::hash::value::canonical_float;
use datasketches::hash::value::raw_bytes;
use googletest::assert_that;
use googletest::prelude::gt;

use crate::serialization_test_data;

fn filter_snapshot(filter: &BloomFilter) -> (u64, u16, u64, u64, Vec<u8>) {
    (
        filter.capacity(),
        filter.num_hashes(),
        filter.seed(),
        filter.bits_used(),
        filter.serialize(),
    )
}

fn test_bloom_filter_file(path: PathBuf, expected_num_items: u64, expected_num_hashes: u16) {
    let bytes = fs::read(&path).unwrap();
    let filter1 = BloomFilter::deserialize(&bytes).unwrap();

    assert_eq!(
        filter1.num_hashes(),
        expected_num_hashes,
        "Wrong num_hashes in {}",
        path.display()
    );
    assert_eq!(
        filter1.is_empty(),
        expected_num_items == 0,
        "Wrong empty state in {}",
        path.display()
    );

    // TCK fixtures insert integers 0..n/10 and, when non-empty, canonical NaN.
    for item in 0..(expected_num_items / 10).min(100) {
        assert!(
            filter1.contains(item),
            "Missing item {item} in {}",
            path.display()
        );
    }
    if expected_num_items > 0 {
        assert!(
            filter1.contains(canonical_float::from_f64(f64::NAN)),
            "Missing canonical NaN in {}",
            path.display()
        );
    }

    let serialized_bytes = filter1.serialize();
    let filter2 = BloomFilter::deserialize(&serialized_bytes).unwrap_or_else(|err| {
        panic!(
            "Deserialization failed after round-trip for {}: {}",
            path.display(),
            err
        )
    });

    assert_eq!(
        filter_snapshot(&filter1),
        filter_snapshot(&filter2),
        "Filter changed after round-trip for {}",
        path.display()
    );
}

#[test]
fn test_java_compatibility() {
    for (n, num_hashes) in [
        (0, 3),
        (0, 5),
        (10_000, 3),
        (10_000, 5),
        (2_000_000, 3),
        (2_000_000, 5),
        (30_000_000, 3),
        (30_000_000, 5),
    ] {
        let filename = format!("bf_n{n}_h{num_hashes}_java.sk");
        let path = serialization_test_data("java_generated_files", &filename);
        test_bloom_filter_file(path, n, num_hashes);
    }
}

#[test]
fn test_cpp_compatibility() {
    for (n, num_hashes) in [
        (0, 3),
        (0, 5),
        (10_000, 3),
        (10_000, 5),
        (2_000_000, 3),
        (2_000_000, 5),
        (30_000_000, 3),
        (30_000_000, 5),
    ] {
        let filename = format!("bf_n{n}_h{num_hashes}_cpp.sk");
        let path = serialization_test_data("cpp_generated_files", &filename);
        test_bloom_filter_file(path, n, num_hashes);
    }
}

#[test]
fn test_go_compatibility() {
    for (n, num_hashes) in [
        (0, 3),
        (0, 5),
        (10_000, 3),
        (10_000, 5),
        (2_000_000, 3),
        (2_000_000, 5),
        (30_000_000, 3),
        (30_000_000, 5),
    ] {
        let filename = format!("bf_n{n}_h{num_hashes}_go.sk");
        let path = serialization_test_data("go_generated_files", &filename);
        test_bloom_filter_file(path, n, num_hashes);
    }
}

#[test]
fn test_go_typed_compatibility() {
    let load_go_filter = |filename: &str| {
        let path = serialization_test_data("go_generated_files", filename);
        let bytes = fs::read(&path).unwrap();
        let filter = BloomFilter::deserialize(&bytes).unwrap();
        assert_eq!(
            filter.capacity(),
            10_000_u64.next_multiple_of(64),
            "{filename}"
        );
        assert_eq!(filter.num_hashes(), 3, "{filename}");
        assert!(!filter.is_empty(), "{filename}");
        assert_eq!(filter.serialize(), bytes, "{filename}");
        filter
    };

    let str_filter = load_go_filter("bf_string_n10000_h3_go.sk");
    for i in 0..1000 {
        let item = i.to_string();
        assert!(
            str_filter.contains(raw_bytes::from_str(&item)),
            "missing string {item}"
        );
    }

    let double_filter = load_go_filter("bf_double_n10000_h3_go.sk");
    for i in 0..1000 {
        assert!(
            double_filter.contains(canonical_float::from_f64(i as f64)),
            "missing double {i}"
        );
    }

    let long_array_filter = load_go_filter("bf_long_array_n10000_h3_go.sk");
    for i in 0..1000_i64 {
        let mut buf = [0_u8; 16];
        buf[..8].copy_from_slice(&i.to_le_bytes());
        buf[8..].copy_from_slice(&i.to_le_bytes());
        assert!(
            long_array_filter.contains(raw_bytes::from_slice(&buf)),
            "missing long array {i}"
        );
    }

    let double_array_filter = load_go_filter("bf_double_array_n10000_h3_go.sk");
    for i in 0..1000 {
        let mut buf = [0_u8; 16];
        let bits = (i as f64).to_bits().to_le_bytes();
        buf[..8].copy_from_slice(&bits);
        buf[8..].copy_from_slice(&bits);
        assert!(
            double_array_filter.contains(raw_bytes::from_slice(&buf)),
            "missing double array {i}"
        );
    }

    let byte_array_filter = load_go_filter("bf_byte_array_n10000_h3_go.sk");
    for i in 0..1000_u32 {
        let b = (i % 256) as u8;
        assert!(
            byte_array_filter.contains(raw_bytes::from_slice(&[b, b, b, b])),
            "missing byte array {i}"
        );
    }
}

#[test]
fn test_rust_compatibility() {
    for (n, num_hashes) in [
        (0_u64, 3),
        (0, 5),
        (10_000, 3),
        (10_000, 5),
        (2_000_000, 3),
        (2_000_000, 5),
        (30_000_000, 3),
        (30_000_000, 5),
    ] {
        let filename = format!("bf_n{n}_h{num_hashes}_rust.sk");
        let path = serialization_test_data("rust_generated_files", &filename);
        let bytes = fs::read(&path).unwrap();
        let filter = BloomFilter::deserialize(&bytes).unwrap();
        assert_eq!(
            filter.capacity(),
            n.max(1000).next_multiple_of(64),
            "Wrong capacity in {}",
            path.display()
        );
        if n > 0 {
            assert!(
                filter.contains(canonical_float::from_f64(f64::NAN)),
                "Missing canonical NaN in {}",
                path.display()
            );
        }
        assert_eq!(
            filter.serialize(),
            bytes,
            "Round-trip bytes differ for {}",
            path.display()
        );
        test_bloom_filter_file(path, n, num_hashes);
    }
}

#[test]
fn test_cached_num_bits_set_is_validated_or_recomputed() {
    const NUM_BITS_SET_OFFSET: usize = 24;

    let mut filter = BloomFilterBuilder::with_accuracy(100, 0.01)
        .build()
        .unwrap();
    filter.insert("apple");
    filter.insert("banana");
    let actual_bits_set = filter.bits_used();
    assert_that!(actual_bits_set, gt(1));

    for serialized_count in [0, actual_bits_set - 1, actual_bits_set + 1] {
        let mut bytes = filter.serialize();
        bytes[NUM_BITS_SET_OFFSET..NUM_BITS_SET_OFFSET + size_of::<u64>()]
            .copy_from_slice(&serialized_count.to_le_bytes());

        assert_eq!(
            BloomFilter::deserialize(&bytes).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }

    let mut dirty = filter.serialize();
    dirty[NUM_BITS_SET_OFFSET..NUM_BITS_SET_OFFSET + size_of::<u64>()]
        .copy_from_slice(&u64::MAX.to_le_bytes());
    let restored = BloomFilter::deserialize(&dirty).unwrap();
    assert_eq!(restored.bits_used(), actual_bits_set);
    assert!(restored.contains("apple"));
    assert!(restored.contains("banana"));
}

#[test]
fn test_invalid_bit_array_lengths_are_rejected() {
    const NUM_LONGS_OFFSET: usize = 16;

    let empty = BloomFilterBuilder::with_accuracy(100, 0.01)
        .build()
        .unwrap();
    let mut non_empty = empty.clone();
    non_empty.insert("apple");

    for base in [empty.serialize(), non_empty.serialize()] {
        for num_longs in [0_i32, -1, i32::MAX] {
            let mut corrupted = base.clone();
            corrupted[NUM_LONGS_OFFSET..NUM_LONGS_OFFSET + size_of::<i32>()]
                .copy_from_slice(&num_longs.to_le_bytes());

            assert_eq!(
                BloomFilter::deserialize(&corrupted).unwrap_err().kind(),
                ErrorKind::InvalidData
            );
        }
    }

    let mut truncated = non_empty.serialize();
    truncated.pop();
    assert_eq!(
        BloomFilter::deserialize(&truncated).unwrap_err().kind(),
        ErrorKind::InvalidData
    );
}
