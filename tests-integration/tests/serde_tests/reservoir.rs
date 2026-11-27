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

use std::collections::HashSet;
use std::fmt::Debug;
use std::fs;
use std::mem::size_of;

use datasketches::codec::SketchBytes;
use datasketches::codec::SketchSlice;
use datasketches::common::ResizeFactor;
use datasketches::error::Error;
use datasketches::error::ErrorKind;
use datasketches::reservoir::ReservoirSketch;
use datasketches::reservoir::ReservoirUnion;
use datasketches::reservoir::ReservoirValue;
use googletest::assert_that;
use googletest::prelude::contains_substring;

use crate::serialization_test_data;

const EXACT_N: [usize; 5] = [1, 10, 32, 100, 128];
const SAMPLING_K: [usize; 3] = [32, 64, 128];
const MAX_ITEMS_SEEN: u64 = 0xFFFF_FFFF_FFFF;

#[derive(Debug, PartialEq, Eq)]
struct NonCloneItem(i64);

impl ReservoirValue for NonCloneItem {
    const MIN_SERIALIZED_SIZE: usize = size_of::<i64>();

    fn serialized_size(_value: &Self) -> usize {
        size_of::<i64>()
    }

    fn serialize(value: &Self, bytes: &mut SketchBytes) {
        bytes.write_i64_le(value.0);
    }

    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error> {
        input
            .read_i64_le()
            .map(Self)
            .map_err(|_| Error::new(ErrorKind::InvalidData, "failed to read NonCloneItem"))
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ZeroSizedItem;

impl ReservoirValue for ZeroSizedItem {
    const MIN_SERIALIZED_SIZE: usize = 0;

    fn serialized_size(_value: &Self) -> usize {
        0
    }

    fn serialize(_value: &Self, _bytes: &mut SketchBytes) {}

    fn deserialize(_input: &mut SketchSlice<'_>) -> Result<Self, Error> {
        Ok(Self)
    }
}

fn check_sketch_suite<T, F>(
    dir: &str,
    prefix: &str,
    lang: &str,
    make_item: F,
    expect_even_sampling_fixture: bool,
) where
    T: ReservoirValue + PartialEq + Debug,
    F: Fn(usize) -> T,
{
    let empty_name = format!("{prefix}_empty_k128_{lang}.sk");
    let empty_bytes = fs::read(serialization_test_data(dir, &empty_name)).unwrap();
    let empty = ReservoirSketch::<T>::deserialize(&empty_bytes).unwrap();
    assert!(empty.is_empty());
    assert!(!empty.is_estimation_mode());
    assert_eq!(empty.k(), 128);
    assert_eq!(empty.total_weight(), 0);
    assert_eq!(empty.num_retained(), 0);
    assert_eq!(empty.serialize(), empty_bytes);

    for n in EXACT_N {
        let name = format!("{prefix}_exact_n{n}_k128_{lang}.sk");
        let bytes = fs::read(serialization_test_data(dir, &name)).unwrap();
        let sketch = ReservoirSketch::<T>::deserialize(&bytes).unwrap();
        assert!(!sketch.is_empty());
        assert!(!sketch.is_estimation_mode());
        assert_eq!(sketch.k(), 128);
        assert_eq!(sketch.total_weight(), n as u64);
        assert_eq!(sketch.num_retained(), n);
        let expected: Vec<T> = (0..n).map(&make_item).collect();
        assert_eq!(sketch.samples(), expected.as_slice(), "{name}");
        assert_eq!(sketch.serialize(), bytes, "{name}");
    }

    for k in SAMPLING_K {
        let name = format!("{prefix}_sampling_n1000_k{k}_{lang}.sk");
        let bytes = fs::read(serialization_test_data(dir, &name)).unwrap();
        let sketch = ReservoirSketch::<T>::deserialize(&bytes).unwrap();
        assert!(!sketch.is_empty());
        assert!(sketch.is_estimation_mode());
        assert_eq!(sketch.k(), k);
        assert_eq!(sketch.total_weight(), 1_000);
        assert_eq!(sketch.num_retained(), k);
        if expect_even_sampling_fixture {
            let expected: Vec<T> = (0..k).map(|i| make_item(i * 2)).collect();
            assert_eq!(sketch.samples(), expected.as_slice(), "{name}");
        }
        assert_eq!(sketch.serialize(), bytes, "{name}");
    }
}

fn check_union_suite<T, F>(
    dir: &str,
    prefix: &str,
    lang: &str,
    make_item: F,
    expect_even_sampling_fixture: bool,
) where
    T: ReservoirValue + Clone + PartialEq + Debug,
    F: Fn(usize) -> T,
{
    let empty_name = format!("{prefix}_empty_maxk128_{lang}.sk");
    let empty_bytes = fs::read(serialization_test_data(dir, &empty_name)).unwrap();
    let empty = ReservoirUnion::<T>::deserialize(&empty_bytes).unwrap();
    assert!(empty.is_empty());
    assert_eq!(empty.max_k(), 128);
    assert!(empty.to_sketch().is_empty());
    assert_eq!(empty.serialize(), empty_bytes);

    for n in EXACT_N {
        let name = format!("{prefix}_exact_n{n}_maxk128_{lang}.sk");
        let bytes = fs::read(serialization_test_data(dir, &name)).unwrap();
        let union = ReservoirUnion::<T>::deserialize(&bytes).unwrap();
        assert!(!union.is_empty());
        assert_eq!(union.max_k(), 128);
        let result = union.to_sketch();
        assert_eq!(result.k(), 128);
        assert_eq!(result.total_weight(), n as u64);
        assert_eq!(result.num_retained(), n);
        let expected: Vec<T> = (0..n).map(&make_item).collect();
        assert_eq!(result.samples(), expected.as_slice(), "{name}");
        assert_eq!(union.serialize(), bytes, "{name}");
    }

    for max_k in SAMPLING_K {
        let name = format!("{prefix}_sampling_n1000_maxk{max_k}_{lang}.sk");
        let bytes = fs::read(serialization_test_data(dir, &name)).unwrap();
        let union = ReservoirUnion::<T>::deserialize(&bytes).unwrap();
        assert!(!union.is_empty());
        assert_eq!(union.max_k(), max_k);
        let result = union.to_sketch();
        assert_eq!(result.k(), max_k);
        assert_eq!(result.total_weight(), 1_000);
        assert_eq!(result.num_retained(), max_k);
        if expect_even_sampling_fixture {
            let expected: Vec<T> = (0..max_k).map(|i| make_item(i * 2)).collect();
            assert_eq!(result.samples(), expected.as_slice(), "{name}");
        }
        assert_eq!(union.serialize(), bytes, "{name}");
    }
}

#[test]
fn test_java_reservoir_compatibility() {
    check_sketch_suite(
        "java_generated_files",
        "reservoir_longs",
        "java",
        |i| i as i64,
        true,
    );
    check_sketch_suite(
        "java_generated_files",
        "reservoir_items_long",
        "java",
        |i| i as i64,
        true,
    );
    check_sketch_suite(
        "java_generated_files",
        "reservoir_items_double",
        "java",
        |i| i as f64,
        true,
    );
    check_sketch_suite(
        "java_generated_files",
        "reservoir_items_string",
        "java",
        |i| format!("item{i}"),
        true,
    );

    check_union_suite(
        "java_generated_files",
        "reservoir_longs_union",
        "java",
        |i| i as i64,
        true,
    );
    check_union_suite(
        "java_generated_files",
        "reservoir_items_union_long",
        "java",
        |i| i as i64,
        true,
    );
    check_union_suite(
        "java_generated_files",
        "reservoir_items_union_double",
        "java",
        |i| i as f64,
        true,
    );
    check_union_suite(
        "java_generated_files",
        "reservoir_items_union_string",
        "java",
        |i| format!("item{i}"),
        true,
    );
}

#[test]
fn test_go_reservoir_compatibility() {
    check_sketch_suite(
        "go_generated_files",
        "reservoir_items_long",
        "go",
        |i| i as i64,
        false,
    );
    check_sketch_suite(
        "go_generated_files",
        "reservoir_items_double",
        "go",
        |i| i as f64,
        false,
    );
    check_sketch_suite(
        "go_generated_files",
        "reservoir_items_string",
        "go",
        |i| format!("item{i}"),
        false,
    );

    check_union_suite(
        "go_generated_files",
        "reservoir_items_union_long",
        "go",
        |i| i as i64,
        false,
    );
    check_union_suite(
        "go_generated_files",
        "reservoir_items_union_double",
        "go",
        |i| i as f64,
        false,
    );
    check_union_suite(
        "go_generated_files",
        "reservoir_items_union_string",
        "go",
        |i| format!("item{i}"),
        false,
    );

    // Also verify Go sampling snapshots contain distinct values within the original 0..1000 stream.
    let bytes = fs::read(serialization_test_data(
        "go_generated_files",
        "reservoir_items_long_sampling_n1000_k64_go.sk",
    ))
    .unwrap();
    let sketch = ReservoirSketch::<i64>::deserialize(&bytes).unwrap();
    let unique: HashSet<i64> = sketch.iter().copied().collect();
    assert_eq!(unique.len(), 64);
    assert!(unique.iter().all(|&v| (0..1_000).contains(&v)));
}

#[test]
fn test_rust_reservoir_snapshots() {
    check_sketch_suite(
        "rust_generated_files",
        "reservoir_items_long",
        "rust",
        |i| i as i64,
        false,
    );
    check_sketch_suite(
        "rust_generated_files",
        "reservoir_items_double",
        "rust",
        |i| i as f64,
        false,
    );
    check_sketch_suite(
        "rust_generated_files",
        "reservoir_items_string",
        "rust",
        |i| format!("item{i}"),
        false,
    );

    check_union_suite(
        "rust_generated_files",
        "reservoir_items_union_long",
        "rust",
        |i| i as i64,
        false,
    );
    check_union_suite(
        "rust_generated_files",
        "reservoir_items_union_double",
        "rust",
        |i| i as f64,
        false,
    );
    check_union_suite(
        "rust_generated_files",
        "reservoir_items_union_string",
        "rust",
        |i| format!("item{i}"),
        false,
    );
}

#[test]
fn test_legacy_serial_version_1_round_trip() {
    // Version 1 empty sketch: p = 15, i = 0 -> encoded = 0x7800 -> k = 32768.
    let expected_v2 = ReservoirSketch::<i64>::with_resize_factor(32_768, ResizeFactor::X4)
        .unwrap()
        .serialize();
    let mut v1_empty = expected_v2.clone();
    v1_empty[1] = 1;
    v1_empty[4..8].fill(0);
    v1_empty[4..6].copy_from_slice(&0x7800_u16.to_le_bytes());

    let decoded_empty = ReservoirSketch::<i64>::deserialize(&v1_empty).unwrap();
    assert!(decoded_empty.is_empty());
    assert_eq!(decoded_empty.k(), 32_768);
    assert_eq!(decoded_empty.resize_factor(), ResizeFactor::X4);
    assert_eq!(decoded_empty.serialize(), expected_v2);

    // Version 1 non-empty union containing a version 1 gadget (p = 8, i = 0 -> 0x4000 -> k = 256).
    let mut union = ReservoirUnion::<i64>::new(256).unwrap();
    for i in 0..32_i64 {
        union.update_value(i);
    }
    let expected_union_v2 = union.serialize();
    let mut v1_union = expected_union_v2.clone();
    v1_union[1] = 1;
    v1_union[4..8].fill(0);
    v1_union[4..6].copy_from_slice(&0x4000_u16.to_le_bytes());
    v1_union[8 + 1] = 1;
    v1_union[8 + 4..8 + 8].fill(0);
    v1_union[8 + 4..8 + 6].copy_from_slice(&0x4000_u16.to_le_bytes());

    let decoded_union = ReservoirUnion::<i64>::deserialize(&v1_union).unwrap();
    assert_eq!(decoded_union.max_k(), 256);
    assert_eq!(decoded_union.serialize(), expected_union_v2);
}

#[test]
fn test_non_clone_and_zero_sized_item_round_trip() {
    let mut union = ReservoirUnion::<NonCloneItem>::new(8).unwrap();
    union.update_value(NonCloneItem(10));
    union.update_value(NonCloneItem(20));

    let union_bytes = union.serialize();
    let decoded_union = ReservoirUnion::<NonCloneItem>::deserialize(&union_bytes).unwrap();
    let sketch = decoded_union.into_sketch();
    assert_eq!(sketch.samples(), &[NonCloneItem(10), NonCloneItem(20)]);

    let sketch_bytes = sketch.serialize();
    let decoded_sketch = ReservoirSketch::<NonCloneItem>::deserialize(&sketch_bytes).unwrap();
    assert_eq!(
        decoded_sketch.into_samples(),
        vec![NonCloneItem(10), NonCloneItem(20)]
    );

    let mut zst = ReservoirSketch::<ZeroSizedItem>::new(4).unwrap();
    for _ in 0..10 {
        zst.update(ZeroSizedItem);
    }
    let zst_bytes = zst.serialize();
    assert_eq!(zst_bytes.len(), 16);
    let restored_zst = ReservoirSketch::<ZeroSizedItem>::deserialize(&zst_bytes).unwrap();
    assert_eq!(restored_zst.total_weight(), 10);
    assert_eq!(restored_zst.num_retained(), 4);
}

#[test]
fn test_deserialize_does_not_overallocate_for_large_declared_k() {
    let empty = ReservoirSketch::<i64>::with_resize_factor(i32::MAX as usize, ResizeFactor::X1)
        .unwrap()
        .serialize();
    let restored_empty = ReservoirSketch::<i64>::deserialize(&empty).unwrap();
    assert_eq!(restored_empty.estimated_size(), 0);

    let mut single = ReservoirSketch::<i64>::with_resize_factor(16, ResizeFactor::X1).unwrap();
    single.update(99);
    let mut single_bytes = single.serialize();
    single_bytes[4..8].copy_from_slice(&(i32::MAX as u32).to_le_bytes());

    let restored_single = ReservoirSketch::<i64>::deserialize(&single_bytes).unwrap();
    assert_eq!(restored_single.k(), i32::MAX as usize);
    assert_eq!(restored_single.num_retained(), 1);
    assert_eq!(restored_single.estimated_size(), size_of::<i64>());
}

#[test]
fn test_sketch_deserialization_rejects_malformed_inputs() {
    let empty = ReservoirSketch::<i64>::new(16).unwrap().serialize();
    let mut nonempty_sketch = ReservoirSketch::<i64>::new(16).unwrap();
    nonempty_sketch.update(7);
    let nonempty = nonempty_sketch.serialize();

    // Truncated preamble.
    assert_eq!(
        ReservoirSketch::<i64>::deserialize(&empty[..4])
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    // Wrong family ID.
    let mut bad_family = empty.clone();
    bad_family[2] = 10;
    assert_eq!(
        ReservoirSketch::<i64>::deserialize(&bad_family)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    // Unsupported serial version.
    for ver in [0_u8, 3, 99] {
        let mut bad_ver = empty.clone();
        bad_ver[1] = ver;
        assert_eq!(
            ReservoirSketch::<i64>::deserialize(&bad_ver)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );
    }

    // Invalid k (< 2 or negative i32) and invalid v1 encoded k.
    for raw_k in [0_u32, 1, 0x8000_0000, u32::MAX] {
        let mut bad_k = empty.clone();
        bad_k[4..8].copy_from_slice(&raw_k.to_le_bytes());
        assert_eq!(
            ReservoirSketch::<i64>::deserialize(&bad_k)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );
    }
    for enc_k in [0x0000_u16, 0xF800, u16::MAX] {
        let mut bad_v1_k = empty.clone();
        bad_v1_k[1] = 1;
        bad_v1_k[4..6].copy_from_slice(&enc_k.to_le_bytes());
        assert_eq!(
            ReservoirSketch::<i64>::deserialize(&bad_v1_k)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );
    }

    // Unsupported flag bits and inconsistent empty flag vs preamble longs.
    let mut bad_flags = empty.clone();
    bad_flags[3] = 0x08;
    assert_eq!(
        ReservoirSketch::<i64>::deserialize(&bad_flags)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    let mut empty_with_two_prelongs = empty.clone();
    empty_with_two_prelongs[0] = 0xC2;
    let mut nonempty_with_one_prelong = nonempty.clone();
    nonempty_with_one_prelong[0] = 0xC1;
    for bad in [empty_with_two_prelongs, nonempty_with_one_prelong] {
        assert_eq!(
            ReservoirSketch::<i64>::deserialize(&bad)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );
    }

    // Non-empty sketch with total_weight == 0 or total_weight > MAX_ITEMS_SEEN.
    for bad_n in [0_u64, MAX_ITEMS_SEEN + 1, u64::MAX] {
        let mut bad_weight = nonempty.clone();
        bad_weight[8..16].copy_from_slice(&bad_n.to_le_bytes());
        assert_eq!(
            ReservoirSketch::<i64>::deserialize(&bad_weight)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );
    }

    // Truncated item payload and trailing bytes.
    assert_eq!(
        ReservoirSketch::<i64>::deserialize(&nonempty[..20])
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
    let mut trailing_empty = empty;
    trailing_empty.push(0);
    let mut trailing_nonempty = nonempty;
    trailing_nonempty.push(0);
    for bad in [trailing_empty, trailing_nonempty] {
        assert_eq!(
            ReservoirSketch::<i64>::deserialize(&bad)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );
    }
}

#[test]
fn test_string_and_union_deserialization_rejects_malformed_inputs() {
    // String item with length exceeding remaining bytes and invalid UTF-8.
    let mut str_sketch = ReservoirSketch::<String>::new(8).unwrap();
    str_sketch.update("abc".to_string());
    let str_bytes = str_sketch.serialize();

    let mut bad_str_len = str_bytes.clone();
    bad_str_len[16..20].copy_from_slice(&1024_u32.to_le_bytes());
    let err = ReservoirSketch::<String>::deserialize(&bad_str_len).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);
    assert_that!(err.message(), contains_substring("expected 1024 bytes"));

    let mut bad_utf8 = str_bytes;
    bad_utf8[20] = 0xFF;
    assert_eq!(
        ReservoirSketch::<String>::deserialize(&bad_utf8)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    // Union malformed cases.
    let empty_union = ReservoirUnion::<i64>::new(16).unwrap().serialize();
    let mut populated_union = ReservoirUnion::<i64>::new(16).unwrap();
    populated_union.update_value(42);
    let nonempty_union = populated_union.serialize();

    let mut bad_union_prelongs = empty_union.clone();
    bad_union_prelongs[0] = 2;
    let mut bad_union_family = empty_union.clone();
    bad_union_family[2] = 11;
    let mut bad_union_flags = empty_union.clone();
    bad_union_flags[3] = 0x80;
    let mut bad_union_missing_gadget = empty_union.clone();
    bad_union_missing_gadget[3] = 0;
    let mut trailing_empty_union = empty_union;
    trailing_empty_union.push(0);
    let mut trailing_nonempty_union = nonempty_union;
    trailing_nonempty_union.push(0);

    for bad in [
        bad_union_prelongs,
        bad_union_family,
        bad_union_flags,
        bad_union_missing_gadget,
        trailing_empty_union,
        trailing_nonempty_union,
    ] {
        assert_eq!(
            ReservoirUnion::<i64>::deserialize(&bad).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }
}
