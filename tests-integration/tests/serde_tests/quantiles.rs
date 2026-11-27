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

use std::fmt::Debug;
use std::fs;

use datasketches::common::SearchCriteria;
use datasketches::error::ErrorKind;
use datasketches::quantiles::QuantilesFloat;
use datasketches::quantiles::QuantilesSketch;
use datasketches::quantiles::QuantilesValue;

use super::kll::NumericString;
use crate::serialization_test_data;

fn check_snapshots<T: QuantilesValue + Ord + Debug>(item_type: &str, item: impl Fn(u64) -> T) {
    for language in ["cpp", "java", "rust"] {
        for n in [0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000] {
            let filename = format!("quantiles_{item_type}_n{n}_{language}.sk");
            let path = serialization_test_data(&format!("{language}_generated_files"), &filename);
            let bytes = fs::read(path).unwrap();
            let sketch = QuantilesSketch::<T>::deserialize(&bytes)
                .unwrap_or_else(|error| panic!("{filename}: {error}"));
            if language == "cpp" {
                assert_eq!(sketch.serialize(), bytes, "{filename}");
            }

            assert_eq!(sketch.k(), 128, "{filename}");
            assert_eq!(sketch.is_estimation_mode(), n >= 256, "{filename}");
            assert_eq!(sketch.total_weight(), n, "{filename}");
            assert_eq!(
                sketch.min_item(),
                (n > 0).then(|| item(1)).as_ref(),
                "{filename}"
            );
            assert_eq!(
                sketch.max_item(),
                (n > 0).then(|| item(n)).as_ref(),
                "{filename}"
            );

            // Each sample's weight is the length of its block in the ascending input stream.
            let mut retained: Vec<_> = sketch.iter().collect();
            retained.sort_unstable_by_key(|&(value, _)| value);
            let mut total_weight = 0;
            for (value, weight) in retained {
                let first = total_weight + 1;
                total_weight += weight;
                assert!(
                    &item(first) <= value && value <= &item(total_weight),
                    "{filename}: {value:?} outside {first}..={total_weight}",
                );
            }
            assert_eq!(total_weight, n, "{filename}");
        }
    }
}

#[test]
fn reads_double_snapshots() {
    check_snapshots("double", |i| QuantilesFloat::<f64>::new(i as f64).unwrap());
}

#[test]
fn reads_numeric_string_snapshots() {
    check_snapshots("string", |i| NumericString(i.to_string()));
}

fn compact_i64(flags: u8, n: u64, min: i64, max: i64, retained: &[i64]) -> Vec<u8> {
    let mut bytes = vec![2, 3, 8, flags, 2, 0, 0, 0]; // v3, Classic Quantiles, k = 2
    bytes.extend_from_slice(&n.to_le_bytes());
    bytes.extend_from_slice(&min.to_le_bytes());
    bytes.extend_from_slice(&max.to_le_bytes());
    for value in retained {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

#[test]
fn writes_sorted_base_items_then_levels_by_weight() {
    let mut sketch = QuantilesSketch::<i64>::new(2).unwrap();
    // Both sampling offsets retain the same items from these repeated values.
    for item in [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 6, 4, 5] {
        sketch.update(item);
    }

    assert_eq!(
        sketch.serialize(),
        compact_i64(0x18, 15, 1, 6, &[4, 5, 6, 3, 3, 1, 2])
    );
}

#[test]
fn reads_unordered_java_strings_in_numeric_order() {
    // Java compact image with items in insertion order: "10", "2", "1".
    let bytes = [
        2, 3, 8, 8, 128, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, // preamble
        1, 0, 0, 0, b'1', 2, 0, 0, 0, b'1', b'0', // min and max
        2, 0, 0, 0, b'1', b'0', 1, 0, 0, 0, b'2', 1, 0, 0, 0, b'1',
    ];
    let sketch = QuantilesSketch::<NumericString>::deserialize(&bytes).unwrap();
    // Rank 0.75 distinguishes numeric, lexicographic, and insertion order.
    assert_eq!(
        sketch.quantile(0.75, SearchCriteria::Inclusive).unwrap(),
        Some(NumericString("10".into())),
    );
}

#[test]
fn rejects_misordered_or_out_of_bounds_retained_items() {
    for bytes in [
        compact_i64(0x18, 11, 1, 11, &[10, 9, 11, 3, 7]), // base marked ordered
        compact_i64(0x08, 11, 1, 11, &[10, 9, 11, 7, 3]), // unordered higher level
        compact_i64(0x08, 11, 1, 11, &[10, 12, 9, 3, 7]), // interior base item above the max
        compact_i64(0x18, 2, 1, 3, &[1, 1]),              // exact sketch missing its maximum
        compact_i64(0x18, 2, 1, 3, &[3, 3]),              // exact sketch missing its minimum
    ] {
        let error = QuantilesSketch::<i64>::deserialize(&bytes).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }
}

#[test]
fn rejects_invalid_headers_and_payload_lengths() {
    let bytes = compact_i64(0x18, 1, 2, 2, &[2]);
    for (offset, value) in [(0, 1), (1, 2), (2, 15), (3, 0x10), (4, 3)] {
        let mut invalid = bytes.clone();
        invalid[offset] = value;
        let error = QuantilesSketch::<i64>::deserialize(&invalid).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }

    for end in [7, 15, bytes.len() - 1] {
        let error = QuantilesSketch::<i64>::deserialize(&bytes[..end]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }

    let mut trailing = bytes;
    trailing.push(0);
    let error = QuantilesSketch::<i64>::deserialize(&trailing).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
}

#[test]
fn reads_the_empty_java_direct_compact_preamble() {
    let bytes = [2, 3, 8, 0x0e, 128, 0, 0, 0];
    let sketch = QuantilesSketch::<QuantilesFloat<f64>>::deserialize(&bytes).unwrap();
    assert!(sketch.is_empty());
    assert_eq!(sketch.k(), 128);
}
