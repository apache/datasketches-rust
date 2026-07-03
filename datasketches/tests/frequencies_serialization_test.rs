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

#![cfg(feature = "frequencies")]

mod common;

use std::fs;

use common::serialization_test_data;
use datasketches::codec::SketchBytes;
use datasketches::codec::SketchSlice;
use datasketches::error::Error;
use datasketches::error::ErrorKind;
use datasketches::frequencies::FrequentItemValue;
use datasketches::frequencies::FrequentItemsSketch;

#[derive(Debug, PartialEq, Eq, Hash)]
struct NonCloneSerializableItem(i64);

impl FrequentItemValue for NonCloneSerializableItem {
    fn serialize_size(_item: &Self) -> usize {
        size_of::<i64>()
    }

    fn serialize_value(&self, bytes: &mut SketchBytes) {
        bytes.write_i64_le(self.0);
    }

    fn deserialize_value(cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
        cursor.read_i64_le().map(Self).map_err(|_| {
            Error::new(
                ErrorKind::InvalidData,
                "failed to read non-clone item bytes",
            )
        })
    }
}

#[test]
fn test_longs_round_trip() {
    let mut sketch: FrequentItemsSketch<i64> = FrequentItemsSketch::new(32);
    for i in 1..=100 {
        sketch.update_with_count(i, i as u64);
    }
    let bytes = sketch.serialize();
    let restored = FrequentItemsSketch::<i64>::deserialize(&bytes).unwrap();
    assert_eq!(restored.total_weight(), sketch.total_weight());
    assert_eq!(restored.estimate(&42), sketch.estimate(&42));
    assert_eq!(restored.maximum_error(), sketch.maximum_error());
}

#[test]
fn test_items_round_trip() {
    let mut sketch = FrequentItemsSketch::new(32);
    sketch.update_with_count("alpha".to_string(), 3);
    sketch.update_with_count("beta".to_string(), 5);
    sketch.update_with_count("gamma".to_string(), 7);

    let bytes = sketch.serialize();
    let restored = FrequentItemsSketch::<String>::deserialize(&bytes).unwrap();
    assert_eq!(restored.total_weight(), sketch.total_weight());
    assert_eq!(restored.estimate(&"beta".to_string()), 5);
    assert_eq!(restored.maximum_error(), sketch.maximum_error());
}

#[test]
fn test_non_clone_item_round_trip() {
    let mut sketch = FrequentItemsSketch::<NonCloneSerializableItem>::new(32);
    sketch.update_with_count(NonCloneSerializableItem(1), 2);
    sketch.update_with_count(NonCloneSerializableItem(2), 5);

    let bytes = sketch.serialize();
    let restored = FrequentItemsSketch::<NonCloneSerializableItem>::deserialize(&bytes).unwrap();

    assert_eq!(restored.total_weight(), sketch.total_weight());
    assert_eq!(restored.estimate(&NonCloneSerializableItem(1)), 2);
    assert_eq!(restored.estimate(&NonCloneSerializableItem(2)), 5);
}

#[test]
fn test_empty_round_trip() {
    let sketch = FrequentItemsSketch::<i64>::new(32);
    let bytes = sketch.serialize();
    // One preamble long, matching the Java and C++ empty encoding.
    assert_eq!(bytes.len(), 8);
    let restored = FrequentItemsSketch::<i64>::deserialize(&bytes).unwrap();
    assert!(restored.is_empty());
    assert_eq!(restored.total_weight(), 0);
    assert_eq!(restored.maximum_error(), 0);
}

#[test]
fn test_purged_to_empty_round_trip() {
    // Saturating the map with count-1 items makes the purge median 1, which
    // removes every counter and leaves a non-trivial sketch empty.
    let mut sketch = FrequentItemsSketch::<i64>::new(32);
    for i in 0..=(32 * 3 / 4) {
        sketch.update(i);
    }
    assert!(sketch.is_empty());
    let restored = FrequentItemsSketch::<i64>::deserialize(&sketch.serialize()).unwrap();
    assert!(restored.is_empty());
}

#[test]
fn test_java_frequent_longs_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10000, 100000, 1000000];
    for n in test_cases {
        let filename = format!("frequent_long_n{}_java.sk", n);
        let path = serialization_test_data("java_generated_files", &filename);
        let bytes = fs::read(&path).unwrap();
        let sketch = FrequentItemsSketch::<i64>::deserialize(&bytes).unwrap();
        assert_eq!(sketch.is_empty(), n == 0);
        if n > 10 {
            assert!(sketch.maximum_error() > 0);
        } else {
            assert_eq!(sketch.maximum_error(), 0);
        }
        assert_eq!(sketch.total_weight(), n);
    }
}

#[test]
fn test_java_frequent_strings_ascii() {
    let path = serialization_test_data("java_generated_files", "frequent_string_ascii_java.sk");
    let bytes = fs::read(&path).unwrap();
    let sketch = FrequentItemsSketch::<String>::deserialize(&bytes).unwrap();
    assert!(!sketch.is_empty());
    assert_eq!(sketch.maximum_error(), 0);
    assert_eq!(sketch.total_weight(), 10);
    assert_eq!(
        sketch.estimate(&"aaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()),
        1
    );
    assert_eq!(
        sketch.estimate(&"bbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()),
        2
    );
    assert_eq!(
        sketch.estimate(&"ccccccccccccccccccccccccccccc".to_string()),
        3
    );
    assert_eq!(
        sketch.estimate(&"ddddddddddddddddddddddddddddd".to_string()),
        4
    );
}

#[test]
fn test_java_frequent_strings_utf8() {
    let path = serialization_test_data("java_generated_files", "frequent_string_utf8_java.sk");
    let bytes = fs::read(&path).unwrap();
    let sketch = FrequentItemsSketch::<String>::deserialize(&bytes).unwrap();
    assert!(!sketch.is_empty());
    assert_eq!(sketch.maximum_error(), 0);
    assert_eq!(sketch.total_weight(), 28);
    assert_eq!(sketch.estimate(&"абвгд".to_string()), 1);
    assert_eq!(sketch.estimate(&"еёжзи".to_string()), 2);
    assert_eq!(sketch.estimate(&"йклмн".to_string()), 3);
    assert_eq!(sketch.estimate(&"опрст".to_string()), 4);
    assert_eq!(sketch.estimate(&"уфхцч".to_string()), 5);
    assert_eq!(sketch.estimate(&"шщъыь".to_string()), 6);
    assert_eq!(sketch.estimate(&"эюя".to_string()), 7);
}

#[test]
fn test_cpp_frequent_longs_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10000, 100000, 1000000];
    for n in test_cases {
        let filename = format!("frequent_long_n{}_cpp.sk", n);
        let path = serialization_test_data("cpp_generated_files", &filename);
        let bytes = fs::read(&path).unwrap();
        let sketch = FrequentItemsSketch::<i64>::deserialize(&bytes);
        if cfg!(windows) {
            if let Err(err) = sketch {
                assert_eq!(err.kind(), ErrorKind::InvalidData);
                assert!(
                    err.message().contains("insufficient data"),
                    "expected insufficient data error, got: {err}"
                );
                continue;
            }
        }
        let sketch = sketch.unwrap();
        assert_eq!(sketch.is_empty(), n == 0);
        if n > 10 {
            assert!(sketch.maximum_error() > 0);
        } else {
            assert_eq!(sketch.maximum_error(), 0);
        }
        assert_eq!(sketch.total_weight(), n);
    }
}

#[test]
fn test_cpp_frequent_strings_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10000, 100000, 1000000];
    for n in test_cases {
        let filename = format!("frequent_string_n{}_cpp.sk", n);
        let path = serialization_test_data("cpp_generated_files", &filename);
        let bytes = fs::read(&path).unwrap();
        let sketch = FrequentItemsSketch::<String>::deserialize(&bytes).unwrap();
        assert_eq!(sketch.is_empty(), n == 0);
        if n > 10 {
            assert!(sketch.maximum_error() > 0);
        } else {
            assert_eq!(sketch.maximum_error(), 0);
        }
        assert_eq!(sketch.total_weight(), n);
    }
}

#[test]
fn test_cpp_frequent_strings_ascii() {
    let path = serialization_test_data("cpp_generated_files", "frequent_string_ascii_cpp.sk");
    let bytes = fs::read(&path).unwrap();
    let sketch = FrequentItemsSketch::<String>::deserialize(&bytes).unwrap();
    assert!(!sketch.is_empty());
    assert_eq!(sketch.maximum_error(), 0);
    assert_eq!(sketch.total_weight(), 10);
    assert_eq!(
        sketch.estimate(&"aaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()),
        1
    );
    assert_eq!(
        sketch.estimate(&"bbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()),
        2
    );
    assert_eq!(
        sketch.estimate(&"ccccccccccccccccccccccccccccc".to_string()),
        3
    );
    assert_eq!(
        sketch.estimate(&"ddddddddddddddddddddddddddddd".to_string()),
        4
    );
}

#[test]
fn test_cpp_frequent_strings_utf8() {
    let path = serialization_test_data("cpp_generated_files", "frequent_string_utf8_cpp.sk");
    let bytes = fs::read(&path).unwrap();
    let sketch = FrequentItemsSketch::<String>::deserialize(&bytes).unwrap();
    assert!(!sketch.is_empty());
    assert_eq!(sketch.maximum_error(), 0);
    assert_eq!(sketch.total_weight(), 28);
    assert_eq!(sketch.estimate(&"абвгд".to_string()), 1);
    assert_eq!(sketch.estimate(&"еёжзи".to_string()), 2);
    assert_eq!(sketch.estimate(&"йклмн".to_string()), 3);
    assert_eq!(sketch.estimate(&"опрст".to_string()), 4);
    assert_eq!(sketch.estimate(&"уфхцч".to_string()), 5);
    assert_eq!(sketch.estimate(&"шщъыь".to_string()), 6);
    assert_eq!(sketch.estimate(&"эюя".to_string()), 7);
}
