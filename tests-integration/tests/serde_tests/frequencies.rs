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

use datasketches::codec::SketchBytes;
use datasketches::codec::SketchSlice;
use datasketches::error::Error;
use datasketches::error::ErrorKind;
use datasketches::frequencies::FrequentItemValue;
use datasketches::frequencies::FrequentItemsSketch;
use googletest::assert_that;
use googletest::prelude::contains_substring;
use googletest::prelude::gt;

use crate::serialization_test_data;

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
    let mut sketch: FrequentItemsSketch<i64> = FrequentItemsSketch::new(32).unwrap();
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
    let mut sketch = FrequentItemsSketch::new(32).unwrap();
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
    let mut sketch = FrequentItemsSketch::<NonCloneSerializableItem>::new(32).unwrap();
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
    let sketch = FrequentItemsSketch::<i64>::new(32).unwrap();
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
    // removes every counter while retaining stream and error state.
    let mut sketch = FrequentItemsSketch::<i64>::new(32).unwrap();
    for i in 0..=(32 * 3 / 4) {
        sketch.update(i);
    }
    assert!(sketch.is_empty());
    assert_eq!(sketch.num_active_items(), 0);
    assert_eq!(sketch.total_weight(), 25);
    assert_eq!(sketch.maximum_error(), 1);
    assert_eq!(sketch.upper_bound(&1000), 1);

    let bytes = sketch.serialize();
    assert_eq!(bytes.len(), 4 * size_of::<u64>());
    let restored = FrequentItemsSketch::<i64>::deserialize(&bytes).unwrap();
    assert!(restored.is_empty());
    assert_eq!(restored.num_active_items(), 0);
    assert_eq!(restored.total_weight(), sketch.total_weight());
    assert_eq!(restored.maximum_error(), sketch.maximum_error());
    assert_eq!(restored.upper_bound(&1000), sketch.upper_bound(&1000));
    assert_eq!(restored.serialize(), bytes);
}

#[test]
fn test_zero_stream_weight_does_not_discard_other_state() {
    // Simulate a wrapped stream weight or an inconsistent but accepted serialized image.
    const STREAM_WEIGHT_OFFSET: usize = 2 * size_of::<u64>();

    let mut active_sketch = FrequentItemsSketch::<i64>::new(32).unwrap();
    active_sketch.update_with_count(7, 3);
    let mut active_bytes = active_sketch.serialize();
    active_bytes[STREAM_WEIGHT_OFFSET..STREAM_WEIGHT_OFFSET + size_of::<u64>()].fill(0);

    let active_restored = FrequentItemsSketch::<i64>::deserialize(&active_bytes).unwrap();
    assert_eq!(active_restored.total_weight(), 0);
    assert_eq!(active_restored.num_active_items(), 1);
    assert_eq!(active_restored.estimate(&7), 3);
    assert_eq!(active_restored.serialize(), active_bytes);

    let mut active_merged = FrequentItemsSketch::<i64>::new(32).unwrap();
    active_merged.merge(&active_restored);
    assert_eq!(active_merged.num_active_items(), 1);
    assert_eq!(active_merged.estimate(&7), 3);

    let mut purged_sketch = FrequentItemsSketch::<i64>::new(32).unwrap();
    for item in 0..=(32 * 3 / 4) {
        purged_sketch.update(item);
    }
    let mut purged_bytes = purged_sketch.serialize();
    purged_bytes[STREAM_WEIGHT_OFFSET..STREAM_WEIGHT_OFFSET + size_of::<u64>()].fill(0);

    let purged_restored = FrequentItemsSketch::<i64>::deserialize(&purged_bytes).unwrap();
    assert_eq!(purged_restored.total_weight(), 0);
    assert_eq!(purged_restored.num_active_items(), 0);
    assert_eq!(purged_restored.maximum_error(), 1);
    assert_eq!(purged_restored.serialize(), purged_bytes);

    let mut purged_merged = FrequentItemsSketch::<i64>::new(32).unwrap();
    purged_merged.merge(&purged_restored);
    assert_eq!(purged_merged.num_active_items(), 0);
    assert_eq!(purged_merged.maximum_error(), 1);
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
            assert_that!(sketch.maximum_error(), gt(0));
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
                assert_that!(err.message(), contains_substring("insufficient data"));
                continue;
            }
        }
        let sketch = sketch.unwrap();
        assert_eq!(sketch.is_empty(), n == 0);
        if n > 10 {
            assert_that!(sketch.maximum_error(), gt(0));
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
            assert_that!(sketch.maximum_error(), gt(0));
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

#[test]
fn test_go_frequent_longs_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10000, 100000, 1000000];
    for n in test_cases {
        let filename = format!("frequent_long_n{n}_go.sk");
        let path = serialization_test_data("go_generated_files", &filename);
        let bytes = fs::read(&path).unwrap();
        let sketch = FrequentItemsSketch::<i64>::deserialize(&bytes).unwrap();
        assert_eq!(sketch.is_empty(), n == 0);
        if n > 10 {
            assert_that!(sketch.maximum_error(), gt(0));
        } else {
            assert_eq!(sketch.maximum_error(), 0);
        }
        assert_eq!(sketch.total_weight(), n);
    }
}

#[test]
fn test_go_frequent_strings_compatibility() {
    let test_cases = [0, 1, 10, 100, 1000, 10000, 100000, 1000000];
    for n in test_cases {
        let filename = format!("frequent_string_n{n}_go.sk");
        let path = serialization_test_data("go_generated_files", &filename);
        let bytes = fs::read(&path).unwrap();
        let sketch = FrequentItemsSketch::<String>::deserialize(&bytes).unwrap();
        assert_eq!(sketch.is_empty(), n == 0);
        if n > 10 {
            assert_that!(sketch.maximum_error(), gt(0));
        } else {
            assert_eq!(sketch.maximum_error(), 0);
        }
        assert_eq!(sketch.total_weight(), n);
    }
}

#[test]
fn test_go_frequent_strings_ascii() {
    let path = serialization_test_data("go_generated_files", "frequent_string_ascii_go.sk");
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
fn test_go_frequent_strings_utf8() {
    let path = serialization_test_data("go_generated_files", "frequent_string_utf8_go.sk");
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
fn test_deserialize_rejects_invalid_map_sizes() {
    let empty = FrequentItemsSketch::<i64>::new(32).unwrap().serialize();
    let mut nonempty_sketch = FrequentItemsSketch::<i64>::new(32).unwrap();
    nonempty_sketch.update(1);
    let nonempty = nonempty_sketch.serialize();

    for (mut bytes, lg_max, lg_cur) in [(empty.clone(), 222, 3), (nonempty, 200, 3), (empty, 10, 1)]
    {
        bytes[3] = lg_max;
        bytes[4] = lg_cur;
        assert_eq!(
            FrequentItemsSketch::<i64>::deserialize(&bytes)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );
    }
}

#[test]
fn test_deserialize_empty_header_does_not_allocate_declared_map() {
    let mut bytes = FrequentItemsSketch::<i64>::new(1usize << 30)
        .unwrap()
        .serialize();
    bytes[4] = 30;
    let restored = FrequentItemsSketch::<i64>::deserialize(&bytes).unwrap();
    assert!(restored.is_empty());
    assert_eq!(restored.lg_max_map_size(), 30);
    assert_eq!(restored.lg_cur_map_size(), 3);
}

#[test]
fn test_deserialize_rejects_lg_cur_inconsistent_with_num_active() {
    let mut sketch = FrequentItemsSketch::<i64>::new(32).unwrap();
    sketch.update(1);
    let mut bytes = sketch.serialize();
    bytes[8..12].copy_from_slice(&100u32.to_le_bytes());
    assert!(FrequentItemsSketch::<i64>::deserialize(&bytes).is_err());
}

#[test]
fn test_deserialize_rejects_num_active_exceeding_payload() {
    let mut sketch = FrequentItemsSketch::<i64>::new(32).unwrap();
    sketch.update(1);
    let mut bytes = sketch.serialize();
    bytes[3] = 30;
    bytes[4] = 30;
    bytes[8..12].copy_from_slice(&700_000_000u32.to_le_bytes());
    assert!(FrequentItemsSketch::<i64>::deserialize(&bytes).is_err());
}
