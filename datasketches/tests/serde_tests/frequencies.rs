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
use googletest::prelude::anything;
use googletest::prelude::contains_substring;
use googletest::prelude::err;
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
    // removes every counter while retaining stream and error state.
    let mut sketch = FrequentItemsSketch::<i64>::new(32);
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

    let mut active_sketch = FrequentItemsSketch::<i64>::new(32);
    active_sketch.update_with_count(7, 3);
    let mut active_bytes = active_sketch.serialize();
    active_bytes[STREAM_WEIGHT_OFFSET..STREAM_WEIGHT_OFFSET + size_of::<u64>()].fill(0);

    let active_restored = FrequentItemsSketch::<i64>::deserialize(&active_bytes).unwrap();
    assert_eq!(active_restored.total_weight(), 0);
    assert_eq!(active_restored.num_active_items(), 1);
    assert_eq!(active_restored.estimate(&7), 3);
    assert_eq!(active_restored.serialize(), active_bytes);

    let mut active_merged = FrequentItemsSketch::<i64>::new(32);
    active_merged.merge(&active_restored);
    assert_eq!(active_merged.num_active_items(), 1);
    assert_eq!(active_merged.estimate(&7), 3);

    let mut purged_sketch = FrequentItemsSketch::<i64>::new(32);
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

    let mut purged_merged = FrequentItemsSketch::<i64>::new(32);
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

// Header field constants for the DataSketches frequent-items format.
const FREQ_SERIAL_VERSION: u8 = 1;
const FREQ_FAMILY_ID: u8 = 10;
const FREQ_PREAMBLE_LONGS_EMPTY: u8 = 1;
const FREQ_PREAMBLE_LONGS_NONEMPTY: u8 = 4;
const FREQ_EMPTY_FLAG_MASK: u8 = 5;

fn empty_header(lg_max: u8, lg_cur: u8) -> Vec<u8> {
    let mut bytes = SketchBytes::with_capacity(8);
    bytes.write_u8(FREQ_PREAMBLE_LONGS_EMPTY);
    bytes.write_u8(FREQ_SERIAL_VERSION);
    bytes.write_u8(FREQ_FAMILY_ID);
    bytes.write_u8(lg_max);
    bytes.write_u8(lg_cur);
    bytes.write_u8(FREQ_EMPTY_FLAG_MASK);
    bytes.write_u16_le(0);
    bytes.into_bytes()
}

// Regression: a corrupt header must never panic (or trigger an oversized
// allocation) inside `with_lg_map_sizes`; deserialize must reject it cleanly.
// Before the fix, `lg_max_map_size = 222` drove `1usize << lg_max` past the
// width of `usize`, panicking with "attempt to shift left with overflow" in
// debug builds.
#[test]
fn test_deserialize_rejects_out_of_range_lg_max_map_size() {
    let bytes = empty_header(222, 5);
    let result = FrequentItemsSketch::<i64>::deserialize(&bytes);
    assert_that!(result, err(anything()));
    assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidData);
}

#[test]
fn test_deserialize_rejects_out_of_range_lg_max_map_size_nonempty() {
    let mut bytes = SketchBytes::with_capacity(16);
    bytes.write_u8(FREQ_PREAMBLE_LONGS_NONEMPTY);
    bytes.write_u8(FREQ_SERIAL_VERSION);
    bytes.write_u8(FREQ_FAMILY_ID);
    bytes.write_u8(200); // lg_max_map_size, out of range
    bytes.write_u8(3); // lg_cur_map_size
    bytes.write_u8(0); // flags (not empty)
    bytes.write_u16_le(0);
    bytes.write_u32_le(0); // active_items
    bytes.write_u32_le(0);
    bytes.write_u64_le(0); // stream_weight
    bytes.write_u64_le(0); // offset
    let result = FrequentItemsSketch::<i64>::deserialize(&bytes.into_bytes());
    assert_that!(result, err(anything()));
}

// `lg_cur_map_size` below the documented minimum is also corruption; the C++
// reference `check_size` rejects it, so the Rust port must too.
#[test]
fn test_deserialize_rejects_undersized_lg_cur_map_size() {
    let bytes = empty_header(10, 1);
    let result = FrequentItemsSketch::<i64>::deserialize(&bytes);
    assert_that!(result, err(anything()));
}

// A well-formed empty header at the exact upper bound must still deserialize.
#[test]
fn test_deserialize_accepts_valid_lg_map_sizes() {
    let bytes = empty_header(10, 3);
    let restored = FrequentItemsSketch::<i64>::deserialize(&bytes).unwrap();
    assert!(restored.is_empty());
    assert_eq!(restored.lg_max_map_size(), 10);
}

// Builds a minimal non-empty (four-preamble-long) header. The caller supplies
// `lg_max`, `lg_cur`, and `active_items`; no item payload is appended, so this
// is only useful for exercising the header-consistency guards that run before
// the payload is read.
fn nonempty_header(lg_max: u8, lg_cur: u8, active_items: u32) -> Vec<u8> {
    let mut bytes = SketchBytes::with_capacity(24);
    bytes.write_u8(FREQ_PREAMBLE_LONGS_NONEMPTY);
    bytes.write_u8(FREQ_SERIAL_VERSION);
    bytes.write_u8(FREQ_FAMILY_ID);
    bytes.write_u8(lg_max);
    bytes.write_u8(lg_cur);
    bytes.write_u8(0); // flags (not empty)
    bytes.write_u16_le(0);
    bytes.write_u32_le(active_items);
    bytes.write_u32_le(0); // unused
    bytes.write_u64_le(0); // stream_weight
    bytes.write_u64_le(0); // offset
    bytes.into_bytes()
}

// Regression for tisonkun's report on #224: the accepted boundary
// `(lg_max, lg_cur) = (30, 30)` on an *empty* header still drove
// `ReversePurgeItemHashMap::new(1 << 30)` (~1e9 slots, multi-GB) before any
// payload was read. An empty sketch holds nothing, so it must now build its map
// at the minimum size and deserialize cheaply instead of attempting the
// allocation. If the fix regressed, this test would OOM/hang rather than fail.
#[test]
fn test_deserialize_empty_header_does_not_over_allocate() {
    let bytes = [1u8, 1, 10, 30, 30, 5, 0, 0];
    let restored = FrequentItemsSketch::<i64>::deserialize(&bytes).unwrap();
    assert!(restored.is_empty());
    assert_eq!(restored.num_active_items(), 0);
    assert_eq!(restored.lg_max_map_size(), 30);
}

// A non-empty header whose `lg_cur_map_size` is too small to hold the claimed
// `active_items` under the load factor is corrupt: a map of `1 << 3` slots has
// capacity 6, so it can never hold 100 active items. Reject it instead of
// trusting the header.
#[test]
fn test_deserialize_rejects_lg_cur_inconsistent_with_num_active() {
    let bytes = nonempty_header(10, 3, 100);
    let result = FrequentItemsSketch::<i64>::deserialize(&bytes);
    assert_that!(result, err(anything()));
    assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidData);
}

// A non-empty header claiming a huge `active_items` that the remaining bytes
// cannot possibly contain must be rejected before any capacity is reserved from
// that count, so a corrupt header cannot drive a multi-GB `Vec` allocation.
#[test]
fn test_deserialize_rejects_num_active_exceeding_payload() {
    // 700_000_000 <= capacity implied by lg_cur = 30, so it clears the capacity
    // guard, but there are no item bytes for it, so the length guard rejects it.
    let bytes = nonempty_header(30, 30, 700_000_000);
    let result = FrequentItemsSketch::<i64>::deserialize(&bytes);
    assert_that!(result, err(anything()));
}
