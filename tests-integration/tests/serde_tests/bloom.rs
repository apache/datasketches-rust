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
use googletest::assert_that;
use googletest::prelude::gt;

use crate::serialization_test_data;

fn test_bloom_filter_file(path: PathBuf, expected_num_items: u64, expected_num_hashes: u16) {
    let bytes = fs::read(&path).unwrap();
    let filter1 = BloomFilter::deserialize(&bytes).unwrap();

    // Verify basic properties
    assert_eq!(
        filter1.num_hashes(),
        expected_num_hashes,
        "Wrong num_hashes in {}",
        path.display()
    );

    // Check empty state
    if expected_num_items == 0 {
        assert!(filter1.is_empty(), "Filter should be empty for n=0");
        assert_eq!(
            filter1.bits_used(),
            0,
            "Empty filter should have 0 bits set"
        );
    } else {
        assert!(
            !filter1.is_empty(),
            "Filter should not be empty for n={}",
            expected_num_items
        );
        assert_that!(filter1.bits_used(), gt(0));
    }

    // Verify the items that were inserted (integers 0 to n/10-1)
    // C++ code: for (uint64_t i = 0; i < n / 10; ++i) bf.update(i);
    let num_inserted = expected_num_items / 10;

    if num_inserted > 0 {
        // Check a sample of inserted items
        // For large n, we only check a sample to keep tests fast
        let sample_size = std::cmp::min(num_inserted, 100);
        let mut false_negatives = 0;

        for i in 0..sample_size {
            if !filter1.contains(&i) {
                false_negatives += 1;
            }
        }

        assert_eq!(
            false_negatives,
            0,
            "Found {} false negatives out of {} items in {}",
            false_negatives,
            sample_size,
            path.display()
        );
    }

    // Serialize and deserialize again to test round-trip
    let serialized_bytes = filter1.serialize();
    let filter2 = BloomFilter::deserialize(&serialized_bytes).unwrap_or_else(|err| {
        panic!(
            "Deserialization failed after round-trip for {}: {}",
            path.display(),
            err
        )
    });

    // Check that both filters are functionally equivalent
    assert_eq!(
        filter1.num_hashes(),
        filter2.num_hashes(),
        "num_hashes mismatch after round-trip for {}",
        path.display()
    );
    assert_eq!(
        filter1.capacity(),
        filter2.capacity(),
        "capacity mismatch after round-trip for {}",
        path.display()
    );
    assert_eq!(
        filter1.bits_used(),
        filter2.bits_used(),
        "bits_used mismatch after round-trip for {}",
        path.display()
    );

    // Verify same items are present after round-trip
    if num_inserted > 0 {
        let sample_size = std::cmp::min(num_inserted, 100);
        for i in 0..sample_size {
            assert_eq!(
                filter1.contains(&i),
                filter2.contains(&i),
                "Item {} presence differs after round-trip",
                i
            );
        }
    }
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
    ] {
        let filename = format!("bf_n{n}_h{num_hashes}_go.sk");
        let path = serialization_test_data("go_generated_files", &filename);
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

        assert!(BloomFilter::deserialize(&bytes).is_err());
    }

    let mut dirty = filter.serialize();
    dirty[NUM_BITS_SET_OFFSET..NUM_BITS_SET_OFFSET + size_of::<u64>()]
        .copy_from_slice(&u64::MAX.to_le_bytes());
    let restored = BloomFilter::deserialize(&dirty).unwrap();
    assert_eq!(restored.bits_used(), actual_bits_set);
    assert!(restored.contains(&"apple"));
    assert!(restored.contains(&"banana"));
}

#[test]
fn test_nonempty_payload_length_is_checked_before_allocating() {
    const NUM_LONGS_OFFSET: usize = 16;

    let mut filter = BloomFilterBuilder::with_accuracy(100, 0.01)
        .build()
        .unwrap();
    filter.insert("apple");
    let mut bytes = filter.serialize();
    bytes[NUM_LONGS_OFFSET..NUM_LONGS_OFFSET + size_of::<i32>()]
        .copy_from_slice(&i32::MAX.to_le_bytes());

    assert!(BloomFilter::deserialize(&bytes).is_err());
}
