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

use crate::serialization_test_data;

const COMMON_TEST_CASES: [(u64, u16); 6] = [
    (0, 3),
    (0, 5),
    (10_000, 3),
    (10_000, 5),
    (2_000_000, 3),
    (2_000_000, 5),
];
const LARGE_TEST_CASES: [(u64, u16); 2] = [(30_000_000, 3), (30_000_000, 5)];

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
        assert!(
            filter1.bits_used() > 0,
            "Non-empty filter should have bits set"
        );
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

fn test_compatibility(language: &str, test_cases: impl IntoIterator<Item = (u64, u16)>) {
    let sub_dir = format!("{language}_generated_files");

    for (n, num_hashes) in test_cases {
        let filename = format!("bf_n{n}_h{num_hashes}_{language}.sk");
        let path = serialization_test_data(&sub_dir, &filename);
        test_bloom_filter_file(path, n, num_hashes);
    }
}

#[test]
fn test_java_compatibility() {
    test_compatibility(
        "java",
        COMMON_TEST_CASES.into_iter().chain(LARGE_TEST_CASES),
    );
}

#[test]
fn test_cpp_compatibility() {
    test_compatibility("cpp", COMMON_TEST_CASES.into_iter().chain(LARGE_TEST_CASES));
}

#[test]
fn test_go_compatibility() {
    test_compatibility("go", COMMON_TEST_CASES);
}
