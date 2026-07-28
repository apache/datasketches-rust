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

use datasketches::bloom::BloomFilterBuilder;

const NUM_BITS: u64 = 65_536;
const NUM_HASHES: u16 = 5;
const SEED: u64 = 123;

fn filter() -> datasketches::bloom::BloomFilter {
    BloomFilterBuilder::with_size(NUM_BITS, NUM_HASHES)
        .seed(SEED)
        .build()
}

#[test]
fn test_membership_statistics_and_reset() {
    let mut filter = filter();
    assert_eq!(filter.capacity(), NUM_BITS as usize);
    assert_eq!(filter.num_hashes(), NUM_HASHES);
    assert_eq!(filter.seed(), SEED);
    assert!(filter.is_empty());
    assert!(!filter.contains(&"apple"));

    assert!(!filter.contains_and_insert(&"apple"));
    assert!(filter.contains_and_insert(&"apple"));
    assert!(filter.bits_used() > 0);
    assert!(filter.load_factor() > 0.0);
    assert!(filter.estimated_fpp() > 0.0);

    filter.reset();
    assert!(filter.is_empty());
    assert_eq!(filter.bits_used(), 0);
    assert!(!filter.contains(&"apple"));
}

#[test]
fn test_union_and_intersection() {
    let mut left = filter();
    left.insert("shared");
    left.insert("left");

    let mut right = filter();
    right.insert("shared");
    right.insert("right");

    let left_bits = left.bits_used();
    let right_bits = right.bits_used();

    let mut intersection = left.clone();
    intersection.intersect(&right);
    assert!(intersection.contains(&"shared"));
    let intersection_bits = intersection.bits_used();
    assert!(intersection_bits <= left_bits);
    assert!(intersection_bits <= right_bits);

    let mut union = left;
    union.union(&right);
    assert!(union.contains(&"shared"));
    assert!(union.contains(&"left"));
    assert!(union.contains(&"right"));
    assert!(union.bits_used() >= left_bits);
    assert!(union.bits_used() >= right_bits);
}

#[test]
fn test_invert_is_reversible() {
    let mut filter = filter();
    filter.insert("apple");
    filter.insert("banana");

    let original = filter.clone();
    let original_bits = filter.bits_used();
    filter.invert();
    assert_eq!(filter.bits_used(), filter.capacity() as u64 - original_bits);

    filter.invert();
    assert_eq!(filter, original);
}

#[test]
fn test_compatibility_checks_all_configuration() {
    let baseline = filter();
    assert!(baseline.is_compatible(&filter()));

    let different_seed = BloomFilterBuilder::with_size(NUM_BITS, NUM_HASHES)
        .seed(SEED + 1)
        .build();
    let different_size = BloomFilterBuilder::with_size(NUM_BITS * 2, NUM_HASHES)
        .seed(SEED)
        .build();
    let different_hashes = BloomFilterBuilder::with_size(NUM_BITS, NUM_HASHES + 1)
        .seed(SEED)
        .build();

    assert!(!baseline.is_compatible(&different_seed));
    assert!(!baseline.is_compatible(&different_size));
    assert!(!baseline.is_compatible(&different_hashes));
}

#[test]
#[should_panic(expected = "Cannot union incompatible Bloom filters")]
fn test_union_rejects_incompatible_filters() {
    let mut left = filter();
    let right = BloomFilterBuilder::with_size(NUM_BITS, NUM_HASHES)
        .seed(SEED + 1)
        .build();
    left.union(&right);
}

#[test]
#[should_panic(expected = "Cannot intersect incompatible Bloom filters")]
fn test_intersection_rejects_incompatible_filters() {
    let mut left = filter();
    let right = BloomFilterBuilder::with_size(NUM_BITS, NUM_HASHES)
        .seed(SEED + 1)
        .build();
    left.intersect(&right);
}

#[test]
fn test_requested_size_rounds_to_word_boundary() {
    let filter = BloomFilterBuilder::with_size(65, 3).build();
    assert_eq!(filter.capacity(), 128);
}

#[test]
#[should_panic(expected = "max_items must be greater than 0")]
fn test_accuracy_builder_rejects_zero_items() {
    BloomFilterBuilder::with_accuracy(0, 0.01);
}

#[test]
#[should_panic(expected = "fpp must be between")]
fn test_accuracy_builder_rejects_invalid_probability() {
    BloomFilterBuilder::with_accuracy(100, 1.5);
}

#[test]
#[should_panic(expected = "num_bits must be between")]
fn test_size_builder_rejects_zero_bits() {
    BloomFilterBuilder::with_size(0, 3);
}

#[test]
#[should_panic(expected = "num_hashes must be between")]
fn test_size_builder_rejects_zero_hashes() {
    BloomFilterBuilder::with_size(128, 0);
}
