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
use datasketches::error::ErrorKind;
use googletest::assert_that;
use googletest::prelude::ge;
use googletest::prelude::gt;
use googletest::prelude::le;

const NUM_BITS: u64 = 65_536;
const NUM_HASHES: u16 = 5;
const SEED: u64 = 123;

fn filter() -> datasketches::bloom::BloomFilter {
    BloomFilterBuilder::with_size(NUM_BITS, NUM_HASHES)
        .seed(SEED)
        .build()
        .unwrap()
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
    assert_that!(filter.bits_used(), gt(0));
    assert_that!(filter.load_factor(), gt(0.0));
    assert_that!(filter.estimated_fpp(), gt(0.0));

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
    intersection.intersect(&right).unwrap();
    assert!(intersection.contains(&"shared"));
    let intersection_bits = intersection.bits_used();
    assert_that!(intersection_bits, le(left_bits));
    assert_that!(intersection_bits, le(right_bits));

    let mut union = left;
    union.union(&right).unwrap();
    assert!(union.contains(&"shared"));
    assert!(union.contains(&"left"));
    assert!(union.contains(&"right"));
    assert_that!(union.bits_used(), ge(left_bits));
    assert_that!(union.bits_used(), ge(right_bits));
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
        .build()
        .unwrap();
    let different_size = BloomFilterBuilder::with_size(NUM_BITS * 2, NUM_HASHES)
        .seed(SEED)
        .build()
        .unwrap();
    let different_hashes = BloomFilterBuilder::with_size(NUM_BITS, NUM_HASHES + 1)
        .seed(SEED)
        .build()
        .unwrap();

    assert!(!baseline.is_compatible(&different_seed));
    assert!(!baseline.is_compatible(&different_size));
    assert!(!baseline.is_compatible(&different_hashes));
}

#[test]
fn test_union_rejects_incompatible_filters() {
    let mut left = filter();
    let right = BloomFilterBuilder::with_size(NUM_BITS, NUM_HASHES)
        .seed(SEED + 1)
        .build()
        .unwrap();
    let error = left.union(&right).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn test_intersection_rejects_incompatible_filters() {
    let mut left = filter();
    let right = BloomFilterBuilder::with_size(NUM_BITS, NUM_HASHES)
        .seed(SEED + 1)
        .build()
        .unwrap();
    let error = left.intersect(&right).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn test_requested_size_rounds_to_word_boundary() {
    let filter = BloomFilterBuilder::with_size(65, 3).build().unwrap();
    assert_eq!(filter.capacity(), 128);
}

#[test]
fn test_accuracy_builder_rejects_zero_items_at_build() {
    let error = BloomFilterBuilder::with_accuracy(0, 0.01)
        .build()
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn test_accuracy_builder_rejects_invalid_probability_at_build() {
    let error = BloomFilterBuilder::with_accuracy(100, 1.5)
        .build()
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn test_size_builder_rejects_zero_bits_at_build() {
    let error = BloomFilterBuilder::with_size(0, 3).build().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn test_size_builder_rejects_zero_hashes_at_build() {
    let error = BloomFilterBuilder::with_size(128, 0).build().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn test_parameter_suggestions_validate_inputs() {
    let errors = [
        BloomFilterBuilder::suggest_num_bits(0, 0.01).unwrap_err(),
        BloomFilterBuilder::suggest_num_bits(1000, f64::NAN).unwrap_err(),
        BloomFilterBuilder::suggest_num_hashes_from_accuracy(0, 10_000).unwrap_err(),
        BloomFilterBuilder::suggest_num_hashes_from_accuracy(1000, 0).unwrap_err(),
        BloomFilterBuilder::suggest_num_hashes_from_fpp(0.0).unwrap_err(),
    ];
    assert!(
        errors
            .iter()
            .all(|error| error.kind() == ErrorKind::InvalidArgument)
    );
}
