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

use datasketches::xor::XorFilter;
use datasketches::xor::XorFilterBuilder;
use datasketches::xor::XorFilterType;

#[test]
fn values_have_no_false_negatives() {
    let mut builder = XorFilterBuilder::new(XorFilterType::Xor8).seed(8123);
    for value in 0..10_000_u64 {
        builder.update(value);
    }
    let filter = builder.build().unwrap();

    for value in 0..10_000_u64 {
        assert!(filter.contains(&value), "false negative for {value}");
    }
}

#[test]
fn precomputed_hashes_have_no_false_negatives() {
    let hashes = (0..10_000_u64)
        .map(|value| value.wrapping_mul(0x9e37_79b9_7f4a_7c15))
        .collect::<Vec<_>>();

    for filter_type in [XorFilterType::Xor8, XorFilterType::Xor16] {
        let filter = XorFilter::from_hashes(hashes.iter().copied(), filter_type).unwrap();
        for &hash in &hashes {
            assert!(filter.contains_hash(hash), "false negative for {hash}");
        }
    }
}

#[test]
fn builder_accepts_mixed_value_types() {
    let mut builder = XorFilterBuilder::new(XorFilterType::Xor16);
    builder.update("datasketches");
    builder.update(42_u64);
    builder.update([1_u8, 2, 3, 4]);
    let filter = builder.build().unwrap();

    assert!(filter.contains("datasketches"));
    assert!(filter.contains(&42_u64));
    assert!(filter.contains(&[1_u8, 2, 3, 4]));
}

#[test]
fn duplicates_are_removed_before_construction() {
    let mut builder = XorFilterBuilder::new(XorFilterType::Xor8);
    builder.extend_hashes([7, 7, 11, 11, 11]);
    assert_eq!(builder.num_items(), 5);

    let filter = builder.build().unwrap();
    assert_eq!(filter.num_items(), 2);
    assert!(filter.contains_hash(7));
    assert!(filter.contains_hash(11));
}

#[test]
fn construction_is_independent_of_input_order() {
    let ascending = XorFilterBuilder::new(XorFilterType::Xor8).seed(424_242);
    let descending = ascending.clone();

    let mut ascending = ascending;
    ascending.extend_hashes(0..50_000_u64);
    let mut descending = descending;
    descending.extend_hashes((0..50_000_u64).rev());

    assert_eq!(
        ascending.build().unwrap().serialize(),
        descending.build().unwrap().serialize()
    );
}

#[test]
fn metadata_describes_the_compact_payload() {
    let filter = XorFilter::from_hashes(0..10_000_u64, XorFilterType::Xor8).unwrap();

    assert!(!filter.is_empty());
    assert_eq!(filter.num_items(), 10_000);
    assert_eq!(filter.filter_type(), XorFilterType::Xor8);
    assert_eq!(filter.bits_per_fingerprint(), 8);
    assert_eq!(filter.num_hashes(), 3);
    assert_eq!(filter.capacity() % 3, 0);
    assert!((9.5..10.5).contains(&filter.bits_per_item()));
    assert!(filter.estimated_size() >= filter.capacity());
    assert_eq!(filter.serialized_size(), 24 + filter.capacity());
}

#[test]
fn empty_filter_is_well_formed() {
    let builder = XorFilterBuilder::new(XorFilterType::Xor16);
    assert!(builder.is_empty());
    let filter = builder.build().unwrap();

    assert!(filter.is_empty());
    assert_eq!(filter.num_items(), 0);
    assert_eq!(filter.capacity(), 30);
    assert_eq!(filter.bits_per_item(), 0.0);
    assert_eq!(filter.serialized_size(), 24 + 60);
}

#[test]
fn false_positive_rates_follow_fingerprint_width() {
    const NUM_ITEMS: u64 = 50_000;
    const NUM_QUERIES: u64 = 100_000;

    let xor8 = XorFilter::from_hashes(0..NUM_ITEMS, XorFilterType::Xor8).unwrap();
    let xor16 = XorFilter::from_hashes(0..NUM_ITEMS, XorFilterType::Xor16).unwrap();

    let false_positives8 = (NUM_ITEMS..NUM_ITEMS + NUM_QUERIES)
        .filter(|&hash| xor8.contains_hash(hash))
        .count();
    let false_positives16 = (NUM_ITEMS..NUM_ITEMS + NUM_QUERIES)
        .filter(|&hash| xor16.contains_hash(hash))
        .count();

    assert!(
        false_positives8 < 1_000,
        "8-bit false-positive count was {false_positives8}"
    );
    assert!(
        false_positives16 < 100,
        "16-bit false-positive count was {false_positives16}"
    );
    assert!(false_positives16 < false_positives8);
}
