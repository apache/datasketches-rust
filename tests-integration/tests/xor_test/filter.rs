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
    let mut builder = XorFilterBuilder::new(XorFilterType::Xor8, 8123);
    for value in 0..10_000_u64 {
        builder.update(&value);
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

    for ty in [
        XorFilterType::Xor8,
        XorFilterType::Xor16,
        XorFilterType::Xor32,
    ] {
        let filter = XorFilter::from_hashes(&hashes, ty, 0).unwrap();
        for &hash in &hashes {
            assert!(filter.contains_hash(hash), "false negative for {hash}");
        }
    }
}

#[test]
fn builder_deduplicates_values_and_hashes() {
    for ty in [
        XorFilterType::Xor8,
        XorFilterType::Xor16,
        XorFilterType::Xor32,
    ] {
        let mut values = XorFilterBuilder::new(ty, 0);
        let mut hashes = XorFilterBuilder::new(ty, 0);
        for value in [7_u64, 7, 11, 11, 11] {
            values.update(&value);
            hashes.update_hash(value);
        }
        hashes.update_hashes([13, 7, 13]);
        let filter = values.build().unwrap();
        assert!(filter.contains(&7_u64));
        assert!(filter.contains(&11_u64));
        assert_eq!(
            hashes.build().unwrap(),
            XorFilter::from_hashes(&[7, 11, 13], ty, 0).unwrap()
        );
    }
}

#[test]
fn false_positive_rates_follow_fingerprint_width() {
    const NUM_ITEMS: u64 = 50_000;
    const NUM_QUERIES: u64 = 100_000;

    let hashes: Vec<_> = (0..NUM_ITEMS).collect();
    let xor8 = XorFilter::from_hashes(&hashes, XorFilterType::Xor8, 0).unwrap();
    let xor16 = XorFilter::from_hashes(&hashes, XorFilterType::Xor16, 0).unwrap();
    let xor32 = XorFilter::from_hashes(&hashes, XorFilterType::Xor32, 0).unwrap();

    let false_positives8 = (NUM_ITEMS..NUM_ITEMS + NUM_QUERIES)
        .filter(|&hash| xor8.contains_hash(hash))
        .count();
    let false_positives16 = (NUM_ITEMS..NUM_ITEMS + NUM_QUERIES)
        .filter(|&hash| xor16.contains_hash(hash))
        .count();
    let false_positives32 = (NUM_ITEMS..NUM_ITEMS + NUM_QUERIES)
        .filter(|&hash| xor32.contains_hash(hash))
        .count();

    assert!(
        false_positives8 < 1_000,
        "8-bit false-positive count was {false_positives8}"
    );
    assert!(
        false_positives16 < 100,
        "16-bit false-positive count was {false_positives16}"
    );
    assert!(
        false_positives32 < 10,
        "32-bit false-positive count was {false_positives32}"
    );
    assert!(false_positives16 < false_positives8);
}
