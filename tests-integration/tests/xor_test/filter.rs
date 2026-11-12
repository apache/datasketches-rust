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

use datasketches::error::ErrorKind;
use datasketches::hash::value::raw_bytes;
use datasketches::xor::XorFilter;
use datasketches::xor::XorFilterBuilder;
use datasketches::xor::XorFilterType;

#[test]
fn values_have_no_false_negatives() {
    let mut builder = XorFilterBuilder::with_seed(XorFilterType::Xor8, 8123);
    for value in 0..10_000_u64 {
        builder.update(value);
    }
    let filter = builder.build().unwrap();
    assert_eq!(filter.filter_type(), XorFilterType::Xor8);

    for value in 0..10_000_u64 {
        assert!(filter.contains(value), "false negative for {value}");
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
        let filter = XorFilter::from_hashes(&hashes, ty).unwrap();
        assert_eq!(filter.filter_type(), ty);
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
        let mut duplicate_values = XorFilterBuilder::new(ty);
        duplicate_values.update(7_u64);
        duplicate_values.update_values([11_u64, 7, 11]);

        let mut distinct_values = XorFilterBuilder::new(ty);
        distinct_values.update_values([11_u64, 7]);
        assert_eq!(
            duplicate_values.build().unwrap(),
            distinct_values.build().unwrap(),
        );

        let mut hashes = XorFilterBuilder::new(ty);
        hashes.update_hash(7);
        hashes.update_hashes([11, 7, 11]);
        assert_eq!(
            hashes.build().unwrap(),
            XorFilter::from_hashes(&[7, 11], ty).unwrap(),
        );
    }
}

#[test]
fn default_seed_constructors_and_builder_clone_behave_consistently() {
    for ty in [
        XorFilterType::Xor8,
        XorFilterType::Xor16,
        XorFilterType::Xor32,
    ] {
        let mut default_builder = XorFilterBuilder::new(ty);
        default_builder.update_values([1_u64, 2, 3]);
        let cloned_builder = default_builder.clone();
        default_builder.update(4_u64);

        let mut zero_seeded_builder = XorFilterBuilder::with_seed(ty, 0);
        zero_seeded_builder.update_values([1_u64, 2, 3]);

        let cloned_filter = cloned_builder.build().unwrap();
        assert_eq!(cloned_filter, zero_seeded_builder.build().unwrap());
        assert_eq!(cloned_filter.filter_type(), ty);
        assert_ne!(cloned_filter, default_builder.build().unwrap());

        assert_eq!(
            XorFilter::from_hashes(&[10, 20, 30], ty).unwrap(),
            XorFilter::from_hashes_with_seed(&[10, 20, 30], ty, 0).unwrap(),
        );
    }
}

#[test]
fn equivalent_borrowed_and_value_inputs_build_identical_filters() {
    let strings = [String::from("alpha"), String::from("beta")];
    let blobs = [vec![0_u8, 1, 2], vec![3_u8, 4, 5]];
    let ids = [7_u64, 11_u64];

    for ty in [
        XorFilterType::Xor8,
        XorFilterType::Xor16,
        XorFilterType::Xor32,
    ] {
        let mut by_str_update = XorFilterBuilder::new(ty);
        for s in &strings {
            by_str_update.update(s);
        }
        let mut by_str_batch = XorFilterBuilder::new(ty);
        by_str_batch.update_values(strings.iter().map(String::as_str));
        let str_filter = by_str_update.build().unwrap();
        assert_eq!(str_filter, by_str_batch.build().unwrap());
        for s in &strings {
            assert!(str_filter.contains(s));
            assert!(str_filter.contains(s.as_str()));
        }

        let mut by_blob_update = XorFilterBuilder::new(ty);
        for blob in &blobs {
            by_blob_update.update(blob);
        }
        let mut by_blob_batch = XorFilterBuilder::new(ty);
        by_blob_batch.update_values(blobs.iter().map(Vec::as_slice));
        let blob_filter = by_blob_update.build().unwrap();
        assert_eq!(blob_filter, by_blob_batch.build().unwrap());
        for blob in &blobs {
            assert!(blob_filter.contains(blob));
            assert!(blob_filter.contains(blob.as_slice()));
        }

        let mut by_mixed_update = XorFilterBuilder::new(ty);
        for id in &ids {
            by_mixed_update.update(id);
        }
        by_mixed_update.update(raw_bytes::from_string(String::from("payload")));
        let mut by_mixed_batch = XorFilterBuilder::new(ty);
        by_mixed_batch.update_values(ids);
        by_mixed_batch.update(raw_bytes::from_str("payload"));
        let mixed_filter = by_mixed_update.build().unwrap();
        assert_eq!(mixed_filter, by_mixed_batch.build().unwrap());
        for id in &ids {
            assert!(mixed_filter.contains(id));
            assert!(mixed_filter.contains(*id));
        }
        assert!(mixed_filter.contains(raw_bytes::from_slice(b"payload")));
    }
}

#[test]
fn false_positive_rates_follow_fingerprint_width() {
    const NUM_ITEMS: u64 = 50_000;
    const NUM_QUERIES: u64 = 100_000;

    let hashes: Vec<_> = (0..NUM_ITEMS).collect();
    let xor8 = XorFilter::from_hashes(&hashes, XorFilterType::Xor8).unwrap();
    let xor16 = XorFilter::from_hashes(&hashes, XorFilterType::Xor16).unwrap();
    let xor32 = XorFilter::from_hashes(&hashes, XorFilterType::Xor32).unwrap();

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

#[test]
fn distinct_construction_is_bounded_and_does_not_deduplicate() {
    for kind in [
        XorFilterType::Xor8,
        XorFilterType::Xor16,
        XorFilterType::Xor32,
    ] {
        let error = XorFilter::from_hashes(&[1, 2, 1], kind).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        assert!(error.message().contains("duplicate hashes"));
        for keys in [vec![], vec![0], vec![u64::MAX], vec![0, u64::MAX]] {
            let f = XorFilter::from_hashes(&keys, kind).unwrap();
            for key in keys {
                assert!(f.contains_hash(key));
            }
        }
    }
}

#[test]
fn construction_recovers_after_an_unpeelable_first_attempt() {
    // This distinct set needs two attempts with base seed 1. All fingerprint widths
    // share the peeling graph, so each must reset the failed attempt's accumulators.
    let keys: Vec<_> = (917_504..917_504 + 16_384u64)
        .map(|i| {
            let mut z = i.wrapping_add(0x9e3779b97f4a7c15);
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
            z ^ (z >> 31)
        })
        .collect();
    for kind in [
        XorFilterType::Xor8,
        XorFilterType::Xor16,
        XorFilterType::Xor32,
    ] {
        let filter = XorFilter::from_hashes_with_seed(&keys, kind, 1).unwrap();
        for &key in &keys {
            assert!(filter.contains_hash(key));
        }
        let mut reversed = keys.clone();
        reversed.reverse();
        assert_eq!(
            filter,
            XorFilter::from_hashes_with_seed(&reversed, kind, 1).unwrap()
        );
    }
}
