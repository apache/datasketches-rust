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
    let mut builder = XorFilterBuilder::new(XorFilterType::Xor8).seed(8123);
    for value in 0..10_000_u64 {
        builder = builder.update(value);
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
        let duplicate_values = XorFilterBuilder::new(ty)
            .update(7_u64)
            .update_values([11_u64, 7, 11]);
        let distinct_values = XorFilterBuilder::new(ty).update_values([11_u64, 7]);
        assert_eq!(
            duplicate_values.build().unwrap(),
            distinct_values.build().unwrap(),
        );

        let hashes = XorFilterBuilder::new(ty)
            .update_hash(7)
            .update_hashes([11, 7, 11]);
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
        let default_builder = XorFilterBuilder::new(ty).update_values([1_u64, 2, 3]);
        let cloned_builder = default_builder.clone();
        let extended_builder = default_builder.update(4_u64);

        let zero_seeded_builder = XorFilterBuilder::new(ty)
            .seed(0)
            .update_values([1_u64, 2, 3]);

        let cloned_filter = cloned_builder.build().unwrap();
        assert_eq!(cloned_filter, zero_seeded_builder.build().unwrap());
        assert_eq!(cloned_filter.filter_type(), ty);
        assert_ne!(cloned_filter, extended_builder.build().unwrap());

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
            by_str_update = by_str_update.update(s);
        }
        let by_str_batch =
            XorFilterBuilder::new(ty).update_values(strings.iter().map(String::as_str));
        let str_filter = by_str_update.build().unwrap();
        assert_eq!(str_filter, by_str_batch.build().unwrap());
        for s in &strings {
            assert!(str_filter.contains(s));
            assert!(str_filter.contains(s.as_str()));
        }

        let mut by_blob_update = XorFilterBuilder::new(ty);
        for blob in &blobs {
            by_blob_update = by_blob_update.update(blob);
        }
        let by_blob_batch =
            XorFilterBuilder::new(ty).update_values(blobs.iter().map(Vec::as_slice));
        let blob_filter = by_blob_update.build().unwrap();
        assert_eq!(blob_filter, by_blob_batch.build().unwrap());
        for blob in &blobs {
            assert!(blob_filter.contains(blob));
            assert!(blob_filter.contains(blob.as_slice()));
        }

        let mut by_mixed_update = XorFilterBuilder::new(ty);
        for id in &ids {
            by_mixed_update = by_mixed_update.update(id);
        }
        by_mixed_update = by_mixed_update.update(raw_bytes::from_string(String::from("payload")));
        let by_mixed_batch = XorFilterBuilder::new(ty)
            .update_values(ids)
            .update(raw_bytes::from_str("payload"));
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
        for duplicate_hashes in [
            [1_u64, 2, 1].as_slice(),
            &[1, 1, 1],
            &[1, 2, 1, 3, 1],
            &[1, 1, 1, 1],
        ] {
            let error = XorFilter::from_hashes(duplicate_hashes, kind).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::InvalidArgument);
            assert!(error.message().contains("duplicate hashes"));

            let seeded_error =
                XorFilter::from_hashes_with_seed(duplicate_hashes, kind, 42).unwrap_err();
            assert_eq!(seeded_error.kind(), ErrorKind::InvalidArgument);
            assert!(seeded_error.message().contains("duplicate hashes"));
        }

        for keys in [vec![], vec![0], vec![u64::MAX], vec![0, u64::MAX]] {
            let f = XorFilter::from_hashes(&keys, kind).unwrap();
            for key in keys {
                assert!(f.contains_hash(key));
            }
        }
    }
}

#[test]
fn builder_chaining_and_seed_override_behave_consistently() {
    for ty in [
        XorFilterType::Xor8,
        XorFilterType::Xor16,
        XorFilterType::Xor32,
    ] {
        let chained = XorFilterBuilder::new(ty)
            .seed(42)
            .update("alpha")
            .update_hash(99)
            .update_values(["beta", "gamma"])
            .update_hashes([100, 101])
            .build()
            .unwrap();

        let mut step_by_step = XorFilterBuilder::new(ty).seed(42);
        step_by_step = step_by_step.update("alpha");
        step_by_step = step_by_step.update_hash(99);
        step_by_step = step_by_step.update_values(["beta", "gamma"]);
        step_by_step = step_by_step.update_hashes([100, 101]);
        assert_eq!(chained, step_by_step.build().unwrap());

        for value in ["alpha", "beta", "gamma"] {
            assert!(chained.contains(value));
        }
        for hash in [99, 100, 101] {
            assert!(chained.contains_hash(hash));
        }

        let overridden = XorFilterBuilder::new(ty)
            .seed(1)
            .update_values(["alpha", "beta", "gamma"])
            .seed(42)
            .build()
            .unwrap();
        assert_eq!(
            overridden,
            XorFilterBuilder::new(ty)
                .seed(42)
                .update_values(["gamma", "alpha", "beta"])
                .build()
                .unwrap(),
        );
        assert_ne!(
            overridden,
            XorFilterBuilder::new(ty)
                .seed(1)
                .update_values(["alpha", "beta", "gamma"])
                .build()
                .unwrap(),
        );
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
        let single_attempt = XorFilter::from_hashes_with_seed(&[keys[0]], kind, 1).unwrap();
        let filter = XorFilter::from_hashes_with_seed(&keys, kind, 1).unwrap();
        assert_ne!(filter.serialize()[8..16], single_attempt.serialize()[8..16]);
        for &key in &keys {
            assert!(filter.contains_hash(key));
        }
        let mut reversed = keys.clone();
        reversed.reverse();
        assert_eq!(
            filter,
            XorFilter::from_hashes_with_seed(&reversed, kind, 1).unwrap(),
        );

        let builder_filter = XorFilterBuilder::new(kind)
            .seed(99)
            .update_hashes(keys.iter().copied())
            .seed(1)
            .build()
            .unwrap();
        assert_eq!(builder_filter, filter);
    }
}
