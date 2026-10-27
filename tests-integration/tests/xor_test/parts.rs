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
use datasketches::xor::XorFilter;
use datasketches::xor::XorFilterBuilder;
use datasketches::xor::XorFilterType;
use datasketches::xor::XorFingerprints;
use xorf::Filter;

#[test]
fn raw_parts_interoperate_with_xorf_in_both_directions() {
    let keys: Vec<_> = (0..16_384u64)
        .map(|n| n.wrapping_mul(0x9e3779b97f4a7c15))
        .collect();
    for kind in [
        XorFilterType::Xor8,
        XorFilterType::Xor16,
        XorFilterType::Xor32,
    ] {
        let (seed, segment, payload) = match kind {
            XorFilterType::Xor8 => {
                let f = xorf::Xor8::from(&keys);
                (
                    f.seed,
                    f.block_length,
                    XorFingerprints::Xor8(f.fingerprints),
                )
            }
            XorFilterType::Xor16 => {
                let f = xorf::Xor16::from(&keys);
                (
                    f.seed,
                    f.block_length,
                    XorFingerprints::Xor16(f.fingerprints),
                )
            }
            XorFilterType::Xor32 => {
                let f = xorf::Xor32::from(&keys);
                (
                    f.seed,
                    f.block_length,
                    XorFingerprints::Xor32(f.fingerprints),
                )
            }
        };
        let imported = XorFilter::from_parts(seed, segment, payload, None).unwrap();
        assert_eq!(imported.num_items(), None);
        assert_eq!(imported.bits_per_item(), None);
        assert_eq!(imported.is_empty(), None);
        assert_eq!(
            imported.serialize().unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        for &key in &keys {
            assert!(imported.contains_hash(key));
        }

        let built = XorFilter::from_distinct_hashes(&keys, kind).unwrap();
        let (seed, block_length, payload, count) = built.clone().into_parts();
        assert_eq!(count, Some(keys.len()));
        // Compare all answers, including false positives, after transferring ownership.
        let reference: Box<dyn Filter<u64>> = match payload {
            XorFingerprints::Xor8(fingerprints) => Box::new(xorf::Xor8 {
                seed,
                block_length,
                fingerprints,
            }),
            XorFingerprints::Xor16(fingerprints) => Box::new(xorf::Xor16 {
                seed,
                block_length,
                fingerprints,
            }),
            XorFingerprints::Xor32(fingerprints) => Box::new(xorf::Xor32 {
                seed,
                block_length,
                fingerprints,
            }),
        };
        for key in keys.iter().copied().chain(0..100_000) {
            assert_eq!(built.contains_hash(key), reference.contains(&key));
        }
    }
}

#[test]
fn raw_parts_reject_invalid_layouts_and_counts() {
    for (segment, length, count) in [
        (0, 0, None),
        (1, 2, None),
        (1, 4, None),
        (usize::MAX, 3, None),
        (1, 3, Some(4)),
    ] {
        assert_eq!(
            XorFilter::from_parts(
                0,
                segment,
                XorFingerprints::Xor8(vec![0; length].into()),
                count
            )
            .unwrap_err()
            .kind(),
            ErrorKind::InvalidArgument
        );
    }
}

#[test]
fn distinct_construction_is_bounded_and_does_not_deduplicate() {
    for kind in [
        XorFilterType::Xor8,
        XorFilterType::Xor16,
        XorFilterType::Xor32,
    ] {
        let error = XorFilter::from_distinct_hashes(&[1, 2, 1], kind).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ConstructionFailed);
        assert!(error.message().contains("100 attempts"));
        for keys in [vec![], vec![0], vec![u64::MAX], vec![0, u64::MAX]] {
            let f = XorFilter::from_distinct_hashes(&keys, kind).unwrap();
            assert_eq!(f.num_items(), Some(keys.len()));
            for key in keys {
                assert!(f.contains_hash(key));
            }
        }
    }
}

#[test]
fn empty_filters_never_match_even_when_the_fingerprint_is_zero() {
    for kind in [
        XorFilterType::Xor8,
        XorFilterType::Xor16,
        XorFilterType::Xor32,
    ] {
        let f = XorFilter::from_hashes([], kind).unwrap();
        // This key mixes to zero under the filter's construction seed.
        assert!(!f.contains_hash(f.seed().wrapping_neg()));
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
    let first_seed = XorFilterBuilder::new(XorFilterType::Xor8)
        .seed(1)
        .build_from_distinct_hashes(&[])
        .unwrap()
        .seed();
    let reference = xorf::Xor8::from(&keys);
    assert_ne!(reference.seed, first_seed);
    for kind in [
        XorFilterType::Xor8,
        XorFilterType::Xor16,
        XorFilterType::Xor32,
    ] {
        let filter = XorFilterBuilder::new(kind)
            .seed(1)
            .build_from_distinct_hashes(&keys)
            .unwrap();
        assert_eq!(filter.seed(), reference.seed);
        for &key in &keys {
            assert!(filter.contains_hash(key));
        }
        let mut reversed = keys.clone();
        reversed.reverse();
        assert_eq!(
            filter,
            XorFilterBuilder::new(kind)
                .seed(1)
                .build_from_distinct_hashes(&reversed)
                .unwrap()
        );
    }
}

#[test]
fn unknown_count_does_not_infer_emptiness_from_zero_fingerprints() {
    for kind in [
        XorFilterType::Xor8,
        XorFilterType::Xor16,
        XorFilterType::Xor32,
    ] {
        let empty = XorFilter::from_hashes([], kind).unwrap();
        let zero_hash = empty.seed().wrapping_neg();
        assert_eq!(empty.is_empty(), Some(true));
        assert!(!empty.contains_hash(zero_hash));
        let (seed, segment, payload, _) = empty.into_parts();
        let unknown = XorFilter::from_parts(seed, segment, payload, None).unwrap();
        assert_eq!(unknown.is_empty(), None);
        assert_eq!(unknown.num_items(), None);
        assert_eq!(unknown.bits_per_item(), None);
        // An omitted count must preserve the raw fingerprint comparison, even for all-zero slots.
        assert!(unknown.contains_hash(zero_hash));
        assert_eq!(
            unknown.serialized_size().unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(
            unknown.serialize().unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        let (seed, segment, payload, count) = unknown.into_parts();
        assert_eq!(count, None);
        let restored = XorFilter::from_parts(seed, segment, payload, count).unwrap();
        assert_eq!(restored.is_empty(), None);
        assert!(restored.contains_hash(zero_hash));
    }
}
