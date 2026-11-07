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

//! Bit-exact Bloom filter behavior across capacities, hash counts, seeds, and item types.
//!
//! The expected digests were recorded with `print_bloom_digests` while bit indexes were still
//! computed with the `%` operator. They cover the serialized bit array and every membership answer,
//! so any change in bit placement fails the test.

use datasketches::bloom::BloomFilter;
use datasketches::bloom::BloomFilterBuilder;
use datasketches::hash::value::raw_bytes;

/// `(num_bits, num_hashes, seed)` for manual-size filters.
const SIZED: &[(u64, u16, u64)] = &[
    (1, 1, 0),
    (1, 3, 9001),
    (63, 2, 1),
    (64, 7, 9001),
    (65, 3, 42),
    (127, 4, 7),
    (1_000, 7, 9001),
    (4_096, 5, 0),
    (4_097, 1, 9001),
    (12_345, 16, 12_345),
    (95_851, 7, 9001),
    (100_003, 2, u64::MAX),
    (131_072, 9, 9001),
    ((1 << 20) + 17, 3, 5),
    (3_000_017, 65, 9001),
];

/// `(max_items, fpp, seed)` for accuracy-mode filters.
const ACCURATE: &[(u64, f64, u64)] = &[
    (1, 0.5, 9001),
    (100, 0.01, 9001),
    (10_000, 0.01, 9001),
    (10_000, 0.001, 1),
    (50_000, 0.05, 77),
];

const ITEMS: u64 = 4_000;

fn splitmix(i: u64) -> u64 {
    let mut z = i.wrapping_add(0x9e3779b97f4a7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

fn fnv1a(digest: &mut u64, bytes: &[u8]) {
    for &byte in bytes {
        *digest ^= u64::from(byte);
        *digest = digest.wrapping_mul(0x100000001b3);
    }
}

/// Exercises `insert`, `contains_and_insert`, and `contains` and digests every answer and the
/// final serialized state.
fn digest(mut filter: BloomFilter) -> u64 {
    let mut digest = 0xcbf29ce484222325;
    for i in 0..ITEMS {
        let value = splitmix(i);
        match i % 4 {
            0 => filter.insert(value),
            1 => filter.insert(raw_bytes::from_slice(
                &value.to_le_bytes()[..(i % 9) as usize],
            )),
            2 => {
                let answer = filter.contains_and_insert(&value);
                fnv1a(&mut digest, &[u8::from(answer)]);
            }
            _ => filter.insert(format!("item-{value}")),
        }
    }
    for i in ITEMS..ITEMS * 2 {
        let value = splitmix(i);
        let answers = [
            filter.contains(&value),
            filter.contains(&splitmix(i - ITEMS)),
            filter.contains(&raw_bytes::from_slice(&value.to_le_bytes())),
        ];
        fnv1a(&mut digest, &answers.map(u8::from));
    }
    fnv1a(&mut digest, &filter.bits_used().to_le_bytes());
    fnv1a(&mut digest, &filter.serialize());
    digest
}

fn sized(num_bits: u64, num_hashes: u16, seed: u64) -> BloomFilter {
    BloomFilterBuilder::with_size(num_bits, num_hashes)
        .seed(seed)
        .build()
        .unwrap()
}

fn accurate(max_items: u64, fpp: f64, seed: u64) -> BloomFilter {
    BloomFilterBuilder::with_accuracy(max_items, fpp)
        .seed(seed)
        .build()
        .unwrap()
}

fn digests() -> Vec<u64> {
    let mut digests = vec![];
    for &(num_bits, num_hashes, seed) in SIZED {
        digests.push(digest(sized(num_bits, num_hashes, seed)));
    }
    for &(max_items, fpp, seed) in ACCURATE {
        digests.push(digest(accurate(max_items, fpp, seed)));
    }
    digests
}

const EXPECTED: &[u64] = &[
    0x4ae0cc4c54b7e6e0,
    0xfc6ea334cedd31a0,
    0xc859e0f5b936c675,
    0x5ae38343b69b20c8,
    0x1ae30896bb8c1521,
    0xa01d61c4beb1a27d,
    0x3062148dbd8d0eb7,
    0xf0b0a71e672383fc,
    0x6e34f662381298cf,
    0xd801cbd396abef14,
    0x2315dfa23723d7a6,
    0xb90ffcb4f53e3c74,
    0xa3b312e5bf7a9219,
    0x8b6dccc497edff51,
    0xd32631baa4bcce38,
    0xc331cf0cad4ccb57,
    0x76f8b788a75b417d,
    0x2315dfa23723d7a6,
    0xc4838d80cbbddae2,
    0x871a278d9fa23b72,
];

#[test]
fn bit_placement_is_stable() {
    assert_eq!(digests(), EXPECTED);
}

#[test]
fn deserialized_filters_place_bits_identically() {
    for &(num_bits, num_hashes, seed) in SIZED {
        let mut original = sized(num_bits, num_hashes, seed);
        for i in 0..64 {
            original.insert(splitmix(i));
        }
        let mut restored = BloomFilter::deserialize(&original.serialize()).unwrap();
        assert_eq!(restored, original);
        for i in 64..ITEMS {
            let value = splitmix(i);
            assert_eq!(
                restored.contains_and_insert(&value),
                original.contains_and_insert(&value)
            );
        }
        assert_eq!(restored.serialize(), original.serialize());
    }
}

#[test]
fn debug_output_lists_only_filter_state() {
    let mut filter = sized(64, 2, 3);
    filter.insert(1u64);
    let bytes = filter.serialize();
    let word = u64::from_le_bytes(bytes[bytes.len() - 8..].try_into().unwrap());
    let expected = format!(
        "BloomFilter {{ seed: 3, num_hashes: 2, num_bits_set: {}, bit_array: [{word}] }}",
        filter.bits_used()
    );
    assert_eq!(format!("{filter:?}"), expected);
}

/// Prints the digest table; run it to regenerate `EXPECTED` after an intended format change.
#[test]
#[ignore]
fn print_bloom_digests() {
    for digest in digests() {
        println!("    0x{digest:016x},");
    }
}
