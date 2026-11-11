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

// Generated from datasketches-java commit 1ed3eab435a4767d672ce5797eb53f66b14d78bb.
// XorFilterBuilder.create([1, 2, 3, Long.MIN_VALUE], bits, 42).toByteArray().
const JAVA_XOR8_IMAGE: &str = concat!(
    "0301160008000000956eeb2f2632d7bd0c00000000000000",
    "000000000000000400dd0000000000000000ce0000c50000000000000000000000000000"
);
const JAVA_XOR16_IMAGE: &str = concat!(
    "0301160010000000956eeb2f2632d7bd0c00000000000000",
    "000000000000000000000000000004e80000dd6e00000000000000000000000000000000",
    "ce9500000000c5b600000000000000000000000000000000000000000000000000000000",
);

#[test]
fn java_images_round_trip_byte_for_byte() {
    let values = [1_u64, 2, 3, 1_u64 << 63];

    for (ty, image) in [
        (XorFilterType::Xor8, JAVA_XOR8_IMAGE),
        (XorFilterType::Xor16, JAVA_XOR16_IMAGE),
    ] {
        let expected = decode_hex(image);
        let mut builder = XorFilterBuilder::new(ty, 42);
        builder.update_values(values.iter());
        let built = builder.build().unwrap();

        let restored = XorFilter::deserialize(&expected).unwrap();
        assert_eq!(restored.serialize(), expected);
        assert_eq!(built.serialize(), expected);
        for value in values {
            assert!(built.contains(&value));
            assert!(
                restored.contains(&value),
                "{ty:?} image did not contain {value}"
            );
        }
    }
}

#[test]
fn serialization_round_trips_standard_fingerprint_types() {
    for ty in [
        XorFilterType::Xor8,
        XorFilterType::Xor16,
        XorFilterType::Xor32,
    ] {
        for count in [0, 1, 10_000_u64] {
            let hashes: Vec<_> = (0..count).collect();
            let original = XorFilter::from_hashes(&hashes, ty, 0).unwrap();
            let bytes = original.serialize();

            let restored = XorFilter::deserialize(&bytes).unwrap();
            assert_eq!(restored, original);
            for hash in 0..count {
                assert!(restored.contains_hash(hash));
            }
        }
    }
}

#[test]
fn every_truncated_image_is_rejected() {
    for ty in [
        XorFilterType::Xor8,
        XorFilterType::Xor16,
        XorFilterType::Xor32,
    ] {
        let hashes: Vec<_> = (0..100_u64).collect();
        let bytes = XorFilter::from_hashes(&hashes, ty, 0).unwrap().serialize();
        for end in 0..bytes.len() {
            let error = XorFilter::deserialize(&bytes[..end]).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::InvalidData, "length {end}");
        }
        assert!(XorFilter::deserialize(&bytes).is_ok());
    }
}

#[test]
fn invalid_preamble_fields_are_rejected() {
    let hashes: Vec<_> = (0..100_u64).collect();
    let valid = XorFilter::from_hashes(&hashes, XorFilterType::Xor8, 0)
        .unwrap()
        .serialize();

    for (offset, value) in [
        (0, 1),
        (0, 2),
        (1, 0),
        (1, 2),
        (2, 21),
        (3, 1),
        (4, 7),
        (4, 24),
        (4, 64),
    ] {
        let mut corrupted = valid.clone();
        corrupted[offset] = value;
        let error = XorFilter::deserialize(&corrupted).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData, "offset {offset}");
    }
}

#[test]
fn unsafe_lengths_are_rejected_before_indexing() {
    let hashes: Vec<_> = (0..100_u64).collect();
    let valid = XorFilter::from_hashes(&hashes, XorFilterType::Xor8, 0)
        .unwrap()
        .serialize();

    for segment_length in [0_i32, -1, i32::MAX] {
        let mut corrupted = valid.clone();
        corrupted[16..20].copy_from_slice(&segment_length.to_le_bytes());
        assert_eq!(
            XorFilter::deserialize(&corrupted).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }
}

#[test]
fn trailing_storage_is_ignored() {
    for kind in [
        XorFilterType::Xor8,
        XorFilterType::Xor16,
        XorFilterType::Xor32,
    ] {
        let hashes: Vec<_> = (0..100_u64).collect();
        let original = XorFilter::from_hashes(&hashes, kind, 0).unwrap();
        let mut bytes = original.serialize();
        bytes.extend_from_slice(&[0xaa; 16]);

        assert_eq!(XorFilter::deserialize(&bytes).unwrap(), original);
    }
}

fn decode_hex(input: &str) -> Vec<u8> {
    assert_eq!(input.len() % 2, 0);
    input
        .as_bytes()
        .chunks_exact(2)
        .map(|digits| {
            let digits = std::str::from_utf8(digits).unwrap();
            u8::from_str_radix(digits, 16).unwrap()
        })
        .collect()
}

#[test]
fn java_utf8_and_raw_byte_inputs_match() {
    use datasketches::hash::value::raw_bytes;
    // The same Java revision, using update(String) and update(byte[]).
    let images = [
        (
            XorFilterType::Xor8,
            "0301160008000000956eeb2f2632d7bd0c00000000000000ccdf00000000000000ef0091000000000000000000000000000000000000000000000000",
        ),
        (
            XorFilterType::Xor16,
            "0301160010000000956eeb2f2632d7bd0c00000000000000ccb5df500000000000000000000000000000ef42000091b8000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
        ),
    ];
    for (kind, image) in images {
        let mut builder = XorFilterBuilder::new(kind, 42);
        for bytes in [b"hello".as_slice(), "数据🔎".as_bytes(), &[], &[0, 1, 255]] {
            builder.update(&raw_bytes::from_slice(bytes));
        }
        let built = builder.build().unwrap();
        let expected = decode_hex(image);
        let restored = XorFilter::deserialize(&expected).unwrap();
        assert_eq!(restored.serialize(), expected);
        assert_eq!(built.serialize(), expected);
        for bytes in [b"hello".as_slice(), "数据🔎".as_bytes(), &[], &[0, 1, 255]] {
            assert!(built.contains(&raw_bytes::from_slice(bytes)));
            assert!(restored.contains(&raw_bytes::from_slice(bytes)));
        }
    }
}

#[test]
fn xor32_image_preserves_full_width_little_endian_slots() {
    // Hand-authored layout fixture, independent of the construction algorithm:
    // family 22/version 1, width 32, three one-slot segments.
    let bytes = decode_hex(concat!(
        "0301160020000000",
        "efcdab8967452301",
        "0100000000000000",
        "67452301efcdab899c5f4186",
    ));
    let filter = XorFilter::deserialize(&bytes).unwrap();
    assert!(filter.contains_hash(0));
    assert_eq!(filter.serialize(), bytes);
}
