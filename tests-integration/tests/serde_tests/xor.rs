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

const JAVA_XOR8_IMAGE: &str = concat!(
    "0301160008030000956eeb2f2632d7bd0c00000004000000",
    "00000000000000000000c500000000000000000000000000dd000000040000000b000000"
);
const JAVA_XOR16_IMAGE: &str = concat!(
    "0301160010030000",
    "956eeb2f2632d7bd",
    "0c00000004000000",
    "0000000000000000",
    "0000000000000000",
    "00000000c5b60000",
    "0000000000000000",
    "0000000000000000",
    "0000000000000000",
    "dd6e000000000000",
    "04e8000000000000",
    "0b23000000000000",
);

#[test]
fn java_images_match_byte_for_byte() {
    let values = [1_u64, 2, 3, 1_u64 << 63];

    for (filter_type, image) in [
        (XorFilterType::Xor8, JAVA_XOR8_IMAGE),
        (XorFilterType::Xor16, JAVA_XOR16_IMAGE),
    ] {
        let expected = decode_hex(image);
        let mut builder = XorFilterBuilder::new(filter_type).seed(42);
        builder.extend(values);
        assert_eq!(builder.build().unwrap().serialize(), expected);

        let restored = XorFilter::deserialize(&expected).unwrap();
        assert_eq!(restored.filter_type(), filter_type);
        assert_eq!(restored.num_items(), values.len());
        for value in values {
            assert!(
                restored.contains(&value),
                "{filter_type:?} image did not contain {value}"
            );
        }
    }
}

#[test]
fn serialization_round_trips_all_fingerprint_types() {
    for filter_type in [
        XorFilterType::Xor8,
        XorFilterType::Xor16,
        XorFilterType::Xor32,
    ] {
        let original = XorFilter::from_hashes(0..10_000_u64, filter_type).unwrap();
        let bytes = original.serialize();
        assert_eq!(bytes.len(), original.serialized_size());

        let restored = XorFilter::deserialize(&bytes).unwrap();
        assert_eq!(restored, original);
        for hash in 0..10_000_u64 {
            assert!(restored.contains_hash(hash));
        }
    }
}

#[test]
fn every_truncated_image_is_rejected() {
    for filter_type in [
        XorFilterType::Xor8,
        XorFilterType::Xor16,
        XorFilterType::Xor32,
    ] {
        let bytes = XorFilter::from_hashes(0..100_u64, filter_type)
            .unwrap()
            .serialize();
        for end in 0..bytes.len() {
            let error = XorFilter::deserialize(&bytes[..end]).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::InvalidData, "length {end}");
        }
        assert!(XorFilter::deserialize(&bytes).is_ok());
    }
}

#[test]
fn invalid_preamble_fields_are_rejected() {
    let valid = XorFilter::from_hashes(0..100_u64, XorFilterType::Xor8)
        .unwrap()
        .serialize();

    for (offset, value) in [(0, 2), (0, 4), (1, 2), (2, 21), (4, 7), (4, 64), (5, 4)] {
        let mut corrupted = valid.clone();
        corrupted[offset] = value;
        let error = XorFilter::deserialize(&corrupted).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData, "offset {offset}");
    }
}

#[test]
fn unsafe_lengths_are_rejected_before_indexing() {
    let valid = XorFilter::from_hashes(0..100_u64, XorFilterType::Xor8)
        .unwrap()
        .serialize();

    for segment_length in [0_i32, -1] {
        let mut corrupted = valid.clone();
        corrupted[16..20].copy_from_slice(&segment_length.to_le_bytes());
        assert_eq!(
            XorFilter::deserialize(&corrupted).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }

    for num_items in [-1_i32, i32::MAX] {
        let mut corrupted = valid.clone();
        corrupted[20..24].copy_from_slice(&num_items.to_le_bytes());
        assert_eq!(
            XorFilter::deserialize(&corrupted).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }
}

#[test]
fn trailing_storage_is_ignored() {
    let original = XorFilter::from_hashes(0..100_u64, XorFilterType::Xor32).unwrap();
    let mut bytes = original.serialize();
    bytes.extend_from_slice(&[0xaa; 16]);

    assert_eq!(XorFilter::deserialize(&bytes).unwrap(), original);
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
