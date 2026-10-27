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

// Generated from datasketches-java commit 1034fb33b6e013334ff870f130b9c388cae5dfa9.
// XorFilterBuilder.create([1, 2, 3, Long.MIN_VALUE], bits, 42).toByteArray().
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
fn java_images_round_trip_byte_for_byte() {
    let values = [1_u64, 2, 3, 1_u64 << 63];

    for (filter_type, image) in [
        (XorFilterType::Xor8, JAVA_XOR8_IMAGE),
        (XorFilterType::Xor16, JAVA_XOR16_IMAGE),
    ] {
        let expected = decode_hex(image);
        let built = XorFilterBuilder::new(filter_type)
            .seed(42)
            .build(values)
            .unwrap();

        let restored = XorFilter::deserialize(&expected).unwrap();
        assert_eq!(restored.serialize().unwrap(), expected);
        assert_eq!(restored.filter_type(), filter_type);
        assert_eq!(restored.num_items(), Some(values.len()));
        for value in values {
            assert!(built.contains(&value));
            assert!(
                restored.contains(&value),
                "{filter_type:?} image did not contain {value}"
            );
        }
    }
}

#[test]
fn serialization_round_trips_standard_fingerprint_types() {
    for filter_type in [XorFilterType::Xor8, XorFilterType::Xor16] {
        let original = XorFilter::from_hashes(0..10_000_u64, filter_type).unwrap();
        let bytes = original.serialize().unwrap();
        assert_eq!(bytes.len(), original.serialized_size().unwrap());

        let restored = XorFilter::deserialize(&bytes).unwrap();
        assert_eq!(restored, original);
        for hash in 0..10_000_u64 {
            assert!(restored.contains_hash(hash));
        }
    }
}

#[test]
fn every_truncated_image_is_rejected() {
    for filter_type in [XorFilterType::Xor8, XorFilterType::Xor16] {
        let bytes = XorFilter::from_hashes(0..100_u64, filter_type)
            .unwrap()
            .serialize()
            .unwrap();
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
        .serialize()
        .unwrap();

    for (offset, value) in [
        (0, 2),
        (0, 4),
        (1, 2),
        (2, 21),
        (4, 7),
        (4, 32),
        (4, 64),
        (5, 4),
    ] {
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
        .serialize()
        .unwrap();

    for segment_length in [0_i32, -1, i32::MAX] {
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
    let original = XorFilter::from_hashes(0..100_u64, XorFilterType::Xor16).unwrap();
    let mut bytes = original.serialize().unwrap();
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

#[test]
fn java_utf8_and_raw_byte_inputs_match() {
    use datasketches::hash::value::raw_bytes;
    // The same merged Java implementation, using update(String) and update(byte[]).
    let images = [
        (
            XorFilterType::Xor8,
            "0301160008030000956eeb2f2632d7bd0c0000000400000000000000000000000000000000df00000000000000000000cc0000003000000091000000",
        ),
        (
            XorFilterType::Xor16,
            "0301160010030000956eeb2f2632d7bd0c000000040000000000000000000000000000000000000000000000000000000000df500000000000000000000000000000000000000000ccb5000000000000301200000000000091b8000000000000",
        ),
    ];
    for (kind, image) in images {
        let built = XorFilterBuilder::new(kind)
            .seed(42)
            .build([
                raw_bytes::from_slice(b"hello"),
                raw_bytes::from_slice("数据🔎".as_bytes()),
                raw_bytes::from_slice(&[]),
                raw_bytes::from_slice(&[0, 1, 255]),
            ])
            .unwrap();
        let expected = decode_hex(image);
        let restored = XorFilter::deserialize(&expected).unwrap();
        assert_eq!(restored.serialize().unwrap(), expected);
        for bytes in [b"hello".as_slice(), "数据🔎".as_bytes(), &[], &[0, 1, 255]] {
            assert!(built.contains(&raw_bytes::from_slice(bytes)));
            assert!(restored.contains(&raw_bytes::from_slice(bytes)));
        }
    }
}

#[test]
fn nonstandard_filters_require_application_defined_storage() {
    let filter = XorFilter::from_hashes(0..10, XorFilterType::Xor32).unwrap();
    assert_eq!(
        filter.serialize().unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        filter.serialized_size().unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
}

#[test]
fn raw_parts_preserve_an_application_storage_layout() {
    use datasketches::codec::SketchSlice;
    use datasketches::xor::XorFingerprints;
    // Synthetic storage fixtures with public xorf 0.13.0 payloads for [0, 1, 2, u64::MAX].
    // This test-only envelope omits cardinality: magic, width:u8, segment:u32, seed:u64,
    // and little-endian slots. It is not a production storage format.
    let images = [
        concat!(
            "54455354080c000000c15c0289ec2d0a91ad000000000000000000ca000000000000000000dc00300000000000000000",
            "0000000000",
        ),
        concat!(
            "54455354100c000000c15c0289ec2d0a91aded000000000000000000000000000000000000ca18000000000000000000",
            "000000000000000000dcb9000030fc0000000000000000000000000000000000000000000000000000",
        ),
        concat!(
            "54455354200c000000c15c0289ec2d0a91adedfc84000000000000000000000000000000000000000000000000000000",
            "000000000000000000ca187b8e0000000000000000000000000000000000000000000000000000000000000000000000",
            "00dcb969ee0000000030fcf3490000000000000000000000000000000000000000000000000000000000000000000000",
            "0000000000000000000000000000000000",
        ),
    ];
    for image in images {
        let bytes = decode_hex(image);
        let mut cursor = SketchSlice::new(&bytes);
        assert_eq!(&bytes[..4], b"TEST");
        let mut magic = [0; 4];
        cursor.read_exact(&mut magic).unwrap();
        let width = cursor.read_u8().unwrap();
        let segment = cursor.read_u32_le().unwrap() as usize;
        let seed = cursor.read_u64_le().unwrap();
        let slots = cursor.remaining().len() / usize::from(width / 8);
        let payload = match width {
            8 => XorFingerprints::Xor8(cursor.remaining().into()),
            16 => {
                XorFingerprints::Xor16((0..slots).map(|_| cursor.read_u16_le().unwrap()).collect())
            }
            32 => {
                XorFingerprints::Xor32((0..slots).map(|_| cursor.read_u32_le().unwrap()).collect())
            }
            _ => unreachable!(),
        };
        let filter = XorFilter::from_parts(seed, segment, payload, None).unwrap();
        for hash in [0, 1, 2, u64::MAX] {
            assert!(filter.contains_hash(hash));
        }
        assert_eq!(filter.capacity(), slots);
        assert_eq!(filter.num_items(), None);

        let (seed, segment, payload, count) = filter.into_parts();
        assert_eq!(count, None);
        let mut encoded = b"TEST".to_vec();
        encoded.push(width);
        encoded.extend((segment as u32).to_le_bytes());
        encoded.extend(seed.to_le_bytes());
        match payload {
            XorFingerprints::Xor8(values) => encoded.extend_from_slice(&values),
            XorFingerprints::Xor16(values) => {
                encoded.extend(values.iter().flat_map(|v| v.to_le_bytes()))
            }
            XorFingerprints::Xor32(values) => {
                encoded.extend(values.iter().flat_map(|v| v.to_le_bytes()))
            }
        }
        assert_eq!(encoded, bytes);
    }
}
