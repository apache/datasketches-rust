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
use datasketches::xor::XorFilterType;
use datasketches::xor::XorFingerprints;

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

    for (ty, image) in [
        (XorFilterType::Xor8, JAVA_XOR8_IMAGE),
        (XorFilterType::Xor16, JAVA_XOR16_IMAGE),
    ] {
        let expected = decode_hex(image);
        let built = XorFilter::from_values(values, ty, 42).unwrap();

        let restored = XorFilter::deserialize(&expected).unwrap();
        assert_eq!(restored.serialize().unwrap(), expected);
        // Peelability does not depend on peeling order, so both builders select the same
        // construction seed and layout. Fingerprint slots may still differ by peeling order.
        assert_eq!(
            built.serialize().unwrap()[..24],
            expected[..24],
            "{ty:?} header differs from Java"
        );
        assert_eq!(restored.filter_type(), ty);
        assert_eq!(restored.num_items(), Some(values.len()));
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
            let bytes = original.serialize().unwrap();

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
        let bytes = XorFilter::from_hashes(&hashes, ty, 0)
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
    let hashes: Vec<_> = (0..100_u64).collect();
    let valid = XorFilter::from_hashes(&hashes, XorFilterType::Xor8, 0)
        .unwrap()
        .serialize()
        .unwrap();

    for (offset, value) in [
        (0, 2),
        (0, 4),
        (1, 2),
        (2, 21),
        (4, 7),
        (4, 24),
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
    let hashes: Vec<_> = (0..100_u64).collect();
    let valid = XorFilter::from_hashes(&hashes, XorFilterType::Xor8, 0)
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
    for kind in [
        XorFilterType::Xor8,
        XorFilterType::Xor16,
        XorFilterType::Xor32,
    ] {
        let hashes: Vec<_> = (0..100_u64).collect();
        let original = XorFilter::from_hashes(&hashes, kind, 0).unwrap();
        let mut bytes = original.serialize().unwrap();
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
        let built = XorFilter::from_values(
            [
                raw_bytes::from_slice(b"hello"),
                raw_bytes::from_slice("数据🔎".as_bytes()),
                raw_bytes::from_slice(&[]),
                raw_bytes::from_slice(&[0, 1, 255]),
            ],
            kind,
            42,
        )
        .unwrap();
        let expected = decode_hex(image);
        let restored = XorFilter::deserialize(&expected).unwrap();
        assert_eq!(restored.serialize().unwrap(), expected);
        // Peelability does not depend on peeling order, so both builders select the same
        // construction seed and layout. Fingerprint slots may still differ by peeling order.
        assert_eq!(
            built.serialize().unwrap()[..24],
            expected[..24],
            "{kind:?} header differs from Java"
        );
        for bytes in [b"hello".as_slice(), "数据🔎".as_bytes(), &[], &[0, 1, 255]] {
            assert!(built.contains(&raw_bytes::from_slice(bytes)));
            assert!(restored.contains(&raw_bytes::from_slice(bytes)));
        }
    }
}

#[test]
fn xor32_image_preserves_full_width_little_endian_slots() {
    // Hand-authored layout fixture, independent of the construction algorithm:
    // family 22/version 1, width 32, three one-slot segments, one item.
    let bytes = decode_hex(concat!(
        "0301160020030000",
        "efcdab8967452301",
        "0100000001000000",
        "67452301efcdab89ffffffff",
    ));
    let filter = XorFilter::from_parts(
        0x0123_4567_89ab_cdef,
        XorFingerprints::Xor32(vec![0x0123_4567, 0x89ab_cdef, u32::MAX].into_boxed_slice()),
        Some(1),
    )
    .unwrap();
    assert_eq!(filter.serialize().unwrap(), bytes);
    assert_eq!(XorFilter::deserialize(&bytes).unwrap(), filter);
}

#[test]
fn raw_parts_preserve_an_application_storage_layout() {
    use datasketches::codec::SketchSlice;
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
        assert_eq!(segment * 3, slots);
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
        let filter = XorFilter::from_parts(seed, payload, None).unwrap();
        for hash in [0, 1, 2, u64::MAX] {
            assert!(filter.contains_hash(hash));
        }
        assert_eq!(filter.num_items(), None);

        let seed = filter.seed();
        let payload = filter.fingerprints();
        let segment = match payload {
            XorFingerprints::Xor8(values) => values.len() / 3,
            XorFingerprints::Xor16(values) => values.len() / 3,
            XorFingerprints::Xor32(values) => values.len() / 3,
        };
        let mut encoded = b"TEST".to_vec();
        encoded.push(width);
        encoded.extend((segment as u32).to_le_bytes());
        encoded.extend(seed.to_le_bytes());
        match payload {
            XorFingerprints::Xor8(values) => encoded.extend_from_slice(values),
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
