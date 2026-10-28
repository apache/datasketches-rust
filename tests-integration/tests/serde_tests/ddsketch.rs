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

use datasketches::ddsketch::CollapseDirection;
use datasketches::ddsketch::DdSketch;
use datasketches::error::ErrorKind;

#[test]
fn roundtrip_preserves_queries_and_future_operations() {
    for direction in [CollapseDirection::Lowest, CollapseDirection::Highest] {
        for values in [
            vec![],
            vec![-0.0],
            vec![f64::from_bits(1)],
            vec![1.0, 1.01, 1.02],
            vec![-f64::MAX, -1.0, 0.0, 1.0, f64::MAX],
            vec![
                -f64::MIN_POSITIVE,
                -f64::from_bits(1),
                f64::from_bits(1),
                f64::MIN_POSITIVE,
            ],
        ] {
            let mut original = DdSketch::with_collapse_direction(8, 0.01, direction).unwrap();
            for value in values {
                original.update_weighted(value, 3).unwrap();
            }
            let bytes = original.serialize();
            let mut restored = DdSketch::deserialize(&bytes).unwrap();
            assert_eq!(restored.serialize(), bytes);
            for q in [0.0, 0.01, 0.5, 0.9, 1.0] {
                assert_eq!(original.quantile(q).unwrap(), restored.quantile(q).unwrap());
                assert_eq!(
                    original.quantile_is_collapsed(q).unwrap(),
                    restored.quantile_is_collapsed(q).unwrap()
                );
            }
            for value in [-1e200, -7.0, 0.0, 7.0, 1e200] {
                original.update(value).unwrap();
                restored.update(value).unwrap();
            }
            let other = restored.clone();
            original.merge(&other).unwrap();
            restored.merge(&other).unwrap();
            assert_eq!(original.serialize(), restored.serialize());

            let mut output = vec![0; bytes.len() + 100];
            let capacity = output.capacity();
            DdSketch::deserialize(&bytes)
                .unwrap()
                .serialize_into(&mut output);
            assert_eq!(output, bytes);
            assert_eq!(output.capacity(), capacity);
        }
    }
}

#[test]
fn version_one_image_is_readable_without_the_writer() {
    // Independently constructed image: gamma = 2, positive bins (1, 2] and
    // (2, 4] with weights 2 and 3. This pins the format as well as its meaning.
    let mut bytes = Vec::from(*b"DDS1");
    bytes.extend_from_slice(&8u32.to_le_bytes());
    bytes.extend_from_slice(&(1.0f64 / 3.0).to_le_bytes());
    bytes.extend_from_slice(&[0, 0, 0, 0]);
    bytes.extend_from_slice(&5u64.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&1.5f64.to_le_bytes());
    bytes.extend_from_slice(&3.0f64.to_le_bytes());
    bytes.extend_from_slice(&0i64.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&1i64.to_le_bytes());
    bytes.extend_from_slice(&2u32.to_le_bytes());
    bytes.extend_from_slice(&2u64.to_le_bytes());
    bytes.extend_from_slice(&3u64.to_le_bytes());
    let sketch = DdSketch::deserialize(&bytes).unwrap();
    assert_eq!(sketch.total_weight(), 5);
    assert!((sketch.quantile(0.5).unwrap().unwrap() - 8.0 / 3.0).abs() < 1e-14);
    assert_eq!(sketch.quantile(0.0).unwrap(), Some(1.5));
    assert_eq!(sketch.quantile(1.0).unwrap(), Some(3.0));
    assert_eq!(sketch.serialize(), bytes);
}

#[test]
fn logarithmic_boundary_rounding_does_not_prevent_reading() {
    let mut sketch = DdSketch::new(8, 1.0 / 3.0).unwrap();
    sketch.update(2.0).unwrap();
    // At a power of gamma, platform math can place the value on either side
    // of the bucket boundary. Both images must remain usable after decoding.
    for index in [1i64, 2] {
        let mut bytes = sketch.serialize();
        bytes[64..72].copy_from_slice(&index.to_le_bytes());
        let mut restored = DdSketch::deserialize(&bytes).unwrap();
        assert_eq!(restored.quantile(0.5).unwrap(), Some(2.0));
        restored.update(8.0).unwrap();
        DdSketch::deserialize(&restored.serialize()).unwrap();
    }
    let mut bytes = sketch.serialize();
    bytes[64..72].copy_from_slice(&3i64.to_le_bytes());
    assert!(DdSketch::deserialize(&bytes).is_err());
}

#[test]
fn malformed_headers_counts_and_extrema_are_rejected() {
    let mut sketch = DdSketch::new(8, 0.1).unwrap();
    sketch.update(-10.0).unwrap();
    sketch.update(10.0).unwrap();
    let original = sketch.serialize();
    for end in 0..original.len() {
        assert_eq!(
            DdSketch::deserialize(&original[..end]).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }
    let mut extra = original.clone();
    extra.push(0);
    assert!(DdSketch::deserialize(&extra).is_err());
    let corruptions: Vec<(usize, Vec<u8>)> = vec![
        (0, b"DDS2".to_vec()),
        (4, 0u32.to_le_bytes().to_vec()),
        (8, f64::NAN.to_le_bytes().to_vec()),
        (8, 0.0f64.to_le_bytes().to_vec()),
        (16, vec![2]),
        (17, vec![2]),
        (18, vec![1]),
        (19, vec![1]),
        (20, 0u64.to_le_bytes().to_vec()),
        (28, u64::MAX.to_le_bytes().to_vec()),
        (36, f64::NAN.to_le_bytes().to_vec()),
        (36, 11.0f64.to_le_bytes().to_vec()),
        (44, f64::INFINITY.to_le_bytes().to_vec()),
        (44, (-0.0f64).to_le_bytes().to_vec()),
        (52, i64::MIN.to_le_bytes().to_vec()),
        (60, u32::MAX.to_le_bytes().to_vec()),
        (64, i64::MAX.to_le_bytes().to_vec()),
        (72, u32::MAX.to_le_bytes().to_vec()),
        (76, 0u64.to_le_bytes().to_vec()),
        (76, u64::MAX.to_le_bytes().to_vec()),
    ];
    for (offset, replacement) in corruptions {
        let mut bytes = original.clone();
        bytes[offset..offset + replacement.len()].copy_from_slice(&replacement);
        assert_eq!(
            DdSketch::deserialize(&bytes).unwrap_err().kind(),
            ErrorKind::InvalidData,
            "offset {offset}"
        );
    }
}

#[test]
fn mutated_images_are_rejected_or_remain_safe_to_use() {
    let mut sketch = DdSketch::new(4, 0.1).unwrap();
    for value in [-1e300, -1.0, 0.0, 1.0, 1e300] {
        sketch.update(value).unwrap();
    }
    let original = sketch.serialize();
    for offset in 0..original.len() {
        for bit in 0..8 {
            let mut bytes = original.clone();
            bytes[offset] ^= 1 << bit;
            if let Ok(mut restored) = DdSketch::deserialize(&bytes) {
                assert_eq!(restored.serialize(), bytes);
                for q in [0.0, 0.25, 0.5, 0.75, 1.0] {
                    assert!(restored.quantile(q).unwrap().unwrap().is_finite());
                }
                restored.update(0.0).unwrap();
                let _ = restored.update(-f64::MAX);
                DdSketch::deserialize(&restored.serialize()).unwrap();
            }
        }
    }
}
