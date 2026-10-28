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
use datasketches::uddsketch::UddSketch;

#[test]
fn roundtrip_then_continue_and_merge() {
    let mut direct = UddSketch::new(16, 0.001).unwrap();
    for i in -1000..1000 {
        direct.update(f64::from(i)).unwrap();
    }
    let mut restored = UddSketch::deserialize(&direct.serialize()).unwrap();
    let mut other = UddSketch::new(16, 0.001).unwrap();
    for value in [f64::from_bits(1), -f64::MAX, 1e100, 0.0, 1e-100] {
        restored.update(value).unwrap();
        other.update(value).unwrap();
    }
    direct.merge(&other).unwrap();
    assert_eq!(restored.serialize(), direct.serialize());
    assert_eq!(restored.relative_error(), direct.relative_error());
}

#[test]
fn serialize_into_replaces_previous_images() {
    let mut sketch = UddSketch::default();
    for i in -1000..1000 {
        sketch.update(f64::from(i)).unwrap();
    }
    let mut bytes = vec![0xff; 32_768];
    sketch.serialize_into(&mut bytes);
    assert_eq!(bytes, sketch.serialize());
    assert_eq!(UddSketch::deserialize(&bytes).unwrap().serialize(), bytes);
    // Successively smaller groups must not retain any bytes from the previous image.
    for values in [&[-0.0, f64::from_bits(1)][..], &[]] {
        sketch.reset();
        for &value in values {
            sketch.update(value).unwrap();
        }
        sketch.serialize_into(&mut bytes);
        assert_eq!(bytes, sketch.serialize());
        assert_eq!(UddSketch::deserialize(&bytes).unwrap().serialize(), bytes);
    }
}

#[test]
fn rust_format_fixture() {
    // This is a local format fixture, not a cross-language TCK snapshot.
    let mut sketch = UddSketch::new(3, 0.01).unwrap();
    sketch.update_weighted(0.0, 7).unwrap();
    let fixture = [
        85, 68, 68, 49, 3, 0, 0, 0, 123, 20, 174, 71, 225, 122, 132, 63, 0, 0, 0, 0, 7, 0, 0, 0, 0,
        0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0,
    ];
    assert_eq!(sketch.serialize(), fixture);
    assert_eq!(UddSketch::deserialize(&fixture).unwrap().total_weight(), 7);
}

#[test]
fn nonzero_mapping_format_fixture() {
    // These UDD1 keys pin the mapping across normal, subnormal, and extreme values.
    let values = [
        (f64::from_bits(1), 1i64),
        (f64::MIN_POSITIVE, 1803),
        (0.1, 37106),
        (1.0, 37221),
        (1.25, 37233),
        (1.5, 37242),
        (1.75, 37249),
        (f64::MAX, 72709),
    ];
    for (value, key) in values {
        for sign in [-1.0, 1.0] {
            let value = sign * value;
            let key = if sign < 0.0 { -key } else { key };
            let mut fixture = b"UDD1".to_vec();
            fixture.extend(3u32.to_le_bytes());
            fixture.extend(0.01f64.to_le_bytes());
            fixture.extend(0u32.to_le_bytes());
            fixture.extend(7u64.to_le_bytes());
            fixture.extend(1u32.to_le_bytes());
            fixture.extend(key.to_le_bytes());
            fixture.extend(7u64.to_le_bytes());
            fixture.extend(value.to_le_bytes());
            fixture.extend(value.to_le_bytes());
            let mut sketch = UddSketch::new(3, 0.01).unwrap();
            sketch.update_weighted(value, 7).unwrap();
            assert_eq!(sketch.serialize(), fixture);
            let restored = UddSketch::deserialize(&fixture).unwrap();
            assert_eq!(restored.serialize(), fixture);
            assert_eq!(restored.quantile(0.5).unwrap(), Some(value));
        }
    }
}

#[test]
fn external_coarse_image_preserves_histogram_not_replay_history() {
    let mut one = UddSketch::default();
    one.update(1.0).unwrap();
    let mut bytes = one.serialize();
    let level = 16u32;
    let key = i64::from_le_bytes(bytes[32..40].try_into().unwrap());
    bytes[16..20].copy_from_slice(&level.to_le_bytes());
    bytes[32..40].copy_from_slice(&(((key - 1) >> level) + 1).to_le_bytes());
    let mut restored = UddSketch::deserialize(&bytes).unwrap();
    assert_eq!(restored.serialize(), bytes);
    let mut other = UddSketch::default();
    for value in [2.0, 4.0] {
        other.update(value).unwrap();
        one.update(value).unwrap();
    }
    restored.merge(&other).unwrap();
    assert_eq!(restored.compaction_level(), level);
    assert_ne!(restored.serialize(), one.serialize());
    let estimate = restored.quantile(0.5).unwrap().unwrap();
    assert!((estimate - 2.0).abs() / 2.0 <= restored.relative_error());
    assert_eq!(one.quantile(0.5).unwrap(), Some(2.0));

    // Imported coarse states also support the interval fast path and checked weights.
    let image = restored.serialize();
    for value in [
        0.0,
        -1.0,
        1.0,
        1.5,
        f64::from_bits(4.0f64.to_bits() - 1),
        4.0,
        f64::MAX,
    ] {
        for weight in [0, 1, u64::MAX - restored.total_weight()] {
            let mut updated = UddSketch::deserialize(&image).unwrap();
            updated.update_weighted(value, weight).unwrap();
            let mut singleton = UddSketch::default();
            singleton.update_weighted(value, weight).unwrap();
            let mut merged = restored.clone();
            merged.merge(&singleton).unwrap();
            assert_eq!(updated.serialize(), merged.serialize());
            assert_eq!(updated.relative_error(), merged.relative_error());
            assert_eq!(
                updated.quantiles(&[0.0, 0.5, 1.0]).unwrap(),
                merged.quantiles(&[0.0, 0.5, 1.0]).unwrap()
            );
            if updated.total_weight() == u64::MAX {
                let bytes = updated.serialize();
                assert!(updated.update(value).is_err());
                assert_eq!(updated.serialize(), bytes);
            }
        }
    }
}

#[test]
fn malformed_images() {
    let mut sketch = UddSketch::new(3, 0.01).unwrap();
    for value in [-1.0, 0.0, 1.0] {
        sketch.update(value).unwrap();
    }
    let bytes = sketch.serialize();
    for len in 0..bytes.len() {
        assert!(UddSketch::deserialize(&bytes[..len]).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(UddSketch::deserialize(&trailing).is_err());
    for (offset, replacement) in [
        (3, vec![2]),
        (4, 2u32.to_le_bytes().to_vec()),
        (8, f64::NAN.to_le_bytes().to_vec()),
        (16, 64u32.to_le_bytes().to_vec()),
        (20, u64::MAX.to_le_bytes().to_vec()),
        (28, u32::MAX.to_le_bytes().to_vec()),
        (32, i64::MIN.to_le_bytes().to_vec()),
        (40, 0u64.to_le_bytes().to_vec()),
        (48, f64::NEG_INFINITY.to_le_bytes().to_vec()),
        (56, 1.0f64.to_le_bytes().to_vec()),
        (64, 0i64.wrapping_sub(1).to_le_bytes().to_vec()),
    ] {
        let mut bad = bytes.clone();
        bad[offset..offset + replacement.len()].copy_from_slice(&replacement);
        assert_eq!(
            UddSketch::deserialize(&bad).unwrap_err().kind(),
            ErrorKind::InvalidData,
            "offset {offset}"
        );
    }
    let mut overflow = bytes;
    overflow[40..48].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(UddSketch::deserialize(&overflow).is_err());
}
