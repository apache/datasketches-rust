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

use std::hash::Hash;

use datasketches::countmin::CountMinSketch;
use datasketches::error::ErrorKind;
use datasketches::hash::value::natural_extend;
use datasketches::hash::value::raw_bytes;

fn assert_same_item<I: Hash, J: Hash>(left: I, right: J) {
    let mut a = CountMinSketch::<i64>::new_v2(3, 1000).unwrap();
    let mut b = a.clone();
    a.update_with_weight(&left, 7);
    b.update_with_weight(&right, 7);
    assert_eq!(a, b);
    assert_eq!(a.estimate(right), 7);
    assert_eq!(b.estimate(left), 7);
}

#[test]
fn v2_integer_wrappers_use_little_endian_64_bit_encoding() {
    assert_same_item(natural_extend::from_i8(-1), -1i64);
    assert_same_item(natural_extend::from_i16(-1), -1i64);
    assert_same_item(natural_extend::from_i32(-1), -1i64);
    assert_same_item(natural_extend::from_u8(255), 255u64);
    assert_same_item(natural_extend::from_u16(65535), 65535u64);
    assert_same_item(natural_extend::from_u32(u32::MAX), u32::MAX as u64);
    assert_same_item(-1i64, u64::MAX);
    assert_same_item(i64::MIN, 1u64 << 63);
    assert_same_item(-1i64, raw_bytes::from_slice(&[0xff; 8]));
    assert_same_item(
        0x0102_0304_0506_0708u64,
        raw_bytes::from_slice(&[8, 7, 6, 5, 4, 3, 2, 1]),
    );
}

#[test]
fn raw_byte_methods_match_wrappers_for_both_versions() {
    let text = "été and a long UTF-8 string crossing the 32-byte hash block boundary";
    for mut sketch in [
        CountMinSketch::<i64>::new(3, 1000).unwrap(),
        CountMinSketch::<i64>::new_v2(3, 1000).unwrap(),
    ] {
        sketch.update_bytes_with_weight(text.as_bytes(), 7);
        assert_eq!(sketch.estimate(raw_bytes::from_str(text)), 7);
        assert_eq!(sketch.estimate_bytes(text.as_bytes()), 7);
        assert_eq!(
            sketch.lower_bound_bytes(text.as_bytes()),
            sketch.lower_bound(raw_bytes::from_str(text))
        );
        assert_eq!(
            sketch.upper_bound_bytes(text.as_bytes()),
            sketch.upper_bound(raw_bytes::from_str(text))
        );
        sketch.update(raw_bytes::from_string(text.to_owned()));
        sketch.update(raw_bytes::from_vec(text.as_bytes().to_vec()));
        assert_eq!(sketch.estimate_bytes(text.as_bytes()), 9);
    }
}

#[test]
fn byte_methods_ignore_empty_inputs_even_in_a_full_sketch() {
    for filled in [false, true] {
        for mut sketch in [
            CountMinSketch::<i64>::new(1, 3).unwrap(),
            CountMinSketch::<i64>::new_v2(1, 3).unwrap(),
        ] {
            if filled {
                for i in 0..100u64 {
                    sketch.update(i);
                }
            }
            let before = sketch.clone();
            sketch.update_bytes(b"");
            sketch.update_bytes_with_weight(b"", i64::MIN);
            assert_eq!(sketch, before);
            assert_eq!(sketch.estimate_bytes(b""), 0);
            assert_eq!(sketch.lower_bound_bytes(b""), 0);
            assert_eq!(sketch.upper_bound_bytes(b""), 0);
        }
    }
}

#[test]
fn only_v2_constructors_limit_the_row_count_to_127() {
    for rows in [128, 255] {
        let legacy = CountMinSketch::<i64>::with_seed(rows, 3, 42).unwrap();
        assert_eq!(legacy.num_hashes(), rows);
        assert_eq!(
            CountMinSketch::<i64>::deserialize_with_seed(&legacy.serialize(), 42).unwrap(),
            legacy
        );
        assert_eq!(
            CountMinSketch::<i64>::with_seed_v2(rows, 3, 42)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidArgument
        );
    }
    assert!(CountMinSketch::<i64>::new_v2(127, 3).is_ok());
    assert!(CountMinSketch::<i64>::new_v2(0, 3).is_err());
}
