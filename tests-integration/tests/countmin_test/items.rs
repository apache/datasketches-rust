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
use datasketches::hash::value::natural_extend;
use datasketches::hash::value::raw_bytes;

#[test]
fn empty_encodings_do_not_update_or_query_counters() {
    fn check<I: Hash>(sketch: &mut CountMinSketch<i64>, item: I) {
        let before = sketch.clone();
        sketch.update_with_weight(&item, 5);
        assert_eq!(*sketch, before);
        sketch.update_with_weight(&item, i64::MIN);
        assert_eq!(*sketch, before);
        assert_eq!(sketch.estimate(&item), 0);
        assert_eq!(sketch.lower_bound(&item), 0);
        assert_eq!(sketch.upper_bound(&item), 0);
    }

    let mut sketch = CountMinSketch::<i64>::new(1, 3).unwrap();
    for populated in [false, true] {
        if populated {
            // Integer 2 and the empty encoding share a bucket with these parameters.
            sketch.update_with_weight(2u64, 5);
        }
        check(&mut sketch, raw_bytes::from_str(""));
        check(&mut sketch, raw_bytes::from_slice(&[]));
        check(&mut sketch, ());
    }
}

fn assert_same_item<I: Hash, J: Hash>(left: I, right: J) {
    let mut a = CountMinSketch::<i64>::new(3, 1000).unwrap();
    let mut b = a.clone();
    a.update_with_weight(&left, 7);
    b.update_with_weight(&right, 7);
    assert_eq!(a, b);
}

#[test]
fn integer_inputs_use_little_endian_64_bit_encoding() {
    assert_same_item(-2i8, -2i64);
    assert_same_item(-2i16, -2i64);
    assert_same_item(-2i32, -2i64);
    assert_same_item(-2isize, -2i64);
    assert_same_item(u8::MAX, u8::MAX as u64);
    assert_same_item(u16::MAX, u16::MAX as u64);
    assert_same_item(u32::MAX, u32::MAX as u64);
    assert_same_item(usize::MAX, usize::MAX as u64);
    assert_same_item(natural_extend::from_i8(-2), -2i64);
    assert_same_item(natural_extend::from_u8(255), 255u64);
    assert_same_item(
        -2i64,
        raw_bytes::from_slice(&[0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
    );
    assert_same_item(
        0x0102_0304_0506_0708u64,
        raw_bytes::from_slice(&[8, 7, 6, 5, 4, 3, 2, 1]),
    );
}
