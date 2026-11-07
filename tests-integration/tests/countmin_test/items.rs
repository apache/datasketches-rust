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

use datasketches::countmin::CountMinItem;
use datasketches::countmin::CountMinSketch;

fn assert_same_item<I: CountMinItem, J: CountMinItem>(left: I, right: J) {
    let mut a = CountMinSketch::<i64>::new(3, 1000).unwrap();
    let mut b = a.clone();
    a.update_with_weight(&left, 7);
    b.update_with_weight(&right, 7);
    assert_eq!(a, b);
    assert_eq!(a.estimate(right), 7);
    assert_eq!(b.estimate(left), 7);
}

#[test]
fn integer_widths_use_the_same_64_bit_encoding() {
    assert_same_item(-1i8, -1i64);
    assert_same_item(-1i16, -1i64);
    assert_same_item(-1i32, -1i64);
    assert_same_item(-1isize, -1i64);
    assert_same_item(u8::MAX, 255u64);
    assert_same_item(u16::MAX, 65535u64);
    assert_same_item(u32::MAX, u32::MAX as u64);
    assert_same_item(1usize, 1u64);
    assert_same_item(1i32, 1u64);
    assert_same_item(-1i64, u64::MAX);
    assert_same_item(i64::MIN, 1u64 << 63);
    assert_same_item(0x0102_0304_0506_0708u64, [8u8, 7, 6, 5, 4, 3, 2, 1]);
}

#[test]
fn owned_and_borrowed_strings_and_bytes_identify_the_same_item() {
    let text = "été and a long UTF-8 string crossing the 32-byte hash block boundary";
    assert_same_item(text, text.to_owned());
    assert_same_item(text, text.as_bytes());
    assert_same_item(text, text.as_bytes().to_vec());
    assert_same_item("apple", *b"apple");
    assert_same_item("apple", b"apple");
    assert_same_item("été", [0xc3u8, 0xa9, 0x74, 0xc3, 0xa9]);
}

#[test]
fn empty_items_are_ignored_even_in_a_full_sketch() {
    fn check<I: CountMinItem>(sketch: &mut CountMinSketch<i64>, item: I) {
        let before = sketch.clone();
        sketch.update(&item);
        sketch.update_with_weight(&item, i64::MIN);
        assert_eq!(*sketch, before);
        assert_eq!(sketch.estimate(&item), 0);
        assert_eq!(sketch.lower_bound(&item), 0);
        assert_eq!(sketch.upper_bound(item), 0);
    }

    for filled in [false, true] {
        let mut sketch = CountMinSketch::<i64>::new(1, 3).unwrap();
        if filled {
            for i in 0..100 {
                sketch.update(i);
            }
        }
        check(&mut sketch, "");
        check(&mut sketch, String::new());
        check(&mut sketch, b"".as_slice());
        check(&mut sketch, [] as [u8; 0]);
        check(&mut sketch, b"");
        check(&mut sketch, Vec::<u8>::new());
    }
}
