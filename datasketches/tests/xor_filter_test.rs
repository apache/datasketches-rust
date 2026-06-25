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

use datasketches::xor::Xor8;

#[test]
fn test_xor8_empty() {
    let filter = Xor8::builder().build(&[]).unwrap();
    assert!(filter.is_empty());
    assert_eq!(filter.len(), 0);
    assert!(!filter.contains(123));
}

#[test]
fn test_xor8_no_false_negatives() {
    let keys: Vec<u64> = (0..10_000).collect();
    let filter = Xor8::builder().build(&keys).unwrap();

    for key in keys {
        assert!(filter.contains(key));
    }
}

#[test]
fn test_xor8_bits_per_entry() {
    let keys: Vec<u64> = (0..100_000).collect();
    let filter = Xor8::builder().build(&keys).unwrap();
    let bpe = (filter.len() as f64) * 8.0 / (keys.len() as f64);

    assert!(bpe < 10.0, "bits per entry is {}", bpe);
}

#[test]
fn test_xor8_deterministic_seed() {
    let keys: Vec<u64> = (0..1_000).collect();
    let filter1 = Xor8::builder().seed(123).build(&keys).unwrap();
    let filter2 = Xor8::builder().seed(123).build(&keys).unwrap();

    assert_eq!(filter1, filter2);
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "xor filter requires distinct keys")]
fn test_xor8_duplicate_keys_panics() {
    let keys = vec![1_u64, 2_u64, 1_u64];
    let _ = Xor8::builder().build(&keys);
}
