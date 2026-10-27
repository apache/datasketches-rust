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

use datasketches::xor::XorFilterBuilder;
use datasketches::xor::XorFilterType;
use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

use crate::xor::hashes_with_offset;

// xorf 0.13 starts SplitMix at 1. Both builders use the same seed sequence;
// equal winning seeds therefore imply equal attempt counts. The offset 917504
// case requires two attempts, while the other listed cases require one.
fn verify_seed(keys: &[u64], kind: XorFilterType) {
    let filter = XorFilterBuilder::new(kind)
        .seed(1)
        .build_from_distinct_hashes(keys)
        .unwrap();
    assert_eq!(filter.seed(), xorf::Xor8::from(keys).seed);
}

#[divan::bench(args = [(1_000, 0), (16_384, 0), (16_384, 917_504), (49_999, 0), (50_000, 0), (50_001, 0), (100_000, 0)])]
fn datasketches_8(bencher: Bencher, case: (usize, u64)) {
    let (cardinality, offset) = case;
    let keys = hashes_with_offset(cardinality, offset);
    verify_seed(&keys, XorFilterType::Xor8);
    bencher
        .counter(ItemsCount::new(cardinality))
        .bench_local(|| {
            black_box(
                XorFilterBuilder::new(XorFilterType::Xor8)
                    .seed(1)
                    .build_from_distinct_hashes(&keys)
                    .unwrap(),
            )
        });
}

#[divan::bench(args = [(1_000, 0), (16_384, 0), (16_384, 917_504), (49_999, 0), (50_000, 0), (50_001, 0), (100_000, 0)])]
fn xorf_8(bencher: Bencher, case: (usize, u64)) {
    let (cardinality, offset) = case;
    let keys = hashes_with_offset(cardinality, offset);
    verify_seed(&keys, XorFilterType::Xor8);
    bencher
        .counter(ItemsCount::new(cardinality))
        .bench_local(|| black_box(xorf::Xor8::from_iterator(keys.iter().copied())));
}

#[divan::bench(args = [(1_000, 0), (16_384, 0), (16_384, 917_504), (49_999, 0), (50_000, 0), (50_001, 0), (100_000, 0)])]
fn datasketches_16(bencher: Bencher, case: (usize, u64)) {
    let (cardinality, offset) = case;
    let keys = hashes_with_offset(cardinality, offset);
    verify_seed(&keys, XorFilterType::Xor16);
    bencher
        .counter(ItemsCount::new(cardinality))
        .bench_local(|| {
            black_box(
                XorFilterBuilder::new(XorFilterType::Xor16)
                    .seed(1)
                    .build_from_distinct_hashes(&keys)
                    .unwrap(),
            )
        });
}

#[divan::bench(args = [(1_000, 0), (16_384, 0), (16_384, 917_504), (49_999, 0), (50_000, 0), (50_001, 0), (100_000, 0)])]
fn xorf_16(bencher: Bencher, case: (usize, u64)) {
    let (cardinality, offset) = case;
    let keys = hashes_with_offset(cardinality, offset);
    verify_seed(&keys, XorFilterType::Xor16);
    bencher
        .counter(ItemsCount::new(cardinality))
        .bench_local(|| black_box(xorf::Xor16::from_iterator(keys.iter().copied())));
}

#[divan::bench(args = [(1_000, 0), (16_384, 0), (16_384, 917_504), (49_999, 0), (50_000, 0), (50_001, 0), (100_000, 0)])]
fn datasketches_32(bencher: Bencher, case: (usize, u64)) {
    let (cardinality, offset) = case;
    let keys = hashes_with_offset(cardinality, offset);
    verify_seed(&keys, XorFilterType::Xor32);
    bencher
        .counter(ItemsCount::new(cardinality))
        .bench_local(|| {
            black_box(
                XorFilterBuilder::new(XorFilterType::Xor32)
                    .seed(1)
                    .build_from_distinct_hashes(&keys)
                    .unwrap(),
            )
        });
}

#[divan::bench(args = [(1_000, 0), (16_384, 0), (16_384, 917_504), (49_999, 0), (50_000, 0), (50_001, 0), (100_000, 0)])]
fn xorf_32(bencher: Bencher, case: (usize, u64)) {
    let (cardinality, offset) = case;
    let keys = hashes_with_offset(cardinality, offset);
    verify_seed(&keys, XorFilterType::Xor32);
    bencher
        .counter(ItemsCount::new(cardinality))
        .bench_local(|| black_box(xorf::Xor32::from_iterator(keys.iter().copied())));
}
