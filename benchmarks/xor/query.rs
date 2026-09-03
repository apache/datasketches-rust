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

use datasketches::xor::XorFilter;
use datasketches::xor::XorFilterType;
use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;
use xorf::Filter as _;

use crate::hash_inputs::ITEMS;

#[divan::bench]
fn datasketches_xor8_present(bencher: Bencher) {
    let filter = XorFilter::from_hashes(0..ITEMS as u64, XorFilterType::Xor8).unwrap();

    bencher.counter(ItemsCount::new(ITEMS)).bench_local(|| {
        for value in 0..ITEMS as u64 {
            black_box(filter.contains_hash(black_box(value)));
        }
    });
}

#[divan::bench]
fn datasketches_xor8_absent(bencher: Bencher) {
    let filter = XorFilter::from_hashes(0..ITEMS as u64, XorFilterType::Xor8).unwrap();

    bencher.counter(ItemsCount::new(ITEMS)).bench_local(|| {
        for value in ITEMS as u64..2 * ITEMS as u64 {
            black_box(filter.contains_hash(black_box(value)));
        }
    });
}

#[divan::bench]
fn datasketches_xor16_present(bencher: Bencher) {
    let filter = XorFilter::from_hashes(0..ITEMS as u64, XorFilterType::Xor16).unwrap();

    bencher.counter(ItemsCount::new(ITEMS)).bench_local(|| {
        for value in 0..ITEMS as u64 {
            black_box(filter.contains_hash(black_box(value)));
        }
    });
}

#[divan::bench]
fn datasketches_xor16_absent(bencher: Bencher) {
    let filter = XorFilter::from_hashes(0..ITEMS as u64, XorFilterType::Xor16).unwrap();

    bencher.counter(ItemsCount::new(ITEMS)).bench_local(|| {
        for value in ITEMS as u64..2 * ITEMS as u64 {
            black_box(filter.contains_hash(black_box(value)));
        }
    });
}

#[divan::bench]
fn xorf_xor8_present(bencher: Bencher) {
    let filter = xorf::Xor8::from_iterator((0..ITEMS).map(|value| value as u64));

    bencher.counter(ItemsCount::new(ITEMS)).bench_local(|| {
        for value in 0..ITEMS as u64 {
            black_box(filter.contains(black_box(&value)));
        }
    });
}

#[divan::bench]
fn xorf_xor8_absent(bencher: Bencher) {
    let filter = xorf::Xor8::from_iterator((0..ITEMS).map(|value| value as u64));

    bencher.counter(ItemsCount::new(ITEMS)).bench_local(|| {
        for value in ITEMS as u64..2 * ITEMS as u64 {
            black_box(filter.contains(black_box(&value)));
        }
    });
}

#[divan::bench]
fn xorf_xor16_present(bencher: Bencher) {
    let filter = xorf::Xor16::from_iterator((0..ITEMS).map(|value| value as u64));

    bencher.counter(ItemsCount::new(ITEMS)).bench_local(|| {
        for value in 0..ITEMS as u64 {
            black_box(filter.contains(black_box(&value)));
        }
    });
}

#[divan::bench]
fn xorf_xor16_absent(bencher: Bencher) {
    let filter = xorf::Xor16::from_iterator((0..ITEMS).map(|value| value as u64));

    bencher.counter(ItemsCount::new(ITEMS)).bench_local(|| {
        for value in ITEMS as u64..2 * ITEMS as u64 {
            black_box(filter.contains(black_box(&value)));
        }
    });
}

#[divan::bench]
fn xorf_xor32_present(bencher: Bencher) {
    let filter = xorf::Xor32::from_iterator((0..ITEMS).map(|value| value as u64));

    bencher.counter(ItemsCount::new(ITEMS)).bench_local(|| {
        for value in 0..ITEMS as u64 {
            black_box(filter.contains(black_box(&value)));
        }
    });
}

#[divan::bench]
fn xorf_xor32_absent(bencher: Bencher) {
    let filter = xorf::Xor32::from_iterator((0..ITEMS).map(|value| value as u64));

    bencher.counter(ItemsCount::new(ITEMS)).bench_local(|| {
        for value in ITEMS as u64..2 * ITEMS as u64 {
            black_box(filter.contains(black_box(&value)));
        }
    });
}
