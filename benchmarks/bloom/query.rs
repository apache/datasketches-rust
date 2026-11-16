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

use std::hint::black_box;

use benchmarks::Bencher;
use benchmarks::Runner;
use datasketches::bloom::BloomFilter;
use datasketches::bloom::BloomFilterBuilder;
use datasketches::hash::value::raw_bytes;

use crate::hash_inputs::ITEMS;
use crate::hash_inputs::bytes_32_values;

fn populated_u64_filter() -> BloomFilter {
    let mut filter = BloomFilterBuilder::with_accuracy(ITEMS as u64, 0.01)
        .build()
        .unwrap();
    for value in 0..ITEMS as u64 {
        filter.insert(value);
    }
    filter
}

fn populated_bytes_32_filter(values: &[[u8; 32]]) -> BloomFilter {
    let mut filter = BloomFilterBuilder::with_accuracy(ITEMS as u64, 0.01)
        .build()
        .unwrap();
    for value in values {
        filter.insert(raw_bytes::from_slice(value));
    }
    filter
}

fn present_u64(bencher: Bencher) {
    let filter = populated_u64_filter();

    bencher.bench_local(|| {
        for value in 0..ITEMS as u64 {
            black_box(filter.contains(black_box(value)));
        }
    });
}

fn absent_u64(bencher: Bencher) {
    let filter = populated_u64_filter();

    bencher.bench_local(|| {
        for value in ITEMS as u64..(ITEMS * 2) as u64 {
            black_box(filter.contains(black_box(value)));
        }
    });
}

fn present_bytes_32(bencher: Bencher) {
    let values = bytes_32_values();
    let filter = populated_bytes_32_filter(&values);

    bencher.bench_local(|| {
        for value in &values {
            black_box(filter.contains(raw_bytes::from_slice(black_box(value))));
        }
    });
}

fn absent_bytes_32(bencher: Bencher) {
    let present = bytes_32_values();
    let filter = populated_bytes_32_filter(&present);
    let absent: Vec<[u8; 32]> = present
        .into_iter()
        .map(|mut bytes| {
            // Distinct high byte ensures none of these 32-byte keys were inserted.
            bytes[31] = 1;
            bytes
        })
        .collect();

    bencher.bench_local(|| {
        for value in &absent {
            black_box(filter.contains(raw_bytes::from_slice(black_box(value))));
        }
    });
}

pub fn register(runner: &mut Runner) {
    runner.case("bloom::query::present_u64", 2_000, present_u64);
    runner.case("bloom::query::absent_u64", 2_000, absent_u64);
    runner.case("bloom::query::present_bytes_32", 1_000, present_bytes_32);
    runner.case("bloom::query::absent_bytes_32", 1_000, absent_bytes_32);
}
