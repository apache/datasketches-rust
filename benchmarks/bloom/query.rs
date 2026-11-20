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
use crate::hash_inputs::bytes_64_values;
use crate::hash_inputs::variable_byte_slices;

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

fn populated_variable_bytes_filter(keys: &[&[u8]]) -> BloomFilter {
    let mut filter = BloomFilterBuilder::with_accuracy(ITEMS as u64, 0.01)
        .build()
        .unwrap();
    for &key in keys {
        filter.insert(raw_bytes::from_slice(key));
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

fn present_variable_bytes(bencher: Bencher) {
    let values = bytes_64_values();
    let keys = variable_byte_slices(&values);
    let filter = populated_variable_bytes_filter(&keys);

    bencher.bench_local(|| {
        for &key in &keys {
            black_box(filter.contains(raw_bytes::from_slice(black_box(key))));
        }
    });
}

fn absent_variable_bytes(bencher: Bencher) {
    let present = bytes_64_values();
    let present_keys = variable_byte_slices(&present);
    let filter = populated_variable_bytes_filter(&present_keys);
    let absent: Vec<[u8; 64]> = (0..ITEMS)
        .map(|i| {
            let mut bytes = [0; 64];
            // Odd low byte avoids 1-byte prefixes (where `i % 64 == 0`), and values at or above
            // `ITEMS` avoid all longer prefixes of `0..ITEMS`.
            let value = ((ITEMS + i) as u64) | 1;
            bytes[..8].copy_from_slice(&value.to_le_bytes());
            bytes
        })
        .collect();
    let absent_keys = variable_byte_slices(&absent);

    bencher.bench_local(|| {
        for &key in &absent_keys {
            black_box(filter.contains(raw_bytes::from_slice(black_box(key))));
        }
    });
}

pub fn register(runner: &mut Runner) {
    runner.case("bloom::query::present_u64", 2_000, present_u64);
    runner.case("bloom::query::absent_u64", 2_000, absent_u64);
    runner.case("bloom::query::present_bytes_32", 1_000, present_bytes_32);
    runner.case("bloom::query::absent_bytes_32", 1_000, absent_bytes_32);
    runner.case(
        "bloom::query::present_variable_bytes",
        1_200,
        present_variable_bytes,
    );
    runner.case(
        "bloom::query::absent_variable_bytes",
        1_200,
        absent_variable_bytes,
    );
}
