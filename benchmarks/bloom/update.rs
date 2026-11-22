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
use datasketches::bloom::BloomFilterBuilder;
use datasketches::hash::value::raw_bytes;

use crate::hash_inputs::ITEMS;
use crate::hash_inputs::bytes_32_values;
use crate::hash_inputs::bytes_64_values;
use crate::hash_inputs::u64_pair;
use crate::hash_inputs::variable_byte_slices;

fn u64(bencher: Bencher) {
    bencher.bench_local(|| {
        let mut filter = BloomFilterBuilder::with_accuracy(ITEMS as u64, 0.01)
            .build()
            .unwrap();
        for value in 0..ITEMS as u64 {
            filter.insert(black_box(value));
        }
        black_box(filter)
    });
}

fn u64_tuple(bencher: Bencher) {
    bencher.bench_local(|| {
        let mut filter = BloomFilterBuilder::with_accuracy(ITEMS as u64, 0.01)
            .build()
            .unwrap();
        for value in 0..ITEMS as u64 {
            filter.insert(black_box(u64_pair(value)));
        }
        black_box(filter)
    });
}

fn bytes_32(bencher: Bencher) {
    let values = bytes_32_values();

    bencher.bench_local(|| {
        let mut filter = BloomFilterBuilder::with_accuracy(ITEMS as u64, 0.01)
            .build()
            .unwrap();
        for value in &values {
            filter.insert(raw_bytes::from_slice(black_box(value)));
        }
        black_box(filter)
    });
}

fn variable_bytes(bencher: Bencher) {
    let values = bytes_64_values();
    let keys = variable_byte_slices(&values);

    bencher.bench_local(|| {
        let mut filter = BloomFilterBuilder::with_accuracy(ITEMS as u64, 0.01)
            .build()
            .unwrap();
        for &key in &keys {
            filter.insert(raw_bytes::from_slice(black_box(key)));
        }
        black_box(filter)
    });
}

fn prefixed_bytes(bencher: Bencher) {
    let values = bytes_64_values();
    let keys = variable_byte_slices(&values);

    bencher.bench_local(|| {
        let mut filter = BloomFilterBuilder::with_accuracy(ITEMS as u64, 0.01)
            .build()
            .unwrap();
        for &key in &keys {
            filter.insert(black_box(key));
        }
        black_box(filter)
    });
}

fn contains_and_insert(
    bencher: Bencher,
    initial_items: u64,
    values: impl Iterator<Item = u64> + Clone,
) {
    let mut populated = BloomFilterBuilder::with_accuracy(ITEMS as u64, 0.01)
        .build()
        .unwrap();
    for value in 0..initial_items {
        populated.insert(value);
    }

    bencher.bench_local(|| {
        let mut filter = populated.clone();
        for value in values.clone() {
            black_box(filter.contains_and_insert(black_box(value)));
        }
        filter
    });
}

pub fn register(runner: &mut Runner) {
    runner.case("bloom::update::u64", 2_000, u64);
    runner.case("bloom::update::u64_pair", 1_800, u64_tuple);
    runner.case("bloom::update::bytes_32", 1_000, bytes_32);
    runner.case("bloom::update::variable_bytes", 1_200, variable_bytes);
    runner.case("bloom::update::prefixed_bytes", 900, prefixed_bytes);
    runner.case(
        "bloom::update::contains_and_insert_present_u64",
        2_000,
        |b| contains_and_insert(b, ITEMS as u64, 0..ITEMS as u64),
    );
    runner.case(
        "bloom::update::contains_and_insert_absent_u64",
        1_000,
        |b| contains_and_insert(b, ITEMS as u64, ITEMS as u64..(2 * ITEMS) as u64),
    );
    runner.case("bloom::update::contains_and_insert_mixed_u64", 1_000, |b| {
        contains_and_insert(
            b,
            ITEMS as u64,
            (0..ITEMS as u64).map(|value| {
                if value % 2 == 0 {
                    value
                } else {
                    value + ITEMS as u64
                }
            }),
        )
    });
    runner.case("bloom::update::contains_and_insert_build_u64", 1_000, |b| {
        contains_and_insert(b, 0, 0..ITEMS as u64)
    });
}
