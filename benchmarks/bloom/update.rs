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

fn contains_and_insert_present_u64(bencher: Bencher) {
    let mut populated = BloomFilterBuilder::with_accuracy(ITEMS as u64, 0.01)
        .build()
        .unwrap();
    for value in 0..ITEMS as u64 {
        populated.insert(value);
    }

    bencher.bench_local(|| {
        let mut filter = populated.clone();
        for value in 0..ITEMS as u64 {
            black_box(filter.contains_and_insert(black_box(value)));
        }
        filter
    });
}

pub fn register(runner: &mut Runner) {
    runner.case("bloom::update::u64", 2_000, u64);
    runner.case("bloom::update::bytes_32", 1_000, bytes_32);
    runner.case(
        "bloom::update::contains_and_insert_present_u64",
        2_000,
        contains_and_insert_present_u64,
    );
}
