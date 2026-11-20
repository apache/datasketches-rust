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
use datasketches::hash::value::raw_bytes;
use datasketches::xor::XorFilter;
use datasketches::xor::XorFilterBuilder;
use datasketches::xor::XorFilterType;

use crate::hash_inputs::ITEMS;
use crate::hash_inputs::bytes_32_values;
use crate::hash_inputs::variable_byte_slices;

fn build_filter<'a>(keys: impl IntoIterator<Item = &'a [u8]>) -> XorFilter {
    XorFilterBuilder::new(XorFilterType::Xor8)
        .update_values(keys.into_iter().map(raw_bytes::from_slice))
        .build()
        .unwrap()
}

fn u64(bencher: Bencher) {
    let filter = XorFilterBuilder::new(XorFilterType::Xor8)
        .update_values(0..ITEMS as u64)
        .build()
        .unwrap();

    bencher.bench_local(|| {
        for value in 0..ITEMS as u64 {
            black_box(filter.contains(black_box(value)));
        }
    });
}

fn bytes_32(bencher: Bencher) {
    let values = bytes_32_values();
    let filter = build_filter(values.iter().map(<[u8; 32]>::as_slice));

    bencher.bench_local(|| {
        for value in &values {
            black_box(filter.contains(raw_bytes::from_slice(black_box(value))));
        }
    });
}

fn variable_bytes(bencher: Bencher) {
    let values = bytes_32_values();
    let keys = variable_byte_slices(&values);
    let filter = build_filter(keys.iter().copied());

    bencher.bench_local(|| {
        for &key in &keys {
            black_box(filter.contains(raw_bytes::from_slice(black_box(key))));
        }
    });
}

pub fn register(runner: &mut Runner) {
    runner.case("xor::query_items::u64", 9_000, u64);
    runner.case("xor::query_items::bytes_32", 4_000, bytes_32);
    runner.case("xor::query_items::variable_bytes", 4_000, variable_bytes);
}
