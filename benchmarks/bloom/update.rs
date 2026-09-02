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

use datasketches::bloom::BloomFilterBuilder;
use datasketches::hash::value::raw_bytes;
use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

use crate::hash_inputs::ITEMS;
use crate::hash_inputs::bytes_32_values;

#[divan::bench]
fn u64(bencher: Bencher) {
    bencher.counter(ItemsCount::new(ITEMS)).bench_local(|| {
        let mut filter = BloomFilterBuilder::with_accuracy(ITEMS as u64, 0.01)
            .build()
            .unwrap();
        for value in 0..ITEMS as u64 {
            filter.insert(black_box(value));
        }
        black_box(filter)
    });
}

#[divan::bench]
fn bytes_32(bencher: Bencher) {
    let values = bytes_32_values();

    bencher.counter(ItemsCount::new(ITEMS)).bench_local(|| {
        let mut filter = BloomFilterBuilder::with_accuracy(ITEMS as u64, 0.01)
            .build()
            .unwrap();
        for value in &values {
            filter.insert(raw_bytes::from_slice(black_box(value)));
        }
        black_box(filter)
    });
}

#[divan::bench]
fn contains_and_insert_present_u64(bencher: Bencher) {
    let mut populated = BloomFilterBuilder::with_accuracy(ITEMS as u64, 0.01)
        .build()
        .unwrap();
    for value in 0..ITEMS as u64 {
        populated.insert(value);
    }

    bencher
        .counter(ItemsCount::new(ITEMS))
        .with_inputs(|| populated.clone())
        .bench_local_values(|mut filter| {
            for value in 0..ITEMS as u64 {
                black_box(filter.contains_and_insert(black_box(&value)));
            }
            filter
        });
}
