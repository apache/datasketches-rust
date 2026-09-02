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

use datasketches::cpc::CpcSketch;
use datasketches::hash::value::raw_bytes;
use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

const ITEMS: usize = 10_000;

#[divan::bench]
fn u64(bencher: Bencher) {
    bencher.counter(ItemsCount::new(ITEMS)).bench_local(|| {
        let mut sketch = CpcSketch::new(11).unwrap();
        for value in 0..ITEMS as u64 {
            sketch.update(black_box(value));
        }
        black_box(sketch)
    });
}

#[divan::bench]
fn bytes_32(bencher: Bencher) {
    let values = (0..ITEMS)
        .map(|value| {
            let mut bytes = [0; 32];
            bytes[..8].copy_from_slice(&(value as u64).to_le_bytes());
            bytes
        })
        .collect::<Vec<_>>();

    bencher.counter(ItemsCount::new(ITEMS)).bench_local(|| {
        let mut sketch = CpcSketch::new(11).unwrap();
        for value in &values {
            sketch.update(raw_bytes::from_slice(black_box(value)));
        }
        black_box(sketch)
    });
}
