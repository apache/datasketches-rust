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

use datasketches::ddsketch::DdSketch;
use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

use super::support;

#[divan::bench(args = [1_000, 100_000])]
fn update(bencher: Bencher, len: usize) {
    let values = support::values(len);
    bencher
        .counter(ItemsCount::new(len))
        .bench_local(|| support::build(black_box(&values), 2048));
}

#[divan::bench]
fn collapsing(bencher: Bencher) {
    let values = support::values(100_000);
    bencher
        .counter(ItemsCount::new(values.len()))
        .bench_local(|| support::build(black_box(&values), 32));
}

#[divan::bench(args = [8, 128])]
fn reset_groups(bencher: Bencher, rows: usize) {
    let values = support::values(64 * rows);
    let mut sketch = DdSketch::default();
    bencher
        .counter(ItemsCount::new(values.len()))
        .bench_local(|| {
            for group in black_box(&values).chunks(rows) {
                sketch.reset();
                for &value in group {
                    sketch.update(value).unwrap();
                }
                black_box(&sketch);
            }
        });
}
