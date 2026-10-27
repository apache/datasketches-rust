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

use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

use super::support::build;
use super::support::values;

#[divan::bench(args = [1_000, 100_000])]
fn update(bencher: Bencher, n: usize) {
    let values = values(n);
    bencher
        .counter(ItemsCount::new(n))
        .bench_local(|| build(black_box(&values)));
}

#[divan::bench(args = [8, 64])]
fn partial_groups(bencher: Bencher, rows: usize) {
    let groups = (0..512)
        .map(|group| {
            (0..rows)
                .map(|row| row as f64 + group as f64 / 512.0)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    bencher
        .counter(ItemsCount::new(512 * rows))
        .bench_local(|| {
            groups
                .iter()
                .map(|values| build(black_box(values)))
                .collect::<Vec<_>>()
        });
}
