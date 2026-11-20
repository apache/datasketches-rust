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

use super::support::prepared_sketch;

const STANDARD_RANKS: [f64; 6] = [0.5, 0.75, 0.9, 0.95, 0.99, 0.999];

fn quantile_single(bencher: Bencher, rank: f64) {
    let sketch = prepared_sketch();

    bencher.bench_local(|| black_box(&sketch).quantile(black_box(rank)).unwrap());
}

fn quantiles_batch(bencher: Bencher) {
    let sketch = prepared_sketch();

    bencher.bench_local(|| {
        black_box(&sketch)
            .quantiles(black_box(&STANDARD_RANKS))
            .unwrap()
    });
}

fn quantiles_sequential(bencher: Bencher) {
    let sketch = prepared_sketch();

    bencher.bench_local(|| {
        for &rank in black_box(&STANDARD_RANKS) {
            black_box(black_box(&sketch).quantile(black_box(rank)).unwrap());
        }
    });
}

pub fn register(runner: &mut Runner) {
    for &(rank, rounds) in &[(0.5, 1_500_000), (0.99, 1_000_000)] {
        runner.case(format!("ddsketch::query::quantile/{rank}"), rounds, |b| {
            quantile_single(b, rank)
        });
    }
    runner.compare(
        "ddsketch::query::quantiles/6",
        500_000,
        &[
            ("batch", &quantiles_batch),
            ("sequential", &quantiles_sequential),
        ],
    );
}
