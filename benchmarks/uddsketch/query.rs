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

use crate::uddsketch::support::prepared_sketch;

const STANDARD_RANKS: [f64; 6] = [0.01, 0.1, 0.5, 0.9, 0.95, 0.99];

fn quantile(bencher: Bencher, max_buckets: u32, rank: f64) {
    let sketch = prepared_sketch(100_000, max_buckets);
    bencher.bench_local(|| {
        black_box(&sketch)
            .quantile(black_box(rank))
            .unwrap()
            .unwrap()
    });
}

fn quantiles_batch(bencher: Bencher, max_buckets: u32) {
    let sketch = prepared_sketch(100_000, max_buckets);
    bencher.bench_local(|| {
        black_box(&sketch)
            .quantiles(black_box(&STANDARD_RANKS))
            .unwrap()
            .unwrap()
    });
}

fn quantiles_sequential(bencher: Bencher, max_buckets: u32) {
    let sketch = prepared_sketch(100_000, max_buckets);
    bencher.bench_local(|| {
        for &rank in black_box(&STANDARD_RANKS) {
            black_box(
                black_box(&sketch)
                    .quantile(black_box(rank))
                    .unwrap()
                    .unwrap(),
            );
        }
    });
}

pub fn register(runner: &mut Runner) {
    for &((max_buckets, rank), rounds) in &[
        ((512, 0.5), 6_000_000),
        ((512, 0.99), 10_000_000),
        ((2_048, 0.5), 2_400_000),
    ] {
        runner.case(
            format!("uddsketch::query::quantile/{max_buckets}/{rank}"),
            rounds,
            |b| quantile(b, max_buckets, rank),
        );
    }
    runner.compare(
        "uddsketch::query::quantiles/6/512",
        1_000_000,
        &[
            ("batch", &|b| quantiles_batch(b, 512)),
            ("sequential", &|b| quantiles_sequential(b, 512)),
        ],
    );
}
