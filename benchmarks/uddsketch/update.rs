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
use datasketches::uddsketch::UddSketch;

use crate::uddsketch::support::DEFAULT_ALPHA;
use crate::uddsketch::support::build_sketch;
use crate::uddsketch::support::values;

fn update(bencher: Bencher, len: usize, max_buckets: u32) {
    let input = values(len);
    bencher.bench_local(|| build_sketch(black_box(&input), max_buckets));
}

fn update_medium_group(bencher: Bencher, distinct_per_group: usize, max_buckets: u32) {
    let input: Vec<f64> = (0..distinct_per_group)
        .map(|i| {
            let idx = (i * 17) % distinct_per_group;
            1.05_f64.powi(idx as i32)
        })
        .collect();
    bencher.bench_local(|| {
        let mut sketch = UddSketch::new(max_buckets, DEFAULT_ALPHA).unwrap();
        for &v in black_box(&input) {
            sketch.update(v);
        }
        black_box(sketch)
    });
}

pub fn register(runner: &mut Runner) {
    for &((len, max_buckets), rounds) in &[
        ((1_000, 512), 24_000),
        ((100_000, 512), 100),
        ((100_000, 2_048), 140),
    ] {
        runner.case(
            format!("uddsketch::update::update/{len}/{max_buckets}"),
            rounds,
            |b| update(b, len, max_buckets),
        );
    }
    for &(distinct, rounds) in &[(24usize, 600_000), (48, 400_000)] {
        runner.case(
            format!("uddsketch::update::update_medium_group/{distinct}/512"),
            rounds,
            |b| update_medium_group(b, distinct, 512),
        );
    }
}
