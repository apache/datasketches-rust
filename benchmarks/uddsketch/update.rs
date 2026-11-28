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

// Reusable sketch lifecycle across small and medium groups (`reset` + `update` + `quantile`),
// matching grouped aggregation operators that reuse one state across groups and materialize each
// group's sorted buckets once at finalization.
fn update_grouped_reset(bencher: Bencher, rows_per_group: usize, max_buckets: u32) {
    const GROUPS: usize = 64;
    let inputs: Vec<f64> = (0..(GROUPS * rows_per_group))
        .map(|i| {
            let group = i / rows_per_group;
            let row = i % rows_per_group;
            let idx = (row * 17 + group * 7) % (rows_per_group * 2);
            1.05_f64.powi(idx as i32) * (1.0 + group as f64 * 0.1)
        })
        .collect();

    bencher.bench_local(|| {
        let mut sketch = UddSketch::new(max_buckets, DEFAULT_ALPHA).unwrap();
        let mut acc = 0.0_f64;
        for chunk in black_box(&inputs).chunks_exact(rows_per_group) {
            sketch.reset();
            for &v in chunk {
                sketch.update(v);
            }
            acc += sketch.quantile(0.5).unwrap().unwrap_or(0.0);
        }
        black_box((sketch, acc))
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
    for &(rows_per_group, rounds) in &[(8usize, 40_000), (24, 12_000), (48, 6_000)] {
        runner.case(
            format!("uddsketch::update::grouped_reset/{rows_per_group}/512"),
            rounds,
            |b| update_grouped_reset(b, rows_per_group, 512),
        );
    }
}
