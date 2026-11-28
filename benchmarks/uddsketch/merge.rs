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

fn merge_batch(bencher: Bencher, partials: usize, rows_per_partial: usize, max_buckets: u32) {
    let all = values(partials * rows_per_partial);
    let sketches: Vec<UddSketch> = all
        .chunks_exact(rows_per_partial)
        .map(|chunk| build_sketch(chunk, max_buckets))
        .collect();

    bencher.bench_local(|| {
        let mut acc = UddSketch::new(max_buckets, DEFAULT_ALPHA).unwrap();
        for sketch in black_box(&sketches) {
            acc.merge(sketch).unwrap();
        }
        black_box(acc)
    });
}

// Multi-way reduction of skewed partial sketches at fine relative accuracy (`alpha = 1e-4`,
// `max_buckets = 16_384`), where the accumulator grows across partitions without saturating on the
// first partial sketch.
fn merge_fine_partials(bencher: Bencher, partials: usize, rows_per_partial: usize) {
    const FINE_MAX_BUCKETS: u32 = 16_384;
    const FINE_ALPHA: f64 = 1e-4;
    let all = values(partials * rows_per_partial);
    let sketches: Vec<UddSketch> = all
        .chunks_exact(rows_per_partial)
        .enumerate()
        .map(|(part, chunk)| {
            let scale = 1.0 + part as f64 * 0.25;
            let mut sketch = UddSketch::new(FINE_MAX_BUCKETS, FINE_ALPHA).unwrap();
            for &v in chunk {
                sketch.update(v * scale);
            }
            sketch
        })
        .collect();

    bencher.bench_local(|| {
        let mut acc = UddSketch::new(FINE_MAX_BUCKETS, FINE_ALPHA).unwrap();
        for sketch in black_box(&sketches) {
            acc.merge(sketch).unwrap();
        }
        black_box(acc)
    });
}

pub fn register(runner: &mut Runner) {
    for &((partials, rows, max_buckets), rounds) in &[
        ((64, 64, 512), 12_000),
        ((64, 16_384, 512), 4_500),
        ((64, 16_384, 2_048), 500),
    ] {
        runner.case(
            format!("uddsketch::merge::batch/{partials}/{rows}/{max_buckets}"),
            rounds,
            |b| merge_batch(b, partials, rows, max_buckets),
        );
    }
    for &(rows, rounds) in &[(8usize, 24_000), (64, 4_500)] {
        runner.case(
            format!("uddsketch::merge::fine_partials/64/{rows}/16384"),
            rounds,
            |b| merge_fine_partials(b, 64, rows),
        );
    }
}
