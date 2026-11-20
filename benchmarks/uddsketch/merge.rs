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

use super::support::DEFAULT_ALPHA;
use super::support::build_sketch;
use super::support::values;

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
}
