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
use crate::uddsketch::support::values;

const FINE_ALPHA: f64 = 1e-4;
const FINE_MAX_BUCKETS: u32 = 16_384;

fn build_sketch_with_alpha(values: &[f64], max_buckets: u32, alpha: f64) -> UddSketch {
    let mut sketch = UddSketch::new(max_buckets, alpha).unwrap();
    for &v in values {
        sketch.update(v);
    }
    sketch
}

fn serialize(bencher: Bencher, len: usize, max_buckets: u32, alpha: f64) {
    let sketch = build_sketch_with_alpha(&values(len), max_buckets, alpha);
    bencher.bench_local(|| black_box(&sketch).serialize());
}

fn deserialize(bencher: Bencher, len: usize, max_buckets: u32, alpha: f64) {
    let bytes = build_sketch_with_alpha(&values(len), max_buckets, alpha).serialize();
    bencher.bench_local(|| UddSketch::deserialize(black_box(&bytes)).unwrap());
}

pub fn register(runner: &mut Runner) {
    for &(max_buckets, alpha, rounds) in &[
        (512, DEFAULT_ALPHA, 750_000),
        (2_048, DEFAULT_ALPHA, 200_000),
        (FINE_MAX_BUCKETS, FINE_ALPHA, 20_000),
    ] {
        runner.case(
            format!("uddsketch::serde::serialize/{max_buckets}"),
            rounds,
            |b| serialize(b, 100_000, max_buckets, alpha),
        );
    }
    runner.case(
        format!("uddsketch::serde::serialize/small_group/64/{FINE_MAX_BUCKETS}"),
        3_600_000,
        |b| serialize(b, 64, FINE_MAX_BUCKETS, FINE_ALPHA),
    );
    for &(max_buckets, alpha, rounds) in &[
        (512, DEFAULT_ALPHA, 750_000),
        (2_048, DEFAULT_ALPHA, 240_000),
        (FINE_MAX_BUCKETS, FINE_ALPHA, 16_000),
    ] {
        runner.case(
            format!("uddsketch::serde::deserialize/{max_buckets}"),
            rounds,
            |b| deserialize(b, 100_000, max_buckets, alpha),
        );
    }
    runner.case(
        format!("uddsketch::serde::deserialize/small_group/64/{FINE_MAX_BUCKETS}"),
        2_500_000,
        |b| deserialize(b, 64, FINE_MAX_BUCKETS, FINE_ALPHA),
    );
}
