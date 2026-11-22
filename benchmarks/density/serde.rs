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
use datasketches::density::DensitySketch;

use super::support::prepared_sketch;

fn serialize(bencher: Bencher, len: usize, dim: u32) {
    let sketch = prepared_sketch(len, dim);

    bencher.bench_local(|| black_box(&sketch).serialize());
}

fn deserialize(bencher: Bencher, len: usize, dim: u32) {
    let bytes = prepared_sketch(len, dim).serialize();

    bencher.bench_local(|| DensitySketch::<f64>::deserialize(black_box(&bytes)).unwrap());
}

pub fn register(runner: &mut Runner) {
    for &((len, dim), rounds) in &[
        ((1_000, 3), 800_000),
        ((1_000, 32), 60_000),
        ((10_000, 3), 450_000),
        ((10_000, 32), 30_000),
    ] {
        runner.case(
            format!("density::serde::serialize/{len}/{dim}"),
            rounds,
            |b| serialize(b, len, dim),
        );
    }
    for &((len, dim), rounds) in &[
        ((1_000, 3), 600_000),
        ((1_000, 32), 60_000),
        ((10_000, 3), 300_000),
        ((10_000, 32), 20_000),
    ] {
        runner.case(
            format!("density::serde::deserialize/{len}/{dim}"),
            rounds,
            |b| deserialize(b, len, dim),
        );
    }
}
