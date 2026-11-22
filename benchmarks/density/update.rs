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

use super::support::build_sketch;
use super::support::points;

fn update(bencher: Bencher, len: usize, dim: u32) {
    let points = points(len, dim);
    bencher.bench_local(|| build_sketch(black_box(&points), dim));
}

pub fn register(runner: &mut Runner) {
    for &((len, dim), rounds) in &[
        ((1_000, 3), 1_000),
        ((1_000, 32), 400),
        ((10_000, 3), 80),
        ((10_000, 32), 30),
    ] {
        runner.case(
            format!("density::update::update/{len}/{dim}"),
            rounds,
            |b| update(b, len, dim),
        );
    }
}
