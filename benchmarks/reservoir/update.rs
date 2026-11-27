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
use super::support::values;

fn update(bencher: Bencher, len: usize, k: usize) {
    let items = values(len);
    bencher.bench_local(|| build_sketch(black_box(&items), k));
}

pub fn register(runner: &mut Runner) {
    for &((len, k), rounds) in &[
        ((1_000, 128), 60_000),
        ((100_000, 128), 600),
        ((100_000, 2_048), 550),
    ] {
        runner.case(
            format!("reservoir::update::update/{len}/{k}"),
            rounds,
            |b| update(b, len, k),
        );
    }
}
