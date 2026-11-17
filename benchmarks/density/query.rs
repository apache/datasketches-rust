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
use super::support::query_point;

fn estimate(bencher: Bencher, len: usize, dim: u32) {
    let sketch = prepared_sketch(len, dim);
    let query = query_point(dim);

    bencher.bench_local(|| {
        black_box(&sketch)
            .estimate(black_box(&query))
            .unwrap()
            .unwrap()
    });
}

pub fn register(runner: &mut Runner) {
    for &((len, dim), rounds) in &[
        ((1_000, 3), 400_000),
        ((1_000, 32), 150_000),
        ((10_000, 3), 200_000),
        ((10_000, 32), 75_000),
    ] {
        runner.case(
            format!("density::query::estimate/{len}/{dim}"),
            rounds,
            |b| estimate(b, len, dim),
        );
    }
}
