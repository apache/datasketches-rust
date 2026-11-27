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

fn estimate_subset_sum(bencher: Bencher, k: usize) {
    let sketch = prepared_sketch(100_000, k);
    let threshold = 50_000_i64;

    bencher.bench_local(|| {
        black_box(&sketch).estimate_subset_sum(|&value| value < black_box(threshold))
    });
}

pub fn register(runner: &mut Runner) {
    for &(k, rounds) in &[(128, 3_000_000), (2_048, 400_000)] {
        runner.case(format!("reservoir::query::subset_sum/{k}"), rounds, |b| {
            estimate_subset_sum(b, k)
        });
    }
}
