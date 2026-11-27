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
use datasketches::reservoir::ReservoirSketch;

use super::support::prepared_sketch;

fn serialize(bencher: Bencher, k: usize) {
    let sketch = prepared_sketch(10_000, k);
    bencher.bench_local(|| black_box(&sketch).serialize());
}

fn deserialize(bencher: Bencher, k: usize) {
    let bytes = prepared_sketch(10_000, k).serialize();
    bencher.bench_local(|| ReservoirSketch::<i64>::deserialize(black_box(&bytes)).unwrap());
}

pub fn register(runner: &mut Runner) {
    for &(k, rounds) in &[(128, 1_500_000), (2_048, 250_000)] {
        runner.case(format!("reservoir::serde::serialize/{k}"), rounds, |b| {
            serialize(b, k)
        });
    }
    for &(k, rounds) in &[(128, 1_500_000), (2_048, 250_000)] {
        runner.case(format!("reservoir::serde::deserialize/{k}"), rounds, |b| {
            deserialize(b, k)
        });
    }
}
