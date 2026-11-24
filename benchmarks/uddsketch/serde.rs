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

use crate::uddsketch::support::prepared_sketch;

fn serialize(bencher: Bencher, max_buckets: u32) {
    let sketch = prepared_sketch(100_000, max_buckets);
    bencher.bench_local(|| black_box(&sketch).serialize());
}

fn deserialize(bencher: Bencher, max_buckets: u32) {
    let bytes = prepared_sketch(100_000, max_buckets).serialize();
    bencher.bench_local(|| UddSketch::deserialize(black_box(&bytes)).unwrap());
}

pub fn register(runner: &mut Runner) {
    for &(max_buckets, rounds) in &[(512, 750_000), (2_048, 200_000)] {
        runner.case(
            format!("uddsketch::serde::serialize/{max_buckets}"),
            rounds,
            |b| serialize(b, max_buckets),
        );
    }
    for &(max_buckets, rounds) in &[(512, 750_000), (2_048, 240_000)] {
        runner.case(
            format!("uddsketch::serde::deserialize/{max_buckets}"),
            rounds,
            |b| deserialize(b, max_buckets),
        );
    }
}
