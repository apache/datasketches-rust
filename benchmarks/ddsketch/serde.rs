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
use datasketches::ddsketch::DdSketch;

use super::support::partial_sketches;
use super::support::prepared_sketch;

fn serialize_full(bencher: Bencher) {
    let sketch = prepared_sketch();

    bencher.bench_local(|| black_box(&sketch).serialize());
}

fn deserialize_full(bencher: Bencher) {
    let bytes = prepared_sketch().serialize();

    bencher.bench_local(|| DdSketch::<u64>::deserialize(black_box(&bytes)).unwrap());
}

fn deserialize_and_merge_partials(bencher: Bencher) {
    let payloads: Vec<Vec<u8>> = partial_sketches()
        .into_iter()
        .map(|s| s.serialize())
        .collect();

    bencher.bench_local(|| {
        let mut merged = DdSketch::<u64>::default();
        for payload in black_box(&payloads) {
            let partial = DdSketch::<u64>::deserialize(payload).unwrap();
            merged.merge(&partial).unwrap();
        }
        black_box(merged)
    });
}

pub fn register(runner: &mut Runner) {
    runner.case("ddsketch::serde::serialize/100000", 50_000, serialize_full);
    runner.case(
        "ddsketch::serde::deserialize/100000",
        40_000,
        deserialize_full,
    );
    runner.case(
        "ddsketch::serde::deserialize_and_merge/64",
        1_500,
        deserialize_and_merge_partials,
    );
}
