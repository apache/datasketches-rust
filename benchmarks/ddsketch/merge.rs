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
use datasketches::ddsketch::MappingKind;
use datasketches::ddsketch::StoreKind;

use super::support::DEFAULT_ACCURACY;
use super::support::build_sketch;
use super::support::latency_values;
use super::support::partial_sketches;

fn merge_pair(bencher: Bencher) {
    let values = latency_values(200_000);
    let left = build_sketch(&values[..100_000], MappingKind::Cubic, StoreKind::default());
    let right = build_sketch(&values[100_000..], MappingKind::Cubic, StoreKind::default());

    bencher.bench_local(|| {
        let mut merged = left.clone();
        merged.merge(black_box(&right)).unwrap();
        black_box(merged)
    });
}

fn merge_partials(bencher: Bencher) {
    let partials = partial_sketches();

    bencher.bench_local(|| {
        let mut merged = DdSketch::new(DEFAULT_ACCURACY).unwrap();
        for partial in black_box(&partials) {
            merged.merge(partial).unwrap();
        }
        black_box(merged)
    });
}

pub fn register(runner: &mut Runner) {
    runner.case("ddsketch::merge::pair", 400_000, merge_pair);
    runner.case("ddsketch::merge::partials/64", 15_000, merge_partials);
}
