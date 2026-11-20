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
use datasketches::hll::HllSketch;
use datasketches::hll::HllType;
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;

fn many_small_groups(bencher: Bencher) {
    const GROUPS: usize = 50_000;
    let mut rng = StdRng::seed_from_u64(7);
    let inputs: Vec<_> = (0..400_000_u64)
        .map(|value| (value, rng.random_range(0..GROUPS)))
        .collect();
    bencher.bench_local(|| {
        let mut groups = vec![HllSketch::new(14, HllType::Hll8).unwrap(); GROUPS];
        for &(value, group) in black_box(&inputs) {
            groups[group].update(value);
        }
        groups
            .iter()
            .map(HllSketch::composite_estimate)
            .collect::<Vec<_>>()
    });
}

pub fn register(runner: &mut Runner) {
    runner.case("hll::aggregation::many_small/50000", 25, many_small_groups);
}
