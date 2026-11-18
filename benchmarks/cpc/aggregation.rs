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
use datasketches::cpc::CpcSketch;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;

fn small_groups(bencher: Bencher) {
    const GROUPS: usize = 16_384;

    // Model grouped distinct counts with mostly singletons, repeated users, and a tail of
    // groups large enough to outgrow the initial table.
    let mut rows = Vec::new();
    for group in 0..GROUPS {
        let users = match group % 64 {
            0 => 16,
            1 => 3,
            2 | 3 => 2,
            _ => 1,
        };
        for user in 0..users {
            rows.extend([(group, (group * 16 + user) as i64); 2]);
        }
    }
    rows.shuffle(&mut StdRng::seed_from_u64(42));

    bencher.bench_local(|| {
        let mut sketches: Vec<_> = (0..GROUPS).map(|_| CpcSketch::new(11).unwrap()).collect();
        for &(group, user) in black_box(&rows) {
            sketches[group].update(user);
        }
        sketches.iter().map(CpcSketch::estimate).sum::<f64>()
    });
}

pub fn register(runner: &mut Runner) {
    runner.case("cpc::aggregation::small_groups", 350, small_groups);
}
