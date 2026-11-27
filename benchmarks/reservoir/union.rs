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
use datasketches::reservoir::ReservoirUnion;

use super::support::build_sketch;
use super::support::values;

fn union_sketches(bencher: Bencher, num_sketches: usize, items_per_sketch: usize, k: usize) {
    let all = values(num_sketches * items_per_sketch);
    let sketches: Vec<_> = all
        .chunks_exact(items_per_sketch)
        .map(|chunk| build_sketch(chunk, k))
        .collect();

    bencher.bench_local(|| {
        let mut union = ReservoirUnion::<i64>::new(k).unwrap();
        for sketch in black_box(&sketches) {
            union.update(sketch);
        }
        black_box(union.into_sketch())
    });
}

pub fn register(runner: &mut Runner) {
    for &((count, len, k), rounds) in &[
        ((32, 64, 128), 35_000),
        ((32, 4_096, 128), 18_000),
        ((32, 4_096, 1_024), 2_400),
    ] {
        runner.case(
            format!("reservoir::union::batch/{count}/{len}/{k}"),
            rounds,
            |b| union_sketches(b, count, len, k),
        );
    }
}
