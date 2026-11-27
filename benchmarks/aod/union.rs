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
use datasketches::aod::ArrayOfDoublesSketch;
use datasketches::aod::ArrayOfDoublesSketchBuilder;
use datasketches::aod::ArrayOfDoublesUnionBuilder;

fn build_sketch(start: u64, count: u64) -> ArrayOfDoublesSketch {
    let mut sketch = ArrayOfDoublesSketchBuilder::default()
        .lg_k(12)
        .num_values(3)
        .build()
        .unwrap();
    for key in start..start + count {
        sketch.update(key, [1.0, 2.0, 3.0]).unwrap();
    }
    sketch
}

fn union_mutable_overlap(bencher: Bencher) {
    let left = build_sketch(0, 10_000);
    let right = build_sketch(5_000, 10_000);

    bencher.bench_local(|| {
        let mut union = ArrayOfDoublesUnionBuilder::default()
            .lg_k(12)
            .num_values(3)
            .build()
            .unwrap();
        union.update(black_box(&left)).unwrap();
        union.update(black_box(&right)).unwrap();
        black_box(union.to_sketch(true))
    });
}

fn union_compact_ordered_overlap(bencher: Bencher) {
    let left = build_sketch(0, 10_000).compact(true);
    let right = build_sketch(5_000, 10_000).compact(true);

    bencher.bench_local(|| {
        let mut union = ArrayOfDoublesUnionBuilder::default()
            .lg_k(12)
            .num_values(3)
            .build()
            .unwrap();
        union.update(black_box(&left)).unwrap();
        union.update(black_box(&right)).unwrap();
        black_box(union.to_sketch(true))
    });
}

pub fn register(runner: &mut Runner) {
    runner.case("aod::union::mutable/3", 2_500, union_mutable_overlap);
    runner.case(
        "aod::union::compact_ordered/3",
        3_500,
        union_compact_ordered_overlap,
    );
}
