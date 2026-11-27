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
use datasketches::aod::ArrayOfDoublesSketchBuilder;
use datasketches::aod::ArrayOfDoublesUnion;
use datasketches::aod::ArrayOfDoublesUnionBuilder;
use datasketches::aod::CompactArrayOfDoublesSketch;

fn build_union(rows: u64, num_values: u8) -> ArrayOfDoublesUnion {
    let mut sketch = ArrayOfDoublesSketchBuilder::default()
        .lg_k(14)
        .num_values(num_values)
        .build()
        .unwrap();
    let values = vec![1.5; usize::from(num_values)];
    for key in 0..rows {
        sketch.update(key, &values).unwrap();
    }
    let mut union = ArrayOfDoublesUnionBuilder::default()
        .lg_k(12)
        .num_values(num_values)
        .build()
        .unwrap();
    union.update(&sketch).unwrap();
    union
}

fn serialize_union(bencher: Bencher, rows: u64, num_values: u8, ordered: bool, direct: bool) {
    let union = build_union(rows, num_values);
    if direct {
        bencher.bench_local(|| black_box(&union).serialize(ordered));
    } else {
        bencher.bench_local(|| black_box(&union).to_sketch(ordered).serialize());
    }
}

fn deserialize_compact(bencher: Bencher, ordered: bool) {
    let mut sketch = ArrayOfDoublesSketchBuilder::default()
        .lg_k(12)
        .num_values(3)
        .build()
        .unwrap();
    for key in 0..10_000_u64 {
        sketch.update(key, [1.0, 2.0, 3.0]).unwrap();
    }
    let bytes = sketch.compact(ordered).serialize();

    bencher.bench_local(|| {
        black_box(CompactArrayOfDoublesSketch::deserialize(black_box(&bytes)).unwrap())
    });
}

pub fn register(runner: &mut Runner) {
    // Reuse each union for repeated serialization, including fixture construction in timing.
    for (rows, rounds) in [(100, 250_000), (20_000, 5_000)] {
        for ordered in [false, true] {
            let order = if ordered { "ordered" } else { "unordered" };
            runner.compare(
                &format!("aod::serde::union::{order}/{rows}"),
                rounds,
                &[
                    ("via_compact", &|b| {
                        serialize_union(b, rows, 3, ordered, false);
                    }),
                    ("direct", &|b| {
                        serialize_union(b, rows, 3, ordered, true);
                    }),
                ],
            );
        }
    }

    runner.case("aod::serde::deserialize/ordered", 15_000, |b| {
        deserialize_compact(b, true);
    });
    runner.case("aod::serde::deserialize/unordered", 8_000, |b| {
        deserialize_compact(b, false);
    });
}
