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
use datasketches::theta::ThetaSketchBuilder;
use datasketches::theta::ThetaUnion;
use datasketches::theta::ThetaUnionBuilder;

fn serialize_union(bencher: Bencher, rows: u64, ordered: bool, direct: bool) {
    let union = union(rows);
    if direct {
        bencher.bench_local(|| black_box(&union).serialize(ordered));
    } else {
        bencher.bench_local(|| black_box(&union).to_sketch(ordered).serialize());
    }
}

fn union(rows: u64) -> ThetaUnion {
    let mut sketch = ThetaSketchBuilder::default().lg_k(14).build().unwrap();
    for value in 0..rows {
        sketch.update(value);
    }
    let mut union = ThetaUnionBuilder::default().lg_k(12).build().unwrap();
    union.update(&sketch).unwrap();
    union
}

pub fn register(runner: &mut Runner) {
    // Reuse each union for repeated serialization, including fixture construction in timing.
    for (rows, rounds) in [(40, 1_000_000), (8_000, 10_000)] {
        for ordered in [false, true] {
            let order = if ordered { "ordered" } else { "unordered" };
            runner.compare(
                &format!("theta::serde::{order}/{rows}"),
                rounds,
                &[
                    ("via_compact", &|b| {
                        serialize_union(b, rows, ordered, false);
                    }),
                    ("direct", &|b| {
                        serialize_union(b, rows, ordered, true);
                    }),
                ],
            );
        }
    }
}
