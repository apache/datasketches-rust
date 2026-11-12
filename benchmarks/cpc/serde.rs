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
use datasketches::cpc::CpcUnion;

const LG_K: u8 = 10;
const UNION_LG_K: u8 = 18;

fn serialize(bencher: Bencher, items: u64) {
    let sketch = sketch(LG_K, items);

    bencher.bench_local(|| black_box(&sketch).serialize());
}

fn deserialize(bencher: Bencher, items: u64) {
    let bytes = sketch(LG_K, items).serialize();

    bencher.bench_local(|| CpcSketch::deserialize(black_box(&bytes)).unwrap());
}

fn serialize_union(bencher: Bencher, items: u64) {
    let union = union(items);

    bencher.bench_local(|| black_box(&union).serialize());
}

fn serialize_union_via_sketch(bencher: Bencher, items: u64) {
    let union = union(items);

    bencher.bench_local(|| black_box(&union).to_sketch().serialize());
}

fn sketch(lg_k: u8, items: u64) -> CpcSketch {
    let mut sketch = CpcSketch::new(lg_k).unwrap();
    for value in 0..items {
        sketch.update(value);
    }
    sketch
}

fn union(items: u64) -> CpcUnion {
    let sketch = sketch(UNION_LG_K, items);
    let mut union = CpcUnion::new(UNION_LG_K).unwrap();
    union.update(&sketch).unwrap();
    union
}

pub fn register(runner: &mut Runner) {
    for &(items, rounds) in &[(200_u64, 400_000), (8_000_u64, 500_000)] {
        runner.case(format!("cpc::serde::serialize/{items:?}"), rounds, |b| {
            serialize(b, items)
        });
    }
    for &(items, rounds) in &[(200_u64, 400_000), (8_000_u64, 100_000)] {
        runner.case(format!("cpc::serde::deserialize/{items:?}"), rounds, |b| {
            deserialize(b, items)
        });
    }
    // Reuse each union for repeated serialization across empty, sparse, hybrid, pinned, and
    // sliding union results.
    for &(items, rounds) in &[
        (0_u64, 4_000_000),
        (20_000_u64, 500),
        (80_000_u64, 200),
        (400_000_u64, 500),
        (2_500_000_u64, 400),
    ] {
        runner.compare(
            &format!("cpc::serde::serialize_union/{items:?}"),
            rounds,
            &[
                ("via sketch", &|b| serialize_union_via_sketch(b, items)),
                ("direct", &|b| serialize_union(b, items)),
            ],
        );
    }
}
