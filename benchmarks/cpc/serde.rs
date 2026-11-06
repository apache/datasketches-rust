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

const LG_K: u8 = 10;

fn serialize(bencher: Bencher, items: u64) {
    let sketch = sketch(items);

    bencher.bench_local(|| black_box(&sketch).serialize());
}

fn deserialize(bencher: Bencher, items: u64) {
    let bytes = sketch(items).serialize();

    bencher.bench_local(|| CpcSketch::deserialize(black_box(&bytes)).unwrap());
}

fn sketch(items: u64) -> CpcSketch {
    let mut sketch = CpcSketch::new(LG_K).unwrap();
    for value in 0..items {
        sketch.update(value);
    }
    sketch
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
}
