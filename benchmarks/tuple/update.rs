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
use datasketches::hash::value::raw_bytes;
use datasketches::tuple::DefaultUpdatePolicy;
use datasketches::tuple::TupleSketchBuilder;

use crate::hash_inputs::ITEMS;
use crate::hash_inputs::bytes_32_values;

fn u64(bencher: Bencher) {
    bencher.bench_local(|| {
        let policy = DefaultUpdatePolicy::<u64>::default();
        let mut sketch = TupleSketchBuilder::new(policy).lg_k(14).build().unwrap();
        for value in 0..ITEMS as u64 {
            sketch.update(black_box(value), 1);
        }
        black_box(sketch)
    });
}

fn bytes_32(bencher: Bencher) {
    let values = bytes_32_values();

    bencher.bench_local(|| {
        let policy = DefaultUpdatePolicy::<u64>::default();
        let mut sketch = TupleSketchBuilder::new(policy).lg_k(14).build().unwrap();
        for value in &values {
            sketch.update(raw_bytes::from_slice(black_box(value)), 1);
        }
        black_box(sketch)
    });
}

pub fn register(runner: &mut Runner) {
    runner.case("tuple::update::u64", 4_000, u64);
    runner.case("tuple::update::bytes_32", 3_000, bytes_32);
}
