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
use datasketches::hash::value::raw_bytes;

use crate::hash_inputs::ITEMS;
use crate::hash_inputs::bytes_32_values;
use crate::hash_inputs::bytes_64_values;
use crate::hash_inputs::variable_byte_slices;

fn u64_1(bencher: Bencher) {
    bencher.bench_local(|| {
        let mut sketch = ArrayOfDoublesSketchBuilder::default()
            .lg_k(14)
            .num_values(1)
            .build()
            .unwrap();
        for value in 0..ITEMS as u64 {
            sketch.update(black_box(value), [1.0]).unwrap();
        }
        black_box(sketch)
    });
}

fn u64_3(bencher: Bencher) {
    bencher.bench_local(|| {
        let mut sketch = ArrayOfDoublesSketchBuilder::default()
            .lg_k(14)
            .num_values(3)
            .build()
            .unwrap();
        for value in 0..ITEMS as u64 {
            sketch.update(black_box(value), [1.0, 2.0, 3.0]).unwrap();
        }
        black_box(sketch)
    });
}

fn bytes_32_3(bencher: Bencher) {
    let values = bytes_32_values();

    bencher.bench_local(|| {
        let mut sketch = ArrayOfDoublesSketchBuilder::default()
            .lg_k(14)
            .num_values(3)
            .build()
            .unwrap();
        for value in &values {
            sketch
                .update(raw_bytes::from_slice(black_box(value)), [1.0, 2.0, 3.0])
                .unwrap();
        }
        black_box(sketch)
    });
}

fn variable_bytes_3(bencher: Bencher) {
    let values = bytes_64_values();
    let keys = variable_byte_slices(&values);

    bencher.bench_local(|| {
        let mut sketch = ArrayOfDoublesSketchBuilder::default()
            .lg_k(14)
            .num_values(3)
            .build()
            .unwrap();
        for &key in &keys {
            sketch
                .update(raw_bytes::from_slice(black_box(key)), [1.0, 2.0, 3.0])
                .unwrap();
        }
        black_box(sketch)
    });
}

pub fn register(runner: &mut Runner) {
    runner.case("aod::update::u64/1", 3_500, u64_1);
    runner.case("aod::update::u64/3", 3_000, u64_3);
    runner.case("aod::update::bytes_32/3", 2_500, bytes_32_3);
    runner.case("aod::update::variable_bytes/3", 1_800, variable_bytes_3);
}
