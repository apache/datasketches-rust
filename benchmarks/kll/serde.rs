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
use datasketches::kll::KllFloat;
use datasketches::kll::KllSketch;

use super::support::prepared_sketch;

fn serialize(bencher: Bencher) {
    let sketch = prepared_sketch();
    bencher.bench_local(|| black_box(&sketch).serialize());
}

fn deserialize(bencher: Bencher) {
    let bytes = prepared_sketch().serialize();
    bencher.bench_local(|| KllSketch::<KllFloat<f64>>::deserialize(black_box(&bytes)).unwrap());
}

pub fn register(runner: &mut Runner) {
    runner.case("kll::serde::serialize", 200_000, serialize);
    runner.case("kll::serde::deserialize", 100_000, deserialize);
}
