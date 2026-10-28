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

use datasketches::uddsketch::UddSketch;
use divan::Bencher;
use divan::black_box;

use super::support::build;
use super::support::values;

#[divan::bench]
fn serialize(bencher: Bencher) {
    let sketch = build(&values(100_000));
    bencher.bench_local(|| black_box(&sketch).serialize());
}

#[divan::bench]
fn serialize_reusing_buffer(bencher: Bencher) {
    let sketch = build(&values(100_000));
    let mut bytes = Vec::new();
    bencher.bench_local(|| {
        black_box(&sketch).serialize_into(black_box(&mut bytes));
        black_box(bytes.len())
    });
}

#[divan::bench]
fn deserialize(bencher: Bencher) {
    let bytes = build(&values(100_000)).serialize();
    bencher.bench_local(|| UddSketch::deserialize(black_box(&bytes)).unwrap());
}
