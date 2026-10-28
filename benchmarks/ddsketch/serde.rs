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

use datasketches::ddsketch::DdSketch;
use divan::Bencher;
use divan::black_box;
use divan::counter::BytesCount;

use super::support;

#[divan::bench]
fn serialize(bencher: Bencher) {
    let sketch = support::build(&support::values(100_000), 2048);
    bencher
        .counter(BytesCount::new(sketch.serialize().len()))
        .bench_local(|| sketch.serialize());
}

#[divan::bench]
fn serialize_reused(bencher: Bencher) {
    let sketch = support::build(&support::values(100_000), 2048);
    let mut out = sketch.serialize();
    bencher.counter(BytesCount::new(out.len())).bench_local(|| {
        sketch.serialize_into(&mut out);
        black_box(&out);
    });
}

#[divan::bench]
fn deserialize(bencher: Bencher) {
    let bytes = support::build(&support::values(100_000), 2048).serialize();
    bencher
        .counter(BytesCount::new(bytes.len()))
        .bench_local(|| DdSketch::deserialize(black_box(&bytes)).unwrap());
}
