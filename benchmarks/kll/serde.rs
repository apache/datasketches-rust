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

use datasketches::kll::KllFloat;
use datasketches::kll::KllSketch;
use divan::Bencher;
use divan::black_box;
use divan::counter::BytesCount;

use super::support::prepared_sketch;

#[divan::bench]
fn serialize(bencher: Bencher) {
    let sketch = prepared_sketch();
    let bytes = sketch.serialize();
    bencher
        .counter(BytesCount::new(bytes.len()))
        .bench_local(|| black_box(&sketch).serialize());
}

#[divan::bench]
fn deserialize(bencher: Bencher) {
    let bytes = prepared_sketch().serialize();
    bencher
        .counter(BytesCount::new(bytes.len()))
        .bench_local(|| KllSketch::<KllFloat<f64>>::deserialize(black_box(&bytes)).unwrap());
}
