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

use datasketches::cpc::CpcSketch;
use divan::Bencher;
use divan::black_box;
use divan::black_box_drop;
use divan::counter::BytesCount;
use divan::counter::ItemsCount;

const LG_K: u8 = 10;

#[divan::bench(args = [200_u64, 8_000_u64])]
fn serialize(bencher: Bencher, items: u64) {
    let sketch = sketch(items);
    let serialized_bytes = sketch.serialize().len();

    bencher
        .counter(ItemsCount::new(items))
        .counter(BytesCount::new(serialized_bytes))
        .bench_local(|| black_box_drop(black_box(&sketch).serialize()));
}

#[divan::bench(args = [200_u64, 8_000_u64])]
fn deserialize(bencher: Bencher, items: u64) {
    let bytes = sketch(items).serialize();

    bencher
        .counter(ItemsCount::new(items))
        .counter(BytesCount::new(bytes.len()))
        .bench_local(|| {
            black_box_drop(CpcSketch::deserialize(black_box(&bytes)).unwrap());
        });
}

fn sketch(items: u64) -> CpcSketch {
    let mut sketch = CpcSketch::new(LG_K);
    for value in 0..items {
        sketch.update(value);
    }
    sketch
}
