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
use datasketches::hll::HllType;
use datasketches::hll::HllUnion;

fn union(rows: u64) -> HllUnion {
    let mut union = HllUnion::new(18).unwrap();
    for value in 0..rows {
        union.update_value(value);
    }
    union
}

fn serialize_union(bencher: Bencher, rows: u64) {
    let union = union(rows);
    bencher.bench_local(|| black_box(&union).serialize());
}

fn serialize_union_via_sketch(bencher: Bencher, rows: u64) {
    let union = union(rows);
    bencher.bench_local(|| black_box(&union).to_sketch(HllType::Hll8).serialize());
}

pub fn register(runner: &mut Runner) {
    let cases = &[
        (0, 15_000_000),
        (4, 12_000_000),
        (100, 2_000_000),
        (80_000, 90_000),
    ];
    for &(rows, rounds) in cases {
        runner.case(
            format!("hll::serde::serialize_union/{rows:?}"),
            rounds,
            |b| serialize_union(b, rows),
        );
    }
    for &(rows, rounds) in cases {
        runner.case(
            format!("hll::serde::serialize_union_via_sketch/{rows:?}"),
            rounds,
            |b| serialize_union_via_sketch(b, rows),
        );
    }
}
