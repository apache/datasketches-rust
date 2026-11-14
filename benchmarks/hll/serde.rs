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

fn serialize_union(bencher: Bencher, rows: u64, hll_type: HllType, direct: bool) {
    let union = union(rows);
    if direct {
        bencher.bench_local(|| black_box(&union).serialize(hll_type));
    } else {
        bencher.bench_local(|| black_box(&union).to_sketch(hll_type).serialize());
    }
}

pub fn register(runner: &mut Runner) {
    let targets = [
        ("hll4", HllType::Hll4, 500),
        ("hll6", HllType::Hll6, 600),
        ("hll8", HllType::Hll8, 50_000),
    ];
    let sparse_cases = [(0, 10_000_000), (4, 8_000_000), (100, 1_500_000)];
    for &(label, hll_type, dense_rounds) in &targets {
        for &(rows, rounds) in &sparse_cases {
            runner.compare(
                &format!("hll::serde::serialize_union/{label}/{rows:?}"),
                rounds,
                &[
                    ("via_sketch", &|b| serialize_union(b, rows, hll_type, false)),
                    ("direct", &|b| serialize_union(b, rows, hll_type, true)),
                ],
            );
        }
        runner.compare(
            &format!("hll::serde::serialize_union/{label}/80000"),
            dense_rounds,
            &[
                ("via_sketch", &|b| {
                    serialize_union(b, 80_000, hll_type, false)
                }),
                ("direct", &|b| serialize_union(b, 80_000, hll_type, true)),
            ],
        );
    }
}
