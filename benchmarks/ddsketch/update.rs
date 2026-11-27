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
use datasketches::ddsketch::MappingKind;
use datasketches::ddsketch::StoreKind;

use super::support::build_sketch;
use super::support::build_weighted_f64_sketch;
use super::support::build_weighted_u64_sketch;
use super::support::fractional_weights;
use super::support::latency_values;

fn update_with(bencher: Bencher, len: usize, mapping: MappingKind, store: StoreKind) {
    let values = latency_values(len);

    bencher.bench_local(|| build_sketch(black_box(&values), mapping, store));
}

fn update_weighted_u64(bencher: Bencher, len: usize) {
    let values = latency_values(len);
    let weights: Vec<u64> = (0..len).map(|i| ((i % 9) + 1) as u64).collect();

    bencher.bench_local(|| {
        build_weighted_u64_sketch(
            black_box(&values),
            black_box(&weights),
            MappingKind::Cubic,
            StoreKind::default(),
        )
    });
}

fn update_weighted_f64(bencher: Bencher, len: usize) {
    let values = latency_values(len);
    let weights = fractional_weights(len);

    bencher.bench_local(|| {
        build_weighted_f64_sketch(
            black_box(&values),
            black_box(&weights),
            MappingKind::Cubic,
            StoreKind::default(),
        )
    });
}

pub fn register(runner: &mut Runner) {
    for &(len, rounds) in &[(1_000, 100_000), (100_000, 1_500)] {
        runner.compare(
            &format!("ddsketch::update::mapping/{len}"),
            rounds,
            &[
                ("Cubic", &|b| {
                    update_with(b, len, MappingKind::Cubic, StoreKind::default())
                }),
                ("Linear", &|b| {
                    update_with(b, len, MappingKind::Linear, StoreKind::default())
                }),
                ("Logarithmic", &|b| {
                    update_with(b, len, MappingKind::Logarithmic, StoreKind::default())
                }),
            ],
        );
    }

    runner.compare(
        "ddsketch::update::store/10000",
        15_000,
        &[
            ("CollapsingLowest", &|b| {
                update_with(b, 10_000, MappingKind::Cubic, StoreKind::default())
            }),
            ("UnboundedDense", &|b| {
                update_with(b, 10_000, MappingKind::Cubic, StoreKind::UnboundedDense)
            }),
            ("Sparse", &|b| {
                update_with(b, 10_000, MappingKind::Cubic, StoreKind::Sparse)
            }),
        ],
    );

    runner.compare(
        "ddsketch::update::weight/100000",
        600,
        &[
            ("u64", &|b| update_weighted_u64(b, 100_000)),
            ("f64", &|b| update_weighted_f64(b, 100_000)),
        ],
    );
}
