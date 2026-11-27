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
use datasketches::ddsketch::DdSketch;
use datasketches::ddsketch::StoreKind;

use super::support::partial_f64_sketches;
use super::support::partial_sketches;
use super::support::prepared_f64_sketch;
use super::support::prepared_sketch;

fn serialize_full_u64(bencher: Bencher) {
    let sketch = prepared_sketch();

    bencher.bench_local(|| black_box(&sketch).serialize());
}

fn serialize_full_f64(bencher: Bencher) {
    let sketch = prepared_f64_sketch();

    bencher.bench_local(|| black_box(&sketch).serialize());
}

fn deserialize_full_u64(bencher: Bencher) {
    let bytes = prepared_sketch().serialize();

    bencher.bench_local(|| DdSketch::<u64>::deserialize(black_box(&bytes)).unwrap());
}

fn deserialize_full_f64(bencher: Bencher) {
    let bytes = prepared_f64_sketch().serialize();

    bencher.bench_local(|| DdSketch::<f64>::deserialize(black_box(&bytes)).unwrap());
}

fn deserialize_and_merge_partials_u64(bencher: Bencher) {
    let payloads: Vec<Vec<u8>> = partial_sketches()
        .into_iter()
        .map(|s| s.serialize())
        .collect();

    bencher.bench_local(|| {
        let mut merged = DdSketch::<u64>::default();
        for payload in black_box(&payloads) {
            let partial = DdSketch::<u64>::deserialize(payload).unwrap();
            merged.merge(&partial).unwrap();
        }
        black_box(merged)
    });
}

fn deserialize_and_merge_partials_f64(bencher: Bencher) {
    let payloads: Vec<Vec<u8>> = partial_f64_sketches()
        .into_iter()
        .map(|s| s.serialize())
        .collect();

    bencher.bench_local(|| {
        let mut merged = DdSketch::<f64>::default();
        for payload in black_box(&payloads) {
            let partial = DdSketch::<f64>::deserialize(payload).unwrap();
            merged.merge(&partial).unwrap();
        }
        black_box(merged)
    });
}

fn compat_serialize_full_u64(bencher: Bencher) {
    let sketch = prepared_sketch();

    bencher.bench_local(|| black_box(&sketch).serialize_compat().unwrap());
}

fn compat_serialize_full_f64(bencher: Bencher) {
    let sketch = prepared_f64_sketch();

    bencher.bench_local(|| black_box(&sketch).serialize_compat().unwrap());
}

fn compat_deserialize_full_u64(bencher: Bencher) {
    let bytes = prepared_sketch().serialize_compat().unwrap();

    bencher.bench_local(|| {
        DdSketch::<u64>::deserialize_compat(black_box(&bytes), StoreKind::default()).unwrap()
    });
}

fn compat_deserialize_full_f64(bencher: Bencher) {
    let bytes = prepared_f64_sketch().serialize_compat().unwrap();

    bencher.bench_local(|| {
        DdSketch::<f64>::deserialize_compat(black_box(&bytes), StoreKind::default()).unwrap()
    });
}

pub fn register(runner: &mut Runner) {
    runner.compare(
        "ddsketch::serde::serialize/100000",
        50_000,
        &[("u64", &serialize_full_u64), ("f64", &serialize_full_f64)],
    );
    runner.compare(
        "ddsketch::serde::deserialize/100000",
        40_000,
        &[
            ("u64", &deserialize_full_u64),
            ("f64", &deserialize_full_f64),
        ],
    );
    runner.compare(
        "ddsketch::serde::deserialize_and_merge/64",
        1_500,
        &[
            ("u64", &deserialize_and_merge_partials_u64),
            ("f64", &deserialize_and_merge_partials_f64),
        ],
    );
    runner.compare(
        "ddsketch::serde::compat_serialize/100000",
        60_000,
        &[
            ("u64", &compat_serialize_full_u64),
            ("f64", &compat_serialize_full_f64),
        ],
    );
    runner.compare(
        "ddsketch::serde::compat_deserialize/100000",
        30_000,
        &[
            ("u64", &compat_deserialize_full_u64),
            ("f64", &compat_deserialize_full_f64),
        ],
    );
}
