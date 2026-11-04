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
use datasketches::xor::XorFilterBuilder;
use datasketches::xor::XorFilterType;
use xorf::Filter as _;

use crate::xor::hashes;

fn datasketches_8_present(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality * 2);
    let filter = XorFilterBuilder::new(XorFilterType::Xor8)
        .build_from_distinct_hashes(&keys[..cardinality])
        .unwrap();
    bencher.bench_local(|| {
        for key in &keys[..cardinality] {
            black_box(filter.contains_hash(black_box(*key)));
        }
    });
}

fn datasketches_8_absent(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality * 2);
    let filter = XorFilterBuilder::new(XorFilterType::Xor8)
        .build_from_distinct_hashes(&keys[..cardinality])
        .unwrap();
    bencher.bench_local(|| {
        for key in &keys[cardinality..] {
            black_box(filter.contains_hash(black_box(*key)));
        }
    });
}

fn xorf_8_present(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality * 2);
    let filter = xorf::Xor8::from(&keys[..cardinality]);
    bencher.bench_local(|| {
        for key in &keys[..cardinality] {
            black_box(filter.contains(black_box(key)));
        }
    });
}

fn xorf_8_absent(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality * 2);
    let filter = xorf::Xor8::from(&keys[..cardinality]);
    bencher.bench_local(|| {
        for key in &keys[cardinality..] {
            black_box(filter.contains(black_box(key)));
        }
    });
}

fn datasketches_16_present(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality * 2);
    let filter = XorFilterBuilder::new(XorFilterType::Xor16)
        .build_from_distinct_hashes(&keys[..cardinality])
        .unwrap();
    bencher.bench_local(|| {
        for key in &keys[..cardinality] {
            black_box(filter.contains_hash(black_box(*key)));
        }
    });
}

fn datasketches_16_absent(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality * 2);
    let filter = XorFilterBuilder::new(XorFilterType::Xor16)
        .build_from_distinct_hashes(&keys[..cardinality])
        .unwrap();
    bencher.bench_local(|| {
        for key in &keys[cardinality..] {
            black_box(filter.contains_hash(black_box(*key)));
        }
    });
}

fn xorf_16_present(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality * 2);
    let filter = xorf::Xor16::from(&keys[..cardinality]);
    bencher.bench_local(|| {
        for key in &keys[..cardinality] {
            black_box(filter.contains(black_box(key)));
        }
    });
}

fn xorf_16_absent(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality * 2);
    let filter = xorf::Xor16::from(&keys[..cardinality]);
    bencher.bench_local(|| {
        for key in &keys[cardinality..] {
            black_box(filter.contains(black_box(key)));
        }
    });
}

fn datasketches_32_present(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality * 2);
    let filter = XorFilterBuilder::new(XorFilterType::Xor32)
        .build_from_distinct_hashes(&keys[..cardinality])
        .unwrap();
    bencher.bench_local(|| {
        for key in &keys[..cardinality] {
            black_box(filter.contains_hash(black_box(*key)));
        }
    });
}

fn datasketches_32_absent(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality * 2);
    let filter = XorFilterBuilder::new(XorFilterType::Xor32)
        .build_from_distinct_hashes(&keys[..cardinality])
        .unwrap();
    bencher.bench_local(|| {
        for key in &keys[cardinality..] {
            black_box(filter.contains_hash(black_box(*key)));
        }
    });
}

fn xorf_32_present(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality * 2);
    let filter = xorf::Xor32::from(&keys[..cardinality]);
    bencher.bench_local(|| {
        for key in &keys[..cardinality] {
            black_box(filter.contains(black_box(key)));
        }
    });
}

fn xorf_32_absent(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality * 2);
    let filter = xorf::Xor32::from(&keys[..cardinality]);
    bencher.bench_local(|| {
        for key in &keys[cardinality..] {
            black_box(filter.contains(black_box(key)));
        }
    });
}

pub fn register(runner: &mut Runner) {
    let cases = &[(16_384, 10_000), (50_000, 3_000)];
    for &(cardinality, rounds) in cases {
        runner.compare(
            &format!("xor::query::8_present/{cardinality}"),
            rounds,
            &[
                ("DataSketches", &|b| datasketches_8_present(b, cardinality)),
                ("xorf", &|b| xorf_8_present(b, cardinality)),
            ],
        );
    }
    for &(cardinality, rounds) in cases {
        runner.compare(
            &format!("xor::query::8_absent/{cardinality}"),
            rounds,
            &[
                ("DataSketches", &|b| datasketches_8_absent(b, cardinality)),
                ("xorf", &|b| xorf_8_absent(b, cardinality)),
            ],
        );
    }
    for &(cardinality, rounds) in cases {
        runner.compare(
            &format!("xor::query::16_present/{cardinality}"),
            rounds,
            &[
                ("DataSketches", &|b| datasketches_16_present(b, cardinality)),
                ("xorf", &|b| xorf_16_present(b, cardinality)),
            ],
        );
    }
    for &(cardinality, rounds) in cases {
        runner.compare(
            &format!("xor::query::16_absent/{cardinality}"),
            rounds,
            &[
                ("DataSketches", &|b| datasketches_16_absent(b, cardinality)),
                ("xorf", &|b| xorf_16_absent(b, cardinality)),
            ],
        );
    }
    for &(cardinality, rounds) in cases {
        runner.compare(
            &format!("xor::query::32_present/{cardinality}"),
            rounds,
            &[
                ("DataSketches", &|b| datasketches_32_present(b, cardinality)),
                ("xorf", &|b| xorf_32_present(b, cardinality)),
            ],
        );
    }
    for &(cardinality, rounds) in cases {
        runner.compare(
            &format!("xor::query::32_absent/{cardinality}"),
            rounds,
            &[
                ("DataSketches", &|b| datasketches_32_absent(b, cardinality)),
                ("xorf", &|b| xorf_32_absent(b, cardinality)),
            ],
        );
    }
}
