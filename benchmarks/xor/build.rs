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

use crate::xor::hashes;

fn datasketches_distinct_8(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality);
    bencher.bench_local(|| {
        black_box(
            XorFilterBuilder::new(XorFilterType::Xor8)
                .build_from_distinct_hashes(&keys)
                .unwrap(),
        )
    });
}

fn datasketches_deduplicate_8(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality);
    bencher.bench_local(|| {
        black_box(
            XorFilterBuilder::new(XorFilterType::Xor8)
                .build_from_hashes(keys.iter().copied())
                .unwrap(),
        )
    });
}

fn xorf_8(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality);
    bencher.bench_local(|| black_box(xorf::Xor8::from_iterator(keys.iter().copied())));
}

fn datasketches_distinct_16(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality);
    bencher.bench_local(|| {
        black_box(
            XorFilterBuilder::new(XorFilterType::Xor16)
                .build_from_distinct_hashes(&keys)
                .unwrap(),
        )
    });
}

fn datasketches_deduplicate_16(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality);
    bencher.bench_local(|| {
        black_box(
            XorFilterBuilder::new(XorFilterType::Xor16)
                .build_from_hashes(keys.iter().copied())
                .unwrap(),
        )
    });
}

fn xorf_16(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality);
    bencher.bench_local(|| black_box(xorf::Xor16::from_iterator(keys.iter().copied())));
}

fn datasketches_distinct_32(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality);
    bencher.bench_local(|| {
        black_box(
            XorFilterBuilder::new(XorFilterType::Xor32)
                .build_from_distinct_hashes(&keys)
                .unwrap(),
        )
    });
}

fn datasketches_deduplicate_32(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality);
    bencher.bench_local(|| {
        black_box(
            XorFilterBuilder::new(XorFilterType::Xor32)
                .build_from_hashes(keys.iter().copied())
                .unwrap(),
        )
    });
}

fn xorf_32(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality);
    bencher.bench_local(|| black_box(xorf::Xor32::from_iterator(keys.iter().copied())));
}

pub fn register(runner: &mut Runner) {
    let cases = &[
        (1_000, 60_000),
        (16_384, 2_000),
        (49_999, 600),
        (50_000, 600),
        (50_001, 600),
        (100_000, 200),
    ];
    for &(cardinality, rounds) in cases {
        runner.compare(
            &format!("xor::build::8/{cardinality}"),
            rounds,
            &[
                ("distinct", &|b| datasketches_distinct_8(b, cardinality)),
                ("deduplicate", &|b| {
                    datasketches_deduplicate_8(b, cardinality)
                }),
                ("xorf", &|b| xorf_8(b, cardinality)),
            ],
        );
    }
    for &(cardinality, rounds) in cases {
        runner.compare(
            &format!("xor::build::16/{cardinality}"),
            rounds,
            &[
                ("distinct", &|b| datasketches_distinct_16(b, cardinality)),
                ("deduplicate", &|b| {
                    datasketches_deduplicate_16(b, cardinality)
                }),
                ("xorf", &|b| xorf_16(b, cardinality)),
            ],
        );
    }
    for &(cardinality, rounds) in cases {
        runner.compare(
            &format!("xor::build::32/{cardinality}"),
            rounds,
            &[
                ("distinct", &|b| datasketches_distinct_32(b, cardinality)),
                ("deduplicate", &|b| {
                    datasketches_deduplicate_32(b, cardinality)
                }),
                ("xorf", &|b| xorf_32(b, cardinality)),
            ],
        );
    }
}
