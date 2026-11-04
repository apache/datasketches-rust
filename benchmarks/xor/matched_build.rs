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

use crate::xor::hashes_with_offset;

// xorf 0.13 starts SplitMix at 1. These are its first two construction seeds.
// Check each result directly: building a peer for validation inside the timed
// workload would dilute the comparison, especially for the single-round cases.
fn verify_seed(seed: u64, offset: u64) {
    let expected = match offset {
        0 => 0x910a_2dec_8902_5cc1,
        917_504 => 0xbeeb_8da1_658e_ec67,
        _ => unreachable!("unregistered input offset"),
    };
    assert_eq!(seed, expected, "construction attempt count changed");
}

fn datasketches_8(bencher: Bencher, case: (usize, u64)) {
    let (cardinality, offset) = case;
    let keys = hashes_with_offset(cardinality, offset);
    bencher.bench_local(|| {
        let filter = XorFilterBuilder::new(XorFilterType::Xor8)
            .seed(1)
            .build_from_distinct_hashes(&keys)
            .unwrap();
        verify_seed(filter.seed(), offset);
        black_box(filter)
    });
}

fn xorf_8(bencher: Bencher, case: (usize, u64)) {
    let (cardinality, offset) = case;
    let keys = hashes_with_offset(cardinality, offset);
    bencher.bench_local(|| {
        let filter = xorf::Xor8::from_iterator(keys.iter().copied());
        verify_seed(filter.seed, offset);
        black_box(filter)
    });
}

fn datasketches_16(bencher: Bencher, case: (usize, u64)) {
    let (cardinality, offset) = case;
    let keys = hashes_with_offset(cardinality, offset);
    bencher.bench_local(|| {
        let filter = XorFilterBuilder::new(XorFilterType::Xor16)
            .seed(1)
            .build_from_distinct_hashes(&keys)
            .unwrap();
        verify_seed(filter.seed(), offset);
        black_box(filter)
    });
}

fn xorf_16(bencher: Bencher, case: (usize, u64)) {
    let (cardinality, offset) = case;
    let keys = hashes_with_offset(cardinality, offset);
    bencher.bench_local(|| {
        let filter = xorf::Xor16::from_iterator(keys.iter().copied());
        verify_seed(filter.seed, offset);
        black_box(filter)
    });
}

fn datasketches_32(bencher: Bencher, case: (usize, u64)) {
    let (cardinality, offset) = case;
    let keys = hashes_with_offset(cardinality, offset);
    bencher.bench_local(|| {
        let filter = XorFilterBuilder::new(XorFilterType::Xor32)
            .seed(1)
            .build_from_distinct_hashes(&keys)
            .unwrap();
        verify_seed(filter.seed(), offset);
        black_box(filter)
    });
}

fn xorf_32(bencher: Bencher, case: (usize, u64)) {
    let (cardinality, offset) = case;
    let keys = hashes_with_offset(cardinality, offset);
    bencher.bench_local(|| {
        let filter = xorf::Xor32::from_iterator(keys.iter().copied());
        verify_seed(filter.seed, offset);
        black_box(filter)
    });
}

pub fn register(runner: &mut Runner) {
    let cases = &[
        ((1_000, 0), 60_000),
        ((16_384, 0), 3_000),
        ((16_384, 917_504), 2_000),
        ((49_999, 0), 600),
        ((50_000, 0), 600),
        ((50_001, 0), 600),
        ((100_000, 0), 200),
        ((8_000_000, 0), 1),
    ];
    for &(case, rounds) in cases {
        runner.compare(
            &format!("xor::matched_build::8/{case:?}"),
            rounds,
            &[
                ("DataSketches", &|b| datasketches_8(b, case)),
                ("xorf", &|b| xorf_8(b, case)),
            ],
        );
    }
    for &(case, rounds) in cases {
        runner.compare(
            &format!("xor::matched_build::16/{case:?}"),
            rounds,
            &[
                ("DataSketches", &|b| datasketches_16(b, case)),
                ("xorf", &|b| xorf_16(b, case)),
            ],
        );
    }
    for &(case, rounds) in cases {
        runner.compare(
            &format!("xor::matched_build::32/{case:?}"),
            rounds,
            &[
                ("DataSketches", &|b| datasketches_32(b, case)),
                ("xorf", &|b| xorf_32(b, case)),
            ],
        );
    }
}
