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

fn build<F>(bencher: Bencher, cardinality: usize, build: impl Fn(&[u64]) -> F) {
    // Multiple independent sets keep a particular seed's retry count on one set
    // from dominating the comparison of default construction policies.
    let inputs: Vec<_> = (0..32)
        .map(|i| hashes_with_offset(cardinality, i * cardinality as u64))
        .collect();
    bencher.bench_local(|| {
        for keys in &inputs {
            black_box(build(keys));
        }
    });
}

pub fn register(runner: &mut Runner) {
    for (cardinality, rounds) in [(128, 10_000), (16_384, 40), (50_000, 12)] {
        runner.compare(
            &format!("xor::build_batches::8/{cardinality}"),
            rounds,
            &[
                ("default_seed", &|b| {
                    build(b, cardinality, |keys| {
                        XorFilterBuilder::new(XorFilterType::Xor8)
                            .build_from_distinct_hashes(keys)
                            .unwrap()
                    })
                }),
                ("matched_seed", &|b| {
                    build(b, cardinality, |keys| {
                        XorFilterBuilder::new(XorFilterType::Xor8)
                            .seed(1)
                            .build_from_distinct_hashes(keys)
                            .unwrap()
                    })
                }),
                ("xorf", &|b| {
                    build(b, cardinality, |keys| xorf::Xor8::from(keys))
                }),
            ],
        );
    }
}
