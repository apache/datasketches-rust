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
use datasketches::xor::XorFilter;
use datasketches::xor::XorFilterType;
use datasketches::xor::XorFingerprints;
use xorf::Filter;

use crate::xor::hashes_with_offset;

const SHARDS: usize = 32;
const QUERIES: usize = 256;

// Storage formats can choose a fingerprint width at runtime. Give both peers
// this dispatch, and import identical payloads without cardinality metadata.
enum Reference {
    Xor8(xorf::Xor8),
    Xor16(xorf::Xor16),
    Xor32(xorf::Xor32),
}

impl Reference {
    fn new(keys: &[u64], kind: XorFilterType) -> Self {
        match kind {
            XorFilterType::Xor8 => Self::Xor8(xorf::Xor8::from(keys)),
            XorFilterType::Xor16 => Self::Xor16(xorf::Xor16::from(keys)),
            XorFilterType::Xor32 => Self::Xor32(xorf::Xor32::from(keys)),
        }
    }

    #[inline(always)]
    fn contains(&self, key: u64) -> bool {
        match self {
            Self::Xor8(filter) => filter.contains(&key),
            Self::Xor16(filter) => filter.contains(&key),
            Self::Xor32(filter) => filter.contains(&key),
        }
    }

    fn into_datasketches(self) -> XorFilter {
        let (seed, segment, fingerprints) = match self {
            Self::Xor8(f) => (
                f.seed,
                f.block_length,
                XorFingerprints::Xor8(f.fingerprints),
            ),
            Self::Xor16(f) => (
                f.seed,
                f.block_length,
                XorFingerprints::Xor16(f.fingerprints),
            ),
            Self::Xor32(f) => (
                f.seed,
                f.block_length,
                XorFingerprints::Xor32(f.fingerprints),
            ),
        };
        XorFilter::from_parts(seed, segment, fingerprints, None).unwrap()
    }
}

fn query<F>(
    bencher: Bencher,
    cardinality: usize,
    present: bool,
    make_filter: impl Fn(&[u64]) -> F,
    contains: impl Fn(&F, u64) -> bool,
) {
    let mut filters = Vec::with_capacity(SHARDS);
    let mut queries = Vec::with_capacity(SHARDS);
    for shard in 0..SHARDS {
        let offset = shard as u64 * cardinality as u64;
        let keys = hashes_with_offset(cardinality, offset);
        filters.push(make_filter(&keys));
        queries.push(if present {
            keys.iter()
                .copied()
                .cycle()
                .take(QUERIES)
                .collect::<Vec<_>>()
        } else {
            hashes_with_offset(QUERIES, (1 << 40) + shard as u64 * QUERIES as u64)
        });
    }
    let filters = black_box(filters);
    let queries = black_box(queries);
    bencher.bench_local(|| {
        for query in 0..QUERIES {
            for (filter, keys) in filters.iter().zip(&queries) {
                black_box(contains(filter, black_box(keys[query])));
            }
        }
    });
}

pub fn register(runner: &mut Runner) {
    for (bits, kind) in [
        (8, XorFilterType::Xor8),
        (16, XorFilterType::Xor16),
        (32, XorFilterType::Xor32),
    ] {
        for cardinality in [128, 16_384] {
            for (name, present) in [("present", true), ("absent", false)] {
                runner.compare(
                    &format!("xor::query_shards::{bits}_{name}/{cardinality}"),
                    20_000,
                    &[
                        ("DataSketches", &|b| {
                            query(
                                b,
                                cardinality,
                                present,
                                |keys| Reference::new(keys, kind).into_datasketches(),
                                XorFilter::contains_hash,
                            )
                        }),
                        ("xorf", &|b| {
                            query(
                                b,
                                cardinality,
                                present,
                                |keys| Reference::new(keys, kind),
                                Reference::contains,
                            )
                        }),
                    ],
                );
            }
        }
    }
}
