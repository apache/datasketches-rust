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

use datasketches::tdigest::TDigestMut;
use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

use super::support::DEFAULT_DIGEST_K;
use super::support::PARTIAL_GROUPS;
use super::support::ROWS_PER_PARTIAL;
use super::support::SMALL_ROWS_PER_PARTIAL;
use super::support::build_digest;
use super::support::partial_value;
use super::support::values;

#[divan::bench(args = [1_000, 100_000])]
fn update(bencher: Bencher, len: usize) {
    let values = values(len);

    bencher
        .counter(ItemsCount::new(len))
        .bench_local(|| build_digest(black_box(&values)));
}

#[divan::bench]
fn small_partial_groups_update(bencher: Bencher) {
    bencher
        .counter(ItemsCount::new(PARTIAL_GROUPS * SMALL_ROWS_PER_PARTIAL))
        .bench_local(|| {
            let mut digests = (0..PARTIAL_GROUPS)
                .map(|_| TDigestMut::default())
                .collect::<Vec<_>>();
            for (group, digest) in digests.iter_mut().enumerate() {
                for row in 0..SMALL_ROWS_PER_PARTIAL {
                    digest.update(partial_value(group, row, SMALL_ROWS_PER_PARTIAL));
                }
            }
            black_box(digests)
        });
}

#[divan::bench]
fn small_partial_groups_update_two_states(bencher: Bencher) {
    bencher
        .counter(ItemsCount::new(PARTIAL_GROUPS * SMALL_ROWS_PER_PARTIAL))
        .bench_local(|| {
            let mut digests = (0..PARTIAL_GROUPS)
                .map(|_| (TDigestMut::default(), TDigestMut::default()))
                .collect::<Vec<_>>();
            for (group, (first, second)) in digests.iter_mut().enumerate() {
                for row in 0..SMALL_ROWS_PER_PARTIAL {
                    let value = partial_value(group, row, SMALL_ROWS_PER_PARTIAL);
                    first.update(value);
                    second.update(value);
                }
            }
            black_box(digests)
        });
}

#[divan::bench]
fn partial_groups_update(bencher: Bencher) {
    bencher
        .counter(ItemsCount::new(PARTIAL_GROUPS * ROWS_PER_PARTIAL))
        .bench_local(|| {
            let mut digests = (0..PARTIAL_GROUPS)
                .map(|_| TDigestMut::new(DEFAULT_DIGEST_K).unwrap())
                .collect::<Vec<_>>();
            for (group, digest) in digests.iter_mut().enumerate() {
                for row in 0..ROWS_PER_PARTIAL {
                    digest.update(partial_value(group, row, ROWS_PER_PARTIAL));
                }
            }
            black_box(digests)
        });
}

#[divan::bench]
fn partial_groups_update_two_states(bencher: Bencher) {
    bencher
        .counter(ItemsCount::new(PARTIAL_GROUPS * ROWS_PER_PARTIAL))
        .bench_local(|| {
            let mut digests = (0..PARTIAL_GROUPS)
                .map(|_| {
                    (
                        TDigestMut::new(DEFAULT_DIGEST_K).unwrap(),
                        TDigestMut::new(DEFAULT_DIGEST_K).unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            for (group, (first, second)) in digests.iter_mut().enumerate() {
                for row in 0..ROWS_PER_PARTIAL {
                    let value = partial_value(group, row, ROWS_PER_PARTIAL);
                    first.update(value);
                    second.update(value);
                }
            }
            black_box(digests)
        });
}
