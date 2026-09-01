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
use super::support::ROWS_PER_PARTIAL;
use super::support::SMALL_ROWS_PER_PARTIAL;
use super::support::build_mut_digest;
use super::support::partial_digests;
use super::support::partial_digests_with;
use super::support::serialized_partial_digests;
use super::support::values;

#[divan::bench]
fn merge(bencher: Bencher) {
    let values = values(200_000);
    let mut left = build_mut_digest(&values[..100_000]);
    let mut right = build_mut_digest(&values[100_000..]);
    black_box(left.rank(0.5));
    black_box(right.rank(0.5));

    bencher
        .counter(ItemsCount::new(values.len()))
        .with_inputs(|| left.clone())
        .bench_local_values(|mut left| {
            left.merge(black_box(&right));
            black_box(left)
        });
}

#[divan::bench(args = [8, 1_640])]
fn unmerged(bencher: Bencher, left_rows: usize) {
    let left = build_mut_digest(&values(left_rows));
    let right = build_mut_digest(&values(1_640));

    bencher
        .counter(ItemsCount::new(left_rows + 1_640))
        .with_inputs(|| left.clone())
        .bench_local_values(|mut left| {
            left.merge(black_box(&right));
            black_box(left)
        });
}

#[divan::bench]
fn small_partials(bencher: Bencher) {
    let partials = partial_digests(64, SMALL_ROWS_PER_PARTIAL)
        .into_iter()
        .map(|mut digest| {
            black_box(digest.rank(0.0));
            digest
        })
        .collect::<Vec<_>>();

    bencher
        .counter(ItemsCount::new(64 * SMALL_ROWS_PER_PARTIAL))
        .bench_local(|| {
            let mut merged = TDigestMut::default();
            for partial in &partials {
                merged.merge(black_box(partial));
            }
            black_box(merged)
        });
}

#[divan::bench]
fn partials(bencher: Bencher) {
    let partials = partial_digests_with(DEFAULT_DIGEST_K, 64, ROWS_PER_PARTIAL)
        .into_iter()
        .map(|mut digest| {
            black_box(digest.rank(0.0));
            digest
        })
        .collect::<Vec<_>>();

    bencher
        .counter(ItemsCount::new(64 * ROWS_PER_PARTIAL))
        .bench_local(|| {
            let mut merged = TDigestMut::new(DEFAULT_DIGEST_K).unwrap();
            for partial in &partials {
                merged.merge(black_box(partial));
            }
            black_box(merged)
        });
}

#[divan::bench(args = [SMALL_ROWS_PER_PARTIAL, ROWS_PER_PARTIAL])]
fn serialized_partials(bencher: Bencher, rows_per_partial: usize) {
    let partials = serialized_partial_digests(64, rows_per_partial);

    bencher
        .counter(ItemsCount::new(64 * rows_per_partial))
        .bench_local(|| {
            let mut merged = TDigestMut::default();
            for partial in black_box(&partials) {
                let partial = TDigestMut::deserialize(partial).unwrap();
                merged.merge(&partial);
            }
            black_box(merged.quantile(0.5))
        });
}

#[divan::bench]
fn serialized_overlapping_partials(bencher: Bencher) {
    let values = values(64 * ROWS_PER_PARTIAL);
    let partials = values
        .chunks_exact(ROWS_PER_PARTIAL)
        .map(|values| build_mut_digest(values).serialize())
        .collect::<Vec<_>>();

    bencher
        .counter(ItemsCount::new(values.len()))
        .bench_local(|| {
            let mut merged = TDigestMut::default();
            for partial in black_box(&partials) {
                let partial = TDigestMut::deserialize(partial).unwrap();
                merged.merge(&partial);
            }
            black_box(merged.quantile(0.5))
        });
}
