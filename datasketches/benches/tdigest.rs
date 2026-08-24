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

use datasketches::tdigest::TDigest;
use datasketches::tdigest::TDigestMut;
use divan::AllocProfiler;
use divan::Bencher;
use divan::black_box;
use divan::black_box_drop;
use divan::counter::BytesCount;
use divan::counter::ItemsCount;

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

const PARTIAL_GROUPS: usize = 512;
const SMALL_ROWS_PER_PARTIAL: usize = 8;
const DEFAULT_DIGEST_K: u16 = 200;
const ROWS_PER_PARTIAL: usize = 64;

fn main() {
    divan::main();
}

#[divan::bench(args = [1_000, 100_000])]
fn update(bencher: Bencher, len: usize) {
    let values = values(len);

    bencher
        .counter(ItemsCount::new(len))
        .bench_local(|| build_digest(black_box(&values)));
}

#[divan::bench(args = [1, 2, 8, 32, 64, 128])]
fn small_digest_lifecycle(bencher: Bencher, rows: usize) {
    let values = values(rows);
    let (serialized_bytes, _) = serialized_state_shape(200, &values);

    bencher
        .counter(ItemsCount::new(rows))
        .counter(BytesCount::new(serialized_bytes))
        .bench_local(|| {
            let mut digest = TDigestMut::default();
            for &value in black_box(&values) {
                digest.update(value);
            }
            let bytes = digest.serialize();
            black_box_drop(bytes);
            black_box_drop(digest);
        });
}

#[divan::bench(args = [10_u16, 200_u16])]
fn partial_digest_lifecycle_by_k(bencher: Bencher, k: u16) {
    let values = values(ROWS_PER_PARTIAL);
    let (serialized_bytes, centroids) = serialized_state_shape(k, &values);
    assert!(matches!(
        (k, serialized_bytes, centroids),
        (10, 224, 12) | (200, 1_056, 64)
    ));

    bencher
        .counter(ItemsCount::new(ROWS_PER_PARTIAL))
        .counter(BytesCount::new(serialized_bytes))
        .bench_local(|| {
            let mut digest = TDigestMut::new(black_box(k));
            for &value in black_box(&values) {
                digest.update(value);
            }
            let bytes = digest.serialize();
            black_box_drop(bytes);
            black_box_drop(digest);
        });
}

#[divan::bench]
fn compress_initial_buffer(bencher: Bencher) {
    // The default k=200 digest buffers 1,640 values before automatic compression.
    let values = values(1_640);
    let digest = build_mut_digest(&values);

    bencher
        .counter(ItemsCount::new(values.len()))
        .with_inputs(|| digest.clone())
        .bench_local_values(|mut digest| black_box(digest.rank(0.5)));
}

#[divan::bench]
fn compress_unmerged_tail(bencher: Bencher) {
    let values = values(3_280);
    let mut digest = build_mut_digest(&values[..1_640]);
    black_box(digest.rank(0.5));
    for &value in &values[1_640..] {
        digest.update(value);
    }

    bencher
        .counter(ItemsCount::new(1_640_usize))
        .with_inputs(|| digest.clone())
        .bench_local_values(|mut digest| black_box(digest.rank(0.5)));
}

#[divan::bench]
fn freeze_initial_buffer(bencher: Bencher) {
    let values = values(1_640);
    let digest = build_mut_digest(&values);

    bencher
        .counter(ItemsCount::new(values.len()))
        .with_inputs(|| digest.clone())
        .bench_local_values(|digest| black_box(digest.freeze()));
}

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
fn unmerged_merge(bencher: Bencher, left_rows: usize) {
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
fn rank(bencher: Bencher) {
    let digest = prepared_digest();

    bencher.bench_local(|| black_box(&digest).rank(black_box(0.531_25)));
}

#[divan::bench]
fn quantile(bencher: Bencher) {
    let digest = prepared_digest();

    bencher.bench_local(|| black_box(&digest).quantile(black_box(0.531_25)));
}

#[divan::bench]
fn cdf_100(bencher: Bencher) {
    let digest = prepared_digest();
    let split_points = (1..=100).map(|i| i as f64 / 101.0).collect::<Vec<_>>();

    bencher
        .counter(ItemsCount::new(split_points.len()))
        .bench_local(|| black_box(&digest).cdf(black_box(&split_points)));
}

#[divan::bench]
fn quantiles_2_sequential(bencher: Bencher) {
    let digest = prepared_digest();

    bencher.bench_local(|| {
        [
            black_box(&digest).quantile(black_box(0.5)),
            black_box(&digest).quantile(black_box(0.95)),
        ]
    });
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
fn small_partial_groups_serialize(bencher: Bencher) {
    let digests = partial_digests(PARTIAL_GROUPS, SMALL_ROWS_PER_PARTIAL);

    bencher
        .counter(ItemsCount::new(PARTIAL_GROUPS))
        .with_inputs(|| digests.clone())
        .bench_local_values(|mut digests| {
            let bytes = digests
                .iter_mut()
                .map(TDigestMut::serialize)
                .collect::<Vec<_>>();
            black_box(bytes)
        });
}

#[divan::bench]
fn small_partial_groups_deserialize(bencher: Bencher) {
    let bytes = serialized_partial_digests(PARTIAL_GROUPS, SMALL_ROWS_PER_PARTIAL);

    bencher
        .counter(ItemsCount::new(PARTIAL_GROUPS))
        .bench_local(|| {
            let digests = bytes
                .iter()
                .map(|bytes| TDigestMut::deserialize(bytes, false).unwrap())
                .collect::<Vec<_>>();
            black_box(digests)
        });
}

#[divan::bench]
fn small_partial_merge(bencher: Bencher) {
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
fn partial_groups_update(bencher: Bencher) {
    bencher
        .counter(ItemsCount::new(PARTIAL_GROUPS * ROWS_PER_PARTIAL))
        .bench_local(|| {
            let mut digests = (0..PARTIAL_GROUPS)
                .map(|_| TDigestMut::new(DEFAULT_DIGEST_K))
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
                        TDigestMut::new(DEFAULT_DIGEST_K),
                        TDigestMut::new(DEFAULT_DIGEST_K),
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

#[divan::bench]
fn partial_groups_serialize(bencher: Bencher) {
    let digests = partial_digests_with(DEFAULT_DIGEST_K, PARTIAL_GROUPS, ROWS_PER_PARTIAL);

    bencher
        .counter(ItemsCount::new(PARTIAL_GROUPS))
        .with_inputs(|| digests.clone())
        .bench_local_values(|mut digests| {
            let bytes = digests
                .iter_mut()
                .map(TDigestMut::serialize)
                .collect::<Vec<_>>();
            black_box(bytes)
        });
}

#[divan::bench]
fn partial_groups_deserialize(bencher: Bencher) {
    let bytes = serialized_partial_digests_with(DEFAULT_DIGEST_K, PARTIAL_GROUPS, ROWS_PER_PARTIAL);

    bencher
        .counter(ItemsCount::new(PARTIAL_GROUPS))
        .bench_local(|| {
            let digests = bytes
                .iter()
                .map(|bytes| TDigestMut::deserialize(bytes, false).unwrap())
                .collect::<Vec<_>>();
            black_box(digests)
        });
}

#[divan::bench]
fn partial_merge(bencher: Bencher) {
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
            let mut merged = TDigestMut::new(DEFAULT_DIGEST_K);
            for partial in &partials {
                merged.merge(black_box(partial));
            }
            black_box(merged)
        });
}

fn prepared_digest() -> TDigest {
    build_digest(&values(100_000))
}

fn build_digest(values: &[f64]) -> TDigest {
    build_mut_digest(values).freeze()
}

fn build_mut_digest(values: &[f64]) -> TDigestMut {
    let mut digest = TDigestMut::default();
    for &value in values {
        digest.update(value);
    }
    digest
}

fn partial_digests(groups: usize, rows_per_group: usize) -> Vec<TDigestMut> {
    partial_digests_with(200, groups, rows_per_group)
}

fn partial_digests_with(k: u16, groups: usize, rows_per_group: usize) -> Vec<TDigestMut> {
    (0..groups)
        .map(|group| {
            let mut digest = TDigestMut::new(k);
            for row in 0..rows_per_group {
                digest.update(partial_value(group, row, rows_per_group));
            }
            digest
        })
        .collect()
}

fn serialized_partial_digests(groups: usize, rows_per_group: usize) -> Vec<Vec<u8>> {
    let bytes = serialized_partial_digests_with(200, groups, rows_per_group);
    assert!(
        bytes
            .iter()
            .all(|bytes| bytes.len() == 32 + rows_per_group * 16)
    );
    assert!(bytes.iter().all(|bytes| {
        u32::from_le_bytes(bytes[8..12].try_into().unwrap()) == rows_per_group as u32
    }));
    bytes
}

fn serialized_partial_digests_with(k: u16, groups: usize, rows_per_group: usize) -> Vec<Vec<u8>> {
    partial_digests_with(k, groups, rows_per_group)
        .into_iter()
        .map(|mut digest| digest.serialize())
        .collect()
}

fn serialized_state_shape(k: u16, values: &[f64]) -> (usize, u32) {
    let mut digest = TDigestMut::new(k);
    for &value in values {
        digest.update(value);
    }
    let bytes = digest.serialize();
    let centroids = match values.len() {
        0 => 0,
        1 => 1,
        _ => u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
    };
    (bytes.len(), centroids)
}

fn partial_value(group: usize, row: usize, rows_per_group: usize) -> f64 {
    (group * rows_per_group + row) as f64
}

fn values(len: usize) -> Vec<f64> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 11) as f64 * (1.0 / ((1_u64 << 53) as f64))
        })
        .collect()
}
