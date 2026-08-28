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
use divan::black_box_drop;
use divan::counter::BytesCount;
use divan::counter::ItemsCount;

use super::support::DEFAULT_DIGEST_K;
use super::support::PARTIAL_GROUPS;
use super::support::ROWS_PER_PARTIAL;
use super::support::SMALL_ROWS_PER_PARTIAL;
use super::support::partial_digests;
use super::support::partial_digests_with;
use super::support::serialized_partial_digests;
use super::support::serialized_partial_digests_with;
use super::support::serialized_state_shape;
use super::support::values;

#[divan::bench(args = [1, 2, 8, 32, 64, 128])]
fn small_lifecycle(bencher: Bencher, rows: usize) {
    let values = values(rows);
    let (serialized_bytes, _) = serialized_state_shape(DEFAULT_DIGEST_K, &values);

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
fn partial_lifecycle_by_k(bencher: Bencher, k: u16) {
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
            let mut digest = TDigestMut::new(black_box(k)).unwrap();
            for &value in black_box(&values) {
                digest.update(value);
            }
            let bytes = digest.serialize();
            black_box_drop(bytes);
            black_box_drop(digest);
        });
}

#[divan::bench]
fn serialize_small_partial_groups(bencher: Bencher) {
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
fn deserialize_small_partial_groups(bencher: Bencher) {
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
fn serialize_partial_groups(bencher: Bencher) {
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
fn deserialize_partial_groups(bencher: Bencher) {
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
