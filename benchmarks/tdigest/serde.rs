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
use datasketches::tdigest::TDigestMut;

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

fn small_lifecycle(bencher: Bencher, rows: usize) {
    let values = values(rows);

    bencher.bench_local(|| {
        let mut digest = TDigestMut::default();
        for &value in black_box(&values) {
            digest.update(value);
        }
        let bytes = digest.serialize();
        drop(black_box(bytes));
        drop(black_box(digest));
    });
}

fn partial_lifecycle_by_k(bencher: Bencher, k: u16) {
    let values = values(ROWS_PER_PARTIAL);
    let (serialized_bytes, centroids) = serialized_state_shape(k, &values);
    assert!(matches!(
        (k, serialized_bytes, centroids),
        (10, 224, 12) | (200, 1_056, 64)
    ));

    bencher.bench_local(|| {
        let mut digest = TDigestMut::new(black_box(k)).unwrap();
        for &value in black_box(&values) {
            digest.update(value);
        }
        let bytes = digest.serialize();
        drop(black_box(bytes));
        drop(black_box(digest));
    });
}

fn serialize_small_partial_groups(bencher: Bencher) {
    let digests = partial_digests(PARTIAL_GROUPS, SMALL_ROWS_PER_PARTIAL);

    bencher.bench_local(|| {
        let mut digests = digests.clone();
        let bytes = digests
            .iter_mut()
            .map(TDigestMut::serialize)
            .collect::<Vec<_>>();
        black_box(bytes)
    });
}

fn deserialize_small_partial_groups(bencher: Bencher) {
    let bytes = serialized_partial_digests(PARTIAL_GROUPS, SMALL_ROWS_PER_PARTIAL);

    bencher.bench_local(|| {
        let digests = bytes
            .iter()
            .map(|bytes| TDigestMut::deserialize(bytes).unwrap())
            .collect::<Vec<_>>();
        black_box(digests)
    });
}

fn serialize_partial_groups(bencher: Bencher) {
    let digests = partial_digests_with(DEFAULT_DIGEST_K, PARTIAL_GROUPS, ROWS_PER_PARTIAL);

    bencher.bench_local(|| {
        let mut digests = digests.clone();
        let bytes = digests
            .iter_mut()
            .map(TDigestMut::serialize)
            .collect::<Vec<_>>();
        black_box(bytes)
    });
}

fn deserialize_partial_groups(bencher: Bencher) {
    let bytes = serialized_partial_digests_with(DEFAULT_DIGEST_K, PARTIAL_GROUPS, ROWS_PER_PARTIAL);

    bencher.bench_local(|| {
        let digests = bytes
            .iter()
            .map(|bytes| TDigestMut::deserialize(bytes).unwrap())
            .collect::<Vec<_>>();
        black_box(digests)
    });
}

pub fn register(runner: &mut Runner) {
    for &(rows, rounds) in &[
        (1, 7_000_000),
        (2, 6_000_000),
        (8, 2_000_000),
        (32, 2_000_000),
        (64, 500_000),
        (128, 200_000),
    ] {
        runner.case(
            format!("tdigest::serde::small_lifecycle/{rows:?}"),
            rounds,
            |b| small_lifecycle(b, rows),
        );
    }
    for &(k, rounds) in &[(10_u16, 400_000), (200_u16, 500_000)] {
        runner.case(
            format!("tdigest::serde::partial_lifecycle_by_k/{k:?}"),
            rounds,
            |b| partial_lifecycle_by_k(b, k),
        );
    }
    runner.case(
        "tdigest::serde::serialize_small_partial_groups",
        10_000,
        serialize_small_partial_groups,
    );
    runner.case(
        "tdigest::serde::deserialize_small_partial_groups",
        10_000,
        deserialize_small_partial_groups,
    );
    runner.case(
        "tdigest::serde::serialize_partial_groups",
        4_000,
        serialize_partial_groups,
    );
    runner.case(
        "tdigest::serde::deserialize_partial_groups",
        4_000,
        deserialize_partial_groups,
    );
}
