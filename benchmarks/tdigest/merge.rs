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
use super::support::ROWS_PER_PARTIAL;
use super::support::SMALL_ROWS_PER_PARTIAL;
use super::support::build_mut_digest;
use super::support::partial_digests;
use super::support::partial_digests_with;
use super::support::serialized_partial_digests;
use super::support::values;

fn merge(bencher: Bencher) {
    let values = values(200_000);
    let mut left = build_mut_digest(&values[..100_000]);
    let mut right = build_mut_digest(&values[100_000..]);
    black_box(left.rank(0.5).unwrap());
    black_box(right.rank(0.5).unwrap());

    bencher.bench_local(|| {
        let mut left = left.clone();
        left.merge(black_box(&right));
        black_box(left)
    });
}

fn unmerged(bencher: Bencher, left_rows: usize) {
    let left = build_mut_digest(&values(left_rows));
    let right = build_mut_digest(&values(1_640));

    bencher.bench_local(|| {
        let mut left = left.clone();
        left.merge(black_box(&right));
        black_box(left)
    });
}

fn small_partials(bencher: Bencher) {
    let partials = partial_digests(64, SMALL_ROWS_PER_PARTIAL)
        .into_iter()
        .map(|mut digest| {
            black_box(digest.quantile(0.5).unwrap());
            digest
        })
        .collect::<Vec<_>>();

    bencher.bench_local(|| {
        let mut merged = TDigestMut::default();
        for partial in &partials {
            merged.merge(black_box(partial));
        }
        black_box(merged)
    });
}

fn partials(bencher: Bencher) {
    let partials = partial_digests_with(DEFAULT_DIGEST_K, 64, ROWS_PER_PARTIAL)
        .into_iter()
        .map(|mut digest| {
            black_box(digest.quantile(0.5).unwrap());
            digest
        })
        .collect::<Vec<_>>();

    bencher.bench_local(|| {
        let mut merged = TDigestMut::new(DEFAULT_DIGEST_K).unwrap();
        for partial in &partials {
            merged.merge(black_box(partial));
        }
        black_box(merged)
    });
}

fn partials_from_iter(bencher: Bencher) {
    let partials = partial_digests_with(DEFAULT_DIGEST_K, 64, ROWS_PER_PARTIAL)
        .into_iter()
        .map(|mut digest| {
            black_box(digest.quantile(0.5).unwrap());
            digest
        })
        .collect::<Vec<_>>();

    bencher.bench_local(|| {
        let partials = partials.clone();
        black_box(partials).into_iter().collect::<TDigestMut>()
    });
}

fn partials_from_iter_cloned(bencher: Bencher) {
    let partials = partial_digests_with(DEFAULT_DIGEST_K, 64, ROWS_PER_PARTIAL)
        .into_iter()
        .map(|mut digest| {
            black_box(digest.quantile(0.5).unwrap());
            digest
        })
        .collect::<Vec<_>>();

    bencher
        // Cloning through the iterator avoids a temporary vector of owned inputs.
        .bench_local(|| black_box(&partials).iter().cloned().collect::<TDigestMut>());
}

fn partials_from_iter_finalize(bencher: Bencher, additional_rows: usize) {
    let partials = partial_digests_with(DEFAULT_DIGEST_K, 64, ROWS_PER_PARTIAL)
        .into_iter()
        .map(|mut digest| {
            black_box(digest.quantile(0.5).unwrap());
            digest
        })
        .collect::<Vec<_>>();
    let updates = values(additional_rows)
        .into_iter()
        .map(|value| value * (64 * ROWS_PER_PARTIAL) as f64)
        .collect::<Vec<_>>();

    bencher.bench_local(|| {
        let partials = partials.clone();
        let mut merged = black_box(partials).into_iter().collect::<TDigestMut>();
        for &value in black_box(&updates) {
            merged.update(value);
        }
        black_box(merged.freeze())
    });
}

fn uncompressed_partials_from_iter(bencher: Bencher) {
    let values = values(64 * ROWS_PER_PARTIAL);
    let partials = values
        .chunks_exact(ROWS_PER_PARTIAL)
        .map(build_mut_digest)
        .collect::<Vec<_>>();

    bencher.bench_local(|| {
        let partials = partials.clone();
        black_box(partials).into_iter().collect::<TDigestMut>()
    });
}

fn serialized_partials(bencher: Bencher, rows_per_partial: usize) {
    let partials = serialized_partial_digests(64, rows_per_partial);

    bencher.bench_local(|| {
        let mut merged = TDigestMut::default();
        for partial in black_box(&partials) {
            let partial = TDigestMut::deserialize(partial).unwrap();
            merged.merge(&partial);
        }
        black_box(merged.quantile(0.5).unwrap())
    });
}

fn serialized_overlapping_partials(bencher: Bencher) {
    let values = values(64 * ROWS_PER_PARTIAL);
    let partials = values
        .chunks_exact(ROWS_PER_PARTIAL)
        .map(|values| build_mut_digest(values).serialize())
        .collect::<Vec<_>>();

    bencher.bench_local(|| {
        let mut merged = TDigestMut::default();
        for partial in black_box(&partials) {
            let partial = TDigestMut::deserialize(partial).unwrap();
            merged.merge(&partial);
        }
        black_box(merged.quantile(0.5).unwrap())
    });
}

fn serialized_partials_from_iter(bencher: Bencher, overlapping: bool) {
    let partials = serialized_merge_inputs(overlapping);

    bencher.bench_local(|| {
        let mut merged = black_box(&partials)
            .iter()
            .map(|partial| TDigestMut::deserialize(partial))
            .collect::<Result<TDigestMut, _>>()
            .unwrap();
        black_box(merged.quantile(0.5).unwrap())
    });
}

fn serialized_partials_in_batches(bencher: Bencher, overlapping: bool) {
    let partials = serialized_merge_inputs(overlapping);

    bencher.bench_local(|| {
        let mut merged = TDigestMut::new(DEFAULT_DIGEST_K).unwrap();
        for batch in black_box(&partials).chunks(16) {
            let batch = batch
                .iter()
                .map(|partial| TDigestMut::deserialize(partial))
                .collect::<Result<TDigestMut, _>>()
                .unwrap();
            merged.merge(&batch);
        }
        black_box(merged.quantile(0.5).unwrap())
    });
}

fn serialized_merge_inputs(overlapping: bool) -> Vec<Vec<u8>> {
    if overlapping {
        values(64 * ROWS_PER_PARTIAL)
            .chunks_exact(ROWS_PER_PARTIAL)
            .map(|values| build_mut_digest(values).serialize())
            .collect()
    } else {
        serialized_partial_digests(64, ROWS_PER_PARTIAL)
    }
}

pub fn register(runner: &mut Runner) {
    runner.case("tdigest::merge::merge", 150_000, merge);
    for &(left_rows, rounds) in &[(8, 20_000), (1_640, 8_000)] {
        runner.case(
            format!("tdigest::merge::unmerged/{left_rows:?}"),
            rounds,
            |b| unmerged(b, left_rows),
        );
    }
    runner.case("tdigest::merge::small_partials", 10_000, small_partials);
    runner.case("tdigest::merge::partials", 7_000, partials);
    runner.case(
        "tdigest::merge::partials_from_iter",
        20_000,
        partials_from_iter,
    );
    runner.case(
        "tdigest::merge::partials_from_iter_cloned",
        20_000,
        partials_from_iter_cloned,
    );
    for &(additional_rows, rounds) in &[(0, 10_000), (64, 10_000), (4_096, 3_000)] {
        runner.case(
            format!("tdigest::merge::partials_from_iter_finalize/{additional_rows:?}"),
            rounds,
            |b| partials_from_iter_finalize(b, additional_rows),
        );
    }
    runner.case(
        "tdigest::merge::uncompressed_partials_from_iter",
        6_000,
        uncompressed_partials_from_iter,
    );
    for &(rows_per_partial, rounds) in
        &[(SMALL_ROWS_PER_PARTIAL, 10_000), (ROWS_PER_PARTIAL, 6_000)]
    {
        runner.case(
            format!("tdigest::merge::serialized_partials/{rows_per_partial:?}"),
            rounds,
            |b| serialized_partials(b, rows_per_partial),
        );
    }
    runner.case(
        "tdigest::merge::serialized_overlapping_partials",
        5_000,
        serialized_overlapping_partials,
    );
    for &(overlapping, rounds) in &[(false, 10_000), (true, 6_000)] {
        runner.case(
            format!("tdigest::merge::serialized_partials_from_iter/{overlapping:?}"),
            rounds,
            |b| serialized_partials_from_iter(b, overlapping),
        );
    }
    for &(overlapping, rounds) in &[(false, 9_000), (true, 7_000)] {
        runner.case(
            format!("tdigest::merge::serialized_partials_in_batches/{overlapping:?}"),
            rounds,
            |b| serialized_partials_in_batches(b, overlapping),
        );
    }
}
