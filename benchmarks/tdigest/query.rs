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
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;

use super::support::build_digest;
use super::support::prepared_digest;
use super::support::values;

fn rank(bencher: Bencher) {
    let digest = prepared_digest();

    bencher.bench_local(|| black_box(&digest).rank(black_box(0.531_25)).unwrap());
}

fn rank_mixed(bencher: Bencher) {
    let digest = prepared_digest();
    let queries = values(256);

    bencher.bench_local(|| {
        for &value in black_box(&queries) {
            black_box(black_box(&digest).rank(black_box(value)).unwrap());
        }
    });
}

fn rank_small(bencher: Bencher, value: f64) {
    let digest = build_digest(&[0., 1., 2., 3., 4., 5.]);

    bencher.bench_local(|| black_box(&digest).rank(black_box(value)).unwrap());
}

fn rank_mut(bencher: Bencher) {
    let mut digest = prepared_digest().unfreeze();

    bencher.bench_local(|| black_box(&mut digest).rank(black_box(0.531_25)).unwrap());
}

fn rank_repeated_means(bencher: Bencher, value: f64) {
    let values = (0..100_000).map(|i| (i % 8) as f64).collect::<Vec<_>>();
    let digest = build_digest(&values);

    bencher.bench_local(|| black_box(&digest).rank(black_box(value)).unwrap());
}

fn quantile(bencher: Bencher) {
    let digest = prepared_digest();

    bencher.bench_local(|| black_box(&digest).quantile(black_box(0.531_25)).unwrap());
}

fn cdf_100(bencher: Bencher) {
    let digest = prepared_digest();
    let split_points = (1..=100).map(|i| i as f64 / 101.0).collect::<Vec<_>>();

    bencher.bench_local(|| black_box(&digest).cdf(black_box(&split_points)).unwrap());
}

fn pmf(bencher: Bencher, num_split_points: usize) {
    let digest = prepared_digest();
    let split_points = (1..=num_split_points)
        .map(|i| i as f64 / (num_split_points + 1) as f64)
        .collect::<Vec<_>>();

    bencher.bench_local(|| black_box(&digest).pmf(black_box(&split_points)).unwrap());
}

fn quantiles_sequential<const SORTED: bool>(bencher: Bencher, num_ranks: usize) {
    let digest = prepared_digest();
    let ranks = query_ranks(num_ranks, SORTED);

    bencher.bench_local(|| {
        for &rank in black_box(&ranks) {
            black_box(black_box(&digest).quantile(black_box(rank)).unwrap());
        }
    });
}

fn quantiles_batch<const SORTED: bool>(bencher: Bencher, num_ranks: usize) {
    let digest = prepared_digest();
    let ranks = query_ranks(num_ranks, SORTED);

    bencher.bench_local(|| black_box(&digest).quantiles(black_box(&ranks)).unwrap());
}

fn query_ranks(num_ranks: usize, sorted: bool) -> Vec<f64> {
    let mut ranks = (1..=num_ranks)
        .map(|rank| rank as f64 / (num_ranks + 1) as f64)
        .collect::<Vec<_>>();
    if !sorted {
        ranks.shuffle(&mut StdRng::seed_from_u64(42));
        if ranks.is_sorted() {
            ranks.reverse();
        }
    }
    ranks
}

pub fn register(runner: &mut Runner) {
    let quantile_cases = &[(2, 2_000_000), (6, 700_000), (100, 40_000)];
    runner.case("tdigest::query::rank", 10_000_000, rank);
    runner.case("tdigest::query::rank_mixed", 40_000, rank_mixed);
    for &(value, rounds) in &[(0.0, 80_000_000), (2.5, 80_000_000), (5.0, 50_000_000)] {
        runner.case(
            format!("tdigest::query::rank_small/{value:?}"),
            rounds,
            |b| rank_small(b, value),
        );
    }
    runner.case("tdigest::query::rank_mut", 9_000_000, rank_mut);
    for &(value, rounds) in &[
        (0.0, 20_000_000),
        (3.0, 10_000_000),
        (3.5, 10_000_000),
        (7.0, 7_000_000),
    ] {
        runner.case(
            format!("tdigest::query::rank_repeated_means/{value:?}"),
            rounds,
            |b| rank_repeated_means(b, value),
        );
    }
    runner.case("tdigest::query::quantile", 4_000_000, quantile);
    runner.case("tdigest::query::cdf_100", 600_000, cdf_100);
    for &(num_split_points, rounds) in &[
        (1, 5_000_000),
        (6, 2_000_000),
        (100, 600_000),
        (512, 100_000),
    ] {
        runner.case(
            format!("tdigest::query::pmf/{num_split_points:?}"),
            rounds,
            |b| pmf(b, num_split_points),
        );
    }
    for &(num_ranks, rounds) in quantile_cases {
        runner.case(
            format!("tdigest::query::quantiles_sequential<true>/{num_ranks:?}"),
            rounds,
            |b| quantiles_sequential::<true>(b, num_ranks),
        );
    }
    for &(num_ranks, rounds) in quantile_cases {
        runner.case(
            format!("tdigest::query::quantiles_sequential<false>/{num_ranks:?}"),
            rounds,
            |b| quantiles_sequential::<false>(b, num_ranks),
        );
    }
    for &(num_ranks, rounds) in quantile_cases {
        runner.case(
            format!("tdigest::query::quantiles_batch<true>/{num_ranks:?}"),
            rounds,
            |b| quantiles_batch::<true>(b, num_ranks),
        );
    }
    for &(num_ranks, rounds) in quantile_cases {
        runner.case(
            format!("tdigest::query::quantiles_batch<false>/{num_ranks:?}"),
            rounds,
            |b| quantiles_batch::<false>(b, num_ranks),
        );
    }
}
