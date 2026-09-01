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

use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

use super::support::prepared_digest;

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
fn quantiles_6_sequential(bencher: Bencher) {
    let digest = prepared_digest();
    let ranks = [0.25, 0.5, 0.75, 0.9, 0.95, 0.99];

    bencher
        .bench_local(|| black_box(ranks).map(|rank| black_box(&digest).quantile(black_box(rank))));
}

#[divan::bench(args = [2, 6, 100])]
fn quantiles_batch(bencher: Bencher, num_ranks: usize) {
    let digest = prepared_digest();
    let ranks = (1..=num_ranks)
        .map(|rank| rank as f64 / (num_ranks + 1) as f64)
        .collect::<Vec<_>>();

    bencher
        .counter(ItemsCount::new(num_ranks))
        .bench_local(|| black_box(&digest).quantiles(black_box(&ranks)));
}
