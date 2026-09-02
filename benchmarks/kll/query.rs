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

use datasketches::common::SearchCriteria;
use datasketches::kll::KllFloat;
use divan::Bencher;
use divan::black_box;

use super::support::prepared_sketch;

#[divan::bench]
fn rank(bencher: Bencher) {
    let sketch = prepared_sketch();
    let item = KllFloat::<f64>::new(500_000.0).unwrap();
    bencher.bench_local(|| black_box(&sketch).rank(black_box(&item), SearchCriteria::Inclusive));
}

#[divan::bench]
fn quantile(bencher: Bencher) {
    let sketch = prepared_sketch();
    bencher.bench_local(|| black_box(&sketch).quantile(black_box(0.5), SearchCriteria::Inclusive));
}

#[divan::bench]
fn sorted_view_quantile(bencher: Bencher) {
    let view = prepared_sketch().sorted_view();
    bencher.bench_local(|| black_box(&view).quantile(black_box(0.5), SearchCriteria::Inclusive));
}

#[divan::bench]
fn batch_quantiles(bencher: Bencher) {
    let sketch = prepared_sketch();
    let ranks = [0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99];
    bencher
        .bench_local(|| black_box(&sketch).quantiles(black_box(&ranks), SearchCriteria::Inclusive));
}
