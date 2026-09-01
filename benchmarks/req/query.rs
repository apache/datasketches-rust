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

use datasketches::req::ReqFloat;
use datasketches::req::SearchCriteria;
use divan::Bencher;
use divan::black_box;

use super::support::prepared_sketch;

#[divan::bench]
fn rank(bencher: Bencher) {
    let sketch = prepared_sketch();
    let item = ReqFloat::<f64>::new(500_000.0).unwrap();

    bencher.bench_local(|| black_box(&sketch).rank(black_box(&item), SearchCriteria::Inclusive));
}

#[divan::bench]
fn quantile(bencher: Bencher) {
    let sketch = prepared_sketch();

    bencher.bench_local(|| black_box(&sketch).quantile(black_box(0.5), SearchCriteria::Inclusive));
}

// Intended pattern for repeated queries: build the view once, query it many
// times. Contrast against `quantile`, which rebuilds the view on every call.
#[divan::bench]
fn sorted_view_quantile(bencher: Bencher) {
    let sketch = prepared_sketch();
    let view = sketch.sorted_view();

    bencher.bench_local(|| black_box(&view).quantile(black_box(0.5), SearchCriteria::Inclusive));
}
