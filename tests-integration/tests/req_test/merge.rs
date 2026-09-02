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

//! Merge behavior for ReqSketch.

use datasketches::common::SearchCriteria;
use datasketches::req::RankAccuracy;
use datasketches::req::ReqSketch;
use googletest::assert_that;
use googletest::prelude::anything;
use googletest::prelude::err;
use googletest::prelude::near;

use super::ReqF64;
use super::req_f64;

#[test]
fn merge_into_empty_preserves_source_distribution() {
    let mut target: ReqSketch<ReqF64> = ReqSketch::new(40, RankAccuracy::HighRank).unwrap();
    let mut source: ReqSketch<ReqF64> = ReqSketch::new(40, RankAccuracy::HighRank).unwrap();

    for i in 0..1000 {
        source.update(req_f64(i as f64));
    }

    target.merge(&source).expect("merge should succeed");
    assert_eq!(target.min_item().copied(), Some(req_f64(0.0)));
    assert_eq!(target.max_item().copied(), Some(req_f64(999.0)));

    let q25 = target
        .quantile(0.25, SearchCriteria::Inclusive)
        .expect("quantile should succeed");
    let q50 = target
        .quantile(0.5, SearchCriteria::Inclusive)
        .expect("quantile should succeed");
    let q75 = target
        .quantile(0.75, SearchCriteria::Inclusive)
        .expect("quantile should succeed");
    let r50 = target
        .rank(&req_f64(500.0), SearchCriteria::Inclusive)
        .expect("rank should succeed");

    assert_that!(*q25, near(250.0, 250.0 * 0.01));
    assert_that!(*q50, near(500.0, 500.0 * 0.01));
    assert_that!(*q75, near(750.0, 750.0 * 0.01));
    assert_that!(r50, near(0.5, 0.5 * 0.01));
}

#[test]
fn merge_two_ranges_preserves_distribution() {
    let mut left: ReqSketch<ReqF64> = ReqSketch::new(100, RankAccuracy::HighRank).unwrap();
    let mut right: ReqSketch<ReqF64> = ReqSketch::new(100, RankAccuracy::HighRank).unwrap();

    for i in 0..1000 {
        left.update(req_f64(i as f64));
    }
    for i in 1000..2000 {
        right.update(req_f64(i as f64));
    }

    left.merge(&right).expect("merge should succeed");
    assert_eq!(left.min_item().copied(), Some(req_f64(0.0)));
    assert_eq!(left.max_item().copied(), Some(req_f64(1999.0)));

    let q25 = left
        .quantile(0.25, SearchCriteria::Inclusive)
        .expect("quantile should succeed");
    let q50 = left
        .quantile(0.5, SearchCriteria::Inclusive)
        .expect("quantile should succeed");
    let q75 = left
        .quantile(0.75, SearchCriteria::Inclusive)
        .expect("quantile should succeed");
    let r50 = left
        .rank(&req_f64(1000.0), SearchCriteria::Inclusive)
        .expect("rank should succeed");

    assert_that!(*q25, near(500.0, 500.0 * 0.02));
    assert_that!(*q50, near(1000.0, 1000.0 * 0.01));
    assert_that!(*q75, near(1500.0, 1500.0 * 0.01));
    assert_that!(r50, near(0.5, 0.5 * 0.01));
}

#[test]
fn merge_rejects_incompatible_accuracy_modes() {
    let mut high_rank: ReqSketch<ReqF64> = ReqSketch::default();
    let low_rank: ReqSketch<ReqF64> = ReqSketch::new(12, RankAccuracy::LowRank).unwrap();

    high_rank.update(req_f64(1.0));
    assert_that!(high_rank.merge(&low_rank), err(anything()));
}

#[test]
fn many_small_merges_preserve_count_bounds_and_median() {
    let mut sketch: ReqSketch<ReqF64> = ReqSketch::default();

    for batch in 0..100 {
        let mut batch_sketch = ReqSketch::default();
        for i in 0..100 {
            batch_sketch.update(req_f64((batch * 100 + i) as f64));
        }
        sketch.merge(&batch_sketch).expect("merge should succeed");
    }

    assert_eq!(sketch.n(), 10_000);
    assert_eq!(sketch.min_item().copied(), Some(req_f64(0.0)));
    assert_eq!(sketch.max_item().copied(), Some(req_f64(9999.0)));

    let median = sketch
        .quantile(0.5, SearchCriteria::Inclusive)
        .expect("quantile should succeed");
    assert_that!(*median, near(4999.5, 500.0));
}
