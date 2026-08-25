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

#![cfg(feature = "req")]

//! Merge behavior for ReqSketch.

use datasketches::req::RankAccuracy;
use datasketches::req::ReqSketch;
use datasketches::req::SearchCriteria;
use googletest::assert_that;
use googletest::prelude::anything;
use googletest::prelude::err;
use googletest::prelude::near;

#[test]
fn merge_into_empty_preserves_source_distribution() {
    let mut target: ReqSketch<f32> = ReqSketch::builder()
        .k(40)
        .expect("valid k")
        .build()
        .expect("build should succeed");
    let mut source: ReqSketch<f32> = ReqSketch::builder()
        .k(40)
        .expect("valid k")
        .build()
        .expect("build should succeed");

    for i in 0..1000 {
        source.update(i as f32);
    }

    target.merge(&source).expect("merge should succeed");
    assert_eq!(target.min_item(), Some(&0.0));
    assert_eq!(target.max_item(), Some(&999.0));

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
        .rank(&500.0, SearchCriteria::Inclusive)
        .expect("rank should succeed");

    assert_that!(q25, near(250.0, 250.0 * 0.01));
    assert_that!(q50, near(500.0, 500.0 * 0.01));
    assert_that!(q75, near(750.0, 750.0 * 0.01));
    assert_that!(r50, near(0.5, 0.5 * 0.01));
}

#[test]
fn merge_two_ranges_preserves_distribution() {
    let mut left: ReqSketch<f32> = ReqSketch::builder()
        .k(100)
        .expect("valid k")
        .build()
        .expect("build should succeed");
    let mut right: ReqSketch<f32> = ReqSketch::builder()
        .k(100)
        .expect("valid k")
        .build()
        .expect("build should succeed");

    for i in 0..1000 {
        left.update(i as f32);
    }
    for i in 1000..2000 {
        right.update(i as f32);
    }

    left.merge(&right).expect("merge should succeed");
    assert_eq!(left.min_item(), Some(&0.0));
    assert_eq!(left.max_item(), Some(&1999.0));

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
        .rank(&1000.0, SearchCriteria::Inclusive)
        .expect("rank should succeed");

    assert_that!(q25, near(500.0, 500.0 * 0.02));
    assert_that!(q50, near(1000.0, 1000.0 * 0.01));
    assert_that!(q75, near(1500.0, 1500.0 * 0.01));
    assert_that!(r50, near(0.5, 0.5 * 0.01));
}

#[test]
fn merge_rejects_incompatible_accuracy_modes() {
    let mut high_rank = ReqSketch::new();
    let low_rank: ReqSketch<f32> = ReqSketch::builder()
        .rank_accuracy(RankAccuracy::LowRank)
        .build()
        .expect("build should succeed");

    high_rank.update(1.0);
    assert_that!(high_rank.merge(&low_rank), err(anything()));
}

#[test]
fn many_small_merges_preserve_count_bounds_and_median() {
    let mut sketch = ReqSketch::new();

    for batch in 0..100 {
        let mut batch_sketch = ReqSketch::new();
        for i in 0..100 {
            batch_sketch.update((batch * 100 + i) as f64);
        }
        sketch.merge(&batch_sketch).expect("merge should succeed");
    }

    assert_eq!(sketch.n(), 10_000);
    assert_eq!(sketch.min_item(), Some(&0.0));
    assert_eq!(sketch.max_item(), Some(&9999.0));

    let median = sketch
        .quantile(0.5, SearchCriteria::Inclusive)
        .expect("quantile should succeed");
    assert_that!(median, near(4999.5, 500.0));
}
