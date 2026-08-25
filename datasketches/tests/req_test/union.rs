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

//! Integration tests for ReqUnion.

use datasketches::req::RankAccuracy;
use datasketches::req::ReqSketch;
use datasketches::req::ReqUnion;
use datasketches::req::SearchCriteria;

#[test]
fn union_equivalent_to_chained_merge() {
    let make = |range: std::ops::Range<u64>| -> ReqSketch<f64> {
        let mut sketch = ReqSketch::new();
        for i in range {
            sketch.update(i as f64);
        }
        sketch
    };
    let s1 = make(0..1000);
    let s2 = make(1000..2000);
    let s3 = make(2000..3000);

    let mut union: ReqUnion<f64> = ReqUnion::new();
    union.merge(&s1).expect("merge should succeed");
    union.merge(&s2).expect("merge should succeed");
    union.merge(&s3).expect("merge should succeed");
    let union_result = union.to_sketch();

    let mut via_merge: ReqSketch<f64> = ReqSketch::new();
    via_merge.merge(&s1).expect("merge should succeed");
    via_merge.merge(&s2).expect("merge should succeed");
    via_merge.merge(&s3).expect("merge should succeed");

    assert_eq!(union_result.n(), via_merge.n());
    assert_eq!(union_result.min_item(), via_merge.min_item());
    assert_eq!(union_result.max_item(), via_merge.max_item());

    let true_median = 1499.5_f64;
    let tolerance = 0.05 * 3000.0;
    let q_union = union_result
        .quantile(0.5, SearchCriteria::Inclusive)
        .expect("quantile should succeed");
    let q_merge = via_merge
        .quantile(0.5, SearchCriteria::Inclusive)
        .expect("quantile should succeed");

    assert!((q_union - true_median).abs() <= tolerance);
    assert!((q_merge - true_median).abs() <= tolerance);
}

#[test]
fn empty_union_returns_empty_sketch() {
    let union: ReqUnion<f64> = ReqUnion::new();
    assert!(union.to_sketch().is_empty());
}

#[test]
fn reset_clears_union_state() {
    let mut sketch: ReqSketch<f64> = ReqSketch::new();
    for i in 0..100 {
        sketch.update(i as f64);
    }

    let mut union: ReqUnion<f64> = ReqUnion::new();
    union.merge(&sketch).expect("merge should succeed");
    assert!(!union.is_empty());

    union.reset();
    assert!(union.is_empty());
}

#[test]
fn try_new_validates_k() {
    assert!(ReqUnion::<f64>::try_new(3, RankAccuracy::HighRank).is_err());
    assert!(ReqUnion::<f64>::try_new(12, RankAccuracy::HighRank).is_ok());
}

#[test]
fn empty_union_uses_default_configuration() {
    let union: ReqUnion<f64> = ReqUnion::new();
    assert_eq!(union.k(), 12);
    assert_eq!(union.rank_accuracy(), RankAccuracy::HighRank);
}

#[test]
fn union_keeps_default_k_when_merging_mismatched_sketch() {
    // The union retains its own k even when fed a sketch built with a different k.
    let mut other = ReqSketch::<f64>::try_new(16, RankAccuracy::HighRank).expect("valid k");
    for i in 0..50 {
        other.update(i as f64);
    }

    let mut union: ReqUnion<f64> = ReqUnion::new();
    union.merge(&other).expect("merge should succeed");

    let result = union.to_sketch();
    assert_eq!(result.k(), 12);
    assert_eq!(result.n(), 50);
}
