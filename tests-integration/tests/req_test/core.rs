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

//! Core ReqSketch construction and update behavior.

use datasketches::common::SearchCriteria;
use datasketches::error::Error;
use datasketches::error::ErrorKind;
use datasketches::req::RankAccuracy;
use datasketches::req::ReqSketch;
use googletest::assert_that;
use googletest::prelude::all;
use googletest::prelude::anything;
use googletest::prelude::approx_eq;
use googletest::prelude::err;
use googletest::prelude::ge;
use googletest::prelude::le;
use googletest::prelude::lt;
use googletest::prelude::near;
use googletest::prelude::none;

use super::ReqF32;
use super::ReqF64;
use super::req_f32;
use super::req_f64;

#[test]
fn empty_sketch_has_default_state_and_rejects_queries() {
    let sketch: ReqSketch<ReqF64> = ReqSketch::default();

    assert_eq!(sketch.k(), 12);
    assert!(sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.n(), 0);
    assert_eq!(sketch.num_retained(), 0);
    assert_that!(sketch.min_item(), none());
    assert_that!(sketch.max_item(), none());

    assert_that!(
        sketch.rank(&req_f64(0.0), SearchCriteria::Inclusive),
        err(anything())
    );
    assert_that!(
        sketch.quantile(0.5, SearchCriteria::Inclusive),
        err(anything())
    );
    assert_that!(
        sketch.pmf(&[req_f64(0.0)], SearchCriteria::Inclusive),
        err(anything())
    );
    assert_that!(
        sketch.cdf(&[req_f64(0.0)], SearchCriteria::Inclusive),
        err(anything())
    );
}

#[test]
fn single_value_hra_answers_exactly() {
    let mut sketch: ReqSketch<ReqF32> = ReqSketch::default();
    sketch.update(req_f32(1.0));

    assert!(!sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.n(), 1);
    assert_eq!(sketch.num_retained(), 1);
    assert_eq!(sketch.min_item().copied(), Some(req_f32(1.0)));
    assert_eq!(sketch.max_item().copied(), Some(req_f32(1.0)));

    assert_that!(
        sketch
            .rank(&req_f32(1.0), SearchCriteria::Exclusive)
            .expect("rank should succeed"),
        approx_eq(0.0)
    );
    assert_that!(
        sketch
            .rank(&req_f32(1.0), SearchCriteria::Inclusive)
            .expect("rank should succeed"),
        approx_eq(1.0)
    );
    assert_that!(
        sketch
            .rank(&req_f32(2.0), SearchCriteria::Exclusive)
            .expect("rank should succeed"),
        approx_eq(1.0)
    );
    assert_that!(
        sketch
            .rank(&req_f32(f32::MAX), SearchCriteria::Inclusive)
            .expect("rank should succeed"),
        approx_eq(1.0)
    );

    for rank in [0.0, 0.5, 1.0] {
        assert_eq!(
            sketch
                .quantile(rank, SearchCriteria::Exclusive)
                .expect("quantile should succeed"),
            req_f32(1.0)
        );
    }
}

#[test]
fn single_value_lra_preserves_configuration() {
    let mut sketch = ReqSketch::<ReqF64>::new(12, RankAccuracy::LowRank).unwrap();
    sketch.update(req_f64(1.0));

    assert_eq!(sketch.rank_accuracy(), RankAccuracy::LowRank);
    assert!(!sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.n(), 1);
    assert_eq!(sketch.num_retained(), 1);
}

#[test]
fn repeated_values_respect_search_criteria() {
    let mut sketch: ReqSketch<ReqF64> = ReqSketch::default();
    for _ in 0..3 {
        sketch.update(req_f64(1.0));
    }
    for _ in 0..3 {
        sketch.update(req_f64(2.0));
    }

    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.n(), 6);
    assert_eq!(sketch.num_retained(), 6);

    assert_that!(
        sketch
            .rank(&req_f64(1.0), SearchCriteria::Exclusive)
            .expect("rank should succeed"),
        approx_eq(0.0)
    );
    assert_that!(
        sketch
            .rank(&req_f64(1.0), SearchCriteria::Inclusive)
            .expect("rank should succeed"),
        approx_eq(0.5)
    );
    assert_that!(
        sketch
            .rank(&req_f64(2.0), SearchCriteria::Exclusive)
            .expect("rank should succeed"),
        approx_eq(0.5)
    );
    assert_that!(
        sketch
            .rank(&req_f64(2.0), SearchCriteria::Inclusive)
            .expect("rank should succeed"),
        approx_eq(1.0)
    );
}

#[test]
fn estimation_mode_compresses_and_keeps_min_max() {
    let mut sketch: ReqSketch<ReqF64> = ReqSketch::default();
    let n = 100_000;

    for i in 0..n {
        sketch.update(req_f64(i as f64));
    }

    assert!(!sketch.is_empty());
    assert!(sketch.is_estimation_mode());
    assert_eq!(sketch.n(), n);
    assert_that!(sketch.num_retained(), lt(n as u32));
    assert_eq!(sketch.min_item().copied(), Some(req_f64(0.0)));
    assert_eq!(sketch.max_item().copied(), Some(req_f64((n - 1) as f64)));

    let r0 = sketch
        .rank(&req_f64(0.0), SearchCriteria::Exclusive)
        .expect("rank should succeed");
    let rmid = sketch
        .rank(&req_f64((n / 2) as f64), SearchCriteria::Exclusive)
        .expect("rank should succeed");
    let rmax = sketch
        .rank(&req_f64(n as f64), SearchCriteria::Exclusive)
        .expect("rank should succeed");

    assert_that!(r0, near(0.0, 1e-3));
    assert_that!(rmid, near(0.5, 0.01));
    assert_that!(rmax, near(1.0, 1e-3));
}

#[test]
fn req_float_adapts_the_non_nan_numeric_order() {
    assert!(ReqF32::new(f32::NAN).is_err());
    assert!(ReqF64::new(f64::NAN).is_err());

    let negative_zero = req_f64(-0.0);
    let positive_zero = req_f64(0.0);
    assert_eq!(negative_zero, positive_zero);
    assert_eq!(negative_zero.cmp(&positive_zero), std::cmp::Ordering::Equal);

    let negative_infinity = req_f64(f64::NEG_INFINITY);
    let infinity = req_f64(f64::INFINITY);
    assert!(negative_infinity < infinity);
}

#[test]
fn small_edge_cases_answer_reasonably() -> Result<(), Error> {
    let mut single: ReqSketch<ReqF64> = ReqSketch::default();
    single.update(req_f64(42.0));
    assert_eq!(
        single.quantile(0.5, SearchCriteria::Inclusive)?,
        req_f64(42.0)
    );

    let mut two_values = ReqSketch::default();
    two_values.update(req_f64(1.0));
    two_values.update(req_f64(100.0));
    let median = two_values.quantile(0.5, SearchCriteria::Inclusive)?;
    assert_that!(*median, all!(ge(1.0), le(100.0)));

    let mut duplicates = ReqSketch::default();
    for _ in 0..100 {
        duplicates.update(req_f64(42.0));
    }
    assert_eq!(
        duplicates.quantile(0.5, SearchCriteria::Inclusive)?,
        req_f64(42.0)
    );

    Ok(())
}

#[test]
fn new_validates_k() {
    assert_that!(
        ReqSketch::<ReqF64>::new(0, RankAccuracy::HighRank),
        err(anything())
    );
    let error = ReqSketch::<ReqF64>::new(3, RankAccuracy::HighRank).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    assert_that!(
        ReqSketch::<ReqF64>::new(4096, RankAccuracy::HighRank),
        err(anything())
    );
    assert!(ReqSketch::<ReqF64>::new(12, RankAccuracy::HighRank).is_ok());
}
