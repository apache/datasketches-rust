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

use datasketches::error::Error;
use datasketches::req::RankAccuracy;
use datasketches::req::ReqSketch;
use datasketches::req::SearchCriteria;
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
use googletest::prelude::ok;

#[test]
fn empty_sketch_has_default_state_and_rejects_queries() {
    let sketch: ReqSketch<f32> = ReqSketch::new();

    assert_eq!(sketch.k(), 12);
    assert!(sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.n(), 0);
    assert_eq!(sketch.num_retained(), 0);
    assert_that!(sketch.min_item(), none());
    assert_that!(sketch.max_item(), none());

    assert_that!(
        sketch.rank(&0.0, SearchCriteria::Inclusive),
        err(anything())
    );
    assert_that!(
        sketch.quantile(0.5, SearchCriteria::Inclusive),
        err(anything())
    );
    assert_that!(
        sketch.pmf(&[0.0], SearchCriteria::Inclusive),
        err(anything())
    );
    assert_that!(
        sketch.cdf(&[0.0], SearchCriteria::Inclusive),
        err(anything())
    );
}

#[test]
fn single_value_hra_answers_exactly() {
    let mut sketch = ReqSketch::new();
    sketch.update(1.0f32);

    assert!(!sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.n(), 1);
    assert_eq!(sketch.num_retained(), 1);
    assert_eq!(sketch.min_item(), Some(&1.0));
    assert_eq!(sketch.max_item(), Some(&1.0));

    assert_that!(
        sketch
            .rank(&1.0, SearchCriteria::Exclusive)
            .expect("rank should succeed"),
        approx_eq(0.0)
    );
    assert_that!(
        sketch
            .rank(&1.0, SearchCriteria::Inclusive)
            .expect("rank should succeed"),
        approx_eq(1.0)
    );
    assert_that!(
        sketch
            .rank(&1.1, SearchCriteria::Exclusive)
            .expect("rank should succeed"),
        approx_eq(1.0)
    );
    assert_that!(
        sketch
            .rank(&f32::INFINITY, SearchCriteria::Inclusive)
            .expect("rank should succeed"),
        approx_eq(1.0)
    );

    for rank in [0.0, 0.5, 1.0] {
        assert_that!(
            sketch
                .quantile(rank, SearchCriteria::Exclusive)
                .expect("quantile should succeed"),
            approx_eq(1.0)
        );
    }
}

#[test]
fn single_value_lra_preserves_configuration() {
    let mut sketch: ReqSketch<f32> = ReqSketch::builder()
        .rank_accuracy(RankAccuracy::LowRank)
        .build()
        .expect("build should succeed");
    sketch.update(1.0f32);

    assert_eq!(sketch.rank_accuracy(), RankAccuracy::LowRank);
    assert!(!sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.n(), 1);
    assert_eq!(sketch.num_retained(), 1);
}

#[test]
fn repeated_values_respect_search_criteria() {
    let mut sketch = ReqSketch::new();
    for _ in 0..3 {
        sketch.update(1.0f32);
    }
    for _ in 0..3 {
        sketch.update(2.0f32);
    }

    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.n(), 6);
    assert_eq!(sketch.num_retained(), 6);

    assert_that!(
        sketch
            .rank(&1.0, SearchCriteria::Exclusive)
            .expect("rank should succeed"),
        approx_eq(0.0)
    );
    assert_that!(
        sketch
            .rank(&1.0, SearchCriteria::Inclusive)
            .expect("rank should succeed"),
        approx_eq(0.5)
    );
    assert_that!(
        sketch
            .rank(&2.0, SearchCriteria::Exclusive)
            .expect("rank should succeed"),
        approx_eq(0.5)
    );
    assert_that!(
        sketch
            .rank(&2.0, SearchCriteria::Inclusive)
            .expect("rank should succeed"),
        approx_eq(1.0)
    );
}

#[test]
fn estimation_mode_compresses_and_keeps_min_max() {
    let mut sketch = ReqSketch::new();
    let n = 100_000;

    for i in 0..n {
        sketch.update(i as f32);
    }

    assert!(!sketch.is_empty());
    assert!(sketch.is_estimation_mode());
    assert_eq!(sketch.n(), n);
    assert_that!(sketch.num_retained(), lt(n as u32));
    assert_eq!(sketch.min_item(), Some(&0.0));
    assert_eq!(sketch.max_item(), Some(&((n - 1) as f32)));

    let r0 = sketch
        .rank(&0.0, SearchCriteria::Exclusive)
        .expect("rank should succeed");
    let rmid = sketch
        .rank(&(n as f32 / 2.0), SearchCriteria::Exclusive)
        .expect("rank should succeed");
    let rmax = sketch
        .rank(&(n as f32), SearchCriteria::Exclusive)
        .expect("rank should succeed");

    assert_that!(r0, near(0.0, 1e-3));
    assert_that!(rmid, near(0.5, 0.01));
    assert_that!(rmax, near(1.0, 1e-3));
}

#[test]
fn nan_updates_are_silently_skipped_for_f64() {
    let mut sketch: ReqSketch<f64> = ReqSketch::new();
    sketch.update(f64::NAN);
    sketch.update(f64::NAN);
    assert!(sketch.is_empty());
    assert_eq!(sketch.n(), 0);

    sketch.update(1.0);
    sketch.update(f64::NAN);
    sketch.update(2.0);
    assert_eq!(sketch.n(), 2);
    assert_eq!(sketch.min_item(), Some(&1.0));
    assert_eq!(sketch.max_item(), Some(&2.0));
}

#[test]
fn nan_updates_are_silently_skipped_for_f32() {
    let mut sketch: ReqSketch<f32> = ReqSketch::new();
    sketch.update(f32::NAN);
    assert!(sketch.is_empty());
    assert_eq!(sketch.n(), 0);

    sketch.update(5.0f32);
    sketch.update(f32::NAN);
    assert_eq!(sketch.n(), 1);
    assert_eq!(
        sketch
            .quantile(0.5, SearchCriteria::Inclusive)
            .expect("quantile should succeed"),
        5.0f32
    );
}

#[test]
fn small_edge_cases_answer_reasonably() -> Result<(), Error> {
    let mut single = ReqSketch::new();
    single.update(42.0);
    assert_eq!(single.quantile(0.5, SearchCriteria::Inclusive)?, 42.0);

    let mut two_values = ReqSketch::new();
    two_values.update(1.0);
    two_values.update(100.0);
    let median = two_values.quantile(0.5, SearchCriteria::Inclusive)?;
    assert_that!(median, all!(ge(1.0), le(100.0)));

    let mut duplicates = ReqSketch::new();
    for _ in 0..100 {
        duplicates.update(42.0);
    }
    assert_eq!(duplicates.quantile(0.5, SearchCriteria::Inclusive)?, 42.0);

    Ok(())
}

#[test]
fn constructors_validate_k() {
    // k must be even and within the supported range; both constructors enforce it.
    assert_that!(
        ReqSketch::<f64>::try_new(0, RankAccuracy::HighRank),
        err(anything())
    );
    assert_that!(
        ReqSketch::<f64>::try_new(3, RankAccuracy::HighRank),
        err(anything())
    ); // odd
    assert_that!(
        ReqSketch::<f64>::try_new(4096, RankAccuracy::HighRank),
        err(anything())
    ); // too large
    assert_that!(
        ReqSketch::<f64>::try_new(12, RankAccuracy::HighRank),
        ok(anything())
    );
    assert_that!(ReqSketch::<f64>::builder().k(5), err(anything())); // odd via builder
    assert_that!(ReqSketch::<f64>::builder().k(12), ok(anything()));
}
