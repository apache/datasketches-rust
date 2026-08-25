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

use approx::assert_relative_eq;
use datasketches::error::Error;
use datasketches::req::RankAccuracy;
use datasketches::req::ReqSketch;
use datasketches::req::SearchCriteria;

#[test]
fn empty_sketch_has_default_state_and_rejects_queries() {
    let sketch: ReqSketch<f32> = ReqSketch::new();

    assert_eq!(sketch.k(), 12);
    assert!(sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.n(), 0);
    assert_eq!(sketch.num_retained(), 0);
    assert!(sketch.min_item().is_none());
    assert!(sketch.max_item().is_none());

    assert!(sketch.rank(&0.0, SearchCriteria::Inclusive).is_err());
    assert!(sketch.quantile(0.5, SearchCriteria::Inclusive).is_err());
    assert!(sketch.pmf(&[0.0], SearchCriteria::Inclusive).is_err());
    assert!(sketch.cdf(&[0.0], SearchCriteria::Inclusive).is_err());
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

    assert_relative_eq!(
        sketch
            .rank(&1.0, SearchCriteria::Exclusive)
            .expect("rank should succeed"),
        0.0
    );
    assert_relative_eq!(
        sketch
            .rank(&1.0, SearchCriteria::Inclusive)
            .expect("rank should succeed"),
        1.0
    );
    assert_relative_eq!(
        sketch
            .rank(&1.1, SearchCriteria::Exclusive)
            .expect("rank should succeed"),
        1.0
    );
    assert_relative_eq!(
        sketch
            .rank(&f32::INFINITY, SearchCriteria::Inclusive)
            .expect("rank should succeed"),
        1.0
    );

    for rank in [0.0, 0.5, 1.0] {
        assert_relative_eq!(
            sketch
                .quantile(rank, SearchCriteria::Exclusive)
                .expect("quantile should succeed"),
            1.0
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

    assert_relative_eq!(
        sketch
            .rank(&1.0, SearchCriteria::Exclusive)
            .expect("rank should succeed"),
        0.0
    );
    assert_relative_eq!(
        sketch
            .rank(&1.0, SearchCriteria::Inclusive)
            .expect("rank should succeed"),
        0.5
    );
    assert_relative_eq!(
        sketch
            .rank(&2.0, SearchCriteria::Exclusive)
            .expect("rank should succeed"),
        0.5
    );
    assert_relative_eq!(
        sketch
            .rank(&2.0, SearchCriteria::Inclusive)
            .expect("rank should succeed"),
        1.0
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
    assert!(sketch.num_retained() < n as u32);
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

    assert!((r0 - 0.0).abs() <= 1e-3);
    assert!((rmid - 0.5).abs() <= 0.01);
    assert!((rmax - 1.0).abs() <= 1e-3);
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
    assert!((1.0..=100.0).contains(&median));

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
    assert!(ReqSketch::<f64>::try_new(0, RankAccuracy::HighRank).is_err());
    assert!(ReqSketch::<f64>::try_new(3, RankAccuracy::HighRank).is_err()); // odd
    assert!(ReqSketch::<f64>::try_new(4096, RankAccuracy::HighRank).is_err()); // too large
    assert!(ReqSketch::<f64>::try_new(12, RankAccuracy::HighRank).is_ok());
    assert!(ReqSketch::<f64>::builder().k(5).is_err()); // odd via builder
    assert!(ReqSketch::<f64>::builder().k(12).is_ok());
}
