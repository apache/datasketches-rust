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

//! Rank error bounds and sigma coverage for ReqSketch.

use datasketches::error::Error;
use datasketches::req::RankAccuracy;
use datasketches::req::ReqSketch;
use datasketches::req::SearchCriteria;
use googletest::assert_that;
use googletest::prelude::all;
use googletest::prelude::ge;
use googletest::prelude::le;
use googletest::prelude::lt;

#[test]
fn bounds_are_nested_and_in_unit_interval() {
    let mut sketch = ReqSketch::builder()
        .k(12)
        .expect("valid k")
        .rank_accuracy(RankAccuracy::HighRank)
        .build()
        .expect("build should succeed");

    for i in 0..50_000 {
        sketch.update(i as f64);
    }

    for rank in [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999] {
        let bounds: Vec<(f64, f64)> = (1..=3u8)
            .map(|sigma| {
                (
                    sketch.rank_lower_bound(rank, sigma),
                    sketch.rank_upper_bound(rank, sigma),
                )
            })
            .collect();

        for (lower, upper) in &bounds {
            assert_that!(*lower, le(*upper));
            assert_that!(*lower, all!(ge(0.0), le(1.0)));
            assert_that!(*upper, all!(ge(0.0), le(1.0)));
        }

        assert_that!(bounds[1].0, le(bounds[0].0));
        assert_that!(bounds[0].1, le(bounds[1].1));
        assert_that!(bounds[2].0, le(bounds[1].0));
        assert_that!(bounds[1].1, le(bounds[2].1));
    }
}

#[test]
fn theoretical_error_bounds_cover_uniform_quantiles() -> Result<(), Error> {
    let mut sketch = ReqSketch::new();
    let n = 50_000;

    for i in 0..n {
        sketch.update(i as f64);
    }

    for rank in [
        0.01, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.92,
        0.95, 0.97, 0.98, 0.99, 0.995, 0.999,
    ] {
        let true_quantile = rank * (n - 1) as f64;
        let estimated_rank = sketch.rank(&true_quantile, SearchCriteria::Inclusive)?;
        let lower = sketch.rank_lower_bound(rank, 3);
        let upper = sketch.rank_upper_bound(rank, 3);
        assert_that!(estimated_rank, all!(ge(lower), le(upper)), "rank: {rank}");
    }

    Ok(())
}

#[test]
fn hra_and_lra_bounds_are_tighter_at_their_target_end() -> Result<(), Error> {
    for rank in [0.05, 0.25, 0.5, 0.75, 0.95] {
        let mut hra = ReqSketch::builder()
            .rank_accuracy(RankAccuracy::HighRank)
            .build()?;
        let mut lra = ReqSketch::builder()
            .rank_accuracy(RankAccuracy::LowRank)
            .build()?;

        for i in 0..10_000 {
            hra.update(i as f64);
            lra.update(i as f64);
        }

        let hra_error =
            (rank - hra.rank_lower_bound(rank, 2)).max(hra.rank_upper_bound(rank, 2) - rank);
        let lra_error =
            (rank - lra.rank_lower_bound(rank, 2)).max(lra.rank_upper_bound(rank, 2) - rank);

        if rank >= 0.75 {
            assert_that!(hra_error, le(lra_error));
        } else if rank <= 0.25 {
            assert_that!(lra_error, le(hra_error));
        }
    }

    Ok(())
}

#[test]
fn exact_mode_bounds_are_tight() {
    let mut sketch = ReqSketch::new();

    for i in 0..20 {
        sketch.update(i as f64);
    }

    assert!(!sketch.is_estimation_mode());

    for rank in [0.1, 0.25, 0.5, 0.75, 0.9] {
        let lower = sketch.rank_lower_bound(rank, 2);
        let upper = sketch.rank_upper_bound(rank, 2);
        assert_that!((upper - lower) / 2.0, lt(0.05));
    }
}

#[test]
fn high_rank_accuracy_matches_tight_thresholds() {
    let mut sketch = ReqSketch::new();
    let n = 50_000;

    for i in 0..n {
        sketch.update(i as f64);
    }

    assert_eq!(sketch.n(), n as u64);

    for rank in [0.5, 0.9, 0.95, 0.99, 0.999] {
        let true_quantile = rank * (n - 1) as f64;
        let estimated_rank = sketch
            .rank(&true_quantile, SearchCriteria::Inclusive)
            .expect("rank should succeed");
        let abs_error = (estimated_rank - rank).abs();
        let max_abs_error = if rank >= 0.99 {
            0.005
        } else if rank >= 0.9 {
            0.01
        } else {
            0.02
        };

        assert_that!(abs_error, le(max_abs_error));
    }

    for rank in [0.9, 0.99, 0.999] {
        let lower = sketch.rank_lower_bound(rank, 3);
        let upper = sketch.rank_upper_bound(rank, 3);
        assert_that!(rank, all!(ge(lower), le(upper)));
    }
}
