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

//! Rank, quantile, PMF, and CDF behavior for ReqSketch.

use datasketches::error::Error;
use datasketches::req::ReqSketch;
use datasketches::req::SearchCriteria;
use googletest::assert_that;
use googletest::prelude::all;
use googletest::prelude::ge;
use googletest::prelude::le;
use googletest::prelude::lt;
use googletest::prelude::near;

use super::ReqF64;
use super::req_f64;

#[test]
fn exact_mode_rank_quantile_pmf_and_cdf_match_reference() {
    let mut sketch: ReqSketch<ReqF64> = ReqSketch::default();
    for i in 1..=10 {
        sketch.update(req_f64(i as f64));
    }

    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.n(), 10);
    assert_eq!(sketch.num_retained(), 10);

    for (value, expected) in [(1.0, 0.0), (2.0, 0.1), (6.0, 0.5), (9.0, 0.8), (10.0, 0.9)] {
        assert_that!(
            sketch
                .rank(&req_f64(value), SearchCriteria::Exclusive)
                .expect("rank should succeed"),
            near(expected, 1e-6)
        );
    }

    for (value, expected) in [(1.0, 0.1), (2.0, 0.2), (5.0, 0.5), (9.0, 0.9), (10.0, 1.0)] {
        assert_that!(
            sketch
                .rank(&req_f64(value), SearchCriteria::Inclusive)
                .expect("rank should succeed"),
            near(expected, 1e-6)
        );
    }

    for (rank, expected) in [(0.0, 1.0), (0.1, 2.0), (0.5, 6.0), (0.9, 10.0), (1.0, 10.0)] {
        assert_eq!(
            *sketch
                .quantile(rank, SearchCriteria::Exclusive)
                .expect("quantile should succeed"),
            expected
        );
    }

    for (rank, expected) in [(0.0, 1.0), (0.1, 1.0), (0.5, 5.0), (0.9, 9.0), (1.0, 10.0)] {
        assert_eq!(
            *sketch
                .quantile(rank, SearchCriteria::Inclusive)
                .expect("quantile should succeed"),
            expected
        );
    }

    let splits = [2.0, 6.0, 9.0].map(req_f64);
    let cdf = sketch
        .cdf(&splits, SearchCriteria::Exclusive)
        .expect("cdf should succeed");
    assert_that!(cdf[0], near(0.1, 1e-6));
    assert_that!(cdf[1], near(0.5, 1e-6));
    assert_that!(cdf[2], near(0.8, 1e-6));
    assert_that!(cdf[3], near(1.0, 1e-6));

    let pmf = sketch
        .pmf(&splits, SearchCriteria::Exclusive)
        .expect("pmf should succeed");
    assert_that!(pmf[0], near(0.1, 1e-6));
    assert_that!(pmf[1], near(0.4, 1e-6));
    assert_that!(pmf[2], near(0.3, 1e-6));
    assert_that!(pmf[3], near(0.2, 1e-6));
}

#[test]
fn pmf_and_cdf_are_consistent() {
    let mut sketch: ReqSketch<ReqF64> = ReqSketch::default();
    for i in 0..1000 {
        sketch.update(req_f64(i as f64));
    }

    let split_points = [100.0, 300.0, 500.0, 700.0, 900.0].map(req_f64);
    let pmf = sketch
        .pmf(&split_points, SearchCriteria::Inclusive)
        .expect("pmf should succeed");
    let cdf = sketch
        .cdf(&split_points, SearchCriteria::Inclusive)
        .expect("cdf should succeed");

    assert_that!(pmf.iter().sum::<f64>(), near(1.0, 1e-10));

    let mut cumulative = 0.0;
    for i in 0..pmf.len() {
        cumulative += pmf[i];
        assert_that!(cdf[i], near(cumulative, 1e-10));
    }
    assert_that!(cdf[cdf.len() - 1], near(1.0, 1e-10));
}

#[test]
fn rank_is_monotonic_and_bounded() {
    let mut sketch: ReqSketch<ReqF64> = ReqSketch::default();
    for i in 0..10_000 {
        sketch.update(req_f64(i as f64));
    }

    let test_values = (0..10_000).step_by(1000).map(|value| req_f64(value as f64));
    let mut last_rank = 0.0;

    for value in test_values {
        let rank = sketch
            .rank(&value, SearchCriteria::Inclusive)
            .expect("rank should succeed");
        assert_that!(rank, ge(last_rank));
        assert_that!(rank, all!(ge(0.0), le(1.0)));
        last_rank = rank;
    }
}

#[test]
fn quantiles_are_monotonic() -> Result<(), Error> {
    let mut sketch: ReqSketch<ReqF64> = ReqSketch::default();
    for i in 0..10_000 {
        sketch.update(req_f64(i as f64));
    }

    let ranks = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9];
    let mut previous = 0.0;

    for rank in ranks {
        let quantile = sketch.quantile(rank, SearchCriteria::Inclusive)?;
        assert_that!(*quantile, ge(previous));
        previous = *quantile;
    }

    Ok(())
}

#[test]
fn rank_quantile_round_trip_is_consistent() -> Result<(), Error> {
    let mut sketch: ReqSketch<ReqF64> = ReqSketch::default();
    for i in 0..10_000 {
        sketch.update(req_f64(i as f64));
    }

    for target_rank in [0.1, 0.25, 0.5, 0.75, 0.9] {
        let quantile = sketch.quantile(target_rank, SearchCriteria::Inclusive)?;
        let recovered_rank = sketch.rank(&quantile, SearchCriteria::Inclusive)?;
        let error = (recovered_rank - target_rank).abs() / target_rank;
        assert_that!(error, lt(0.2));
    }

    Ok(())
}

#[test]
fn search_criteria_rank_consistency() -> Result<(), Error> {
    let mut sketch: ReqSketch<ReqF64> = ReqSketch::default();
    for i in 0..1000 {
        sketch.update(req_f64(i as f64));
    }

    for value in [100.0, 250.0, 500.0, 750.0].map(req_f64) {
        let inclusive_rank = sketch.rank(&value, SearchCriteria::Inclusive)?;
        let exclusive_rank = sketch.rank(&value, SearchCriteria::Exclusive)?;

        assert_that!(exclusive_rank, le(inclusive_rank));
        assert_that!(inclusive_rank, all!(ge(0.0), le(1.0)));
        assert_that!(exclusive_rank, all!(ge(0.0), le(1.0)));
    }

    Ok(())
}

#[test]
fn signed_zeros_share_rank_and_cannot_be_distinct_splits() -> Result<(), Error> {
    let mut sketch: ReqSketch<ReqF64> = ReqSketch::default();
    let negative_zero = req_f64(-0.0);
    let positive_zero = req_f64(0.0);
    sketch.update(negative_zero);
    sketch.update(positive_zero);

    for value in [negative_zero, positive_zero] {
        assert_eq!(sketch.rank(&value, SearchCriteria::Exclusive)?, 0.0);
        assert_eq!(sketch.rank(&value, SearchCriteria::Inclusive)?, 1.0);
    }

    assert!(
        sketch
            .pmf(&[negative_zero, positive_zero], SearchCriteria::Inclusive)
            .is_err()
    );

    Ok(())
}
