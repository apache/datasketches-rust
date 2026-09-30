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
use datasketches::error::ErrorKind;
use datasketches::kll::KllSketch;

const DEFAULT_K: u16 = 200;
const NUMERIC_NOISE_TOLERANCE: f64 = 1e-6;

#[test]
fn empty_queries_return_none_and_invalid_queries_return_errors() {
    let sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    assert_eq!(sketch.rank(&0, SearchCriteria::Inclusive), None);
    assert!(matches!(
        sketch.quantile(0.5, SearchCriteria::Inclusive),
        Ok(None)
    ));
    assert!(matches!(
        sketch.quantiles(&[0.25, 0.75], SearchCriteria::Inclusive),
        Ok(None)
    ));
    assert!(matches!(
        sketch.pmf(&[0], SearchCriteria::Inclusive),
        Ok(None)
    ));
    assert!(matches!(
        sketch.cdf(&[0], SearchCriteria::Inclusive),
        Ok(None)
    ));

    for rank in [-1.0, f64::NAN, 1.1] {
        let error = sketch
            .quantile(rank, SearchCriteria::Inclusive)
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }
    let error = sketch
        .quantiles(&[0.5, 1.1], SearchCriteria::Inclusive)
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    for error in [
        sketch.cdf(&[1, 0], SearchCriteria::Inclusive).unwrap_err(),
        sketch.pmf(&[1, 0], SearchCriteria::Inclusive).unwrap_err(),
    ] {
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }

    let view = sketch.sorted_view();
    assert_eq!(view.rank(&0, SearchCriteria::Inclusive), None);
    assert!(matches!(
        view.quantile(0.5, SearchCriteria::Inclusive),
        Ok(None)
    ));
    assert!(matches!(
        view.quantiles(&[0.25, 0.75], SearchCriteria::Inclusive),
        Ok(None)
    ));
    assert!(matches!(
        view.pmf(&[0], SearchCriteria::Inclusive),
        Ok(None)
    ));
    assert!(matches!(
        view.cdf(&[0], SearchCriteria::Inclusive),
        Ok(None)
    ));

    for rank in [-1.0, f64::NAN, 1.1] {
        let error = view.quantile(rank, SearchCriteria::Inclusive).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }
    let error = view
        .quantiles(&[0.5, 1.1], SearchCriteria::Inclusive)
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    for error in [
        view.cdf(&[1, 0], SearchCriteria::Inclusive).unwrap_err(),
        view.pmf(&[1, 0], SearchCriteria::Inclusive).unwrap_err(),
    ] {
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }
}

#[test]
fn inclusive_and_exclusive_semantics_cover_duplicates() {
    let mut sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    for item in [1, 1, 2, 2] {
        sketch.update(item);
    }

    assert_eq!(sketch.rank(&1, SearchCriteria::Exclusive).unwrap(), 0.0);
    assert_eq!(sketch.rank(&1, SearchCriteria::Inclusive).unwrap(), 0.5);
    assert_eq!(sketch.rank(&2, SearchCriteria::Exclusive).unwrap(), 0.5);
    assert_eq!(sketch.rank(&2, SearchCriteria::Inclusive).unwrap(), 1.0);
    assert_eq!(
        sketch
            .quantile(0.5, SearchCriteria::Inclusive)
            .unwrap()
            .unwrap(),
        1
    );
    assert_eq!(
        sketch
            .quantile(0.5, SearchCriteria::Exclusive)
            .unwrap()
            .unwrap(),
        2
    );
}

#[test]
fn exact_mode_queries_match_the_stream() {
    let mut sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    for item in 1..=100 {
        sketch.update(item);
    }

    assert_eq!(
        sketch
            .quantile(0.0, SearchCriteria::Inclusive)
            .unwrap()
            .unwrap(),
        1
    );
    assert_eq!(
        sketch
            .quantile(0.5, SearchCriteria::Inclusive)
            .unwrap()
            .unwrap(),
        50
    );
    assert_eq!(
        sketch
            .quantile(1.0, SearchCriteria::Inclusive)
            .unwrap()
            .unwrap(),
        100
    );
    for item in 1..=100 {
        assert_eq!(
            sketch.rank(&item, SearchCriteria::Inclusive).unwrap(),
            item as f64 / 100.0
        );
    }
}

#[test]
fn estimation_mode_queries_preserve_deterministic_invariants() {
    let mut sketch = KllSketch::<i64>::new(64).unwrap();
    for item in 0..10_000 {
        sketch.update(item);
    }

    let mut previous_rank = 0.0;
    for item in (0..10_000).step_by(100) {
        let rank = sketch.rank(&item, SearchCriteria::Inclusive).unwrap();
        assert!(rank >= previous_rank);
        assert!((0.0..=1.0).contains(&rank));
        previous_rank = rank;
    }
    assert_eq!(sketch.min_item(), Some(&0));
    assert_eq!(sketch.max_item(), Some(&9_999));
    assert!(sketch.normalized_rank_error() < sketch.normalized_pmf_error());
}

#[test]
fn rank_cdf_and_pmf_are_consistent() {
    let mut sketch = KllSketch::<i64>::new(64).unwrap();
    for item in 0..10_000 {
        sketch.update(item);
    }
    let split_points: Vec<_> = (100..10_000).step_by(100).collect();

    for criteria in [SearchCriteria::Inclusive, SearchCriteria::Exclusive] {
        let cdf = sketch.cdf(&split_points, criteria).unwrap().unwrap();
        let pmf = sketch.pmf(&split_points, criteria).unwrap().unwrap();
        let mut subtotal = 0.0;
        for (index, split_point) in split_points.iter().enumerate() {
            subtotal += pmf[index];
            assert!((cdf[index] - subtotal).abs() <= NUMERIC_NOISE_TOLERANCE);
            assert_eq!(cdf[index], sketch.rank(split_point, criteria).unwrap());
        }
        assert!((pmf.iter().sum::<f64>() - 1.0).abs() <= NUMERIC_NOISE_TOLERANCE);
    }
}

#[test]
fn sorted_view_supports_repeated_and_batch_queries() {
    let mut sketch = KllSketch::<i64>::new(64).unwrap();
    for item in 0..1_000 {
        sketch.update(item);
    }
    let view = sketch.sorted_view();
    let ranks = [0.0, 0.25, 0.5, 0.75, 1.0];
    let quantiles = sketch
        .quantiles(&ranks, SearchCriteria::Inclusive)
        .unwrap()
        .unwrap();

    assert_eq!(view.len(), sketch.num_retained());
    assert_eq!(view.total_weight(), sketch.n());
    assert_eq!(
        view.quantiles(&ranks, SearchCriteria::Inclusive)
            .unwrap()
            .unwrap(),
        quantiles,
    );
    for (&rank, quantile) in ranks.iter().zip(&quantiles) {
        assert_eq!(
            view.quantile(rank, SearchCriteria::Inclusive)
                .unwrap()
                .unwrap(),
            *quantile
        );
        assert_eq!(
            view.rank(quantile, SearchCriteria::Inclusive).unwrap(),
            sketch.rank(quantile, SearchCriteria::Inclusive).unwrap()
        );
    }

    sketch.update(2_000);
    assert_eq!(view.total_weight(), 1_000);
    assert_eq!(
        view.quantile(1.0, SearchCriteria::Inclusive)
            .unwrap()
            .unwrap(),
        999
    );
}
