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
fn quantile_endpoints_preserve_extrema_after_compaction() {
    let mut sketch = KllSketch::<i64>::new(8).unwrap();
    for item in 0..8 {
        sketch.update(item);
    }
    // Compact an even run with distinct extrema before inserting an interior item.
    // Either sampling parity discards one of the two endpoints.
    sketch.update(3);
    assert!(sketch.is_estimation_mode());
    let view = sketch.sorted_view();

    for criteria in [SearchCriteria::Inclusive, SearchCriteria::Exclusive] {
        assert_eq!(sketch.quantile(0.0, criteria).unwrap(), Some(0));
        assert_eq!(sketch.quantile(1.0, criteria).unwrap(), Some(7));
        assert_eq!(
            sketch.quantiles(&[0.0, 1.0], criteria).unwrap(),
            Some(vec![0, 7])
        );
    }

    sketch.reset();
    for criteria in [SearchCriteria::Inclusive, SearchCriteria::Exclusive] {
        assert_eq!(sketch.quantile(0.0, criteria).unwrap(), None);
        assert_eq!(sketch.quantile(1.0, criteria).unwrap(), None);
        assert_eq!(view.quantile(0.0, criteria).unwrap(), Some(0));
        assert_eq!(view.quantile(1.0, criteria).unwrap(), Some(7));
        assert_eq!(
            view.quantiles(&[0.0, 1.0], criteria).unwrap(),
            Some(vec![0, 7])
        );
    }
}

#[test]
fn quantile_endpoints_with_large_stream_weight() {
    let mut sketch = KllSketch::<i64>::new(8).unwrap();
    sketch.update(0);
    for _ in 0..53 {
        sketch.merge(&sketch.clone()).unwrap();
    }
    sketch.update(1);
    assert_eq!(sketch.n(), (1 << 53) + 1);
    let view = sketch.sorted_view();

    for criteria in [SearchCriteria::Inclusive, SearchCriteria::Exclusive] {
        assert_eq!(sketch.quantile(0.0, criteria).unwrap(), Some(0));
        assert_eq!(sketch.quantile(1.0, criteria).unwrap(), Some(1));
        assert_eq!(view.quantile(0.0, criteria).unwrap(), Some(0));
        assert_eq!(view.quantile(1.0, criteria).unwrap(), Some(1));
    }
}

#[test]
fn valid_queries_on_empty_sketch_return_no_result() {
    let sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    let view = sketch.sorted_view();
    for criteria in [SearchCriteria::Inclusive, SearchCriteria::Exclusive] {
        assert_eq!(sketch.rank(&0, criteria), None);
        assert_eq!(view.rank(&0, criteria), None);
        for rank in [0.0, 0.5, 1.0] {
            assert_eq!(sketch.quantile(rank, criteria).unwrap(), None);
            assert_eq!(view.quantile(rank, criteria).unwrap(), None);
        }
        assert_eq!(sketch.quantiles(&[0.0, 0.5, 1.0], criteria).unwrap(), None);
        assert_eq!(view.quantiles(&[0.0, 0.5, 1.0], criteria).unwrap(), None);
        assert_eq!(sketch.cdf(&[0], criteria).unwrap(), None);
        assert_eq!(sketch.pmf(&[0], criteria).unwrap(), None);
        assert_eq!(view.cdf(&[0], criteria).unwrap(), None);
        assert_eq!(view.pmf(&[0], criteria).unwrap(), None);
    }
}

#[test]
fn empty_argument_slices_preserve_the_sketch_empty_state() {
    for n in [0, 1] {
        let mut sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
        for item in 0..n {
            sketch.update(item);
        }
        let view = sketch.sorted_view();
        let quantiles = (n != 0).then(Vec::new);
        let distribution = (n != 0).then(|| vec![1.0]);
        for criteria in [SearchCriteria::Inclusive, SearchCriteria::Exclusive] {
            assert_eq!(sketch.quantiles(&[], criteria).unwrap(), quantiles);
            assert_eq!(view.quantiles(&[], criteria).unwrap(), quantiles);
            assert_eq!(sketch.cdf(&[], criteria).unwrap(), distribution);
            assert_eq!(sketch.pmf(&[], criteria).unwrap(), distribution);
            assert_eq!(view.cdf(&[], criteria).unwrap(), distribution);
            assert_eq!(view.pmf(&[], criteria).unwrap(), distribution);
        }
    }
}

#[test]
fn invalid_arguments_are_rejected_regardless_of_sketch_state() {
    for n in [0, 1, 1_000] {
        let mut sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
        for item in 0..n {
            sketch.update(item);
        }
        let view = sketch.sorted_view();
        for criteria in [SearchCriteria::Inclusive, SearchCriteria::Exclusive] {
            for rank in [f64::NAN, f64::NEG_INFINITY, -0.1, 1.1, f64::INFINITY] {
                for error in [
                    sketch.quantile(rank, criteria).unwrap_err(),
                    view.quantile(rank, criteria).unwrap_err(),
                    sketch.quantiles(&[0.5, rank], criteria).unwrap_err(),
                    view.quantiles(&[0.5, rank], criteria).unwrap_err(),
                ] {
                    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
                }
            }
            for splits in [[1, 1], [2, 1]] {
                for error in [
                    sketch.cdf(&splits, criteria).unwrap_err(),
                    sketch.pmf(&splits, criteria).unwrap_err(),
                    view.cdf(&splits, criteria).unwrap_err(),
                    view.pmf(&splits, criteria).unwrap_err(),
                ] {
                    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
                }
            }
        }
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
        sketch.quantile(0.5, SearchCriteria::Inclusive).unwrap(),
        Some(1)
    );
    assert_eq!(
        sketch.quantile(0.5, SearchCriteria::Exclusive).unwrap(),
        Some(2)
    );
}

#[test]
fn exact_mode_queries_match_the_stream() {
    let mut sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    for item in 1..=100 {
        sketch.update(item);
    }

    assert_eq!(
        sketch.quantile(0.0, SearchCriteria::Inclusive).unwrap(),
        Some(1)
    );
    assert_eq!(
        sketch.quantile(0.5, SearchCriteria::Inclusive).unwrap(),
        Some(50)
    );
    assert_eq!(
        sketch.quantile(1.0, SearchCriteria::Inclusive).unwrap(),
        Some(100)
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
        quantiles
    );
    for (&rank, quantile) in ranks.iter().zip(&quantiles) {
        assert_eq!(
            view.quantile(rank, SearchCriteria::Inclusive).unwrap(),
            Some(*quantile)
        );
        assert_eq!(
            view.rank(quantile, SearchCriteria::Inclusive).unwrap(),
            sketch.rank(quantile, SearchCriteria::Inclusive).unwrap()
        );
    }

    sketch.update(2_000);
    assert_eq!(view.total_weight(), 1_000);
    assert_eq!(
        view.quantile(1.0, SearchCriteria::Inclusive).unwrap(),
        Some(999)
    );
}
