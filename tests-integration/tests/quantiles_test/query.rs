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

use datasketches::common::SearchCriteria::Exclusive;
use datasketches::common::SearchCriteria::Inclusive;
use datasketches::error::ErrorKind;
use datasketches::quantiles::QuantilesFloat;
use datasketches::quantiles::QuantilesSketch;

use super::CountedItem;
use super::count_clones;

#[test]
fn empty_queries_validate_arguments_before_returning_no_result() {
    let sketch = QuantilesSketch::<i32>::default();
    assert_eq!(sketch.rank(&1, Inclusive), None);
    assert_eq!(sketch.quantile(0.5, Inclusive).unwrap(), None);
    assert_eq!(sketch.quantiles(&[0.0, 1.0], Inclusive).unwrap(), None);
    assert_eq!(sketch.cdf(&[1, 2], Inclusive).unwrap(), None);
    assert_eq!(sketch.pmf(&[1, 2], Inclusive).unwrap(), None);
    assert_eq!(sketch.quantiles(&[], Inclusive).unwrap(), None);
    assert_eq!(sketch.cdf(&[], Inclusive).unwrap(), None);
    assert_eq!(sketch.pmf(&[], Inclusive).unwrap(), None);

    for rank in [-0.1, 1.1, f64::NAN] {
        let error = sketch.quantile(rank, Inclusive).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }
    let error = sketch.quantiles(&[0.0, f64::NAN], Inclusive).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    let error = sketch.cdf(&[1, 1], Inclusive).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    let error = sketch.pmf(&[2, 1], Inclusive).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn invalid_queries_return_errors_without_cloning_items() {
    let mut sketch = QuantilesSketch::default();
    sketch.update(CountedItem(0));
    let clones = count_clones(|| {
        let error = sketch.quantile(f64::NAN, Inclusive).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        let error = sketch.quantiles(&[0.5, f64::NAN], Inclusive).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        let split_points = [CountedItem(1), CountedItem(1)];
        let error = sketch.cdf(&split_points, Inclusive).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        let error = sketch.pmf(&split_points, Inclusive).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    });
    assert_eq!(clones, 0);
}

#[test]
fn empty_batches_return_results_without_cloning_items() {
    let mut sketch = QuantilesSketch::default();
    sketch.update(CountedItem(0));
    let clones = count_clones(|| {
        assert_eq!(sketch.quantiles(&[], Inclusive).unwrap(), Some(vec![]));
        assert_eq!(sketch.cdf(&[], Inclusive).unwrap(), Some(vec![1.0]));
        assert_eq!(sketch.pmf(&[], Inclusive).unwrap(), Some(vec![1.0]));
    });
    assert_eq!(clones, 0);
}

#[test]
fn queries_reuse_the_sorted_view_after_an_empty_merge() {
    let mut sketch = QuantilesSketch::default();
    sketch.update(CountedItem(1));
    sketch.update(CountedItem(2));
    let split = [CountedItem(1)];
    let ranks = [0.25, 0.75];
    sketch.cdf(&split, Inclusive).unwrap();
    sketch.merge(&QuantilesSketch::default());

    let clones = count_clones(|| {
        sketch.cdf(&split, Inclusive).unwrap();
        sketch.pmf(&split, Inclusive).unwrap();
        sketch.quantiles(&ranks, Inclusive).unwrap();
        sketch.quantile(0.5, Inclusive).unwrap();
    });
    assert!(clones <= ranks.len() + 1, "cloned {clones} items");
}

#[test]
fn the_median_reflects_updates_and_merges() {
    let mut sketch = QuantilesSketch::default();
    sketch.update(3);
    assert_eq!(sketch.quantile(0.5, Inclusive).unwrap(), Some(3));

    sketch.update(1);
    assert_eq!(sketch.quantile(0.5, Inclusive).unwrap(), Some(1));

    let mut other = QuantilesSketch::default();
    other.update(2);
    sketch.merge(&other);
    assert_eq!(sketch.quantile(0.5, Inclusive).unwrap(), Some(2));
}

#[test]
fn estimated_size_includes_the_view_once_a_query_builds_it() {
    let mut sketch = QuantilesSketch::default();
    sketch.update(1);
    sketch.update(2);
    let size_before_query = sketch.estimated_size();

    sketch.rank(&1, Inclusive);
    sketch.quantile(0.0, Inclusive).unwrap();
    sketch.quantile(1.0, Exclusive).unwrap();
    assert_eq!(sketch.estimated_size(), size_before_query);

    sketch.quantile(0.5, Inclusive).unwrap();
    assert!(sketch.estimated_size() > size_before_query);

    let cloned = sketch.clone();
    let size_before_query = cloned.estimated_size();
    cloned.quantile(0.5, Inclusive).unwrap();
    assert!(cloned.estimated_size() > size_before_query);
}

#[test]
fn exact_queries_respect_search_criteria_at_repeated_items() {
    let mut sketch = QuantilesSketch::default();
    for item in [3, 1, 2, 1] {
        sketch.update(item);
    }

    assert_eq!(sketch.rank(&1, Inclusive), Some(0.5));
    assert_eq!(sketch.rank(&1, Exclusive), Some(0.0));
    assert_eq!(sketch.quantile(0.5, Inclusive).unwrap(), Some(1));
    assert_eq!(sketch.quantile(0.5, Exclusive).unwrap(), Some(2));
    assert_eq!(
        sketch.cdf(&[1, 2], Inclusive).unwrap(),
        Some(vec![0.5, 0.75, 1.0])
    );
    assert_eq!(
        sketch.pmf(&[1, 2], Exclusive).unwrap(),
        Some(vec![0.0, 0.5, 0.5])
    );
}

#[test]
fn quantile_endpoints_return_the_recorded_extrema() {
    let mut sketch = QuantilesSketch::new(2).unwrap();
    for item in [0.0, -0.0, -0.0, -0.0, -0.0] {
        sketch.update(QuantilesFloat::<f64>::new(item).unwrap());
    }

    // Signed zeros compare equal. The recorded minimum and maximum both keep the initial +0.0.
    let expected = 0.0f64.to_bits();
    let ranks = [0.0, 1.0];
    let view = sketch.sorted_view();
    for criteria in [Inclusive, Exclusive] {
        for rank in ranks {
            let item = sketch.quantile(rank, criteria).unwrap().unwrap();
            assert_eq!(item.to_bits(), expected);
            let item = view.quantile(rank, criteria).unwrap().unwrap();
            assert_eq!(item.to_bits(), expected);
        }

        for items in [
            sketch.quantiles(&ranks, criteria).unwrap().unwrap(),
            view.quantiles(&ranks, criteria).unwrap().unwrap(),
        ] {
            let bits: Vec<_> = items.iter().map(|item| item.to_bits()).collect();
            assert_eq!(bits, [expected, expected]);
        }
    }
}

#[test]
fn inclusive_quantile_uses_the_unrounded_rank_product() {
    let mut sketch = QuantilesSketch::default();
    for item in 1..=100 {
        sketch.update(item);
    }

    // In f64, 0.14 * 100 is slightly greater than 14.
    assert_eq!(sketch.quantile(0.14, Inclusive).unwrap(), Some(15));
}

#[test]
fn a_discarded_extremum_still_contributes_weight_to_queries() {
    let mut sketch = QuantilesSketch::new(2).unwrap();
    for item in 1..=4 {
        sketch.update(item);
    }

    let view = sketch.sorted_view();
    assert_eq!(view.len(), sketch.num_retained() + 1);
    assert_eq!(view.total_weight(), 4);
    assert_eq!(
        view.quantiles(&[0.875, 0.25], Inclusive).unwrap(),
        Some(vec![4, 1])
    );
    assert_eq!(sketch.quantile(0.25, Inclusive).unwrap(), Some(1));
    assert_eq!(sketch.quantile(0.75, Exclusive).unwrap(), Some(4));

    // The discarded endpoint receives one unit. The retained endpoint still has weight two.
    if sketch.iter().any(|(&item, _)| item == 1) {
        assert_eq!(sketch.rank(&4, Exclusive), Some(0.75));
    } else {
        assert_eq!(sketch.rank(&1, Inclusive), Some(0.25));
    }
}
