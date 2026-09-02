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

use std::cmp::Ordering;

use datasketches::error::ErrorKind;
use datasketches::kll::KllComparator;
use datasketches::kll::KllSketch;
use datasketches::kll::SearchCriteria;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DirectionalOrder {
    descending: bool,
}

impl KllComparator<i64> for DirectionalOrder {
    fn compare(&self, left: &i64, right: &i64) -> Ordering {
        if self.descending {
            right.cmp(left)
        } else {
            left.cmp(right)
        }
    }

    fn is_compatible(&self, other: &Self) -> bool {
        self == other
    }
}

#[test]
fn merge_preserves_weight_extrema_and_query_invariants() {
    let mut left = KllSketch::<f32>::new(200).unwrap();
    let mut right = KllSketch::<f32>::new(200).unwrap();
    for item in 0..10_000 {
        left.update(item as f32);
        right.update((19_999 - item) as f32);
    }

    left.merge(&right).unwrap();

    assert_eq!(left.n(), 20_000);
    assert_eq!(left.min_item(), Some(&0.0));
    assert_eq!(left.max_item(), Some(&19_999.0));
    assert_eq!(left.sorted_view().total_weight(), left.n());
    let quantiles = left
        .quantiles(&[0.0, 0.25, 0.5, 0.75, 1.0], SearchCriteria::Inclusive)
        .unwrap();
    assert!(quantiles.windows(2).all(|pair| pair[0] <= pair[1]));
}

#[test]
fn merge_tracks_the_smallest_estimation_k() {
    let mut left = KllSketch::<f32>::new(256).unwrap();
    let mut right = KllSketch::<f32>::new(128).unwrap();
    for item in 0..10_000 {
        left.update(item as f32);
        right.update((20_000 - item) as f32);
    }

    left.merge(&right).unwrap();

    assert_eq!(left.min_k(), right.min_k());
    assert_eq!(left.normalized_rank_error(), right.normalized_rank_error());
    assert_eq!(left.normalized_pmf_error(), right.normalized_pmf_error());
}

#[test]
fn merging_an_empty_lower_k_sketch_does_not_change_accuracy() {
    let mut sketch = KllSketch::<f32>::new(256).unwrap();
    for item in 0..10_000 {
        sketch.update(item as f32);
    }
    let empty = KllSketch::<f32>::new(128).unwrap();
    let rank_error = sketch.normalized_rank_error();

    sketch.merge(&empty).unwrap();

    assert_eq!(sketch.n(), 10_000);
    assert_eq!(sketch.normalized_rank_error(), rank_error);
}

#[test]
fn merge_updates_extrema_from_either_side() {
    let mut first = KllSketch::<f32>::new(200).unwrap();
    let mut second = KllSketch::<f32>::new(200).unwrap();
    first.update(1.0);
    second.update(2.0);

    second.merge(&first).unwrap();

    assert_eq!(second.min_item(), Some(&1.0));
    assert_eq!(second.max_item(), Some(&2.0));
}

#[test]
fn merge_rejects_incompatible_comparators_without_mutation() {
    let mut ascending =
        KllSketch::new_with_comparator(200, DirectionalOrder { descending: false }).unwrap();
    let mut descending =
        KllSketch::new_with_comparator(200, DirectionalOrder { descending: true }).unwrap();
    ascending.update(1);
    descending.update(2);

    let error = ascending.merge(&descending).unwrap_err();

    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    assert_eq!(ascending.n(), 1);
    assert_eq!(ascending.min_item(), Some(&1));
    assert_eq!(ascending.max_item(), Some(&1));
}
