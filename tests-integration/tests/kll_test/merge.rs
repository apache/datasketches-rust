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
use datasketches::kll::KllSketch;

#[test]
fn merge_preserves_weight_extrema_and_query_invariants() {
    let mut left = KllSketch::<i64>::new(200).unwrap();
    let mut right = KllSketch::<i64>::new(200).unwrap();
    for item in 0..10_000 {
        left.update(item);
        right.update(19_999 - item);
    }

    left.merge(&right);

    assert_eq!(left.total_weight(), 20_000);
    assert_eq!(left.min_item(), Some(&0));
    assert_eq!(left.max_item(), Some(&19_999));
    assert_eq!(left.sorted_view().total_weight(), left.total_weight());
    let quantiles = left
        .quantiles(&[0.0, 0.25, 0.5, 0.75, 1.0], SearchCriteria::Inclusive)
        .unwrap()
        .unwrap();
    assert!(quantiles.windows(2).all(|pair| pair[0] <= pair[1]));
}

#[test]
fn merge_tracks_the_smallest_estimation_k() {
    let mut left = KllSketch::<i64>::new(256).unwrap();
    let mut right = KllSketch::<i64>::new(128).unwrap();
    for item in 0..10_000 {
        left.update(item);
        right.update(20_000 - item);
    }

    left.merge(&right);

    assert_eq!(left.min_k(), right.min_k());
    assert_eq!(left.normalized_rank_error(), right.normalized_rank_error());
    assert_eq!(left.normalized_pmf_error(), right.normalized_pmf_error());
}

#[test]
fn merging_an_empty_lower_k_sketch_does_not_change_accuracy() {
    let mut sketch = KllSketch::<i64>::new(256).unwrap();
    for item in 0..10_000 {
        sketch.update(item);
    }
    let empty = KllSketch::<i64>::new(128).unwrap();
    let rank_error = sketch.normalized_rank_error();

    sketch.merge(&empty);

    assert_eq!(sketch.total_weight(), 10_000);
    assert_eq!(sketch.normalized_rank_error(), rank_error);
}

#[test]
fn merge_updates_extrema_from_either_side() {
    let mut first = KllSketch::<i64>::new(200).unwrap();
    let mut second = KllSketch::<i64>::new(200).unwrap();
    first.update(1);
    second.update(2);

    second.merge(&first);

    assert_eq!(second.min_item(), Some(&1));
    assert_eq!(second.max_item(), Some(&2));
}

#[test]
fn merge_interleaves_populated_higher_levels_without_compaction() {
    // At `k = 8`, nine levels have total capacity `72`, so each `weight = 256` (`1 << 8`) call
    // exceeds `free_capacity` and lands directly in level 8 (`[1, 3]` and `[2, 4]`) while four
    // retained items stay below level capacity `8`, avoiding random compaction.
    let mut left = KllSketch::<i64>::new(8).unwrap();
    left.update_with_weight(1, 256);
    left.update_with_weight(3, 256);

    let mut right = KllSketch::<i64>::new(8).unwrap();
    right.update_with_weight(2, 256);
    right.update_with_weight(4, 256);

    left.merge(&right);

    assert_eq!(left.total_weight(), 1_024);
    assert_eq!(left.num_retained(), 4);
    for (item, expected_rank) in [(1, 0.25), (2, 0.5), (3, 0.75), (4, 1.0)] {
        assert_eq!(
            left.rank(&item, SearchCriteria::Inclusive),
            Some(expected_rank)
        );
    }
}
