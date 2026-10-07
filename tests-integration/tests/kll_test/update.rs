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

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use datasketches::common::SearchCriteria;
use datasketches::kll::KllSketch;

const DEFAULT_K: u16 = 200;
const RANK_EPS_FOR_K_200: f64 = 0.0133;

#[test]
fn zero_weight_is_a_no_op() {
    let mut sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    sketch.update_with_weight(1, 0);
    assert!(sketch.is_empty());
    assert_eq!(sketch.min_item(), None);
}

#[test]
fn weighted_update_matches_repeated_updates_in_exact_mode() {
    let mut weighted = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    let mut repeated = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    for item in 1..=15 {
        weighted.update_with_weight(item, item as u64);
        for _ in 0..item {
            repeated.update(item);
        }
    }
    assert!(!weighted.is_estimation_mode());
    assert_eq!(weighted, repeated);
}

#[test]
fn single_item_with_large_weight() {
    let mut sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    let weight = (1u64 << 40) + 12_345;
    sketch.update_with_weight(7, weight);
    assert_eq!(sketch.n(), weight);
    assert_eq!(sketch.num_retained(), weight.count_ones() as usize);
    assert_eq!(sketch.min_item(), Some(&7));
    assert_eq!(sketch.max_item(), Some(&7));
    assert_eq!(sketch.rank(&7, SearchCriteria::Inclusive).unwrap(), 1.0);
    assert_eq!(sketch.rank(&7, SearchCriteria::Exclusive).unwrap(), 0.0);
    assert_eq!(sketch.quantile(0.5, SearchCriteria::Inclusive).unwrap(), 7);
}

#[test]
fn maximum_weight() {
    let mut sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    sketch.update(1);
    sketch.update_with_weight(2, u64::MAX - 1);
    assert_eq!(sketch.n(), u64::MAX);
    assert_eq!(sketch.min_item(), Some(&1));
    assert_eq!(sketch.max_item(), Some(&2));
    assert_eq!(sketch.quantile(0.5, SearchCriteria::Inclusive).unwrap(), 2);

    let decoded = KllSketch::<i64>::deserialize(&sketch.serialize()).unwrap();
    assert_eq!(decoded, sketch);
}

#[test]
fn weighted_updates_into_a_full_sketch() {
    let mut sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    for item in 0..1_000 {
        sketch.update(item);
    }
    sketch.update_with_weight(-1, 1_000);
    sketch.update_with_weight(2_000, 3_000);
    assert_eq!(sketch.n(), 5_000);
    assert_eq!(sketch.min_item(), Some(&-1));
    assert_eq!(sketch.max_item(), Some(&2_000));
    let rank = sketch.rank(&-1, SearchCriteria::Inclusive).unwrap();
    assert!((rank - 0.2).abs() <= RANK_EPS_FOR_K_200, "rank {rank}");
    let rank = sketch.rank(&2_000, SearchCriteria::Exclusive).unwrap();
    assert!((rank - 0.4).abs() <= RANK_EPS_FOR_K_200, "rank {rank}");
}

#[test]
fn weighted_updates_in_estimation_mode() {
    let mut sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    let weights: Vec<u64> = (0..10_000).map(|item| (item % 13) * 1_000 + 1).collect();
    for (item, &weight) in weights.iter().enumerate() {
        sketch.update_with_weight(item as i64, weight);
    }
    let total: u64 = weights.iter().sum();
    assert_eq!(sketch.n(), total);
    assert!(sketch.is_estimation_mode());
    assert_eq!(sketch.min_item(), Some(&0));
    assert_eq!(sketch.max_item(), Some(&9_999));

    let mut weight_below = 0u64;
    for (item, &weight) in weights.iter().enumerate() {
        let true_rank = weight_below as f64 / total as f64;
        let rank = sketch
            .rank(&(item as i64), SearchCriteria::Exclusive)
            .unwrap();
        assert!(
            (rank - true_rank).abs() <= RANK_EPS_FOR_K_200,
            "item {item}: rank {rank}, true rank {true_rank}"
        );
        weight_below += weight;
    }

    let decoded = KllSketch::<i64>::deserialize(&sketch.serialize()).unwrap();
    assert_eq!(decoded.n(), sketch.n());
    assert_eq!(decoded.num_retained(), sketch.num_retained());
}

#[test]
fn weighted_update_of_strings() {
    let mut sketch = KllSketch::<String>::new(DEFAULT_K).unwrap();
    sketch.update_with_weight("b".to_string(), 3);
    sketch.update_with_weight("a".to_string(), 1_000);
    sketch.update_with_weight("c".to_string(), 1 << 20);
    assert_eq!(sketch.n(), 3 + 1_000 + (1 << 20));
    assert_eq!(sketch.min_item().map(String::as_str), Some("a"));
    assert_eq!(sketch.max_item().map(String::as_str), Some("c"));
}

#[test]
fn weight_overflow_preserves_state() {
    let mut sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    sketch.update_with_weight(0, u64::MAX - 1);
    let before = sketch.serialize();

    assert!(catch_unwind(AssertUnwindSafe(|| sketch.update_with_weight(1, 2))).is_err());
    assert!(sketch.serialize() == before, "overflow changed the sketch");

    sketch.update_with_weight(1, 1);
    assert_eq!(sketch.n(), u64::MAX);
    assert_eq!(sketch.max_item(), Some(&1));
}
