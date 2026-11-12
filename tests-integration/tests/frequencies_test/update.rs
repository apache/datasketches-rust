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

use std::hash::Hash;
use std::hash::Hasher;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use datasketches::error::ErrorKind;
use datasketches::frequencies::ErrorType;
use datasketches::frequencies::FrequentItemsSketch;
use googletest::assert_that;
use googletest::prelude::ge;
use googletest::prelude::gt;
use googletest::prelude::le;
use googletest::prelude::near;

#[test]
fn test_longs_update_with_zero_count_is_noop() {
    let mut sketch: FrequentItemsSketch<i64> = FrequentItemsSketch::new(8).unwrap();
    sketch.update_with_weight(1, 0);

    assert!(sketch.is_empty());
    assert_eq!(sketch.total_weight(), 0);
    assert_eq!(sketch.num_active_items(), 0);
}

#[test]
fn test_capacity_and_epsilon_helpers() {
    let longs: FrequentItemsSketch<i64> = FrequentItemsSketch::new(8).unwrap();
    assert_eq!(longs.current_map_capacity(), 6);
    assert_eq!(longs.maximum_map_capacity(), 6);
    assert_eq!(longs.max_map_size(), 8);
    assert_eq!(longs.lg_cur_map_size(), 3);
    assert_eq!(longs.lg_max_map_size(), 3);

    let epsilon = FrequentItemsSketch::<i64>::epsilon_for_max_map_size(1024).unwrap();
    let expected = 3.5 / 1024.0;
    assert_that!(epsilon, near(expected, 1e-12));

    let apriori = FrequentItemsSketch::<i64>::apriori_error(1024, 10_000).unwrap();
    assert_that!(apriori, near(expected * 10_000.0, 1e-9));

    let invalid_epsilon = FrequentItemsSketch::<i64>::epsilon_for_max_map_size(6).unwrap_err();
    assert_eq!(invalid_epsilon.kind(), ErrorKind::InvalidArgument);
    let invalid_apriori = FrequentItemsSketch::<i64>::apriori_error(4, 10_000).unwrap_err();
    assert_eq!(invalid_apriori.kind(), ErrorKind::InvalidArgument);

    let items: FrequentItemsSketch<i32> = FrequentItemsSketch::new(1024).unwrap();
    assert_that!(items.epsilon(), near(expected, 1e-12));
    assert_eq!(items.current_map_capacity(), 6);
    assert_eq!(items.maximum_map_capacity(), 768);
    assert_eq!(items.max_map_size(), 1024);
    assert_eq!(items.lg_max_map_size(), 10);
}

#[test]
fn test_longs_empty() {
    let sketch: FrequentItemsSketch<i64> = FrequentItemsSketch::new(8).unwrap();

    assert!(sketch.is_empty());
    assert_eq!(sketch.num_active_items(), 0);
    assert_eq!(sketch.total_weight(), 0);
    assert_eq!(sketch.estimate(&42), 0);
    assert_eq!(sketch.lower_bound(&42), 0);
    assert_eq!(sketch.upper_bound(&42), 0);
    assert_eq!(sketch.maximum_error(), 0);
}

#[test]
fn test_items_borrowed_key_updates_and_queries() {
    let mut sketch = FrequentItemsSketch::<String>::new(16).unwrap();

    sketch.update_ref("alpha");
    sketch.update_ref("alpha");
    sketch.update_with_weight_ref("beta", 3);
    sketch.update_with_weight_ref("ignored", 0);
    for item in [
        "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa", "lambda", "mu",
    ] {
        sketch.update_ref(item);
    }

    assert!(!sketch.is_empty());
    assert_eq!(sketch.total_weight(), 15);
    assert_eq!(sketch.num_active_items(), 12);
    assert_eq!(sketch.estimate("alpha"), 2);
    assert_eq!(sketch.lower_bound("alpha"), 2);
    assert_eq!(sketch.upper_bound("alpha"), 2);
    assert_eq!(sketch.estimate("beta"), 3);
    assert_eq!(sketch.estimate("ignored"), 0);
    assert_eq!(sketch.estimate("missing"), 0);

    let owned = "alpha".to_string();
    assert_eq!(sketch.estimate(&owned), 2);
}

#[test]
fn test_longs_several_items_no_resize_no_purge() {
    let mut sketch: FrequentItemsSketch<i64> = FrequentItemsSketch::new(8).unwrap();
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    sketch.update(4);
    sketch.update(2);
    sketch.update(3);
    sketch.update(2);

    assert!(!sketch.is_empty());
    assert_eq!(sketch.total_weight(), 7);
    assert_eq!(sketch.num_active_items(), 4);
    assert_eq!(sketch.estimate(&1), 1);
    assert_eq!(sketch.estimate(&2), 3);
    assert_eq!(sketch.estimate(&3), 2);
    assert_eq!(sketch.estimate(&4), 1);
    assert_eq!(sketch.maximum_error(), 0);
}

#[test]
fn test_items_several_items_no_resize_no_purge() {
    let mut sketch = FrequentItemsSketch::new(8).unwrap();
    let a = "a".to_string();
    let b = "b".to_string();
    let c = "c".to_string();
    let d = "d".to_string();
    sketch.update(a.clone());
    sketch.update(b.clone());
    sketch.update(c.clone());
    sketch.update(d.clone());
    sketch.update(b.clone());
    sketch.update(c.clone());
    sketch.update(b.clone());

    assert!(!sketch.is_empty());
    assert_eq!(sketch.total_weight(), 7);
    assert_eq!(sketch.num_active_items(), 4);
    assert_eq!(sketch.estimate(&a), 1);
    assert_eq!(sketch.estimate(&b), 3);
    assert_eq!(sketch.estimate(&c), 2);
    assert_eq!(sketch.estimate(&d), 1);
    assert_eq!(sketch.maximum_error(), 0);

    let rows = sketch.frequent_items(ErrorType::NoFalsePositives);
    assert_eq!(rows.len(), 4);

    let rows = sketch.frequent_items_with_threshold(ErrorType::NoFalsePositives, 2);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].item(), &b);

    sketch.reset();
    assert!(sketch.is_empty());
    assert_eq!(sketch.num_active_items(), 0);
    assert_eq!(sketch.total_weight(), 0);
}

#[test]
fn test_items_several_items_with_resize_no_purge() {
    let mut sketch = FrequentItemsSketch::new(16).unwrap();
    let a = "a".to_string();
    let b = "b".to_string();
    let c = "c".to_string();
    let d = "d".to_string();
    sketch.update(a.clone());
    sketch.update(b.clone());
    sketch.update(c.clone());
    sketch.update(d.clone());
    sketch.update(b.clone());
    sketch.update(c.clone());
    sketch.update(b.clone());
    for item in ["e", "f", "g", "h", "i", "j", "k", "l"] {
        sketch.update(item.to_string());
    }

    assert!(!sketch.is_empty());
    assert_eq!(sketch.total_weight(), 15);
    assert_eq!(sketch.num_active_items(), 12);
    assert_eq!(sketch.estimate(&a), 1);
    assert_eq!(sketch.estimate(&b), 3);
    assert_eq!(sketch.estimate(&c), 2);
    assert_eq!(sketch.estimate(&d), 1);
    assert_eq!(sketch.maximum_error(), 0);
}

#[test]
fn test_longs_estimation_mode() {
    let mut sketch: FrequentItemsSketch<i64> = FrequentItemsSketch::new(8).unwrap();
    sketch.update_with_weight(1, 10);
    for item in 2..=6 {
        sketch.update(item);
    }
    sketch.update_with_weight(7, 15);
    for item in 8..=12 {
        sketch.update(item);
    }

    assert!(!sketch.is_empty());
    assert_eq!(sketch.total_weight(), 35);
    assert_that!(sketch.maximum_error(), gt(0));

    let items = sketch.frequent_items(ErrorType::NoFalsePositives);
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].item(), &7);
    assert_eq!(items[0].estimate(), 15);
    assert_eq!(items[1].item(), &1);
    assert_eq!(items[1].estimate(), 10);

    let items = sketch.frequent_items(ErrorType::NoFalseNegatives);
    assert_that!(items.len(), ge(2));
    assert_that!(items.len(), le(12));
}

#[test]
fn test_longs_purge_keeps_heavy_hitters() {
    let mut sketch: FrequentItemsSketch<i64> = FrequentItemsSketch::new(8).unwrap();
    sketch.update_with_weight(1, 10);
    for item in 2..=7 {
        sketch.update(item);
    }

    assert_eq!(sketch.total_weight(), 16);
    assert_eq!(sketch.maximum_error(), 1);
    assert_eq!(sketch.estimate(&1), 10);
    assert_eq!(sketch.lower_bound(&1), 9);

    let rows = sketch.frequent_items(ErrorType::NoFalsePositives);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].item(), &1);
    assert_eq!(rows[0].estimate(), 10);
}

#[test]
fn test_longs_merge_estimation_mode() {
    let mut sketch1: FrequentItemsSketch<i64> = FrequentItemsSketch::new(16).unwrap();
    sketch1.update_with_weight(1, 9);
    for item in 2..=14 {
        sketch1.update(item);
    }
    assert_that!(sketch1.maximum_error(), gt(0));

    let mut sketch2: FrequentItemsSketch<i64> = FrequentItemsSketch::new(16).unwrap();
    for item in 8..=20 {
        sketch2.update(item);
    }
    sketch2.update_with_weight(21, 11);
    assert_that!(sketch2.maximum_error(), gt(0));

    sketch1.merge(&sketch2);
    assert!(!sketch1.is_empty());
    assert_eq!(sketch1.total_weight(), 46);
    assert_that!(sketch1.num_active_items(), ge(2));

    let items = sketch1.frequent_items_with_threshold(ErrorType::NoFalsePositives, 2);
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].item(), &21);
    assert_that!(items[0].estimate(), ge(11));
    assert_eq!(items[1].item(), &1);
    assert_that!(items[1].estimate(), ge(9));
}

#[test]
fn test_items_merge_exact_mode() {
    let mut sketch1 = FrequentItemsSketch::new(8).unwrap();
    let a = "a".to_string();
    let b = "b".to_string();
    let c = "c".to_string();
    sketch1.update(a.clone());
    sketch1.update(b.clone());
    sketch1.update(b.clone());

    let mut sketch2 = FrequentItemsSketch::new(8).unwrap();
    sketch2.update(b.clone());
    sketch2.update(c.clone());

    sketch1.merge(&sketch2);

    assert!(!sketch1.is_empty());
    assert_eq!(sketch1.total_weight(), 5);
    assert_eq!(sketch1.num_active_items(), 3);
    assert_eq!(sketch1.estimate(&a), 1);
    assert_eq!(sketch1.estimate(&b), 3);
    assert_eq!(sketch1.estimate(&c), 1);
    assert_eq!(sketch1.maximum_error(), 0);
}

#[test]
fn test_longs_merge_empty_is_noop() {
    let mut sketch: FrequentItemsSketch<i64> = FrequentItemsSketch::new(8).unwrap();
    sketch.update(1);

    let empty: FrequentItemsSketch<i64> = FrequentItemsSketch::new(8).unwrap();
    sketch.merge(&empty);

    assert_eq!(sketch.total_weight(), 1);
    assert_eq!(sketch.num_active_items(), 1);
    assert_eq!(sketch.estimate(&1), 1);
}

#[test]
fn test_purged_state_survives_merge_and_reset() {
    let mut purged = FrequentItemsSketch::new(256).unwrap();
    for item in 0..193 {
        purged.update(item);
    }
    assert!(!purged.is_empty());
    assert_eq!(purged.num_active_items(), 0);
    assert_eq!(purged.total_weight(), 193);
    assert_eq!(purged.maximum_error(), 1);

    let mut merged = FrequentItemsSketch::new(256).unwrap();
    merged.merge(&purged);

    assert!(!merged.is_empty());
    assert_eq!(merged.num_active_items(), 0);
    assert_eq!(merged.total_weight(), purged.total_weight());
    assert_eq!(merged.maximum_error(), purged.maximum_error());
    assert_eq!(merged.upper_bound(&0), 1);

    let mut nonempty = FrequentItemsSketch::new(256).unwrap();
    nonempty.update(1000);
    nonempty.merge(&purged);
    assert_eq!(nonempty.total_weight(), 194);
    assert_eq!(nonempty.maximum_error(), 1);
    assert_eq!(nonempty.upper_bound(&0), 1);

    purged.reset();
    assert!(purged.is_empty());
    assert_eq!(purged.total_weight(), 0);
    assert_eq!(purged.maximum_error(), 0);
}

#[test]
fn test_update_weight_overflow_preserves_state() {
    let mut sketch = FrequentItemsSketch::<String>::new(8).unwrap();
    sketch.update_with_weight_ref("a", u64::MAX - 1);
    sketch.update("b".to_string());
    assert_eq!(sketch.total_weight(), u64::MAX);
    let before = sketch.serialize();

    assert!(catch_unwind(AssertUnwindSafe(|| sketch.update("c".to_string()))).is_err());
    assert_eq!(sketch.serialize(), before);
    assert!(catch_unwind(AssertUnwindSafe(|| sketch.update_ref("a"))).is_err());
    assert_eq!(sketch.serialize(), before);
    assert!(!sketch.is_empty());
}

#[test]
fn test_merge_weight_overflow_preserves_state() {
    let mut sketch = FrequentItemsSketch::<i64>::new(8).unwrap();
    sketch.update_with_weight(1, u64::MAX - 7);
    let mut purged = FrequentItemsSketch::<i64>::new(8).unwrap();
    for item in 0..7 {
        purged.update(item);
    }
    assert_eq!(purged.num_active_items(), 0);

    sketch.merge(&purged);
    assert_eq!(sketch.total_weight(), u64::MAX);
    assert_eq!(sketch.maximum_error(), 1);
    let before = sketch.serialize();
    assert!(catch_unwind(AssertUnwindSafe(|| sketch.merge(&purged))).is_err());
    assert_eq!(sketch.serialize(), before);
    assert!(!sketch.is_empty());
}

#[test]
fn test_longs_reset() {
    let mut sketch: FrequentItemsSketch<i64> = FrequentItemsSketch::new(8).unwrap();
    sketch.update_with_weight(1, 3);
    sketch.update_with_weight(2, 2);
    sketch.reset();

    assert!(sketch.is_empty());
    assert_eq!(sketch.total_weight(), 0);
    assert_eq!(sketch.num_active_items(), 0);
    assert_eq!(sketch.lg_max_map_size(), 3);
}

#[test]
fn test_invalid_map_size_returns_error() {
    for max_map_size in [1, 2, 4, 6] {
        let error = FrequentItemsSketch::<i64>::new(max_map_size).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }
}

#[test]
fn test_map_size_above_cross_language_limit_returns_error() {
    let error = FrequentItemsSketch::<i64>::new(1 << 31).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[derive(Debug, PartialEq, Eq)]
struct CollidingItem(usize);

impl Hash for CollidingItem {
    fn hash<H: Hasher>(&self, state: &mut H) {
        0u64.hash(state);
    }
}

#[test]
fn test_colliding_items_preserve_frequency_bounds() {
    let mut sketch = FrequentItemsSketch::new(64).unwrap();
    let mut counts = [1u64; 100];
    for count in counts.iter_mut().step_by(16) {
        *count = 100;
    }
    for (item, &count) in counts.iter().enumerate() {
        sketch.update_with_weight(CollidingItem(item), count);
    }
    assert!(sketch.maximum_error() > 0);

    // Exercise subsequent updates to both a heavy hitter and a light item.
    for item in [0, 1] {
        sketch.update_with_weight(CollidingItem(item), 10);
        counts[item] += 10;
    }
    assert_eq!(sketch.total_weight(), counts.iter().sum::<u64>());
    for (item, &count) in counts.iter().enumerate() {
        assert!(sketch.lower_bound(&CollidingItem(item)) <= count);
        assert!(sketch.upper_bound(&CollidingItem(item)) >= count);
    }
}
