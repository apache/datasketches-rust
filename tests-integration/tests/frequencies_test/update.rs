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

use datasketches::error::ErrorKind;
use datasketches::frequencies::ErrorType;
use datasketches::frequencies::FrequentItemsSketch;
use googletest::assert_that;
use googletest::prelude::ge;
use googletest::prelude::gt;
use googletest::prelude::le;
use googletest::prelude::near;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TestItem(i32);

#[derive(Debug, PartialEq, Eq, Hash)]
struct NonCloneItem(i32);

#[test]
fn test_non_clone_items_update_and_query() {
    let mut sketch = FrequentItemsSketch::<NonCloneItem>::new(8).unwrap();

    sketch.update(NonCloneItem(7));
    sketch.update_with_count(NonCloneItem(11), 3);

    assert_eq!(sketch.total_weight(), 4);
    assert_eq!(sketch.num_active_items(), 2);
    assert_eq!(sketch.estimate(&NonCloneItem(7)), 1);
    assert_eq!(sketch.lower_bound(&NonCloneItem(11)), 3);
    assert_eq!(sketch.upper_bound(&NonCloneItem(11)), 3);
}

#[test]
fn test_longs_update_with_zero_count_is_noop() {
    let mut sketch: FrequentItemsSketch<i64> = FrequentItemsSketch::new(8).unwrap();
    sketch.update_with_count(1, 0);

    assert!(sketch.is_empty());
    assert_eq!(sketch.total_weight(), 0);
    assert_eq!(sketch.num_active_items(), 0);
}

#[test]
fn test_items_update_with_zero_count_is_noop() {
    let mut sketch = FrequentItemsSketch::new(8).unwrap();
    sketch.update_with_count("a".to_string(), 0);

    assert!(sketch.is_empty());
    assert_eq!(sketch.total_weight(), 0);
    assert_eq!(sketch.num_active_items(), 0);
}

#[test]
fn test_count_arithmetic_saturates() {
    let mut sketch = FrequentItemsSketch::new(8).unwrap();
    for item in 0..7 {
        sketch.update(item);
    }
    assert_eq!(sketch.maximum_error(), 1);

    sketch.update_with_count(99, u64::MAX);
    sketch.update_ref(&99);

    let mut other = FrequentItemsSketch::new(8).unwrap();
    other.update(99);
    sketch.merge(&other);

    assert_eq!(sketch.total_weight(), u64::MAX);
    assert_eq!(sketch.lower_bound(&99), u64::MAX);
    assert_eq!(sketch.estimate(&99), u64::MAX);
    assert_eq!(sketch.upper_bound(&99), u64::MAX);

    let rows = sketch.frequent_items(ErrorType::NoFalseNegatives);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].estimate(), u64::MAX);
    assert_eq!(rows[0].upper_bound(), u64::MAX);
    assert_eq!(rows[0].lower_bound(), u64::MAX);
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
fn test_items_empty() {
    let sketch: FrequentItemsSketch<String> = FrequentItemsSketch::new(8).unwrap();
    let item = "a".to_string();

    assert!(sketch.is_empty());
    assert_eq!(sketch.num_active_items(), 0);
    assert_eq!(sketch.total_weight(), 0);
    assert_eq!(sketch.estimate(&item), 0);
    assert_eq!(sketch.lower_bound(&item), 0);
    assert_eq!(sketch.upper_bound(&item), 0);
    assert_eq!(sketch.maximum_error(), 0);
}

#[test]
fn test_longs_one_item() {
    let mut sketch: FrequentItemsSketch<i64> = FrequentItemsSketch::new(8).unwrap();
    sketch.update(10);

    assert!(!sketch.is_empty());
    assert_eq!(sketch.num_active_items(), 1);
    assert_eq!(sketch.total_weight(), 1);
    assert_eq!(sketch.estimate(&10), 1);
    assert_eq!(sketch.lower_bound(&10), 1);
    assert_eq!(sketch.upper_bound(&10), 1);
}

#[test]
fn test_items_one_item() {
    let mut sketch = FrequentItemsSketch::new(8).unwrap();
    let item = "a".to_string();
    sketch.update(item.clone());

    assert!(!sketch.is_empty());
    assert_eq!(sketch.num_active_items(), 1);
    assert_eq!(sketch.total_weight(), 1);
    assert_eq!(sketch.estimate(&item), 1);
    assert_eq!(sketch.lower_bound(&item), 1);
    assert_eq!(sketch.upper_bound(&item), 1);
}

#[test]
fn test_items_borrowed_key_updates_and_queries() {
    let mut sketch = FrequentItemsSketch::<String>::new(16).unwrap();

    sketch.update_ref("alpha");
    sketch.update_ref("alpha");
    sketch.update_with_count_ref("beta", 3);
    sketch.update_with_count_ref("ignored", 0);
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
    sketch.update_with_count(1, 10);
    for item in 2..=6 {
        sketch.update(item);
    }
    sketch.update_with_count(7, 15);
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
fn test_items_estimation_mode() {
    let mut sketch: FrequentItemsSketch<i32> = FrequentItemsSketch::new(8).unwrap();
    sketch.update_with_count(1, 10);
    for item in 2..=6 {
        sketch.update(item);
    }
    sketch.update_with_count(7, 15);
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
    sketch.update_with_count(1, 10);
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
fn test_items_purge_keeps_heavy_hitters() {
    let mut sketch = FrequentItemsSketch::new(8).unwrap();
    sketch.update_with_count("a".to_string(), 10);
    for item in ["b", "c", "d", "e", "f", "g"] {
        sketch.update(item.to_string());
    }

    assert_eq!(sketch.total_weight(), 16);
    assert_eq!(sketch.maximum_error(), 1);
    assert_eq!(sketch.estimate(&"a".to_string()), 10);
    assert_eq!(sketch.lower_bound(&"a".to_string()), 9);

    let rows = sketch.frequent_items(ErrorType::NoFalsePositives);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].item(), "a");
    assert_eq!(rows[0].estimate(), 10);
}

#[test]
fn test_items_custom_type() {
    let mut sketch: FrequentItemsSketch<TestItem> = FrequentItemsSketch::new(8).unwrap();
    sketch.update_with_count(TestItem(1), 10);
    for item in 2..=7 {
        sketch.update(TestItem(item));
    }
    let item = TestItem(8);
    sketch.update(item);

    assert!(!sketch.is_empty());
    assert_eq!(sketch.total_weight(), 17);
    assert_eq!(sketch.estimate(&TestItem(1)), 10);

    let rows = sketch.frequent_items(ErrorType::NoFalsePositives);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].item(), &TestItem(1));
    assert_eq!(rows[0].estimate(), 10);
}

#[test]
fn test_longs_merge_estimation_mode() {
    let mut sketch1: FrequentItemsSketch<i64> = FrequentItemsSketch::new(16).unwrap();
    sketch1.update_with_count(1, 9);
    for item in 2..=14 {
        sketch1.update(item);
    }
    assert_that!(sketch1.maximum_error(), gt(0));

    let mut sketch2: FrequentItemsSketch<i64> = FrequentItemsSketch::new(16).unwrap();
    for item in 8..=20 {
        sketch2.update(item);
    }
    sketch2.update_with_count(21, 11);
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
fn test_items_merge_estimation_mode() {
    let mut sketch1: FrequentItemsSketch<i32> = FrequentItemsSketch::new(16).unwrap();
    sketch1.update_with_count(1, 9);
    for item in 2..=14 {
        sketch1.update(item);
    }
    assert_that!(sketch1.maximum_error(), gt(0));

    let mut sketch2: FrequentItemsSketch<i32> = FrequentItemsSketch::new(16).unwrap();
    for item in 8..=20 {
        sketch2.update(item);
    }
    sketch2.update_with_count(21, 11);
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
fn test_longs_merge_exact_mode() {
    let mut sketch1: FrequentItemsSketch<i64> = FrequentItemsSketch::new(8).unwrap();
    sketch1.update(1);
    sketch1.update(2);
    sketch1.update(2);

    let mut sketch2: FrequentItemsSketch<i64> = FrequentItemsSketch::new(8).unwrap();
    sketch2.update(2);
    sketch2.update(3);

    sketch1.merge(&sketch2);

    assert!(!sketch1.is_empty());
    assert_eq!(sketch1.total_weight(), 5);
    assert_eq!(sketch1.num_active_items(), 3);
    assert_eq!(sketch1.estimate(&1), 1);
    assert_eq!(sketch1.estimate(&2), 3);
    assert_eq!(sketch1.estimate(&3), 1);
    assert_eq!(sketch1.maximum_error(), 0);
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
fn test_items_merge_empty_is_noop() {
    let mut sketch: FrequentItemsSketch<i32> = FrequentItemsSketch::new(8).unwrap();
    sketch.update(1);

    let empty: FrequentItemsSketch<i32> = FrequentItemsSketch::new(8).unwrap();
    sketch.merge(&empty);

    assert_eq!(sketch.total_weight(), 1);
    assert_eq!(sketch.num_active_items(), 1);
    assert_eq!(sketch.estimate(&1), 1);
}

#[test]
fn test_merge_preserves_purged_empty_state() {
    let mut purged: FrequentItemsSketch<i64> = FrequentItemsSketch::new(32).unwrap();
    for item in 0..=(32 * 3 / 4) {
        purged.update(item);
    }
    assert!(purged.is_empty());
    assert_eq!(purged.total_weight(), 25);
    assert_eq!(purged.maximum_error(), 1);

    let mut merged: FrequentItemsSketch<i64> = FrequentItemsSketch::new(32).unwrap();
    merged.merge(&purged);

    assert!(merged.is_empty());
    assert_eq!(merged.num_active_items(), 0);
    assert_eq!(merged.total_weight(), purged.total_weight());
    assert_eq!(merged.maximum_error(), purged.maximum_error());
    assert_eq!(merged.upper_bound(&1000), purged.upper_bound(&1000));
}

#[test]
fn test_row_equality_changes_with_updates() {
    let mut sketch: FrequentItemsSketch<i32> = FrequentItemsSketch::new(8).unwrap();
    sketch.update(1);
    let rows1 = sketch.frequent_items(ErrorType::NoFalsePositives);
    assert_eq!(rows1.len(), 1);
    let row1 = rows1[0].clone();

    sketch.update(1);
    let rows2 = sketch.frequent_items(ErrorType::NoFalsePositives);
    assert_eq!(rows2.len(), 1);
    let row2 = rows2[0].clone();

    assert_ne!(row1, row2);
    assert_eq!(row2.item(), &1);
    assert_eq!(row2.estimate(), 2);
}

#[test]
fn test_longs_reset() {
    let mut sketch: FrequentItemsSketch<i64> = FrequentItemsSketch::new(8).unwrap();
    sketch.update_with_count(1, 3);
    sketch.update_with_count(2, 2);
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
    let error = FrequentItemsSketch::<i64>::new(1usize << 31).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn test_estimated_size() {
    let mut sketch: FrequentItemsSketch<i64> = FrequentItemsSketch::new(64).unwrap();
    assert_eq!(sketch.estimated_size(), 344);

    // The internal map grows from its starting size up to the maximum size.
    for i in 0..100 {
        sketch.update(i);
    }
    assert_eq!(sketch.estimated_size(), 1800);
}
