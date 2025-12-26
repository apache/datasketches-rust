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

use datasketches::frequencies::ErrorType;
use datasketches::frequencies::FrequentItemsSketch;
use datasketches::frequencies::FrequentLongsSketch;

#[test]
fn test_longs_empty() {
    let sketch = FrequentLongsSketch::new(8);

    assert!(sketch.is_empty());
    assert_eq!(sketch.get_num_active_items(), 0);
    assert_eq!(sketch.get_total_weight(), 0);
    assert_eq!(sketch.get_estimate(42), 0);
    assert_eq!(sketch.get_lower_bound(42), 0);
    assert_eq!(sketch.get_upper_bound(42), 0);
    assert_eq!(sketch.get_maximum_error(), 0);
}

#[test]
fn test_items_empty() {
    let sketch: FrequentItemsSketch<String> = FrequentItemsSketch::new(8);
    let item = "a".to_string();

    assert!(sketch.is_empty());
    assert_eq!(sketch.get_num_active_items(), 0);
    assert_eq!(sketch.get_total_weight(), 0);
    assert_eq!(sketch.get_estimate(&item), 0);
    assert_eq!(sketch.get_lower_bound(&item), 0);
    assert_eq!(sketch.get_upper_bound(&item), 0);
    assert_eq!(sketch.get_maximum_error(), 0);
}

#[test]
fn test_longs_one_item() {
    let mut sketch = FrequentLongsSketch::new(8);
    sketch.update(10);

    assert!(!sketch.is_empty());
    assert_eq!(sketch.get_num_active_items(), 1);
    assert_eq!(sketch.get_total_weight(), 1);
    assert_eq!(sketch.get_estimate(10), 1);
    assert_eq!(sketch.get_lower_bound(10), 1);
    assert_eq!(sketch.get_upper_bound(10), 1);
}

#[test]
fn test_items_one_item() {
    let mut sketch = FrequentItemsSketch::new(8);
    let item = "a".to_string();
    sketch.update(item.clone());

    assert!(!sketch.is_empty());
    assert_eq!(sketch.get_num_active_items(), 1);
    assert_eq!(sketch.get_total_weight(), 1);
    assert_eq!(sketch.get_estimate(&item), 1);
    assert_eq!(sketch.get_lower_bound(&item), 1);
    assert_eq!(sketch.get_upper_bound(&item), 1);
}

#[test]
fn test_longs_several_items_no_resize_no_purge() {
    let mut sketch = FrequentLongsSketch::new(8);
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    sketch.update(4);
    sketch.update(2);
    sketch.update(3);
    sketch.update(2);

    assert!(!sketch.is_empty());
    assert_eq!(sketch.get_total_weight(), 7);
    assert_eq!(sketch.get_num_active_items(), 4);
    assert_eq!(sketch.get_estimate(1), 1);
    assert_eq!(sketch.get_estimate(2), 3);
    assert_eq!(sketch.get_estimate(3), 2);
    assert_eq!(sketch.get_estimate(4), 1);
    assert_eq!(sketch.get_maximum_error(), 0);
}

#[test]
fn test_items_several_items_no_resize_no_purge() {
    let mut sketch = FrequentItemsSketch::new(8);
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
    assert_eq!(sketch.get_total_weight(), 7);
    assert_eq!(sketch.get_num_active_items(), 4);
    assert_eq!(sketch.get_estimate(&a), 1);
    assert_eq!(sketch.get_estimate(&b), 3);
    assert_eq!(sketch.get_estimate(&c), 2);
    assert_eq!(sketch.get_estimate(&d), 1);
    assert_eq!(sketch.get_maximum_error(), 0);

    let rows = sketch.get_frequent_items(ErrorType::NoFalsePositives);
    assert_eq!(rows.len(), 4);

    let rows = sketch.get_frequent_items_with_threshold(ErrorType::NoFalsePositives, 2);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].item(), &b);

    sketch.reset();
    assert!(sketch.is_empty());
    assert_eq!(sketch.get_num_active_items(), 0);
    assert_eq!(sketch.get_total_weight(), 0);
}

#[test]
fn test_items_several_items_with_resize_no_purge() {
    let mut sketch = FrequentItemsSketch::new(16);
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
    assert_eq!(sketch.get_total_weight(), 15);
    assert_eq!(sketch.get_num_active_items(), 12);
    assert_eq!(sketch.get_estimate(&a), 1);
    assert_eq!(sketch.get_estimate(&b), 3);
    assert_eq!(sketch.get_estimate(&c), 2);
    assert_eq!(sketch.get_estimate(&d), 1);
    assert_eq!(sketch.get_maximum_error(), 0);
}

#[test]
fn test_longs_purge_keeps_heavy_hitters() {
    let mut sketch = FrequentLongsSketch::new(8);
    sketch.update_with_count(1, 10);
    for item in 2..=7 {
        sketch.update(item);
    }

    assert_eq!(sketch.get_total_weight(), 16);
    assert_eq!(sketch.get_maximum_error(), 1);
    assert_eq!(sketch.get_estimate(1), 10);
    assert_eq!(sketch.get_lower_bound(1), 9);

    let rows = sketch.get_frequent_items(ErrorType::NoFalsePositives);
    assert_eq!(rows.len(), 1);
    assert_eq!(*rows[0].item(), 1);
    assert_eq!(rows[0].estimate(), 10);
}

#[test]
fn test_items_purge_keeps_heavy_hitters() {
    let mut sketch = FrequentItemsSketch::new(8);
    sketch.update_with_count("a".to_string(), 10);
    for item in ["b", "c", "d", "e", "f", "g"] {
        sketch.update(item.to_string());
    }

    assert_eq!(sketch.get_total_weight(), 16);
    assert_eq!(sketch.get_maximum_error(), 1);
    assert_eq!(sketch.get_estimate(&"a".to_string()), 10);
    assert_eq!(sketch.get_lower_bound(&"a".to_string()), 9);

    let rows = sketch.get_frequent_items(ErrorType::NoFalsePositives);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].item(), "a");
    assert_eq!(rows[0].estimate(), 10);
}

#[test]
fn test_longs_merge_exact_mode() {
    let mut sketch1 = FrequentLongsSketch::new(8);
    sketch1.update(1);
    sketch1.update(2);
    sketch1.update(2);

    let mut sketch2 = FrequentLongsSketch::new(8);
    sketch2.update(2);
    sketch2.update(3);

    sketch1.merge(&sketch2);

    assert!(!sketch1.is_empty());
    assert_eq!(sketch1.get_total_weight(), 5);
    assert_eq!(sketch1.get_num_active_items(), 3);
    assert_eq!(sketch1.get_estimate(1), 1);
    assert_eq!(sketch1.get_estimate(2), 3);
    assert_eq!(sketch1.get_estimate(3), 1);
    assert_eq!(sketch1.get_maximum_error(), 0);
}

#[test]
fn test_items_merge_exact_mode() {
    let mut sketch1 = FrequentItemsSketch::new(8);
    let a = "a".to_string();
    let b = "b".to_string();
    let c = "c".to_string();
    sketch1.update(a.clone());
    sketch1.update(b.clone());
    sketch1.update(b.clone());

    let mut sketch2 = FrequentItemsSketch::new(8);
    sketch2.update(b.clone());
    sketch2.update(c.clone());

    sketch1.merge(&sketch2);

    assert!(!sketch1.is_empty());
    assert_eq!(sketch1.get_total_weight(), 5);
    assert_eq!(sketch1.get_num_active_items(), 3);
    assert_eq!(sketch1.get_estimate(&a), 1);
    assert_eq!(sketch1.get_estimate(&b), 3);
    assert_eq!(sketch1.get_estimate(&c), 1);
    assert_eq!(sketch1.get_maximum_error(), 0);
}

#[test]
#[should_panic(expected = "count may not be negative")]
fn test_longs_negative_count_panics() {
    let mut sketch = FrequentLongsSketch::new(8);
    sketch.update_with_count(1, -1);
}

#[test]
#[should_panic(expected = "count may not be negative")]
fn test_items_negative_count_panics() {
    let mut sketch = FrequentItemsSketch::new(8);
    sketch.update_with_count("a".to_string(), -1);
}

#[test]
#[should_panic(expected = "value must be power of 2")]
fn test_longs_invalid_map_size_panics() {
    FrequentLongsSketch::new(6);
}

#[test]
#[should_panic(expected = "value must be power of 2")]
fn test_items_invalid_map_size_panics() {
    let _ = FrequentItemsSketch::<String>::new(6);
}
