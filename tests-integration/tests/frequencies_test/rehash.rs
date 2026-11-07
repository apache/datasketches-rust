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

//! Growing the item map must keep every item with its exact weight.

use std::hash::Hash;
use std::hash::Hasher;

use datasketches::frequencies::FrequentItemsSketch;

fn weight(i: u64) -> u64 {
    i % 5 + 1
}

// Stay below the purge threshold and check all counts at each growth boundary.
fn check_growth<T: Eq + Hash>(max_map_size: usize, distinct: u64, item: impl Fn(u64) -> T) {
    let mut sketch = FrequentItemsSketch::new(max_map_size).unwrap();
    let initial_capacity = sketch.current_map_capacity();
    let mut total_weight = 0;
    for i in 0..distinct {
        let capacity = sketch.current_map_capacity();
        sketch.update_with_count(item(i), weight(i));
        total_weight += weight(i);
        if sketch.current_map_capacity() != capacity || i + 1 == distinct {
            assert_eq!(sketch.total_weight(), total_weight);
            assert_eq!(sketch.num_active_items() as u64, i + 1);
            assert_eq!(sketch.maximum_error(), 0);
            for j in 0..=i {
                assert_eq!(sketch.estimate(&item(j)), weight(j), "item {j} after {i}");
            }
        }
    }
    assert!(sketch.current_map_capacity() > initial_capacity);
}

#[test]
fn test_longs_growth_keeps_every_item() {
    check_growth(1 << 12, 2_000, |i| (i * 7_919 + 13) as i64);
}

#[test]
fn test_items_growth_keeps_every_item() {
    check_growth(1 << 12, 2_000, |i| format!("item-{i}"));
}

#[derive(Debug, PartialEq, Eq)]
struct CollidingItem(u64);

impl Hash for CollidingItem {
    fn hash<H: Hasher>(&self, state: &mut H) {
        0u64.hash(state);
    }
}

#[test]
fn test_colliding_items_survive_growth_updates_and_purge() {
    let mut sketch = FrequentItemsSketch::new(64).unwrap();
    for i in 0..48 {
        sketch.update_with_count(CollidingItem(i), if i % 4 == 0 { 100 } else { 1 });
    }
    assert_eq!(sketch.current_map_capacity(), sketch.maximum_map_capacity());
    assert_eq!(sketch.maximum_error(), 0);
    for i in 0..48 {
        assert_eq!(
            sketch.estimate(&CollidingItem(i)),
            if i % 4 == 0 { 100 } else { 1 }
        );
    }

    for i in (0..48).step_by(4) {
        sketch.update_with_count(CollidingItem(i), 7);
    }
    assert_eq!(sketch.num_active_items(), 48);
    assert_eq!(sketch.maximum_error(), 0);

    // The new key triggers a purge with median 1. Deleting light items from
    // the collision chain must preserve access to every displaced heavy item.
    sketch.update(CollidingItem(48));
    assert_eq!(sketch.maximum_error(), 1);
    assert_eq!(sketch.num_active_items(), 12);
    for i in 0..49 {
        if i < 48 && i % 4 == 0 {
            assert_eq!(sketch.estimate(&CollidingItem(i)), 107);
            assert_eq!(sketch.lower_bound(&CollidingItem(i)), 106);
        } else {
            assert_eq!(sketch.estimate(&CollidingItem(i)), 0);
            assert_eq!(sketch.upper_bound(&CollidingItem(i)), 1);
        }
    }

    sketch.update_with_count(CollidingItem(44), 3);
    sketch.update_with_count(CollidingItem(1), 5);
    assert_eq!(sketch.estimate(&CollidingItem(44)), 110);
    assert_eq!(sketch.estimate(&CollidingItem(1)), 6);
    assert_eq!(sketch.num_active_items(), 13);
    assert_eq!(sketch.total_weight(), 12 * 107 + 37 + 3 + 5);
}
