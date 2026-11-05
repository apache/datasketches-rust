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

use datasketches::frequencies::FrequentItemsSketch;

fn weight(i: u64) -> u64 {
    i % 5 + 1
}

/// Feeds distinct items below the purge threshold, so the map doubles several times and no
/// counter is ever decremented, and checks every estimate after each batch.
fn check_growth<T: Clone + Eq + std::hash::Hash>(
    max_map_size: usize,
    distinct: u64,
    item: impl Fn(u64) -> T,
    estimate: impl Fn(&FrequentItemsSketch<T>, &T) -> u64,
) {
    let mut sketch = FrequentItemsSketch::new(max_map_size).unwrap();
    let initial_capacity = sketch.current_map_capacity();
    let mut total_weight = 0;
    for i in 0..distinct {
        sketch.update_with_count(item(i), weight(i));
        total_weight += weight(i);
        if (i + 1) % 250 == 0 || i + 1 == distinct {
            assert_eq!(sketch.total_weight(), total_weight);
            assert_eq!(sketch.num_active_items() as u64, i + 1);
            assert_eq!(sketch.maximum_error(), 0);
            for j in 0..=i {
                assert_eq!(estimate(&sketch, &item(j)), weight(j), "item {j} after {i}");
            }
        }
    }
    assert!(sketch.current_map_capacity() > initial_capacity);
}

#[test]
fn test_longs_growth_keeps_every_item() {
    check_growth(
        1 << 12,
        2_000,
        |i| (i * 7_919 + 13) as i64,
        |sketch, item| sketch.estimate(item),
    );
}

#[test]
fn test_items_growth_keeps_every_item() {
    check_growth(
        1 << 12,
        2_000,
        |i| format!("item-{i}"),
        |sketch, item| sketch.estimate(item),
    );
}

#[test]
fn test_growth_survives_serialization() {
    let mut sketch: FrequentItemsSketch<i64> = FrequentItemsSketch::new(1 << 12).unwrap();
    for i in 0..2_000 {
        sketch.update_with_count((i * 7_919 + 13) as i64, weight(i));
    }
    let bytes = sketch.serialize();
    let restored = FrequentItemsSketch::<i64>::deserialize(&bytes).unwrap();
    assert_eq!(restored.serialize(), bytes);
    assert_eq!(restored.total_weight(), sketch.total_weight());
    for i in 0..2_000 {
        assert_eq!(restored.estimate(&((i * 7_919 + 13) as i64)), weight(i));
    }
}
