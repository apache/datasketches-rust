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

//! Public iterator behavior for ReqSketch.

use datasketches::req::ReqSketch;

#[test]
fn iterator_weights_sum_to_n_and_items_are_in_range() {
    let mut sketch = ReqSketch::new();
    for i in 0..1000 {
        sketch.update(i as f64);
    }

    let total_weight: u64 = sketch.iter().map(|(_, weight)| weight).sum();
    assert_eq!(total_weight, sketch.n());

    for (item, weight) in sketch.iter() {
        assert!(weight >= 1);
        assert!(item >= *sketch.min_item().expect("non-empty sketch"));
        assert!(item <= *sketch.max_item().expect("non-empty sketch"));
    }
}

#[test]
fn small_sketch_iterator_reports_unit_weights() {
    // Below the compaction threshold every retained item still has weight 1.
    let mut sketch = ReqSketch::new();
    for i in 0..10 {
        sketch.update(i as f64);
    }

    let items: Vec<(f64, u64)> = sketch.iter().collect();
    assert_eq!(items.len(), 10);
    assert!(items.iter().all(|&(_, weight)| weight == 1));
}

#[test]
fn empty_sketch_iterator_yields_nothing() {
    let sketch: ReqSketch<i32> = ReqSketch::new();
    assert_eq!(sketch.iter().count(), 0);
}

#[test]
fn compaction_promotes_surviving_items_to_higher_weights() {
    // After enough updates to trigger compaction, surviving items are promoted up a
    // level at double weight, so the maximum item weight exceeds 1.
    let mut sketch = ReqSketch::new();
    for i in 0..100_000 {
        sketch.update(i as f64);
    }

    let max_weight = sketch.iter().map(|(_, weight)| weight).max().unwrap();
    assert!(
        max_weight > 1,
        "expected promoted items, got max weight {max_weight}"
    );

    // Every weight is a power of two (2^level).
    assert!(sketch.iter().all(|(_, weight)| weight.is_power_of_two()));
}
