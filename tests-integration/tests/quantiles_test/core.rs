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

use std::cmp::Reverse;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use datasketches::common::SearchCriteria;
use datasketches::error::ErrorKind;
use datasketches::quantiles::QuantilesSketch;

#[test]
fn k_must_be_a_power_of_two_in_the_supported_range() {
    assert_eq!(QuantilesSketch::<i32>::default().k(), 128);
    for k in [2, 4, 32_768] {
        assert_eq!(QuantilesSketch::<i32>::new(k).unwrap().k(), k);
    }
    for k in [0, 1, 3, 32_769] {
        let error = QuantilesSketch::<i32>::new(k).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument, "k = {k}");
    }
}

#[test]
fn estimation_starts_at_twice_k() {
    for k in [2, 32_768] {
        let mut sketch = QuantilesSketch::new(k).unwrap();
        let threshold = 2 * u64::from(k);
        for item in 1..threshold {
            sketch.update(item);
        }

        assert!(!sketch.is_estimation_mode(), "k = {k}");
        assert_eq!(sketch.total_weight(), threshold - 1);
        assert_eq!(sketch.num_retained() as u64, threshold - 1);
        assert!(sketch.iter().all(|(_, weight)| weight == 1));

        sketch.update(threshold);

        assert!(sketch.is_estimation_mode(), "k = {k}");
        assert_eq!(sketch.total_weight(), threshold);
        assert_eq!(sketch.num_retained(), usize::from(k));
        assert!(sketch.iter().all(|(_, weight)| weight == 2));
    }
}

#[test]
fn quantiles_follow_the_order_of_owned_items() {
    let mut sketch = QuantilesSketch::default();
    for item in ["beta", "gamma", "alpha"] {
        sketch.update(Reverse(item.to_owned()));
    }

    let values = sketch
        .quantiles(&[0.0, 0.5, 1.0], SearchCriteria::Inclusive)
        .unwrap()
        .unwrap();
    assert_eq!(
        values,
        [
            Reverse("gamma".to_owned()),
            Reverse("beta".to_owned()),
            Reverse("alpha".to_owned()),
        ]
    );
}

#[test]
fn weight_overflow_preserves_state() {
    let mut sketch = QuantilesSketch::new(4).unwrap();
    sketch.update(0);
    // Starting from one item, doubling the weight and adding one 63 times reaches u64::MAX.
    for _ in 0..63 {
        sketch.merge(&sketch.clone());
        sketch.update(0);
    }
    assert_eq!(sketch.total_weight(), u64::MAX);
    let retained_items = |sketch: &QuantilesSketch<i32>| {
        let mut items: Vec<_> = sketch
            .iter()
            .map(|(&item, weight)| (item, weight))
            .collect();
        items.sort_unstable();
        items
    };
    let retained = retained_items(&sketch);

    let mut source = QuantilesSketch::new(2).unwrap();
    for _ in 0..4 {
        source.update(-1);
    }

    let assert_unchanged = |sketch: &QuantilesSketch<i32>| {
        assert_eq!(sketch.k(), 4);
        assert_eq!(sketch.total_weight(), u64::MAX);
        assert_eq!(sketch.min_item(), Some(&0));
        assert_eq!(sketch.max_item(), Some(&0));
        assert_eq!(retained_items(sketch), retained);
    };

    assert!(catch_unwind(AssertUnwindSafe(|| sketch.update(1))).is_err());
    assert_unchanged(&sketch);
    assert!(catch_unwind(AssertUnwindSafe(|| sketch.merge(&source))).is_err());
    assert_unchanged(&sketch);
}
