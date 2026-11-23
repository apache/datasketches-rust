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
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use datasketches::error::ErrorKind;
use datasketches::kll::KllFloat;
use datasketches::kll::KllSketch;

const DEFAULT_K: u16 = 200;
const MIN_K: u16 = 8;
const MAX_K: u16 = u16::MAX;

#[test]
fn k_limits() {
    KllSketch::<i64>::new(MIN_K).unwrap();
    KllSketch::<i64>::new(MAX_K).unwrap();

    let error = KllSketch::<i64>::new(MIN_K - 1).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn empty_and_reset_state() {
    let mut sketch = KllSketch::<i64>::new(64).unwrap();
    assert!(sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.total_weight(), 0);
    assert_eq!(sketch.num_retained(), 0);
    assert_eq!(sketch.min_item(), None);
    assert_eq!(sketch.max_item(), None);

    for item in 0..10_000 {
        sketch.update(item);
    }
    assert!(sketch.is_estimation_mode());
    assert!(sketch.num_retained() > 0);

    sketch.reset();
    assert_eq!(sketch.k(), 64);
    assert_eq!(sketch.min_k(), 64);
    assert!(sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.total_weight(), 0);
    assert_eq!(sketch.num_retained(), 0);
    assert_eq!(sketch.min_item(), None);
    assert_eq!(sketch.max_item(), None);
}

#[test]
fn float_adapter_rejects_nan_and_orders_values() {
    assert_eq!(
        KllFloat::<f32>::new(f32::NAN).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        KllFloat::<f64>::new(f64::NAN).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );

    let f32_values = [f32::NEG_INFINITY, -1.0, -0.0, 0.0, 1.0, f32::INFINITY];
    for &left in &f32_values {
        for &right in &f32_values {
            let expected = left.partial_cmp(&right).unwrap();
            let left_val = KllFloat::<f32>::new(left).unwrap();
            let right_val = KllFloat::<f32>::new(right).unwrap();
            assert_eq!(left_val.cmp(&right_val), expected);
            assert_eq!(left_val == right_val, expected == Ordering::Equal);
        }
    }

    let f64_values = [f64::NEG_INFINITY, -1.0, -0.0, 0.0, 1.0, f64::INFINITY];
    for &left in &f64_values {
        for &right in &f64_values {
            let expected = left.partial_cmp(&right).unwrap();
            let left_val = KllFloat::<f64>::new(left).unwrap();
            let right_val = KllFloat::<f64>::new(right).unwrap();
            assert_eq!(left_val.cmp(&right_val), expected);
            assert_eq!(left_val == right_val, expected == Ordering::Equal);
        }
    }

    let mut sketch = KllSketch::new(DEFAULT_K).unwrap();
    sketch.update(KllFloat::<f32>::new(0.0).unwrap());
    assert_eq!(sketch.min_item().map(|value| **value), Some(0.0));
}

#[test]
fn retained_count_stays_consistent_through_compaction_and_roundtrip() {
    let mut sketch = KllSketch::<i64>::new(32).unwrap();
    for item in 0..100_000 {
        sketch.update(item);
        assert!(sketch.num_retained() <= sketch.total_weight() as usize);
    }

    let decoded = KllSketch::<i64>::deserialize(&sketch.serialize()).unwrap();
    assert_eq!(decoded.total_weight(), sketch.total_weight());
    assert_eq!(decoded.num_retained(), sketch.num_retained());
    assert_eq!(decoded.min_item(), Some(&0));
    assert_eq!(decoded.max_item(), Some(&99_999));
}

#[test]
fn weight_overflow_preserves_state() {
    let mut one = KllSketch::<i64>::new(8).unwrap();
    one.update(0);
    let mut sketch = one.clone();
    // Doubling and adding one reaches the exact limit through valid public operations.
    for _ in 0..63 {
        sketch.merge(&sketch.clone());
        sketch.update(0);
    }
    assert_eq!(sketch.total_weight(), u64::MAX);
    let before = sketch.serialize();

    sketch.merge(&KllSketch::new(8).unwrap());
    assert!(catch_unwind(AssertUnwindSafe(|| sketch.update(1))).is_err());
    assert!(sketch.serialize() == before, "overflow changed the sketch");
    assert!(catch_unwind(AssertUnwindSafe(|| sketch.merge(&one))).is_err());
    assert!(sketch.serialize() == before, "overflow changed the sketch");
}
