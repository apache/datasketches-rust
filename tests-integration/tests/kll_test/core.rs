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
    assert_eq!(sketch.n(), 0);
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
    assert_eq!(sketch.n(), 0);
    assert_eq!(sketch.num_retained(), 0);
    assert_eq!(sketch.min_item(), None);
    assert_eq!(sketch.max_item(), None);
}

#[test]
fn float_adapter_rejects_nan() {
    assert_eq!(
        KllFloat::<f32>::new(f32::NAN).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );

    let mut sketch = KllSketch::new(DEFAULT_K).unwrap();
    sketch.update(KllFloat::<f32>::new(0.0).unwrap());
    assert_eq!(sketch.min_item().map(|value| **value), Some(0.0));
}

#[test]
fn retained_count_stays_consistent_through_compaction_and_roundtrip() {
    let mut sketch = KllSketch::<i64>::new(32).unwrap();
    for item in 0..100_000 {
        sketch.update(item);
        assert!(sketch.num_retained() <= sketch.n() as usize);
    }

    let decoded = KllSketch::<i64>::deserialize(&sketch.serialize()).unwrap();
    assert_eq!(decoded.n(), sketch.n());
    assert_eq!(decoded.num_retained(), sketch.num_retained());
    assert_eq!(decoded.min_item(), Some(&0));
    assert_eq!(decoded.max_item(), Some(&99_999));
}
