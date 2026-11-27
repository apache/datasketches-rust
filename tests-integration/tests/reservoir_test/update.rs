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

use std::collections::HashSet;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use datasketches::common::ResizeFactor;
use datasketches::error::ErrorKind;
use datasketches::reservoir::ReservoirSketch;

const MAX_ITEMS_SEEN: u64 = 0xFFFF_FFFF_FFFF;

#[test]
fn rejects_invalid_k_bounds() {
    for bad_k in [0_usize, 1, (i32::MAX as usize) + 1, usize::MAX] {
        let err = ReservoirSketch::<i64>::new(bad_k).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidArgument);
    }
}

#[test]
fn exact_mode_preserves_insertion_order_and_transitions_to_sampling() {
    let mut sketch = ReservoirSketch::<i64>::new(16).unwrap();
    assert!(sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.k(), 16);
    assert_eq!(sketch.resize_factor(), ResizeFactor::X8);
    assert_eq!(sketch.total_weight(), 0);
    assert_eq!(sketch.num_retained(), 0);
    assert_eq!(sketch.estimated_size(), 0);

    for i in 0..16_i64 {
        sketch.update(i);
    }
    assert!(!sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.total_weight(), 16);
    assert_eq!(sketch.num_retained(), 16);
    let expected: Vec<i64> = (0..16).collect();
    assert_eq!(sketch.samples(), expected.as_slice());

    for i in 16..500_i64 {
        sketch.update(i);
    }
    assert!(sketch.is_estimation_mode());
    assert_eq!(sketch.total_weight(), 500);
    assert_eq!(sketch.num_retained(), 16);

    let unique: HashSet<i64> = (&sketch).into_iter().copied().collect();
    assert_eq!(unique.len(), 16);
    assert!(unique.iter().all(|&v| (0..500).contains(&v)));

    sketch.reset();
    assert!(sketch.is_empty());
    assert!(!sketch.is_estimation_mode());
    assert_eq!(sketch.total_weight(), 0);
    assert_eq!(sketch.num_retained(), 0);
    assert_eq!(sketch.estimated_size(), 0);
}

#[test]
fn supports_all_resize_factors_and_growth_steps() {
    for rf in [
        ResizeFactor::X1,
        ResizeFactor::X2,
        ResizeFactor::X4,
        ResizeFactor::X8,
    ] {
        let mut sketch = ReservoirSketch::<i64>::with_resize_factor(1_000, rf).unwrap();
        assert_eq!(sketch.resize_factor(), rf);
        assert_eq!(sketch.estimated_size(), 0);

        sketch.update(0);
        if rf == ResizeFactor::X1 {
            assert_eq!(sketch.estimated_size(), 1_000 * size_of::<i64>());
        } else {
            assert!(sketch.estimated_size() < 1_000 * size_of::<i64>());
        }

        for i in 1..2_000_i64 {
            sketch.update(i);
        }
        assert_eq!(sketch.num_retained(), 1_000);
        assert_eq!(sketch.total_weight(), 2_000);
        assert_eq!(sketch.estimated_size(), 1_000 * size_of::<i64>());
    }
}

#[test]
fn marginal_inclusion_probabilities_are_uniform() {
    // Stream of N = 20 items into reservoir of size k = 5 across 40,000 independent trials:
    // Each item has exact marginal probability k / N = 0.25 -> expected 10,000 appearances,
    // std dev = sqrt(40,000 * 0.25 * 0.75) ≈ 86.6; 6 sigma ≈ 520.
    const N: usize = 20;
    const K: usize = 5;
    const TRIALS: usize = 40_000;
    const EXPECTED: usize = TRIALS * K / N;
    const TOLERANCE: usize = 550;

    let mut counts = [0_usize; N];
    for _ in 0..TRIALS {
        let mut sketch = ReservoirSketch::<usize>::new(K).unwrap();
        for item in 0..N {
            sketch.update(item);
        }
        for &sampled in sketch.samples() {
            counts[sampled] += 1;
        }
    }

    for (item, &count) in counts.iter().enumerate() {
        assert!(
            count.abs_diff(EXPECTED) <= TOLERANCE,
            "item {item} appeared {count} times, expected {EXPECTED} +/- {TOLERANCE}"
        );
    }
}

#[test]
fn large_total_weight_updates_and_overflow_panics_atomically() {
    let mut base = ReservoirSketch::<i64>::new(2).unwrap();
    base.update(0);
    base.update(1);
    let mut raw = base.serialize();
    raw[8..16].copy_from_slice(&(MAX_ITEMS_SEEN - 1).to_le_bytes());

    let mut sketch = ReservoirSketch::<i64>::deserialize(&raw).unwrap();
    assert_eq!(sketch.total_weight(), MAX_ITEMS_SEEN - 1);

    sketch.update(2);
    assert_eq!(sketch.total_weight(), MAX_ITEMS_SEEN);
    let snapshot_at_max = sketch.serialize();

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        sketch.update(3);
    }));
    assert!(panicked.is_err());
    assert_eq!(sketch.total_weight(), MAX_ITEMS_SEEN);
    assert_eq!(sketch.serialize(), snapshot_at_max);
}

#[test]
fn debug_works_without_debug_bound_on_item() {
    struct Opaque(#[allow(dead_code)] u64);

    let mut sketch = ReservoirSketch::<Opaque>::new(4).unwrap();
    sketch.update(Opaque(1));
    let rendered = format!("{sketch:?}");
    assert!(rendered.contains("ReservoirSketch"));
    assert!(rendered.contains("total_weight: 1"));
    assert_eq!(sketch.into_iter().count(), 1);
}
