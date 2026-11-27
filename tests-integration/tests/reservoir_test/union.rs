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

use datasketches::error::ErrorKind;
use datasketches::reservoir::ReservoirSketch;
use datasketches::reservoir::ReservoirUnion;

const MAX_ITEMS_SEEN: u64 = 0xFFFF_FFFF_FFFF;

fn populated_sketch(start: i64, count: usize, k: usize) -> ReservoirSketch<i64> {
    let mut sketch = ReservoirSketch::<i64>::new(k).unwrap();
    for offset in 0..count {
        sketch.update(start + offset as i64);
    }
    sketch
}

#[test]
fn union_validates_max_k_and_handles_empty_states() {
    for bad_max_k in [0_usize, 1, (i32::MAX as usize) + 1] {
        let err = ReservoirUnion::<i64>::new(bad_max_k).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidArgument);
    }

    let mut union = ReservoirUnion::<i64>::new(64).unwrap();
    assert!(union.is_empty());
    assert_eq!(union.max_k(), 64);
    assert_eq!(union.estimated_size(), 0);
    assert!(union.to_sketch().is_empty());
    assert_eq!(union.to_sketch().k(), 64);

    // Resetting an already empty union is safe, and merging an empty sketch keeps it empty.
    union.reset();
    let empty_sketch = ReservoirSketch::<i64>::new(16).unwrap();
    union.update(&empty_sketch);
    assert!(union.is_empty());
    assert_eq!(union.serialize().len(), 8);
}

#[test]
fn union_promotes_small_exact_sketch_and_preserves_small_sampling_sketch() {
    let exact_small = populated_sketch(0, 32, 32);
    let mut union = ReservoirUnion::<i64>::new(128).unwrap();
    union.update(&exact_small);
    let promoted = union.to_sketch();
    assert_eq!(promoted.k(), 128);
    assert_eq!(promoted.total_weight(), 32);
    assert_eq!(promoted.num_retained(), 32);

    let sampling_small = populated_sketch(0, 200, 32);
    let mut union2 = ReservoirUnion::<i64>::new(128).unwrap();
    union2.update(&sampling_small);
    let preserved = union2.to_sketch();
    assert_eq!(preserved.k(), 32);
    assert_eq!(preserved.total_weight(), 200);
    assert_eq!(preserved.num_retained(), 32);

    // Resetting restores the full `max_k` budget for subsequent updates.
    union2.reset();
    assert!(union2.is_empty());
    union2.update_value(999);
    assert_eq!(union2.to_sketch().k(), 128);
}

#[test]
fn union_exercises_downsample_standard_swap_and_both_weighted_directions() {
    // 1. Downsampling incoming sketch with k > max_k.
    let big = populated_sketch(0, 2_000, 512);
    let mut union = ReservoirUnion::<i64>::new(128).unwrap();
    union.update(&big);
    assert_eq!(union.to_sketch().k(), 128);
    assert_eq!(union.to_sketch().total_weight(), 2_000);
    assert_eq!(union.to_sketch().num_retained(), 128);

    // 2. Standard merge where gadget is under-full (`uni`) and incoming is in sampling mode
    //    (`gen`).
    let under_full = populated_sketch(0, 40, 128);
    let sampling = populated_sketch(1_000, 500, 128);
    let mut swap_union = ReservoirUnion::<i64>::new(128).unwrap();
    swap_union.update(&under_full);
    swap_union.update(&sampling);
    swap_union.update_value(9_999);
    let res = swap_union.to_sketch();
    assert_eq!(res.k(), 128);
    assert_eq!(res.total_weight(), 541);
    assert_eq!(res.num_retained(), 128);

    // 3. Weighted merge in both orderings (heavy + light, and light + heavy).
    let heavy = populated_sketch(0, 8_000, 128);
    let light = populated_sketch(10_000, 1_000, 128);

    let mut u_hl = ReservoirUnion::<i64>::new(128).unwrap();
    u_hl.update(&heavy);
    u_hl.update(&light);
    let r_hl = u_hl.into_sketch();
    assert_eq!(r_hl.k(), 128);
    assert_eq!(r_hl.total_weight(), 9_000);
    assert_eq!(r_hl.num_retained(), 128);
    assert_eq!(r_hl.iter().copied().collect::<HashSet<_>>().len(), 128);

    let mut u_lh = ReservoirUnion::<i64>::new(128).unwrap();
    u_lh.update(&light);
    u_lh.update(&heavy);
    let r_lh = u_lh.into_sketch();
    assert_eq!(r_lh.k(), 128);
    assert_eq!(r_lh.total_weight(), 9_000);
    assert_eq!(r_lh.num_retained(), 128);
    assert_eq!(r_lh.iter().copied().collect::<HashSet<_>>().len(), 128);
}

#[test]
fn union_weighted_merge_preserves_marginal_probabilities() {
    // Disjoint streams [0..40) (size 40, k=8) and [40..60) (size 20, k=8) merged into max_k=8:
    // Combined stream has 60 items; 2/3 of items are in [0..40), so expected count from [0..40) per
    // merged sample of 8 is 8 * (40 / 60) = 5.3333...
    const TRIALS: usize = 15_000;
    let mut left_count = 0_usize;

    for _ in 0..TRIALS {
        let left = populated_sketch(0, 40, 8);
        let right = populated_sketch(40, 20, 8);
        let mut union = ReservoirUnion::<i64>::new(8).unwrap();
        union.update(&right);
        union.update(&left);
        let merged = union.into_sketch();
        left_count += merged.samples().iter().filter(|&&x| x < 40).count();
    }

    let expected = (TRIALS * 8 * 40) as f64 / 60.0;
    let relative_err = ((left_count as f64) - expected).abs() / expected;
    assert!(
        relative_err < 0.02,
        "left_count={left_count}, expected={expected:.1}, rel_err={relative_err:.4}"
    );
}

#[test]
fn union_overflow_panics_without_corrupting_existing_state() {
    let mut near_max = ReservoirSketch::<i64>::new(2).unwrap();
    near_max.update(1);
    near_max.update(2);
    let mut raw = near_max.serialize();
    raw[8..16].copy_from_slice(&(MAX_ITEMS_SEEN - 1).to_le_bytes());
    let near_max = ReservoirSketch::<i64>::deserialize(&raw).unwrap();

    let mut union = ReservoirUnion::<i64>::new(2).unwrap();
    union.update(&near_max);
    let before = union.serialize();

    let extra = populated_sketch(10, 2, 2);
    let panicked = catch_unwind(AssertUnwindSafe(|| {
        union.update(&extra);
    }));
    assert!(panicked.is_err());
    assert_eq!(union.serialize(), before);
}
