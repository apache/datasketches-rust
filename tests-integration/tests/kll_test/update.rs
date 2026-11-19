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

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use datasketches::common::SearchCriteria;
use datasketches::kll::KllSketch;

const DEFAULT_K: u16 = 200;
const MIN_K: u16 = 8;
const NUMERIC_NOISE_TOLERANCE: f64 = 1e-6;

#[test]
fn zero_weight_is_a_no_op() {
    let mut sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    sketch.update_with_weight(10, 0);
    assert!(sketch.is_empty());
    assert_eq!(sketch.total_weight(), 0);
    assert_eq!(sketch.num_retained(), 0);
    assert_eq!(sketch.min_item(), None);
    assert_eq!(sketch.max_item(), None);

    sketch.update_with_weight(10, 5);
    let before = sketch.serialize();
    sketch.update_with_weight(-100, 0);
    sketch.update_with_weight(100, 0);
    assert_eq!(sketch.min_item(), Some(&10));
    assert_eq!(sketch.max_item(), Some(&10));
    assert_eq!(sketch.serialize(), before);
}

#[test]
fn exact_mode_and_capacity_boundary_match_repeated_updates() {
    let mut weighted = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    let mut repeated = KllSketch::<i64>::new(DEFAULT_K).unwrap();

    // Weights 1..=15 sum to 120; adding 80 reaches the exact k=200 capacity boundary.
    for (item, weight) in (1..=15).map(|i| (i, i as u64)).chain([(16, 80)]) {
        weighted.update_with_weight(item, weight);
        for _ in 0..weight {
            repeated.update(item);
        }
    }

    assert_eq!(weighted.total_weight(), DEFAULT_K as u64);
    assert_eq!(weighted.num_retained(), DEFAULT_K as usize);
    assert!(!weighted.is_estimation_mode());
    assert_eq!(weighted.serialize(), repeated.serialize());

    // One additional unit crosses capacity and triggers compaction on the weight == 1 path.
    weighted.update_with_weight(17, 1);
    assert_eq!(weighted.total_weight(), DEFAULT_K as u64 + 1);
    assert!(weighted.is_estimation_mode());
    assert_eq!(weighted.max_item(), Some(&17));
}

#[test]
fn binary_level_decomposition_and_top_level_folding() {
    // Cover both an even weight (leaving level zero empty) and an odd multi-bit weight.
    for weight in [1u64 << 40, (1u64 << 40) + 12_345] {
        let mut sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
        sketch.update_with_weight(7, weight);

        assert_eq!(sketch.total_weight(), weight);
        assert!(sketch.is_estimation_mode());
        assert_eq!(sketch.num_retained(), weight.count_ones() as usize);
        assert_eq!(sketch.sorted_view().total_weight(), weight);
        assert_eq!(sketch.min_item(), Some(&7));
        assert_eq!(sketch.max_item(), Some(&7));
        assert_eq!(sketch.rank(&7, SearchCriteria::Inclusive), Some(1.0));
        assert_eq!(sketch.rank(&7, SearchCriteria::Exclusive), Some(0.0));
        assert_eq!(
            sketch.quantile(0.5, SearchCriteria::Inclusive).unwrap(),
            Some(7)
        );

        let bytes = sketch.serialize();
        let decoded = KllSketch::<i64>::deserialize(&bytes).unwrap();
        assert_eq!(decoded.serialize(), bytes);
    }

    // At MIN_K = 8, folding u64::MAX - 8 places 15 copies at level 60 (above level_capacity = 8)
    // without overflowing MAX_NUM_LEVELS (61).
    let mut sketch = KllSketch::<i64>::new(MIN_K).unwrap();
    for item in 1..=8 {
        sketch.update(item);
    }
    sketch.update_with_weight(9, u64::MAX - 8);

    assert_eq!(sketch.total_weight(), u64::MAX);
    assert_eq!(sketch.sorted_view().total_weight(), u64::MAX);
    assert_eq!(sketch.min_item(), Some(&1));
    assert_eq!(sketch.max_item(), Some(&9));
    assert_eq!(
        sketch.quantile(0.0, SearchCriteria::Inclusive).unwrap(),
        Some(1)
    );
    assert_eq!(
        sketch.quantile(0.5, SearchCriteria::Inclusive).unwrap(),
        Some(9)
    );
    assert_eq!(
        sketch.quantile(1.0, SearchCriteria::Inclusive).unwrap(),
        Some(9)
    );

    let bytes = sketch.serialize();
    let decoded = KllSketch::<i64>::deserialize(&bytes).unwrap();
    assert_eq!(decoded.total_weight(), u64::MAX);
    assert_eq!(decoded.num_retained(), sketch.num_retained());
    assert_eq!(decoded.serialize(), bytes);
}

#[test]
fn merging_sparse_and_top_folded_weighted_sketches() {
    let mut sparse = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    sparse.update_with_weight(100, 1u64 << 40);

    let mut unit = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    for item in 1..=5 {
        unit.update(item);
    }

    let mut unit_into_sparse = sparse.clone();
    unit_into_sparse.merge(&unit);
    let mut sparse_into_unit = unit;
    sparse_into_unit.merge(&sparse);

    let expected_weight = (1u64 << 40) + 5;
    for merged in [&unit_into_sparse, &sparse_into_unit] {
        assert_eq!(merged.total_weight(), expected_weight);
        assert_eq!(merged.sorted_view().total_weight(), expected_weight);
        assert_eq!(merged.num_retained(), 6);
        assert_eq!(merged.min_item(), Some(&1));
        assert_eq!(merged.max_item(), Some(&100));
        assert_eq!(
            merged.rank(&5, SearchCriteria::Inclusive),
            Some(5.0 / expected_weight as f64)
        );
        assert_eq!(
            merged.rank(&100, SearchCriteria::Exclusive),
            Some(5.0 / expected_weight as f64)
        );
        assert_eq!(merged.rank(&100, SearchCriteria::Inclusive), Some(1.0));

        let bytes = merged.serialize();
        let decoded = KllSketch::<i64>::deserialize(&bytes).unwrap();
        assert_eq!(decoded.serialize(), bytes);
    }

    let mut top_left = KllSketch::<i64>::new(MIN_K).unwrap();
    top_left.update_with_weight(1, 7u64 << 60);
    let mut top_right = KllSketch::<i64>::new(MIN_K).unwrap();
    top_right.update_with_weight(2, (8u64 << 60) + 3);
    top_left.merge(&top_right);

    let top_total = (15u64 << 60) + 3;
    assert_eq!(top_left.total_weight(), top_total);
    assert_eq!(top_left.sorted_view().total_weight(), top_total);
    assert_eq!(top_left.num_retained(), 7 + 8 + 2);
    assert_eq!(top_left.min_item(), Some(&1));
    assert_eq!(top_left.max_item(), Some(&2));
    assert_eq!(
        top_left.rank(&1, SearchCriteria::Inclusive),
        Some((7u64 << 60) as f64 / top_total as f64)
    );

    let bytes = top_left.serialize();
    let decoded = KllSketch::<i64>::deserialize(&bytes).unwrap();
    assert_eq!(decoded.serialize(), bytes);
}

#[test]
fn weighted_updates_into_populated_sketch_preserve_disjoint_mass_and_min_k() {
    let mut sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    for item in 0..1_000 {
        sketch.update(item);
    }
    // Every retained item from 0..1_000 lies strictly inside (-1, 2_000), and adding 13 binary
    // level items stays well below the 12-level capacity (636), so no further compaction occurs.
    sketch.update_with_weight(-1, 1_000);
    sketch.update_with_weight(2_000, 3_000);

    assert_eq!(sketch.total_weight(), 5_000);
    assert_eq!(sketch.min_item(), Some(&-1));
    assert_eq!(sketch.max_item(), Some(&2_000));
    assert_eq!(sketch.rank(&-1, SearchCriteria::Inclusive), Some(0.2));
    assert_eq!(sketch.rank(&0, SearchCriteria::Exclusive), Some(0.2));
    assert_eq!(sketch.rank(&2_000, SearchCriteria::Exclusive), Some(0.4));
    assert_eq!(sketch.rank(&2_000, SearchCriteria::Inclusive), Some(1.0));

    let mut lower_k = KllSketch::<i64>::new(64).unwrap();
    for item in 0..1_000 {
        lower_k.update(item);
    }
    sketch.merge(&lower_k);
    assert_eq!(sketch.min_k(), 64);

    sketch.update_with_weight(500, 3);
    sketch.update_with_weight(600, 100_000);
    assert_eq!(sketch.k(), DEFAULT_K);
    assert_eq!(sketch.min_k(), 64);
}

#[test]
fn estimation_mode_stream_preserves_deterministic_invariants() {
    let mut sketch = KllSketch::<i64>::new(32).unwrap();
    let mut expected_total = 0u64;
    for item in 0..500i64 {
        let weight = match item % 4 {
            0 => 1,
            1 => (item as u64 % 7) + 2,
            2 => ((item as u64 % 11) + 1) * 100,
            _ => ((item as u64 % 9) + 1) * 250 + 1,
        };
        sketch.update_with_weight(item, weight);
        expected_total += weight;
    }

    assert!(sketch.is_estimation_mode());
    assert_eq!(sketch.total_weight(), expected_total);
    assert_eq!(sketch.sorted_view().total_weight(), expected_total);
    assert_eq!(sketch.min_item(), Some(&0));
    assert_eq!(sketch.max_item(), Some(&499));

    let split_points: Vec<_> = (25..500).step_by(25).collect();
    for criteria in [SearchCriteria::Inclusive, SearchCriteria::Exclusive] {
        let cdf = sketch.cdf(&split_points, criteria).unwrap().unwrap();
        let pmf = sketch.pmf(&split_points, criteria).unwrap().unwrap();
        let mut previous_rank = 0.0;
        let mut subtotal = 0.0;
        for (index, split_point) in split_points.iter().enumerate() {
            let rank = sketch.rank(split_point, criteria).unwrap();
            assert!(rank >= previous_rank);
            assert!((0.0..=1.0).contains(&rank));
            previous_rank = rank;

            subtotal += pmf[index];
            assert!((cdf[index] - subtotal).abs() <= NUMERIC_NOISE_TOLERANCE);
            assert_eq!(cdf[index], rank);
        }
        assert!((pmf.iter().sum::<f64>() - 1.0).abs() <= NUMERIC_NOISE_TOLERANCE);
    }

    let bytes = sketch.serialize();
    let decoded = KllSketch::<i64>::deserialize(&bytes).unwrap();
    assert_eq!(decoded.total_weight(), sketch.total_weight());
    assert_eq!(decoded.num_retained(), sketch.num_retained());
    assert_eq!(decoded.serialize(), bytes);
}

#[test]
fn owned_string_items_roundtrip_across_update_paths() {
    let mut sketch = KllSketch::<String>::new(DEFAULT_K).unwrap();
    sketch.update_with_weight("ignored".to_owned(), 0);
    sketch.update_with_weight("m".to_owned(), 1);
    sketch.update_with_weight("b".to_owned(), 4);
    sketch.update_with_weight("a".to_owned(), 1_000);
    sketch.update_with_weight("z".to_owned(), 1u64 << 62);

    let expected_total = 1 + 4 + 1_000 + (1u64 << 62);
    assert_eq!(sketch.total_weight(), expected_total);
    assert_eq!(sketch.min_item().map(String::as_str), Some("a"));
    assert_eq!(sketch.max_item().map(String::as_str), Some("z"));

    let bytes = sketch.serialize();
    let decoded = KllSketch::<String>::deserialize(&bytes).unwrap();
    assert_eq!(decoded.total_weight(), expected_total);
    assert_eq!(decoded.min_item().map(String::as_str), Some("a"));
    assert_eq!(decoded.max_item().map(String::as_str), Some("z"));
    assert_eq!(decoded.serialize(), bytes);
}

#[test]
fn weighted_update_overflow_preserves_state() {
    let mut sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    sketch.update_with_weight(10, u64::MAX - 5);
    let before = sketch.serialize();

    for bad_weight in [6, u64::MAX] {
        assert!(
            catch_unwind(AssertUnwindSafe(
                || sketch.update_with_weight(20, bad_weight)
            ))
            .is_err()
        );
        assert_eq!(sketch.serialize(), before, "overflow changed the sketch");
    }

    sketch.update_with_weight(20, 5);
    assert_eq!(sketch.total_weight(), u64::MAX);
    assert_eq!(sketch.max_item(), Some(&20));
    let at_max = sketch.serialize();

    // Zero remains a no-op at u64::MAX; both weight == 1 and weight >= 2 panic unchanged.
    sketch.update_with_weight(30, 0);
    assert_eq!(sketch.serialize(), at_max);
    for bad_weight in [1, 2] {
        assert!(
            catch_unwind(AssertUnwindSafe(
                || sketch.update_with_weight(30, bad_weight)
            ))
            .is_err()
        );
        assert_eq!(sketch.serialize(), at_max, "overflow changed the sketch");
    }
}
