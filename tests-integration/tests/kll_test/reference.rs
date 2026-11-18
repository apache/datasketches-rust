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

//! KLL queries checked against a reference that rebuilds the sorted view from the serialized
//! levels, and update/merge invariants for long streams.
//!
//! KLL compaction draws random bits, so sketches differ between runs; every check therefore
//! derives its expectation from the sketch under test rather than from fixed values.

use std::cmp::Ordering;

use datasketches::common::SearchCriteria;
use datasketches::kll::KllFloat;
use datasketches::kll::KllSketch;

fn splitmix(i: u64) -> u64 {
    let mut z = i.wrapping_add(0x9e3779b97f4a7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

/// A float wrapper ordered like `KllFloat<f64>`: `-0.0` and `0.0` compare equal but keep their
/// bits.
#[derive(Clone, Copy, PartialEq)]
struct Item(f64);

impl Eq for Item {}

impl PartialOrd for Item {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Item {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap()
    }
}

#[derive(Clone)]
struct Entry {
    item: Item,
    cumulative_weight: u64,
}

/// Reads the levels of a serialized `KllSketch<KllFloat<f64>>` in memory order.
fn serialized_levels(bytes: &[u8]) -> Vec<Vec<f64>> {
    let read_u32 = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
    let read_f64 = |at: usize| f64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
    assert_eq!(
        bytes[0], 5,
        "expected the full preamble of a multi-item sketch"
    );
    let level_zero_sorted = bytes[3] & 2 != 0;
    let num_levels = usize::from(bytes[18]);
    let offsets: Vec<usize> = (0..num_levels)
        .map(|i| read_u32(20 + 4 * i) as usize)
        .collect();
    // Level offsets are followed by the minimum and maximum items.
    let mut at = 20 + 4 * num_levels + 16;
    let retained = (bytes.len() - at) / 8;
    let mut levels = vec![];
    for level in 0..num_levels {
        let len = if level + 1 < num_levels {
            offsets[level + 1] - offsets[level]
        } else {
            retained - (offsets[level] - offsets[0])
        };
        let mut items: Vec<f64> = (0..len).map(|i| read_f64(at + 8 * i)).collect();
        if level == 0 && !level_zero_sorted {
            // An unsorted level zero is written in reverse memory order.
            items.reverse();
        }
        levels.push(items);
        at += 8 * len;
    }
    assert_eq!(at, bytes.len());
    levels
}

/// Builds the sorted view the way the original implementation did: sort level zero, then merge
/// the levels pairwise, taking the left entry on ties.
fn reference_view(levels: &[Vec<f64>], level_zero_sorted: bool) -> Vec<Entry> {
    let mut runs: Vec<Vec<Entry>> = vec![];
    for (level_index, level) in levels.iter().enumerate() {
        let mut run: Vec<Entry> = level
            .iter()
            .map(|&item| Entry {
                item: Item(item),
                cumulative_weight: 1 << level_index,
            })
            .collect();
        if level_index == 0 && !level_zero_sorted {
            // Mirror the library call so that equal items end up in the same order.
            #[allow(clippy::unnecessary_sort_by)]
            run.sort_unstable_by(|left, right| left.item.cmp(&right.item));
        }
        if !run.is_empty() {
            runs.push(run);
        }
    }
    while runs.len() > 1 {
        let mut merged_runs = vec![];
        let mut iter = runs.into_iter();
        while let Some(left) = iter.next() {
            match iter.next() {
                Some(right) => {
                    let mut merged = Vec::with_capacity(left.len() + right.len());
                    let (mut i, mut j) = (0, 0);
                    while i < left.len() && j < right.len() {
                        if left[i].item > right[j].item {
                            merged.push(right[j].clone());
                            j += 1;
                        } else {
                            merged.push(left[i].clone());
                            i += 1;
                        }
                    }
                    merged.extend_from_slice(&left[i..]);
                    merged.extend_from_slice(&right[j..]);
                    merged_runs.push(merged);
                }
                None => merged_runs.push(left),
            }
        }
        runs = merged_runs;
    }
    let mut entries = runs.pop().unwrap_or_default();
    let mut total = 0;
    for entry in &mut entries {
        total += entry.cumulative_weight;
        entry.cumulative_weight = total;
    }
    entries
}

/// Inclusive quantile from the reference view (the smallest item whose cumulative weight
/// reaches `ceil(rank * n)`).
fn reference_quantile(view: &[Entry], rank: f64) -> f64 {
    let total = view.last().unwrap().cumulative_weight;
    let weight = (rank * total as f64).ceil() as u64;
    let index = view.partition_point(|entry| entry.cumulative_weight < weight);
    view[index.min(view.len() - 1)].item.0
}

fn check_view_matches_reference(sketch: &KllSketch<KllFloat<f64>>) {
    let bytes = sketch.serialize();
    let levels = serialized_levels(&bytes);
    let view = reference_view(&levels, bytes[3] & 2 != 0);
    assert_eq!(view.last().unwrap().cumulative_weight, sketch.n());

    let sorted_view = sketch.sorted_view();
    assert_eq!(sorted_view.len(), view.len());
    for step in 1..200 {
        let rank = f64::from(step) / 200.0;
        let expected = reference_quantile(&view, rank);
        for actual in [
            sketch
                .quantile(rank, SearchCriteria::Inclusive)
                .unwrap()
                .unwrap(),
            sorted_view
                .quantile(rank, SearchCriteria::Inclusive)
                .unwrap()
                .unwrap(),
        ] {
            assert_eq!(
                actual.into_inner().to_bits(),
                expected.to_bits(),
                "rank {rank}"
            );
        }
    }
    for entry in view.iter().step_by(53) {
        let item = KllFloat::<f64>::new(entry.item.0).unwrap();
        let expected = sorted_view.rank(&item, SearchCriteria::Inclusive).unwrap();
        assert_eq!(
            sketch.rank(&item, SearchCriteria::Inclusive).unwrap(),
            expected
        );
    }
}

fn value(i: u64, distinct: u64) -> f64 {
    match splitmix(i) % distinct {
        // Signed zeros compare equal, so their order in the view is observable.
        0 => -0.0,
        1 => 0.0,
        v => (v as f64 - distinct as f64 / 2.0) / 7.0,
    }
}

#[test]
fn sorted_view_matches_pairwise_merge_reference() {
    for &(k, n, distinct) in &[
        (8, 5_000, 4),
        (8, 20_000, 1_000_000),
        (20, 3_000, 3),
        (200, 1_000, 50),
        (200, 100_000, 64),
        (200, 100_000, 1 << 40),
        (1_000, 50_000, 12),
    ] {
        let mut sketch = KllSketch::<KllFloat<f64>>::new(k).unwrap();
        for i in 0..n {
            sketch.update(KllFloat::<f64>::new(value(i, distinct)).unwrap());
        }
        check_view_matches_reference(&sketch);

        // A sorted level zero is merged without the initial sort.
        let restored = KllSketch::<KllFloat<f64>>::deserialize(&sketch.serialize()).unwrap();
        check_view_matches_reference(&restored);
    }
}

#[test]
fn merged_sketches_match_reference_and_invariants() {
    for &k in &[8, 50, 200] {
        let mut merged = KllSketch::<KllFloat<f64>>::new(k).unwrap();
        let mut total = 0;
        for part in 0..12u64 {
            let mut sketch = KllSketch::<KllFloat<f64>>::new(k).unwrap();
            let len = 1 + splitmix(part) % 30_000;
            for i in 0..len {
                sketch.update(KllFloat::<f64>::new(value(part * 1_000_003 + i, 500)).unwrap());
            }
            total += len;
            merged.merge(&sketch).unwrap();
            assert_eq!(merged.n(), total);
            check_view_matches_reference(&merged);
        }
    }
}

#[test]
fn long_streams_keep_invariants() {
    for &k in &[8, 33, 200, 1_000] {
        let mut sketch = KllSketch::<KllFloat<f64>>::new(k).unwrap();
        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;
        for i in 0..100_000u64 {
            let v = (splitmix(i) >> 11) as f64;
            min = min.min(v);
            max = max.max(v);
            sketch.update(KllFloat::<f64>::new(v).unwrap());
            if i % 24_999 == 0 {
                let bytes = sketch.serialize();
                // Deserialization validates level order, weights, and capacity.
                let restored = KllSketch::<KllFloat<f64>>::deserialize(&bytes).unwrap();
                assert_eq!(restored.serialize(), bytes);
                assert_eq!(restored.n(), i + 1);
            }
        }
        assert_eq!(sketch.min_item().unwrap().into_inner(), min);
        assert_eq!(sketch.max_item().unwrap().into_inner(), max);
        assert!(sketch.is_estimation_mode());
        // The retained count stays within the configured accuracy budget.
        let levels = serialized_levels(&sketch.serialize());
        let retained: usize = levels.iter().map(Vec::len).sum();
        assert_eq!(retained, sketch.num_retained());
        let weight: u64 = levels
            .iter()
            .enumerate()
            .map(|(level, items)| (items.len() as u64) << level)
            .sum();
        assert_eq!(weight, sketch.n());
        for level in &levels[1..] {
            assert!(level.is_sorted());
        }
        // Ranks of uniform values stay within the normalized rank error bound (with margin).
        let epsilon = sketch.normalized_rank_error() * 2.0;
        for q in 1..20 {
            let rank = f64::from(q) / 20.0;
            let item = sketch
                .quantile(rank, SearchCriteria::Inclusive)
                .unwrap()
                .unwrap();
            let true_rank = (item.into_inner() - min) / (max - min);
            assert!(
                (true_rank - rank).abs() < epsilon + 0.01,
                "k {k} rank {rank}"
            );
        }
    }
}

/// Deserialization rejects a retained item outside the serialized minimum and maximum and names
/// the first such item in level order, both in the sorted levels above level zero and in an
/// unsorted level zero.
#[test]
fn out_of_range_items_are_reported_at_the_first_offender() {
    for &(k, n) in &[(8, 2_000), (20, 777), (200, 150), (200, 20_000)] {
        let mut sketch = KllSketch::<KllFloat<f64>>::new(k).unwrap();
        for i in 0..n {
            sketch.update(KllFloat::<f64>::new(value(i, 1_000_000)).unwrap());
        }
        let bytes = sketch.serialize();
        let levels = serialized_levels(&bytes);
        let min_at = 20 + 4 * usize::from(bytes[18]);
        let mut retained: Vec<f64> = levels.iter().flatten().copied().collect();
        retained.sort_by(f64::total_cmp);
        for cut in [1, retained.len() / 3, retained.len() - 1] {
            for raise_min in [true, false] {
                let (low, high) = if raise_min {
                    (retained[cut], retained[retained.len() - 1])
                } else {
                    (retained[0], retained[retained.len() - 1 - cut])
                };
                let mut patched = bytes.clone();
                patched[min_at..min_at + 8].copy_from_slice(&low.to_le_bytes());
                patched[min_at + 8..min_at + 16].copy_from_slice(&high.to_le_bytes());
                let expected = levels.iter().enumerate().find_map(|(level, items)| {
                    items
                        .iter()
                        .position(|&v| v < low || v > high)
                        .map(|index| (level, index))
                });
                let Some((level, index)) = expected else {
                    continue;
                };
                let error = KllSketch::<KllFloat<f64>>::deserialize(&patched)
                    .expect_err("an item outside the bounds must be rejected")
                    .to_string();
                assert!(
                    error.contains(&format!(
                        "retained item at level {level}, index {index} is outside the serialized minimum and maximum"
                    )),
                    "k={k} n={n} cut={cut} raise_min={raise_min}: {error}"
                );
            }
        }
    }
}
