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

use datasketches::common::SearchCriteria;
use datasketches::quantiles::QuantilesSketch;

use super::CountedItem;
use super::count_clones;

#[test]
fn a_smaller_source_lowers_k_only_after_compaction() {
    let mut source = QuantilesSketch::new(2).unwrap();
    for _ in 0..3 {
        source.update(10);
    }
    let mut compacted_source = source.clone();
    compacted_source.update(10);

    for target_weight in [1, 16] {
        let mut target = QuantilesSketch::new(8).unwrap();
        for _ in 0..target_weight {
            target.update(1);
        }

        let mut exact_source_merge = target.clone();
        exact_source_merge.merge(&source);
        assert_eq!(exact_source_merge.k(), 8);
        assert_eq!(exact_source_merge.is_estimation_mode(), target_weight == 16);
        assert_eq!(exact_source_merge.total_weight(), target_weight + 3);
        let source_items = exact_source_merge
            .iter()
            .filter(|&(&item, weight)| item == 10 && weight == 1)
            .count();
        assert_eq!(source_items, 3);

        target.merge(&compacted_source);
        assert_eq!(target.k(), 2);
        assert!(target.is_estimation_mode());
        assert_eq!(target.total_weight(), target_weight + 4);
        assert_eq!(
            target.iter().map(|(_, weight)| weight).sum::<u64>(),
            target_weight + 4
        );
    }
}

#[test]
fn a_larger_source_cannot_raise_k_of_an_exact_target() {
    let mut target = QuantilesSketch::new(2).unwrap();
    target.update(1);
    let mut source = QuantilesSketch::new(8).unwrap();
    for _ in 0..16 {
        source.update(10);
    }

    target.merge(&source);

    assert_eq!(target.k(), 2);
    assert!(target.is_estimation_mode());
    assert_eq!(target.total_weight(), 17);
    assert_eq!(target.num_retained(), 3);
    assert_eq!(target.rank(&1, SearchCriteria::Inclusive), Some(1.0 / 17.0));
    assert_eq!(target.iter().map(|(_, weight)| weight).sum::<u64>(), 17);
}

#[test]
fn merging_different_k_preserves_the_weight_of_separate_value_groups() {
    let mut smaller = QuantilesSketch::new(2).unwrap();
    let mut larger = QuantilesSketch::new(8).unwrap();
    for _ in 0..16 {
        smaller.update(1);
        larger.update(10);
    }

    let mut smaller_target = smaller.clone();
    smaller_target.merge(&larger);
    let mut larger_target = larger;
    larger_target.merge(&smaller);

    // At k = 2 the sorted runs contain 2 ones and 2 tens, all of weight 8.
    // Either sampling offset keeps 1 of each, so both values retain half the total weight.
    for merged in [smaller_target, larger_target] {
        assert_eq!(merged.k(), 2);
        assert_eq!(merged.total_weight(), 32);
        assert_eq!(merged.rank(&1, SearchCriteria::Inclusive), Some(0.5));
    }
}

#[test]
fn downsampling_uses_one_offset_for_the_whole_level() {
    let mut source = QuantilesSketch::new(8).unwrap();
    // Either compaction parity leaves one copy of each value in the source level.
    for item in 0..8 {
        source.update(item);
        source.update(item);
    }
    let mut target = QuantilesSketch::new(2).unwrap();
    target.merge(&source);

    let mut samples: Vec<_> = target
        .iter()
        .map(|(&item, weight)| (item, weight))
        .collect();
    samples.sort_unstable();
    let [(first, first_weight), (second, second_weight)] = samples[..] else {
        panic!("expected two samples, got {samples:?}");
    };
    assert_eq!(second - first, 4);
    assert_eq!((first_weight, second_weight), (8, 8));
}

#[test]
fn downsampling_clones_only_the_samples_it_keeps() {
    let mut source = QuantilesSketch::new(8).unwrap();
    for value in 0..16 {
        source.update(CountedItem(value));
    }
    let mut target = QuantilesSketch::new(2).unwrap();
    let clones = count_clones(|| target.merge(&source));

    // The target retains two samples and stores the two exact extrema separately.
    assert!(clones <= 4, "cloned {clones} items, expected at most 4");
}

#[test]
fn updates_compact_across_occupied_levels_after_merges() {
    let mut target = QuantilesSketch::new(2).unwrap();
    let mut source = QuantilesSketch::new(4).unwrap();
    for value in 0..31 {
        target.update(value);
        source.update(31 + value);
    }

    target.merge(&source);

    assert_eq!(target.total_weight(), 62);
    assert_eq!(target.iter().map(|(_, weight)| weight).sum::<u64>(), 62);
    assert_eq!(target.min_item(), Some(&0));
    assert_eq!(target.max_item(), Some(&61));

    let mut exact = QuantilesSketch::new(8).unwrap();
    exact.update(62);
    target.merge(&exact);
    target.update(63);

    assert_eq!(target.total_weight(), 64);
    let weights: Vec<_> = target.iter().map(|(_, weight)| weight).collect();
    assert_eq!(weights, [32, 32]);
    assert_eq!(target.min_item(), Some(&0));
    assert_eq!(target.max_item(), Some(&63));
}
