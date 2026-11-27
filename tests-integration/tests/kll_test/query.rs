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
use datasketches::error::ErrorKind;
use datasketches::kll::KllFloat;
use datasketches::kll::KllSketch;

const DEFAULT_K: u16 = 200;
const NUMERIC_NOISE_TOLERANCE: f64 = 1e-6;

#[test]
fn one_weighted_item_can_have_two_distinct_endpoint_representations() {
    let mut sketch = KllSketch::new(8).unwrap();
    sketch.update_with_weight(KllFloat::<f64>::new(-0.0).unwrap(), 256);
    assert_eq!(sketch.num_retained(), 1);

    // The last three doubles are the minimum, the maximum, and the retained item.
    let mut bytes = sketch.serialize();
    let max_offset = bytes.len() - 2 * size_of::<f64>();
    bytes[max_offset..max_offset + size_of::<f64>()].copy_from_slice(&0.0_f64.to_le_bytes());
    let sketch = KllSketch::<KllFloat<f64>>::deserialize(&bytes).unwrap();
    let view = sketch.sorted_view();

    assert_eq!(view.len(), 1);
    assert_eq!(view.total_weight(), 256);
    let ranks = [0.0, 0.5, 1.0];
    let expected_bits = [
        (-0.0_f64).to_bits(),
        (-0.0_f64).to_bits(),
        0.0_f64.to_bits(),
    ];
    for quantiles in [
        sketch.quantiles(&ranks, SearchCriteria::Inclusive),
        view.quantiles(&ranks, SearchCriteria::Inclusive),
    ] {
        let quantiles = quantiles.unwrap().unwrap();
        let bits: Vec<_> = quantiles.iter().map(|item| item.to_bits()).collect();
        assert_eq!(bits, expected_bits);
    }
}

#[test]
fn compacted_extrema_stay_in_query_distribution() {
    let mut sketch = KllSketch::<i64>::new(8).unwrap();
    for item in 0..8 {
        sketch.update(item);
    }
    // Compact an even run with distinct extrema before inserting an interior item.
    // Either sampling parity discards one of the two endpoints.
    sketch.update(3);
    assert!(sketch.is_estimation_mode());
    let view = sketch.sorted_view();
    let weight = sketch.total_weight() as f64;

    assert_eq!(
        sketch
            .quantile(1.0 / weight, SearchCriteria::Inclusive)
            .unwrap(),
        Some(0)
    );
    assert_eq!(
        view.quantile(1.0 / weight, SearchCriteria::Inclusive)
            .unwrap(),
        Some(0)
    );
    assert_eq!(
        sketch
            .quantile(8.5 / weight, SearchCriteria::Inclusive)
            .unwrap(),
        Some(7)
    );
    assert_eq!(
        view.quantile(8.5 / weight, SearchCriteria::Inclusive)
            .unwrap(),
        Some(7)
    );
    assert_eq!(
        sketch
            .quantile(0.5 / weight, SearchCriteria::Exclusive)
            .unwrap(),
        Some(0)
    );
    assert_eq!(
        view.quantile(0.5 / weight, SearchCriteria::Exclusive)
            .unwrap(),
        Some(0)
    );
    assert_eq!(
        sketch
            .quantile(8.0 / weight, SearchCriteria::Exclusive)
            .unwrap(),
        Some(7)
    );
    assert_eq!(
        view.quantile(8.0 / weight, SearchCriteria::Exclusive)
            .unwrap(),
        Some(7)
    );

    assert!(sketch.rank(&0, SearchCriteria::Inclusive).unwrap() >= 1.0 / weight);
    assert!(sketch.rank(&6, SearchCriteria::Inclusive).unwrap() <= 8.0 / weight);
    assert!(sketch.rank(&1, SearchCriteria::Exclusive).unwrap() >= 1.0 / weight);
    assert!(sketch.rank(&7, SearchCriteria::Exclusive).unwrap() <= 8.0 / weight);

    for criteria in [SearchCriteria::Inclusive, SearchCriteria::Exclusive] {
        for item in -1..=8 {
            assert_eq!(sketch.rank(&item, criteria), view.rank(&item, criteria));
        }
        assert_eq!(
            sketch.cdf(&[0, 3, 6, 7], criteria).unwrap(),
            view.cdf(&[0, 3, 6, 7], criteria).unwrap()
        );
    }
}

#[test]
fn missing_extrema_preserve_weights_and_rank_quantile_consistency() {
    // Mirrors the `IncludeMinMax` / `quantiles_sorted_view` reference fixture:
    // level 1 holds `[2, 4, 6, 7]` (weight 2 each, total 8) with stream extrema `1` and `8`,
    // yielding cumulative distribution `[(1, 1), (2, 2), (4, 4), (6, 6), (7, 7), (8, 8)]`.
    let sketch = decode_kll_i64(1, 8, &[&[], &[2, 4, 6, 7]]);
    let view = sketch.sorted_view();
    assert_eq!(view.len(), 6);
    assert_eq!(view.total_weight(), 8);

    let expected_inclusive = [
        (0, 0.0),
        (1, 0.125),
        (2, 0.25),
        (4, 0.5),
        (6, 0.75),
        (7, 0.875),
        (8, 1.0),
    ];
    for (item, expected_rank) in expected_inclusive {
        assert_eq!(
            sketch.rank(&item, SearchCriteria::Inclusive),
            Some(expected_rank)
        );
        assert_eq!(
            view.rank(&item, SearchCriteria::Inclusive),
            Some(expected_rank)
        );
    }
    let expected_exclusive = [
        (1, 0.0),
        (2, 0.125),
        (4, 0.25),
        (6, 0.5),
        (7, 0.75),
        (8, 0.875),
        (9, 1.0),
    ];
    for (item, expected_rank) in expected_exclusive {
        assert_eq!(
            sketch.rank(&item, SearchCriteria::Exclusive),
            Some(expected_rank)
        );
        assert_eq!(
            view.rank(&item, SearchCriteria::Exclusive),
            Some(expected_rank)
        );
    }
    assert_eq!(
        sketch
            .quantiles(&[0.125, 0.25, 0.875, 0.9], SearchCriteria::Inclusive)
            .unwrap(),
        Some(vec![1, 2, 7, 8])
    );
    assert_eq!(
        sketch
            .quantiles(&[0.05, 0.125, 0.75, 0.875], SearchCriteria::Exclusive)
            .unwrap(),
        Some(vec![1, 2, 7, 8])
    );

    // Unit-weight level-0 neighbors (`2` and `6`) give their sole weight to `1` and `8`,
    // single higher-level item (`[5]` at weight 2) splits across `1` and `8`, and a single-level
    // image (`[5, 5]`) keeps `sketch.rank` aligned with `SortedView`.
    for levels in [
        &[&[2i64, 6][..], &[4][..]][..],
        &[&[][..], &[5][..]][..],
        &[&[5, 5][..]][..],
    ] {
        let sketch = decode_kll_i64(1, 8, levels);
        let view = sketch.sorted_view();
        assert_eq!(view.total_weight(), sketch.total_weight());
        assert!(view.len() <= view.total_weight() as usize);

        for criteria in [SearchCriteria::Inclusive, SearchCriteria::Exclusive] {
            for item in 0..=9 {
                assert_eq!(sketch.rank(&item, criteria), view.rank(&item, criteria));
            }
            let cdf = sketch.cdf(&[1, 4, 8], criteria).unwrap().unwrap();
            assert_eq!(cdf[0], sketch.rank(&1, criteria).unwrap());
            assert_eq!(cdf[1], sketch.rank(&4, criteria).unwrap());
            assert_eq!(cdf[2], sketch.rank(&8, criteria).unwrap());
        }
        assert_eq!(
            sketch
                .quantile(
                    1.0 / sketch.total_weight() as f64,
                    SearchCriteria::Inclusive
                )
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            sketch
                .quantile(
                    (sketch.total_weight() as f64 - 0.5) / sketch.total_weight() as f64,
                    SearchCriteria::Inclusive,
                )
                .unwrap(),
            Some(8)
        );
    }
}

fn decode_kll_i64(min_item: i64, max_item: i64, levels: &[&[i64]]) -> KllSketch<i64> {
    const K: u16 = 8;
    const M: u8 = 8;
    const PREAMBLE_INTS_FULL: u8 = 5;
    const SERIAL_VERSION_1: u8 = 1;
    const KLL_FAMILY_ID: u8 = 15;
    const FLAG_LEVEL_ZERO_SORTED: u8 = 1 << 1;

    let num_levels = levels.len();
    let capacity = (K as u32) * (num_levels as u32);
    let num_retained: u32 = levels.iter().map(|level| level.len() as u32).sum();
    let total_weight: u64 = levels
        .iter()
        .enumerate()
        .map(|(level, items)| (items.len() as u64) << level)
        .sum();

    let mut bytes = Vec::new();
    bytes.extend_from_slice(&[
        PREAMBLE_INTS_FULL,
        SERIAL_VERSION_1,
        KLL_FAMILY_ID,
        FLAG_LEVEL_ZERO_SORTED,
    ]);
    bytes.extend_from_slice(&K.to_le_bytes());
    bytes.extend_from_slice(&[M, 0]);
    bytes.extend_from_slice(&total_weight.to_le_bytes());
    bytes.extend_from_slice(&K.to_le_bytes());
    bytes.extend_from_slice(&[num_levels as u8, 0]);

    let mut offset = capacity - num_retained;
    for level in levels {
        bytes.extend_from_slice(&offset.to_le_bytes());
        offset += level.len() as u32;
    }
    bytes.extend_from_slice(&min_item.to_le_bytes());
    bytes.extend_from_slice(&max_item.to_le_bytes());
    for level in levels {
        for &item in *level {
            bytes.extend_from_slice(&item.to_le_bytes());
        }
    }

    KllSketch::<i64>::deserialize(&bytes).unwrap()
}

#[test]
fn quantile_endpoints_preserve_extrema_after_compaction() {
    let mut sketch = KllSketch::<i64>::new(8).unwrap();
    for item in 0..8 {
        sketch.update(item);
    }
    // Compact an even run with distinct extrema before inserting an interior item.
    // Either sampling parity discards one of the two endpoints.
    sketch.update(3);
    assert!(sketch.is_estimation_mode());
    let view = sketch.sorted_view();

    for criteria in [SearchCriteria::Inclusive, SearchCriteria::Exclusive] {
        assert_eq!(sketch.quantile(0.0, criteria).unwrap(), Some(0));
        assert_eq!(sketch.quantile(1.0, criteria).unwrap(), Some(7));
        assert_eq!(
            sketch.quantiles(&[0.0, 1.0], criteria).unwrap(),
            Some(vec![0, 7])
        );
    }

    sketch.reset();
    for criteria in [SearchCriteria::Inclusive, SearchCriteria::Exclusive] {
        assert_eq!(sketch.quantile(0.0, criteria).unwrap(), None);
        assert_eq!(sketch.quantile(1.0, criteria).unwrap(), None);
        assert_eq!(view.quantile(0.0, criteria).unwrap(), Some(0));
        assert_eq!(view.quantile(1.0, criteria).unwrap(), Some(7));
        assert_eq!(
            view.quantiles(&[0.0, 1.0], criteria).unwrap(),
            Some(vec![0, 7])
        );
    }
}

#[test]
fn quantile_endpoints_with_large_stream_weight() {
    let mut sketch = KllSketch::<i64>::new(8).unwrap();
    sketch.update(0);
    for _ in 0..53 {
        sketch.merge(&sketch.clone());
    }
    sketch.update(1);
    assert_eq!(sketch.total_weight(), (1 << 53) + 1);
    let view = sketch.sorted_view();

    for criteria in [SearchCriteria::Inclusive, SearchCriteria::Exclusive] {
        assert_eq!(sketch.quantile(0.0, criteria).unwrap(), Some(0));
        assert_eq!(sketch.quantile(1.0, criteria).unwrap(), Some(1));
        assert_eq!(view.quantile(0.0, criteria).unwrap(), Some(0));
        assert_eq!(view.quantile(1.0, criteria).unwrap(), Some(1));
    }
}

#[test]
fn valid_queries_on_empty_sketch_return_no_result() {
    let sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    let view = sketch.sorted_view();
    for criteria in [SearchCriteria::Inclusive, SearchCriteria::Exclusive] {
        assert_eq!(sketch.rank(&0, criteria), None);
        assert_eq!(view.rank(&0, criteria), None);
        for rank in [0.0, 0.5, 1.0] {
            assert_eq!(sketch.quantile(rank, criteria).unwrap(), None);
            assert_eq!(view.quantile(rank, criteria).unwrap(), None);
        }
        assert_eq!(sketch.quantiles(&[0.0, 0.5, 1.0], criteria).unwrap(), None);
        assert_eq!(view.quantiles(&[0.0, 0.5, 1.0], criteria).unwrap(), None);
        assert_eq!(sketch.cdf(&[0], criteria).unwrap(), None);
        assert_eq!(sketch.pmf(&[0], criteria).unwrap(), None);
        assert_eq!(view.cdf(&[0], criteria).unwrap(), None);
        assert_eq!(view.pmf(&[0], criteria).unwrap(), None);
    }
}

#[test]
fn empty_argument_slices_preserve_the_sketch_empty_state() {
    for n in [0, 1] {
        let mut sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
        for item in 0..n {
            sketch.update(item);
        }
        let view = sketch.sorted_view();
        let quantiles = (n != 0).then(Vec::new);
        let distribution = (n != 0).then(|| vec![1.0]);
        for criteria in [SearchCriteria::Inclusive, SearchCriteria::Exclusive] {
            assert_eq!(sketch.quantiles(&[], criteria).unwrap(), quantiles);
            assert_eq!(view.quantiles(&[], criteria).unwrap(), quantiles);
            assert_eq!(sketch.cdf(&[], criteria).unwrap(), distribution);
            assert_eq!(sketch.pmf(&[], criteria).unwrap(), distribution);
            assert_eq!(view.cdf(&[], criteria).unwrap(), distribution);
            assert_eq!(view.pmf(&[], criteria).unwrap(), distribution);
        }
    }
}

#[test]
fn invalid_arguments_are_rejected_regardless_of_sketch_state() {
    for n in [0, 1, 1_000] {
        let mut sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
        for item in 0..n {
            sketch.update(item);
        }
        let view = sketch.sorted_view();
        for criteria in [SearchCriteria::Inclusive, SearchCriteria::Exclusive] {
            for rank in [f64::NAN, f64::NEG_INFINITY, -0.1, 1.1, f64::INFINITY] {
                for error in [
                    sketch.quantile(rank, criteria).unwrap_err(),
                    view.quantile(rank, criteria).unwrap_err(),
                    sketch.quantiles(&[0.5, rank], criteria).unwrap_err(),
                    view.quantiles(&[0.5, rank], criteria).unwrap_err(),
                ] {
                    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
                }
            }
            for splits in [[1, 1], [2, 1]] {
                for error in [
                    sketch.cdf(&splits, criteria).unwrap_err(),
                    sketch.pmf(&splits, criteria).unwrap_err(),
                    view.cdf(&splits, criteria).unwrap_err(),
                    view.pmf(&splits, criteria).unwrap_err(),
                ] {
                    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
                }
            }
        }
    }
}

#[test]
fn inclusive_and_exclusive_semantics_cover_duplicates() {
    let mut sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    for item in [1, 1, 2, 2] {
        sketch.update(item);
    }

    assert_eq!(sketch.rank(&1, SearchCriteria::Exclusive).unwrap(), 0.0);
    assert_eq!(sketch.rank(&1, SearchCriteria::Inclusive).unwrap(), 0.5);
    assert_eq!(sketch.rank(&2, SearchCriteria::Exclusive).unwrap(), 0.5);
    assert_eq!(sketch.rank(&2, SearchCriteria::Inclusive).unwrap(), 1.0);
    assert_eq!(
        sketch.quantile(0.5, SearchCriteria::Inclusive).unwrap(),
        Some(1)
    );
    assert_eq!(
        sketch.quantile(0.5, SearchCriteria::Exclusive).unwrap(),
        Some(2)
    );

    // Equal values make the expected ranks independent of compaction sampling.
    let mut compacted = KllSketch::<i64>::new(8).unwrap();
    for _ in 0..9 {
        compacted.update(1);
    }
    assert!(compacted.is_estimation_mode());
    assert_eq!(compacted.rank(&1, SearchCriteria::Exclusive), Some(0.0));
    assert_eq!(compacted.rank(&1, SearchCriteria::Inclusive), Some(1.0));
}

#[test]
fn exact_mode_queries_match_the_stream() {
    let mut sketch = KllSketch::<i64>::new(DEFAULT_K).unwrap();
    for item in 1..=100 {
        sketch.update(item);
    }

    assert_eq!(
        sketch.quantile(0.0, SearchCriteria::Inclusive).unwrap(),
        Some(1)
    );
    assert_eq!(
        sketch.quantile(0.5, SearchCriteria::Inclusive).unwrap(),
        Some(50)
    );
    assert_eq!(
        sketch.quantile(1.0, SearchCriteria::Inclusive).unwrap(),
        Some(100)
    );
    for item in 1..=100 {
        assert_eq!(
            sketch.rank(&item, SearchCriteria::Inclusive).unwrap(),
            item as f64 / 100.0
        );
    }
}

#[test]
fn estimation_mode_queries_preserve_deterministic_invariants() {
    let mut sketch = KllSketch::<i64>::new(64).unwrap();
    for item in 0..10_000 {
        sketch.update(item);
    }

    let mut previous_rank = 0.0;
    for item in (0..10_000).step_by(100) {
        let rank = sketch.rank(&item, SearchCriteria::Inclusive).unwrap();
        assert!(rank >= previous_rank);
        assert!((0.0..=1.0).contains(&rank));
        previous_rank = rank;
    }
    assert_eq!(sketch.min_item(), Some(&0));
    assert_eq!(sketch.max_item(), Some(&9_999));
    assert!(sketch.normalized_rank_error() < sketch.normalized_pmf_error());
}

#[test]
fn rank_cdf_and_pmf_are_consistent() {
    let mut sketch = KllSketch::<i64>::new(64).unwrap();
    for item in 0..10_000 {
        sketch.update(item);
    }
    let split_points: Vec<_> = (100..10_000).step_by(100).collect();

    for criteria in [SearchCriteria::Inclusive, SearchCriteria::Exclusive] {
        let cdf = sketch.cdf(&split_points, criteria).unwrap().unwrap();
        let pmf = sketch.pmf(&split_points, criteria).unwrap().unwrap();
        let mut subtotal = 0.0;
        for (index, split_point) in split_points.iter().enumerate() {
            subtotal += pmf[index];
            assert!((cdf[index] - subtotal).abs() <= NUMERIC_NOISE_TOLERANCE);
            assert_eq!(cdf[index], sketch.rank(split_point, criteria).unwrap());
        }
        assert!((pmf.iter().sum::<f64>() - 1.0).abs() <= NUMERIC_NOISE_TOLERANCE);
    }
}

#[test]
fn sorted_view_supports_repeated_and_batch_queries() {
    let mut sketch = KllSketch::<i64>::new(64).unwrap();
    for item in 0..1_000 {
        sketch.update(item);
    }
    let view = sketch.sorted_view();
    let ranks = [0.0, 0.25, 0.5, 0.75, 1.0];
    let quantiles = sketch
        .quantiles(&ranks, SearchCriteria::Inclusive)
        .unwrap()
        .unwrap();

    assert!((sketch.num_retained()..=sketch.num_retained() + 2).contains(&view.len()));
    assert_eq!(view.total_weight(), sketch.total_weight());
    assert_eq!(
        view.quantiles(&ranks, SearchCriteria::Inclusive)
            .unwrap()
            .unwrap(),
        quantiles
    );
    for (&rank, quantile) in ranks.iter().zip(&quantiles) {
        assert_eq!(
            view.quantile(rank, SearchCriteria::Inclusive).unwrap(),
            Some(*quantile)
        );
        assert_eq!(
            view.rank(quantile, SearchCriteria::Inclusive).unwrap(),
            sketch.rank(quantile, SearchCriteria::Inclusive).unwrap()
        );
    }

    sketch.update(2_000);
    assert_eq!(view.total_weight(), 1_000);
    assert_eq!(
        view.quantile(1.0, SearchCriteria::Inclusive).unwrap(),
        Some(999)
    );
}
