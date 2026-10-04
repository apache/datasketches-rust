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

use std::mem::size_of;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use datasketches::tdigest::TDigestMut;
use googletest::assert_that;
use googletest::prelude::eq;
use googletest::prelude::is_finite;
use googletest::prelude::near;

#[test]
fn test_empty() {
    let mut tdigest = TDigestMut::new(10).unwrap();
    assert!(tdigest.is_empty());
    assert_eq!(tdigest.k(), 10);
    assert_eq!(tdigest.total_weight(), 0);
    assert_eq!(tdigest.min_value(), None);
    assert_eq!(tdigest.max_value(), None);
    assert_eq!(tdigest.rank(0.0), None);
    assert_eq!(tdigest.quantile(0.5), None);
    assert_eq!(tdigest.quantiles(&[0.0, 0.5, 1.0]), None);
    assert_eq!(tdigest.quantiles(&[]), None);

    let split_points = [0.0];
    assert_eq!(tdigest.pmf(&split_points), None);
    assert_eq!(tdigest.cdf(&split_points), None);

    let tdigest = TDigestMut::new(10).unwrap().freeze();
    assert!(tdigest.is_empty());
    assert_eq!(tdigest.k(), 10);
    assert_eq!(tdigest.total_weight(), 0);
    assert_eq!(tdigest.min_value(), None);
    assert_eq!(tdigest.max_value(), None);
    assert_eq!(tdigest.rank(0.0), None);
    assert_eq!(tdigest.quantile(0.5), None);
    assert_eq!(tdigest.quantiles(&[0.0, 0.5, 1.0]), None);
    assert_eq!(tdigest.quantiles(&[]), None);

    let split_points = [0.0];
    assert_eq!(tdigest.pmf(&split_points), None);
    assert_eq!(tdigest.cdf(&split_points), None);
}

#[test]
fn test_one_value() {
    let mut tdigest = TDigestMut::new(100).unwrap();
    tdigest.update(1.0);
    assert_eq!(tdigest.k(), 100);
    assert_eq!(tdigest.total_weight(), 1);
    assert_eq!(tdigest.min_value(), Some(1.0));
    assert_eq!(tdigest.max_value(), Some(1.0));
    assert_eq!(tdigest.rank(0.99), Some(0.0));
    assert_eq!(tdigest.rank(1.0), Some(0.5));
    assert_eq!(tdigest.rank(1.01), Some(1.0));
    assert_eq!(tdigest.quantile(0.0), Some(1.0));
    assert_eq!(tdigest.quantile(0.5), Some(1.0));
    assert_eq!(tdigest.quantile(1.0), Some(1.0));
    let ranks = [1.0, 0.5, 0.0, 0.5];
    assert_eq!(tdigest.quantiles(&ranks), Some(vec![1.0; ranks.len()]));
    assert_eq!(
        tdigest.freeze().quantiles(&ranks),
        Some(vec![1.0; ranks.len()])
    );
}

#[test]
fn test_empty_split_points_define_one_bin() {
    let mut tdigest = TDigestMut::new(100).unwrap();
    tdigest.update(1.0);

    assert_eq!(tdigest.cdf(&[]), Some(vec![1.0]));
    assert_eq!(tdigest.pmf(&[]), Some(vec![1.0]));

    let tdigest = tdigest.freeze();
    assert_eq!(tdigest.cdf(&[]), Some(vec![1.0]));
    assert_eq!(tdigest.pmf(&[]), Some(vec![1.0]));
}

#[test]
fn test_maximum_k() {
    let mut tdigest = TDigestMut::new(u16::MAX).unwrap();
    tdigest.update(1.0);

    let tdigest = tdigest.freeze();
    assert_eq!(tdigest.k(), u16::MAX);
    assert_eq!(tdigest.quantile(0.5), Some(1.0));
}

#[test]
fn test_estimated_size_reuses_buffer_after_compression() {
    const K: u16 = 200;
    const TARGET_CENTROIDS: usize = 410;
    const MAX_UNMERGED: usize = TARGET_CENTROIDS * 4;

    let inline_size = size_of::<TDigestMut>();
    let mut tdigest = TDigestMut::new(K).unwrap();
    assert_eq!(tdigest.estimated_size(), inline_size);

    for value in 0..MAX_UNMERGED {
        tdigest.update(value as f64);
    }
    let size_before_compression = tdigest.estimated_size();
    assert!(size_before_compression > inline_size);
    tdigest.rank(0.5);
    assert!(tdigest.estimated_size() <= size_before_compression);

    for value in MAX_UNMERGED..10_000 {
        tdigest.update(value as f64);
    }
    let size_before_compression = tdigest.estimated_size();
    tdigest.rank(0.5);
    assert!(tdigest.estimated_size() <= size_before_compression);

    let mut left = TDigestMut::new(K).unwrap();
    for value in 0..8 {
        left.update(value as f64);
    }
    let mut right = TDigestMut::new(K).unwrap();
    for value in 0..MAX_UNMERGED {
        right.update(value as f64);
    }
    let right_size = right.estimated_size();
    left.merge(&right);
    assert_eq!(left.total_weight(), (MAX_UNMERGED + 8) as u64);
    assert_eq!(right.total_weight(), MAX_UNMERGED as u64);
    assert_eq!(right.estimated_size(), right_size);

    let mut full_left = TDigestMut::new(K).unwrap();
    for value in 0..MAX_UNMERGED {
        full_left.update(value as f64);
    }
    let combined_size = full_left.estimated_size() + right_size;
    full_left.merge(&right);
    assert_eq!(full_left.total_weight(), (MAX_UNMERGED * 2) as u64);
    assert!(full_left.estimated_size() <= combined_size + combined_size / 2);

    let mutable_size = full_left.estimated_size();
    let frozen = full_left.freeze();
    assert!(frozen.estimated_size() <= mutable_size);
}

#[test]
fn test_many_values() {
    let n = 10000;

    let mut tdigest = TDigestMut::default();
    for i in 0..n {
        tdigest.update(i as f64);
    }

    assert!(!tdigest.is_empty());
    assert_eq!(tdigest.total_weight(), n);
    assert_eq!(tdigest.min_value(), Some(0.0));
    assert_eq!(tdigest.max_value(), Some((n - 1) as f64));

    assert_that!(tdigest.rank(0.0).unwrap(), near(0.0, 0.0001));
    assert_that!(tdigest.rank((n / 4) as f64).unwrap(), near(0.25, 0.0001));
    assert_that!(tdigest.rank((n / 2) as f64).unwrap(), near(0.5, 0.0001));
    assert_that!(
        tdigest.rank((n * 3 / 4) as f64).unwrap(),
        near(0.75, 0.0001)
    );
    assert_that!(tdigest.rank(n as f64).unwrap(), eq(1.0));
    assert_that!(tdigest.quantile(0.0).unwrap(), eq(0.0));
    assert_that!(
        tdigest.quantile(0.5).unwrap(),
        near((n / 2) as f64, 0.03 * (n / 2) as f64)
    );
    assert_that!(
        tdigest.quantile(0.9).unwrap(),
        near((n as f64) * 0.9, 0.01 * (n as f64) * 0.9)
    );
    assert_that!(
        tdigest.quantile(0.95).unwrap(),
        near((n as f64) * 0.95, 0.01 * (n as f64) * 0.95)
    );
    assert_that!(tdigest.quantile(1.0).unwrap(), eq((n - 1) as f64));

    let split_points = [n as f64 / 2.0];
    let pmf = tdigest.pmf(&split_points).unwrap();
    assert_eq!(pmf.len(), 2);
    assert_that!(pmf[0], near(0.5, 0.0001));
    assert_that!(pmf[1], near(0.5, 0.0001));
    let cdf = tdigest.cdf(&split_points).unwrap();
    assert_eq!(cdf.len(), 2);
    assert_that!(cdf[0], near(0.5, 0.0001));
    assert_that!(cdf[1], eq(1.0));
}

#[test]
fn test_rank_two_values() {
    let mut tdigest = TDigestMut::new(100).unwrap();
    tdigest.update(1.0);
    tdigest.update(2.0);
    assert_eq!(tdigest.rank(0.99), Some(0.0));
    assert_eq!(tdigest.rank(1.0), Some(0.25));
    assert_eq!(tdigest.rank(1.25), Some(0.375));
    assert_eq!(tdigest.rank(1.5), Some(0.5));
    assert_eq!(tdigest.rank(1.75), Some(0.625));
    assert_eq!(tdigest.rank(2.0), Some(0.75));
    assert_eq!(tdigest.rank(2.01), Some(1.0));
}

#[test]
fn test_rank_repeated_values() {
    let mut tdigest = TDigestMut::new(100).unwrap();
    tdigest.update(1.0);
    tdigest.update(1.0);
    tdigest.update(1.0);
    tdigest.update(1.0);
    assert_eq!(tdigest.rank(0.99), Some(0.0));
    assert_eq!(tdigest.rank(1.0), Some(0.5));
    assert_eq!(tdigest.rank(1.01), Some(1.0));
}

#[test]
fn test_repeated_blocks() {
    let mut tdigest = TDigestMut::new(100).unwrap();
    tdigest.update(1.0);
    tdigest.update(2.0);
    tdigest.update(2.0);
    tdigest.update(3.0);
    assert_eq!(tdigest.rank(0.99), Some(0.0));
    assert_eq!(tdigest.rank(1.0), Some(0.125));
    assert_eq!(tdigest.rank(2.0), Some(0.5));
    assert_eq!(tdigest.rank(3.0), Some(0.875));
    assert_eq!(tdigest.rank(3.01), Some(1.0));
}

#[test]
fn test_merge_small() {
    let mut td1 = TDigestMut::new(10).unwrap();
    td1.update(1.0);
    td1.update(2.0);
    let mut td2 = TDigestMut::new(10).unwrap();
    td2.update(2.0);
    td2.update(3.0);
    td1.merge(&td2);
    assert_eq!(td1.min_value(), Some(1.0));
    assert_eq!(td1.max_value(), Some(3.0));
    assert_eq!(td1.total_weight(), 4);
    assert_eq!(td1.rank(0.99), Some(0.0));
    assert_eq!(td1.rank(1.0), Some(0.125));
    assert_eq!(td1.rank(2.0), Some(0.5));
    assert_eq!(td1.rank(3.0), Some(0.875));
    assert_eq!(td1.rank(3.01), Some(1.0));
}

#[test]
fn test_merge_large() {
    let n = 10000;

    let mut td1 = TDigestMut::new(10).unwrap();
    let mut td2 = TDigestMut::new(10).unwrap();
    let sup = n / 2;
    for i in 0..sup {
        td1.update(i as f64);
        td2.update((sup + i) as f64);
    }
    td1.merge(&td2);

    assert_eq!(td1.total_weight(), n);
    assert_eq!(td1.min_value(), Some(0.0));
    assert_eq!(td1.max_value(), Some((n - 1) as f64));

    assert_that!(td1.rank(0.0).unwrap(), near(0.0, 0.0001));
    assert_that!(td1.rank((n / 4) as f64).unwrap(), near(0.25, 0.0001));
    assert_that!(td1.rank((n / 2) as f64).unwrap(), near(0.5, 0.0001));
    assert_that!(td1.rank((n * 3 / 4) as f64).unwrap(), near(0.75, 0.0001));
    assert_that!(td1.rank(n as f64).unwrap(), eq(1.0));
}

#[test]
fn test_mixed_k_merge_retains_receiver_k() {
    for (left_k, right_k, right_count) in [(200, 50, 1_000), (50, 200, 1_000), (200, 10, 1)] {
        let mut left = TDigestMut::new(left_k).unwrap();
        let mut right = TDigestMut::new(right_k).unwrap();
        for value in 0..10_000 {
            left.update(value as f64);
        }
        for value in 0..right_count {
            right.update((value + 10_000) as f64);
        }

        left.merge(&right);

        assert_eq!(left.k(), left_k);
        assert_eq!(left.total_weight(), 10_000 + right_count);
        assert_eq!(left.quantile(0.0), Some(0.0));
        assert_eq!(left.quantile(1.0), Some((9_999 + right_count) as f64));
    }
}

#[test]
fn test_from_iter_uses_one_result_with_the_smallest_nonempty_k() {
    let mut first = TDigestMut::new(100).unwrap();
    let mut second = TDigestMut::new(50).unwrap();
    let empty = TDigestMut::new(10).unwrap();
    for value in 0..1_000 {
        first.update(value as f64);
        second.update((value + 1_000) as f64);
    }
    let _ = first.quantile(0.5);
    let _ = second.quantile(0.5);

    let mut merged = [first, empty, second].into_iter().collect::<TDigestMut>();

    assert_eq!(merged.k(), 50);
    assert_eq!(merged.total_weight(), 2_000);
    assert_eq!(merged.min_value(), Some(0.0));
    assert_eq!(merged.max_value(), Some(1_999.0));
    let quantiles = (0..=100)
        .map(|rank| merged.quantile(rank as f64 / 100.).unwrap())
        .collect::<Vec<_>>();
    assert!(quantiles.windows(2).all(|pair| pair[0] <= pair[1]));
}

#[test]
fn test_from_iter_matches_single_digest_for_uncompressed_inputs() {
    let values = [3.0, 1.0, 2.0, 2.0, 5.0, 4.0];
    let partials = values
        .chunks(3)
        .map(|values| {
            let mut digest = TDigestMut::new(100).unwrap();
            for &value in values {
                digest.update(value);
            }
            digest
        })
        .collect::<Vec<_>>();
    let mut merged = partials.into_iter().collect::<TDigestMut>();

    let mut expected = TDigestMut::new(100).unwrap();
    for value in values {
        expected.update(value);
    }

    assert_eq!(merged.serialize(), expected.serialize());
}

#[test]
fn test_from_iter_matches_single_compression_for_interleaved_runs() {
    let runs: [&[(f64, u64)]; 4] = [
        &[(0.0, 1), (3.0, 2), (3.0, 5), (6.0, 100), (9.0, 1)],
        &[(0.0, 7), (2.0, 3), (5.0, 4), (8.0, 2), (9.0, 10)],
        &[(1.0, 2), (3.0, 6), (4.0, 1), (7.0, 8), (9.0, 20)],
        &[(0.0, 2), (3.0, 9), (6.0, 7), (9.0, 30)],
    ];
    for k in [10, 100] {
        for reverse in [false, true] {
            let make_digest = |centroids: &[(f64, u64)]| {
                let mut digest = deserialize_with_centroids(k, -10.0, 20.0, centroids);
                if reverse {
                    let mut bytes = digest.serialize();
                    bytes[5] |= 1 << 2; // reverse-merge flag
                    digest = TDigestMut::deserialize(&bytes).unwrap();
                }
                digest
            };
            let mut ordered = runs
                .iter()
                .flat_map(|run| run.iter().copied())
                .collect::<Vec<_>>();
            ordered.sort_by(|left, right| left.0.total_cmp(&right.0));

            // Borrowed merge puts the right input first on ties. Keeping the last centroid on
            // the left reproduces the concatenated runs' stable order in one compression pass.
            let last = ordered.pop().unwrap();
            let mut expected = make_digest(&[last]);
            expected.merge(&make_digest(&ordered));
            let mut merged = runs.into_iter().map(make_digest).collect::<TDigestMut>();

            assert_eq!(merged.serialize(), expected.serialize());
            assert_eq!(merged.quantile(0.0), Some(-10.0));
            assert_eq!(merged.quantile(1.0), Some(20.0));

            // A collected digest must still support enough updates to grow and compress again.
            for value in 0..1_000 {
                merged.update(value as f64);
                expected.update(value as f64);
            }
            assert_eq!(merged.serialize(), expected.serialize());
        }
    }
}

#[test]
fn test_from_iter_handles_mixed_compressed_and_buffered_inputs() {
    let mut expected = TDigestMut::new(100).unwrap();
    let partials = (0..4)
        .map(|run| {
            let mut digest = TDigestMut::new(100).unwrap();
            for row in 0..60 {
                let value = ((row * 13 + run * 7) % 37) as f64;
                digest.update(value);
                expected.update(value);
                if run == 2 && row == 29 {
                    let _ = digest.quantile(0.5);
                }
            }
            if run == 1 {
                let _ = digest.quantile(0.5);
            }
            digest
        })
        .collect::<Vec<_>>();
    let mut merged = partials.into_iter().collect::<TDigestMut>();

    assert_eq!(merged.total_weight(), 240);
    assert_eq!(merged.serialize(), expected.serialize());
}

#[test]
fn test_serialized_batch_merge_tree_preserves_weight_and_quantiles() {
    let mut values = Vec::new();
    let partials = (0..64)
        .map(|input| {
            let mut digest = TDigestMut::default();
            // Unequal weights, empty states, and repeated values exercise intermediate merges.
            let rows = [0, 1, 8, 64, 1_024][input % 5];
            for row in 0..rows {
                let value = ((row * 37 + input * 113) % 1_024) as f64 - 512.0;
                values.push(value);
                digest.update(value);
            }
            digest.serialize()
        })
        .collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);

    for fan_in in [2, 7, 64] {
        let mut states = partials.clone();
        while states.len() > 1 {
            states = states
                .chunks(fan_in)
                .map(|batch| {
                    let mut merged = batch
                        .iter()
                        .map(|bytes| TDigestMut::deserialize(bytes))
                        .collect::<Result<TDigestMut, _>>()
                        .unwrap();
                    merged.serialize()
                })
                .collect();
        }

        let mut merged = TDigestMut::deserialize(&states[0]).unwrap();
        assert_eq!(merged.total_weight(), values.len() as u64);
        assert_eq!(merged.quantile(0.0), values.first().copied());
        assert_eq!(merged.quantile(1.0), values.last().copied());

        let mut previous = values[0];
        for rank in [0.01, 0.5, 0.9, 0.99] {
            let estimate = merged.quantile(rank).unwrap();
            assert!((previous..=values[values.len() - 1]).contains(&estimate));
            // Check this fixture's empirical rank, allowing equal values to span a rank interval.
            let lower =
                values.partition_point(|value| *value < estimate) as f64 / values.len() as f64;
            let upper =
                values.partition_point(|value| *value <= estimate) as f64 / values.len() as f64;
            assert!(
                (lower - 0.01..=upper + 0.01).contains(&rank),
                "fan_in={fan_in}, rank={rank}, estimate={estimate}, observed=[{lower}, {upper}]"
            );
            previous = estimate;
        }
    }
}

#[test]
fn test_from_iter_checks_total_weight_before_compression() {
    for compressed in [false, true] {
        let heavy = deserialize_with_centroids(100, 0.0, 0.0, &[(0.0, u64::MAX - 2)]);
        let [one, two, extra] = [1.0, 2.0, 3.0].map(|value| {
            let mut digest = TDigestMut::new(100).unwrap();
            digest.update(value);
            if compressed {
                let _ = digest.quantile(0.5);
            }
            digest
        });
        let mut at_limit = [heavy.clone(), one.clone(), two.clone()]
            .into_iter()
            .collect::<TDigestMut>();
        assert_eq!(at_limit.total_weight(), u64::MAX);
        assert_eq!(at_limit.quantile(0.0), Some(0.0));
        assert_eq!(at_limit.quantile(1.0), Some(2.0));

        assert!(
            catch_unwind(|| [heavy, one, two, extra].into_iter().collect::<TDigestMut>()).is_err()
        );
    }
}

#[test]
fn test_from_iter_handles_empty_and_single_input_without_recompression() {
    let empty = std::iter::empty::<TDigestMut>().collect::<TDigestMut>();
    assert!(empty.is_empty());

    let mut input = TDigestMut::new(50).unwrap();
    for value in 0..1_000 {
        input.update(value as f64);
    }
    let serialized = input.serialize();
    let mut collected = [TDigestMut::new(10).unwrap(), input]
        .into_iter()
        .collect::<TDigestMut>();

    assert_eq!(collected.k(), 50);
    assert_eq!(collected.serialize(), serialized);
}

#[test]
fn test_invalid_inputs() {
    let n = 100;

    let mut td = TDigestMut::new(10).unwrap();
    for _ in 0..n {
        td.update(f64::NAN);
    }
    assert!(td.is_empty());

    let mut td = TDigestMut::new(10).unwrap();
    for _ in 0..n {
        td.update(f64::INFINITY);
    }
    assert!(td.is_empty());

    let mut td = TDigestMut::new(10).unwrap();
    for _ in 0..n {
        td.update(f64::NEG_INFINITY);
    }
    assert!(td.is_empty());

    let mut td = TDigestMut::new(10).unwrap();
    for i in 0..n {
        if i % 2 == 0 {
            td.update(f64::INFINITY);
        } else {
            td.update(f64::NEG_INFINITY);
        }
    }
    assert!(td.is_empty());
}

#[test]
fn test_extreme_values_produce_finite_quantiles() {
    let mut tdigest = TDigestMut::default();
    for i in 0..10_000 {
        tdigest.update(if i % 2 == 0 { f64::MAX } else { -f64::MAX });
    }

    assert_eq!(tdigest.total_weight(), 10_000);
    assert_eq!(tdigest.min_value(), Some(-f64::MAX));
    assert_eq!(tdigest.max_value(), Some(f64::MAX));
    for rank in [0.25, 0.5, 0.75] {
        let quantile = tdigest.quantile(rank).unwrap();
        assert_that!(quantile, is_finite(), "quantile at rank {rank}");
    }
}

#[test]
fn test_rank_interpolation_across_extreme_finite_values() {
    for scale in [1., f64::MAX] {
        let mut digest = TDigestMut::default();
        digest.update(-scale);
        digest.update(scale);
        let digest = digest.freeze();
        // The two centroid centers have ranks 1/4 and 3/4, independently of scale.
        let points = [-scale, -scale / 2., 0., scale / 2., scale];
        for (point, expected) in points.into_iter().zip([0.25, 0.375, 0.5, 0.625, 0.75]) {
            assert_that!(
                digest.rank(point).unwrap(),
                near(expected, 4. * f64::EPSILON)
            );
        }
    }
}

#[test]
fn test_tail_interpolation_across_extreme_finite_values() {
    let max = f64::MAX;
    let left = deserialize_with_centroids(100, -max, max, &[(max / 2., 10), (max, 1)]);
    let right = deserialize_with_centroids(100, -max, max, &[(-max, 1), (-max / 2., 10)]);
    // Weight 3 is halfway from the left tail's endpoint (weight 1) to its center (weight 5).
    // The expected value is therefore (-MAX + MAX/2)/2 = -MAX/4; the right tail mirrors it.
    for (digest, rank, expected) in [(left, 3. / 11., -0.25), (right, 8. / 11., 0.25)] {
        let digest = digest.freeze();
        assert_that!(
            digest.quantile(rank).unwrap() / max,
            near(expected, 4. * f64::EPSILON)
        );
        assert_that!(
            digest.rank(expected * max).unwrap(),
            near(rank, 4. * f64::EPSILON)
        );
    }
}

#[test]
fn test_estimate_repeat_values() {
    let mut tdigest = TDigestMut::default();
    for _ in 0..20 {
        tdigest.update(1.0);
    }
    assert_eq!(tdigest.quantile(0.9), Some(1.0));
}

/// Builds a digest whose centroids carry the given weights.
///
/// Compression never merges the extreme centroids, so digests built through `update` and `merge`
/// always keep unit-weight tails. Heavier tails arrive only through deserialization, including the
/// reference implementation format, and they select the tail interpolation branches.
fn deserialize_with_centroids(k: u16, min: f64, max: f64, centroids: &[(f64, u64)]) -> TDigestMut {
    const PREAMBLE_LONGS: u8 = 2;
    const SERIAL_VERSION: u8 = 1;
    const FAMILY_TDIGEST: u8 = 20;

    let mut bytes = vec![PREAMBLE_LONGS, SERIAL_VERSION, FAMILY_TDIGEST];
    bytes.extend_from_slice(&k.to_le_bytes());
    bytes.push(0); // flags
    bytes.extend_from_slice(&0u16.to_le_bytes()); // unused
    bytes.extend_from_slice(&(centroids.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes()); // buffered values
    bytes.extend_from_slice(&min.to_le_bytes());
    bytes.extend_from_slice(&max.to_le_bytes());
    for (mean, weight) in centroids {
        bytes.extend_from_slice(&mean.to_le_bytes());
        bytes.extend_from_slice(&weight.to_le_bytes());
    }
    TDigestMut::deserialize(&bytes).unwrap()
}

fn assert_quantile_queries(tdigest: &TDigestMut, ranks: &[f64], expected: &[f64]) {
    let frozen = tdigest.clone().freeze();
    for actual in [
        tdigest.clone().quantiles(ranks).unwrap(),
        frozen.quantiles(ranks).unwrap(),
    ] {
        assert_eq!(actual.len(), expected.len());
        for (actual, &expected) in actual.into_iter().zip(expected) {
            assert_that!(actual, near(expected, 1e-12));
        }
    }
    let mut mutable = tdigest.clone();
    for (&rank, &expected) in ranks.iter().zip(expected) {
        assert_that!(mutable.quantile(rank).unwrap(), near(expected, 1e-12));
        assert_that!(frozen.quantile(rank).unwrap(), near(expected, 1e-12));
    }
}

#[test]
fn test_quantile_moves_toward_the_nearer_bracketing_centroid() {
    let mut tdigest =
        deserialize_with_centroids(100, -1.0, 21.0, &[(0.0, 4), (10.0, 4), (20.0, 4)]);

    assert_eq!(tdigest.total_weight(), 12);
    // Ranks 2/12 and 6/12 sit exactly on the two centroids bracketing the first interval.
    assert_that!(tdigest.quantile(2.0 / 12.0).unwrap(), near(0.0, 1e-12));
    assert_that!(tdigest.quantile(3.0 / 12.0).unwrap(), near(2.5, 1e-12));
    assert_that!(tdigest.quantile(4.0 / 12.0).unwrap(), near(5.0, 1e-12));
    assert_that!(tdigest.quantile(5.0 / 12.0).unwrap(), near(7.5, 1e-12));
    assert_that!(tdigest.quantile(6.0 / 12.0).unwrap(), near(10.0, 1e-12));
}

#[test]
fn test_quantile_right_tail_stays_within_max() {
    let mut tdigest =
        deserialize_with_centroids(100, 0.0, 100.0, &[(10.0, 10), (50.0, 10), (90.0, 10)]);

    assert_eq!(tdigest.max_value(), Some(100.0));
    assert_that!(tdigest.quantile(0.9).unwrap(), near(95.0, 1e-12));
    assert_that!(tdigest.quantile(29.0 / 30.0).unwrap(), near(100.0, 1e-12));
    // Mirrors the left tail, which interpolates from min up to the first centroid mean.
    assert_that!(tdigest.quantile(1.0 / 30.0).unwrap(), near(0.0, 1e-12));
    assert_that!(tdigest.quantile(5.0 / 30.0).unwrap(), near(10.0, 1e-12));
}

#[test]
fn test_quantile_handles_two_sample_last_centroid() {
    let mut tdigest =
        deserialize_with_centroids(100, 0.0, 100.0, &[(0.0, 1), (50.0, 1), (90.0, 2)]);

    assert_eq!(tdigest.quantile(0.75), Some(100.0));
}

#[test]
fn test_single_centroid_preserves_stored_tail_information() {
    let digest = deserialize_with_centroids(100, 0., 100., &[(50., 10)]);
    // Ten samples have a centroid center at weight 5. The tail spans weights 1 through 5,
    // so weight 2.5 lies 3/8 of the way from the stored minimum to the mean.
    assert_quantile_queries(
        &digest,
        &[0., 0.1, 0.25, 0.5, 0.75, 0.9, 1.],
        &[0., 0., 18.75, 50., 81.25, 100., 100.],
    );
    let digest = digest.freeze();
    for (value, rank) in [
        (0., 0.05),
        (18.75, 0.25),
        (50., 0.5),
        (81.25, 0.75),
        (100., 0.95),
    ] {
        assert_that!(digest.rank(value).unwrap(), near(rank, 1e-12));
    }
}

#[test]
fn test_mutable_rank_preserves_weighted_single_centroid_tails() {
    let mut digest = deserialize_with_centroids(100, 0., 100., &[(50., 10)]);
    let frozen = digest.clone().freeze();
    let points = [-1., 0., 18.75, 50., 81.25, 100., 101.];
    let expected = [0., 0.05, 0.25, 0.5, 0.75, 0.95, 1.];

    // A quarter rank is mass 2.5: 3/8 of the tail from mass 1 at min to mass 5 at mean.
    // Repeating the queries must preserve the result before and after creating a view.
    for _ in 0..2 {
        for (&point, &rank) in points.iter().zip(&expected) {
            assert_eq!(digest.rank(point), Some(rank));
            assert_eq!(frozen.rank(point), Some(rank));
        }
        assert_eq!(&digest.cdf(&points).unwrap()[..points.len()], &expected);
    }
}

#[test]
fn test_small_compression_preserves_weighted_centroids_and_tie_order() {
    // Exercise the no-merge bound at two k values and a tiny input whose K_2 normalizer
    // is negative. Existing weighted centroids must remain intact in every case.
    for (k, middle_weight) in [(200, 95), (u16::MAX, 2), (u16::MAX, 32_762)] {
        for reverse in [false, true] {
            let mut digest = deserialize_with_centroids(
                k,
                -10.,
                10.,
                &[(-10., 1), (-0., 1), (0., middle_weight), (1., 1), (10., 1)],
            );
            let mut bytes = digest.serialize();
            if reverse {
                bytes[5] |= 1 << 2;
            }
            let mut digest = TDigestMut::deserialize(&bytes).unwrap();
            digest.update(0.);

            // Buffered values precede retained centroids on ties, including signed zeros.
            let mut expected = deserialize_with_centroids(
                k,
                -10.,
                10.,
                &[
                    (-10., 1),
                    (0., 1),
                    (-0., 1),
                    (0., middle_weight),
                    (1., 1),
                    (10., 1),
                ],
            )
            .serialize();
            if !reverse {
                expected[5] |= 1 << 2;
            }
            assert_eq!(digest.total_weight(), middle_weight + 5);
            assert_eq!(digest.serialize(), expected);
        }
    }
}

#[test]
fn test_compression_preserves_a_small_centroid_weight() {
    for weight in [1_u64 << 20, 1_u64 << 40, 1_u64 << 54] {
        for reverse in [false, true] {
            let mut digest = deserialize_with_centroids(
                10,
                0.,
                f64::MAX,
                &[(0., weight), (1., weight), (1e300, 1), (f64::MAX, weight)],
            );
            let mut bytes = digest.serialize();
            if reverse {
                bytes[5] |= 1 << 2;
            }
            let mut digest = TDigestMut::deserialize(&bytes).unwrap();
            digest.update(0.);

            // Read the merged mean directly: quantile interpolation must not hide a bad mean.
            let bytes = digest.serialize();
            let centroid = bytes[32..]
                .chunks_exact(16)
                .find(|c| u64::from_le_bytes(c[8..].try_into().unwrap()) == weight + 1)
                .expect("the middle centroids should merge");
            let mean = f64::from_le_bytes(centroid[..8].try_into().unwrap());
            // The exact mean is (weight * 1 + 1e300) / (weight + 1). The first term is far
            // below one ULP of the numerator, so this division is an independent reference.
            let expected = 1e300 / (weight + 1) as f64;
            assert_that!(mean / expected, near(1., 4. * f64::EPSILON));
        }
    }
}

#[test]
fn test_opposite_sign_merge_preserves_a_representable_correction() {
    let outer_weight = 1_u64 << 56;
    for magnitude in [1., f64::MAX] {
        for exponent in [54, 55] {
            let total_weight = 1_u64 << exponent;
            // The exact mean is magnitude * (1 - 2 / total_weight). At 2^54 it
            // rounds to the next value toward zero; at 2^55 it rounds back to magnitude.
            let expected = if exponent == 54 {
                magnitude.next_down()
            } else {
                magnitude
            };
            for negative_heavy in [false, true] {
                let (left_weight, right_weight, expected) = if negative_heavy {
                    (total_weight - 1, 1, -expected)
                } else {
                    (1, total_weight - 1, expected)
                };
                for reverse in [false, true] {
                    for owned in [false, true] {
                        let mut digest = deserialize_with_centroids(
                            10,
                            -f64::MAX,
                            f64::MAX,
                            &[
                                (-f64::MAX, outer_weight),
                                (-magnitude, left_weight),
                                (magnitude, right_weight),
                                (f64::MAX, outer_weight),
                            ],
                        );
                        let mut bytes = digest.serialize();
                        if reverse {
                            bytes[5] |= 1 << 2;
                        }
                        let mut digest = TDigestMut::deserialize(&bytes).unwrap();
                        if owned {
                            let other = deserialize_with_centroids(
                                10,
                                f64::MAX,
                                f64::MAX,
                                &[(f64::MAX, 1)],
                            );
                            digest = [digest, other].into_iter().collect();
                        } else {
                            digest.update(-f64::MAX);
                        }
                        let bytes = digest.serialize();
                        let centroid = bytes[32..]
                            .chunks_exact(16)
                            .find(|c| u64::from_le_bytes(c[8..].try_into().unwrap()) == total_weight)
                            .unwrap_or_else(|| {
                                panic!("middle pair did not merge: magnitude={magnitude:?}, exponent={exponent}, negative_heavy={negative_heavy}, reverse={reverse}, owned={owned}")
                            });
                        let mean = f64::from_le_bytes(centroid[..8].try_into().unwrap());
                        assert_eq!(
                            mean, expected,
                            "magnitude={magnitude:?}, exponent={exponent}, negative_heavy={negative_heavy}, reverse={reverse}, owned={owned}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn test_compression_keeps_merged_means_within_their_endpoints() {
    let tiny = f64::from_bits(1);
    let heavy = 1_u64 << 54;
    for (left, right) in [
        (0., tiny),
        (tiny, 2. * tiny),
        (
            f64::from_bits(f64::MIN_POSITIVE.to_bits() - 1),
            f64::MIN_POSITIVE,
        ),
        (1., f64::from_bits(1_f64.to_bits() + 1)),
        (f64::from_bits(f64::MAX.to_bits() - 1), f64::MAX),
        (f64::MAX / 4., f64::MAX),
        (-f64::MAX, -f64::MAX / 4.),
        (-2. * tiny, -tiny),
        (-tiny, tiny),
        (-f64::MAX, f64::MAX),
    ] {
        for (left_weight, right_weight) in [(1, 1), (1, heavy), (heavy, 1)] {
            for reverse in [false, true] {
                for owned in [false, true] {
                    let mut digest = deserialize_with_centroids(
                        10,
                        -f64::MAX,
                        f64::MAX,
                        &[
                            (-f64::MAX, heavy),
                            (left, left_weight),
                            (right, right_weight),
                            (f64::MAX, heavy),
                        ],
                    );
                    let mut bytes = digest.serialize();
                    if reverse {
                        bytes[5] |= 1 << 2;
                    }
                    let mut digest = TDigestMut::deserialize(&bytes).unwrap();
                    if owned {
                        let other =
                            deserialize_with_centroids(10, f64::MAX, f64::MAX, &[(f64::MAX, 1)]);
                        digest = [digest, other].into_iter().collect();
                    } else {
                        digest.update(-f64::MAX);
                    }

                    // Inspect compression directly; query interpolation could hide an overshoot.
                    let bytes = digest.serialize();
                    let centroid = bytes[32..]
                        .chunks_exact(16)
                        .find(|c| {
                            u64::from_le_bytes(c[8..].try_into().unwrap())
                                == left_weight + right_weight
                        })
                        .unwrap_or_else(|| {
                            panic!(
                                "middle pair {left:?}:{left_weight}, {right:?}:{right_weight} did not merge (reverse={reverse}, owned={owned})"
                            )
                        });
                    let mean = f64::from_le_bytes(centroid[..8].try_into().unwrap());
                    assert!(
                        mean.is_finite() && (left..=right).contains(&mean),
                        "{mean:?} is outside [{left:?}, {right:?}] (weights={left_weight}:{right_weight}, reverse={reverse}, owned={owned})"
                    );
                }
            }
        }
    }
}

#[test]
fn test_signed_zero_and_symmetric_centroids_survive_interpolation() {
    for magnitude in [0., f64::from_bits(1), 1., f64::MAX] {
        let heavy = 1_u64 << 54;
        for reverse in [false, true] {
            let mut digest = deserialize_with_centroids(
                10,
                -f64::MAX,
                f64::MAX,
                &[
                    (-f64::MAX, heavy),
                    (-magnitude, 1),
                    (magnitude, 1),
                    (f64::MAX, heavy),
                ],
            );
            let mut bytes = digest.serialize();
            if reverse {
                bytes[5] |= 1 << 2;
            }
            let mut digest = TDigestMut::deserialize(&bytes).unwrap();
            digest.update(-f64::MAX);
            let bytes = digest.serialize();
            let merged = bytes[32..]
                .chunks_exact(16)
                .find(|c| u64::from_le_bytes(c[8..].try_into().unwrap()) == 2)
                .expect("the equally weighted middle centroids should merge");
            // Equal and opposite contributions cancel exactly, including subnormal values.
            assert_eq!(f64::from_le_bytes(merged[..8].try_into().unwrap()), 0.);
        }
    }

    let mut digest =
        deserialize_with_centroids(100, -1., 1., &[(-1., 1), (-0., 2), (0., 2), (1., 1)]);
    assert_eq!(digest.rank(-0.), Some(0.5));
    assert_eq!(digest.rank(0.), Some(0.5));
    assert_quantile_queries(&digest, &[1. / 3., 0.5, 2. / 3.], &[0., 0., 0.]);
    assert_eq!(digest.cdf(&[-0.]), Some(vec![0.5, 1.]));
    assert_eq!(digest.pmf(&[0.]), Some(vec![0.5, 0.5]));
}

#[test]
fn test_batch_quantiles_match_scalar_queries_in_input_order() {
    let mut tdigest = TDigestMut::new(100).unwrap();
    for value in 0..10_000 {
        tdigest.update(((value * 37) % 1_003) as f64);
    }

    let frozen = tdigest.clone().freeze();
    for ranks in [
        vec![-0.0, 0.0, 0.001, 0.25, 0.5, 0.5, 0.99, 1.0],
        vec![0.99, 0.0, 0.5, 1.0, 0.001, -0.0, 0.5, 0.25],
        vec![1.0, 0.99, 0.5, 0.5, 0.25, 0.001, 0.0],
        vec![],
    ] {
        let expected = ranks
            .iter()
            .map(|&rank| frozen.quantile(rank).unwrap())
            .collect::<Vec<_>>();
        // Each mutable batch starts with the original buffered values still pending.
        assert_eq!(tdigest.clone().quantiles(&ranks), Some(expected.clone()));
        assert_eq!(frozen.quantiles(&ranks), Some(expected));
    }
}

#[test]
fn test_batch_quantiles_cross_centroid_and_tail_boundaries() {
    let tdigest =
        deserialize_with_centroids(100, 0.0, 100.0, &[(10.0, 10), (50.0, 10), (90.0, 10)]);
    // Query masses span both tails and the centers of all three centroids.
    let queries = [
        (0.0, 0.0),
        (1.0, 0.0),
        (3.0, 5.0),
        (5.0, 10.0),
        (10.0, 30.0),
        (15.0, 50.0),
        (15.0, 50.0),
        (20.0, 70.0),
        (25.0, 90.0),
        (27.0, 95.0),
        (29.0, 100.0),
        (30.0, 100.0),
    ];
    let mut reversed = queries;
    reversed.reverse();
    let mut unordered = queries;
    unordered.rotate_left(5);

    for queries in [queries, reversed, unordered] {
        let ranks = queries.map(|(weight, _)| weight / 30.0);
        let expected = queries.map(|(_, quantile)| quantile);
        assert_quantile_queries(&tdigest, &ranks, &expected);
    }
    // Batches confined to one region can finish before reaching the centroid scan.
    for (weight, expected) in queries {
        assert_quantile_queries(&tdigest, &[weight / 30.0; 3], &[expected; 3]);
    }
}

#[test]
fn test_quantiles_respect_two_sample_tail_boundaries() {
    let tdigest = deserialize_with_centroids(100, 0.0, 100.0, &[(10.0, 2), (90.0, 2)]);
    // These tails are discontinuous: the left endpoint steps to the first mean, while the right
    // endpoint steps from the last mean to max. Neither tail may divide by a zero span.
    let ranks = [
        0.0,
        0.25_f64.next_down(),
        0.25,
        0.25_f64.next_up(),
        0.5,
        0.75_f64.next_down(),
        0.75,
        0.75,
        0.75_f64.next_up(),
        1.0,
    ];
    let expected = [0.0, 0.0, 10.0, 10.0, 50.0, 90.0, 100.0, 100.0, 100.0, 100.0];
    assert_quantile_queries(&tdigest, &ranks, &expected);
}

#[test]
fn test_quantiles_interpolate_around_singleton_centroids() {
    let tdigest = deserialize_with_centroids(
        100,
        0.0,
        60.0,
        &[(0.0, 1), (10.0, 3), (20.0, 1), (40.0, 5), (60.0, 1)],
    );
    let ranks = [
        0.0, 0.75, 1.0, 1.75, 2.5, 3.25, 4.0, 4.5, 4.75, 5.0, 6.25, 7.5, 8.75, 10.0, 11.0,
    ]
    .map(|weight| weight / 11.0);
    let expected = [
        0.0, 0.0, 0.0, 5.0, 10.0, 15.0, 20.0, 20.0, 20.0, 20.0, 30.0, 40.0, 50.0, 60.0, 60.0,
    ];
    assert_quantile_queries(&tdigest, &ranks, &expected);
}

#[test]
fn test_quantiles_stay_within_extrema_at_large_total_weights() {
    for total_weight in [1_u64 << 53, 1_u64 << 54, u64::MAX] {
        let tdigest = deserialize_with_centroids(
            100,
            0.0,
            100.0,
            &[(10.0, total_weight - 2), (50.0, 1), (90.0, 1)],
        );
        let frozen = tdigest.clone().freeze();
        let ranks = [0.0, 0.5, 1.0_f64.next_down(), 1.0, 1.0];
        for quantiles in [
            tdigest.clone().quantiles(&ranks).unwrap(),
            frozen.quantiles(&ranks).unwrap(),
            ranks.map(|rank| frozen.quantile(rank).unwrap()).to_vec(),
        ] {
            assert_eq!(quantiles[0], 0.0);
            assert_eq!(&quantiles[3..], &[100.0, 100.0]);
            assert!(
                quantiles.is_sorted(),
                "weight {total_weight}: {quantiles:?}"
            );
            assert!(
                quantiles.iter().all(|value| (0.0..=100.0).contains(value)),
                "weight {total_weight}: {quantiles:?}"
            );
        }
    }
}

#[test]
fn test_queries_stay_ordered_at_large_centroid_centers() {
    for total_weight in [1_u64 << 53, 1_u64 << 54, u64::MAX] {
        for centroids in [
            vec![(10., total_weight - 5), (50., 1), (90., 4)],
            vec![(0., 1), (90., total_weight - 1)],
        ] {
            let digest = deserialize_with_centroids(100, 0., 100., &centroids).freeze();
            // Probe either side of a heavy centroid's center and the rounded upper endpoint.
            let ranks = [
                0.,
                0.5_f64.next_down(),
                0.5,
                0.5_f64.next_up(),
                1.0_f64.next_down(),
                1.,
            ];
            let quantiles = digest.quantiles(&ranks).unwrap();
            assert_eq!(quantiles[0], 0.);
            assert_eq!(quantiles[5], 100.);
            assert!(
                quantiles.is_sorted(),
                "centroids {centroids:?}: {quantiles:?}"
            );
            assert!(quantiles.iter().all(|value| (0.0..=100.0).contains(value)));

            let cdf = digest.cdf(&[0., 5., 10., 50., 90., 95., 100.]).unwrap();
            assert!(cdf.is_sorted(), "centroids {centroids:?}: {cdf:?}");
            assert!(cdf.iter().all(|rank| (0.0..=1.0).contains(rank)));
        }
    }
}

#[test]
fn test_quantile_right_tail_uses_the_same_center_as_rank() {
    for (total_weight, last_weight, rank) in [
        (1_u64 << 53, 5, 1. - f64::EPSILON),
        ((1_u64 << 53) + 1, 4, 1_f64.next_down()),
        (1_u64 << 54, 9, 1. - f64::EPSILON),
        (u64::MAX, 4097, 1_f64.next_down()),
    ] {
        let mut digest = deserialize_with_centroids(
            100,
            0.,
            100.,
            &[(0., total_weight - last_weight), (90., last_weight)],
        );
        // These ranks land exactly on the rounded mass N - last_weight/2. Quantile must
        // return the centroid mean there, before interpolating toward the stored maximum.
        assert_eq!(digest.rank(90.), Some(rank));
        assert_quantile_queries(&digest, &[rank, rank, 1.], &[90., 90., 100.]);
    }

    // At N = 2^53 + 2 the last center and N - 1 round to the same mass. The zero-width
    // tail must stay finite and ordered, just as a two-sample tail does at smaller counts.
    let total_weight = (1_u64 << 53) + 2;
    let digest = deserialize_with_centroids(100, 0., 100., &[(0., total_weight - 3), (90., 3)]);
    let rank = 1. - f64::EPSILON;
    let ranks = [rank.next_down(), rank, rank.next_up(), 1.];
    let quantiles = digest.clone().quantiles(&ranks).unwrap();
    assert!(quantiles.is_sorted());
    assert!(quantiles.iter().all(|value| (0.0..=100.0).contains(value)));
    assert_eq!(quantiles[3], 100.);
    assert_quantile_queries(&digest, &ranks, &quantiles);
}

#[test]
fn test_rank_stays_monotonic_at_the_right_tail_boundary() {
    let digest = deserialize_with_centroids(100, 0., 300., &[(0., 1), (100., 29)]).freeze();
    let points = [100., 100_f64.next_up(), 150., 300.];
    let cdf = digest.cdf(&points).unwrap();
    // The last center is at weight 1 + 29/2 = 15.5. Entering the tail must not lower its rank.
    assert_eq!(cdf[0], 15.5 / 30.);
    assert!(cdf.is_sorted(), "CDF at {points:?}: {cdf:?}");
}

#[test]
fn test_batch_quantiles_reject_invalid_ranks() {
    for values in [&[][..], &[1.0, 2.0, 3.0][..]] {
        let mut tdigest = TDigestMut::default();
        for &value in values {
            tdigest.update(value);
        }
        let frozen = tdigest.clone().freeze();
        for invalid in [
            -f64::EPSILON,
            1.0 + f64::EPSILON,
            f64::NAN,
            f64::NEG_INFINITY,
            f64::INFINITY,
        ] {
            let ranks = [0.5, invalid];
            assert!(catch_unwind(AssertUnwindSafe(|| tdigest.quantiles(&ranks))).is_err());
            assert!(catch_unwind(|| frozen.quantiles(&ranks)).is_err());
        }
    }
}

#[test]
fn test_rank_left_tail_is_a_fraction_of_the_total_weight() {
    let mut tdigest =
        deserialize_with_centroids(100, 0.0, 100.0, &[(10.0, 10), (50.0, 10), (90.0, 10)]);

    assert_that!(tdigest.rank(5.0).unwrap(), near(0.1, 1e-12));
    assert_that!(tdigest.rank(10.0).unwrap(), near(5.0 / 30.0, 1e-12));
    // The right tail is the mirror image and pins the scale the left tail must match.
    assert_that!(tdigest.rank(95.0).unwrap(), near(0.9, 1e-12));
    assert_that!(tdigest.rank(90.0).unwrap(), near(25.0 / 30.0, 1e-12));

    let pmf = tdigest.pmf(&[5.0, 95.0]).unwrap();
    assert_that!(pmf[0], near(0.1, 1e-12));
    assert_that!(pmf[1], near(0.8, 1e-12));
    assert_that!(pmf[2], near(0.1, 1e-12));
}

#[test]
fn test_merge_preserves_min_max_from_other() {
    // Heavy extreme centroids are legal after deserialization: `min`/`max` can differ from the
    // first and last centroid means. Merge must keep those stored extrema, not re-derive them.
    let other = deserialize_with_centroids(100, 0.0, 100.0, &[(10.0, 10), (50.0, 10), (90.0, 10)]);
    assert_eq!(other.min_value(), Some(0.0));
    assert_eq!(other.max_value(), Some(100.0));

    let mut empty = TDigestMut::new(100).unwrap();
    empty.merge(&other);
    assert_eq!(empty.min_value(), Some(0.0));
    assert_eq!(empty.max_value(), Some(100.0));
    assert_eq!(empty.quantile(0.0), Some(0.0));
    assert_eq!(empty.quantile(1.0), Some(100.0));
    assert_eq!(empty.rank(0.0), Some(0.5 / 30.0));
    assert_eq!(empty.rank(100.0), Some(1.0 - 0.5 / 30.0));
    assert_eq!(empty.rank(-1.0), Some(0.0));
    assert_eq!(empty.rank(101.0), Some(1.0));

    let mut left = deserialize_with_centroids(100, 5.0, 40.0, &[(10.0, 4), (20.0, 4), (30.0, 4)]);
    let right = deserialize_with_centroids(100, -10.0, 80.0, &[(0.0, 4), (40.0, 4), (70.0, 4)]);
    left.merge(&right);
    assert_eq!(left.min_value(), Some(-10.0));
    assert_eq!(left.max_value(), Some(80.0));
    assert_eq!(left.quantile(0.0), Some(-10.0));
    assert_eq!(left.quantile(1.0), Some(80.0));
}

#[test]
fn weight_overflow_preserves_buffer_and_extrema() {
    let mut one = TDigestMut::new(20).unwrap();
    one.update(0.0);
    let mut sketch = one.clone();
    for _ in 0..63 {
        sketch.merge(&sketch.clone());
        sketch.update(0.0);
    }
    assert_eq!(sketch.total_weight(), u64::MAX);
    let before = sketch.clone().serialize();

    sketch.update(f64::NAN);
    sketch.merge(&TDigestMut::default());
    assert!(catch_unwind(AssertUnwindSafe(|| sketch.update(1.0))).is_err());
    assert_eq!(sketch.clone().serialize(), before);
    assert!(catch_unwind(AssertUnwindSafe(|| sketch.merge(&one))).is_err());
    assert_eq!(sketch.clone().serialize(), before);

    let mut target = TDigestMut::default();
    target.update(1.0);
    let before = target.clone().serialize();
    assert!(catch_unwind(AssertUnwindSafe(|| target.merge(&sketch))).is_err());
    assert_eq!(target.serialize(), before);
}
