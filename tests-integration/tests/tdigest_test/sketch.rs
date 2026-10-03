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
    for scale in [1., 1e300, f64::MAX] {
        let mut digest = TDigestMut::default();
        digest.update(-scale);
        digest.update(scale);
        let digest = digest.freeze();
        let points = [-scale, -scale / 2., 0., scale / 2., scale];
        for (point, expected) in points.into_iter().zip([0.25, 0.375, 0.5, 0.625, 0.75]) {
            assert_that!(digest.rank(point).unwrap(), near(expected, 1e-12));
        }
        assert_eq!(
            digest.cdf(&points).unwrap(),
            [0.25, 0.375, 0.5, 0.625, 0.75, 1.]
        );
        assert_eq!(
            digest.pmf(&points).unwrap(),
            [0.25, 0.125, 0.125, 0.125, 0.125, 0.25]
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
fn test_quantile_tails_interpolate_extreme_finite_values() {
    let max = f64::MAX;
    let left = deserialize_with_centroids(100, -max, max, &[(max / 2., 10), (max, 1)]);
    let right = deserialize_with_centroids(100, -max, max, &[(-max, 1), (-max / 2., 10)]);
    for (digest, rank, expected) in [
        (left.clone(), 1. / 11., -1.),
        (left, 3. / 11., -0.25),
        (right.clone(), 8. / 11., 0.25),
        (right, 10. / 11., 1.),
    ] {
        let frozen = digest.clone().freeze();
        assert_that!(frozen.quantile(rank).unwrap() / max, near(expected, 1e-12));
        let mut mutable = digest;
        let values = mutable.quantiles(&[rank, 0., 1., rank]).unwrap();
        assert_eq!(values, frozen.quantiles(&[rank, 0., 1., rank]).unwrap());
        assert_eq!(values[1..3], [-max, max]);
        assert!(values.iter().all(|value| value.is_finite()));
    }
}

#[test]
fn test_single_centroid_preserves_stored_tail_information() {
    let digest = deserialize_with_centroids(100, 0., 100., &[(50., 10)]);
    assert_quantile_queries(
        &digest,
        &[0., 0.1, 0.5, 0.9, 1.],
        &[0., 0., 50., 100., 100.],
    );
    let digest = digest.freeze();
    assert_that!(digest.rank(0.).unwrap(), near(0.05, 1e-12));
    assert_that!(digest.rank(50.).unwrap(), near(0.5, 1e-12));
    assert_that!(digest.rank(100.).unwrap(), near(0.95, 1e-12));
}

#[test]
fn test_compression_preserves_a_small_centroid_weight() {
    let weight = 1_u64 << 54;
    let mut digest = deserialize_with_centroids(
        10,
        0.,
        f64::MAX,
        &[(0., weight), (1., weight), (1e300, 1), (f64::MAX, weight)],
    );
    let mut bytes = digest.serialize();
    bytes[5] |= 1 << 2; // Compress from the right, merging the light centroid into the heavy one.
    let mut digest = TDigestMut::deserialize(&bytes).unwrap();
    digest.update(0.);

    // The middle centroid's center is at the median. Its mean retains the light sample's mass.
    let expected = 1e300 / weight as f64;
    assert_that!(digest.quantile(0.5).unwrap() / expected, near(1., 1e-12));
    assert!(TDigestMut::deserialize(&digest.serialize()).is_ok());
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
        for centroids in [
            vec![(10., total_weight - 2), (50., 1), (90., 1)],
            vec![(10., total_weight - 5), (50., 1), (90., 4)],
            vec![(0., 1), (90., total_weight - 1)],
        ] {
            let tdigest = deserialize_with_centroids(100, 0., 100., &centroids);
            let frozen = tdigest.clone().freeze();
            let ranks = [
                0.,
                0.5_f64.next_down(),
                0.5,
                0.5_f64.next_up(),
                1.0_f64.next_down(),
                1.,
                1.,
            ];
            for quantiles in [
                tdigest.clone().quantiles(&ranks).unwrap(),
                frozen.quantiles(&ranks).unwrap(),
                ranks.map(|rank| frozen.quantile(rank).unwrap()).to_vec(),
            ] {
                assert_eq!(quantiles[0], 0.);
                assert_eq!(&quantiles[5..], &[100., 100.]);
                assert!(
                    quantiles.is_sorted(),
                    "centroids {centroids:?}: {quantiles:?}"
                );
                assert!(
                    quantiles.iter().all(|value| (0.0..=100.0).contains(value)),
                    "centroids {centroids:?}: {quantiles:?}"
                );
            }
            let splits = [0., 5., 10., 50., 90., 95., 100.];
            let cdf = frozen.cdf(&splits).unwrap();
            assert!(cdf.is_sorted(), "centroids {centroids:?}: {cdf:?}");
            assert!(
                frozen
                    .pmf(&splits)
                    .unwrap()
                    .iter()
                    .all(|p| (0.0..=1.0).contains(p))
            );
        }
    }
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
