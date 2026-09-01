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
fn test_mixed_k_merge_uses_smaller_k() {
    let mut left = TDigestMut::new(200).unwrap();
    let mut right = TDigestMut::new(50).unwrap();
    for value in 0..1_000 {
        left.update(value as f64);
        right.update((value + 1_000) as f64);
    }

    left.try_merge(&right).unwrap();

    assert_eq!(left.k(), 50);
    assert_eq!(left.total_weight(), 2_000);
    assert_eq!(left.min_value(), Some(0.0));
    assert_eq!(left.max_value(), Some(1_999.0));
}

#[test]
fn test_merge_many_uses_one_result_with_the_smallest_nonempty_k() {
    let mut merged = TDigestMut::new(200).unwrap();
    let mut first = TDigestMut::new(100).unwrap();
    let mut second = TDigestMut::new(50).unwrap();
    let empty = TDigestMut::new(10).unwrap();
    for value in 0..1_000 {
        first.update(value as f64);
        second.update((value + 1_000) as f64);
    }

    merged.merge_many([&first, &empty, &second]).unwrap();

    assert_eq!(merged.k(), 50);
    assert_eq!(merged.total_weight(), 2_000);
    assert_eq!(merged.min_value(), Some(0.0));
    assert_eq!(merged.max_value(), Some(1_999.0));
    assert_quantiles_are_nondecreasing(&mut merged);
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
fn test_batch_quantiles_match_scalar_queries_in_input_order() {
    let mut tdigest = TDigestMut::new(100).unwrap();
    for value in 0..10_000 {
        tdigest.update(((value * 37) % 1_003) as f64);
    }
    let tdigest = tdigest.freeze();

    for ranks in [
        vec![0.0, 0.001, 0.25, 0.5, 0.5, 0.99, 1.0],
        vec![0.99, 0.0, 0.5, 1.0, 0.001, 0.5, 0.25],
        vec![],
    ] {
        let expected = ranks
            .iter()
            .map(|&rank| tdigest.quantile(rank).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(tdigest.quantiles(&ranks), Some(expected));
    }
}

fn assert_quantiles_are_nondecreasing(tdigest: &mut TDigestMut) {
    let ranks = (0..=100).map(|rank| rank as f64 / 100.).collect::<Vec<_>>();
    let quantiles = tdigest.quantiles(&ranks).unwrap();
    assert!(quantiles.windows(2).all(|pair| pair[0] <= pair[1]));
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
fn test_try_merge_reports_total_weight_overflow_without_mutating_receiver() {
    let mut left = deserialize_with_centroids(100, 0.0, 0.0, &[(0.0, u64::MAX)]);
    let right = deserialize_with_centroids(50, 1.0, 1.0, &[(1.0, 1)]);

    let error = left.try_merge(&right).unwrap_err();

    assert_eq!(
        error.kind(),
        datasketches::error::ErrorKind::InvalidArgument
    );
    assert_eq!(left.k(), 100);
    assert_eq!(left.total_weight(), u64::MAX);
    assert_eq!(left.min_value(), Some(0.0));
    assert_eq!(left.max_value(), Some(0.0));
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
