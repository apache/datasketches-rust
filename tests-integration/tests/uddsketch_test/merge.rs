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
use datasketches::uddsketch::UddSketch;

#[test]
fn states_are_invariant_across_permutations_partitions_and_merge_trees() {
    let mut dataset = Vec::with_capacity(2060);
    for i in 0..2048 {
        let mag = (1.0 + (i % 41) as f64 / 41.0) * 2f64.powi((i / 41) - 25);
        dataset.push(if i % 3 == 0 { -mag } else { mag });
    }
    dataset.extend([
        -f64::MAX,
        f64::MAX,
        -f64::from_bits(1),
        f64::from_bits(1),
        -f64::MIN_POSITIVE,
        f64::MIN_POSITIVE,
        -0.0,
        0.0,
        0.0,
    ]);

    let build = |slice: &[f64]| {
        let mut s = UddSketch::new(128, 0.01).unwrap();
        for &v in slice {
            s.update(v);
        }
        s
    };

    let reference = build(&dataset);
    let expected_bytes = reference.serialize();
    let query_ranks = [0.0, 0.01, 0.1, 0.5, 0.9, 0.99, 1.0];
    let expected_quantiles = reference.quantiles(&query_ranks).unwrap().unwrap();
    assert!(reference.num_collapses() > 0);

    // Deterministic permutation: ascending, descending, and pseudo-random Fisher-Yates shuffle.
    let mut ascending = dataset.clone();
    ascending.sort_by(f64::total_cmp);

    let mut descending = ascending.clone();
    descending.reverse();

    let mut shuffled = dataset.clone();
    let mut rng = 0x1234_5678_9abc_def0_u64;
    for i in (1..shuffled.len()).rev() {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        shuffled.swap(i, (rng as usize) % (i + 1));
    }

    for order in [&dataset, &ascending, &descending, &shuffled] {
        assert_eq!(build(order).serialize(), expected_bytes);

        for chunk_size in [1, 13, 127, 600] {
            let partials: Vec<UddSketch> = order.chunks(chunk_size).map(build).collect();

            // Left-fold merge via serialized roundtrip.
            let mut left = UddSketch::new(128, 0.01).unwrap();
            for part in &partials {
                let decoded = UddSketch::deserialize(&part.serialize()).unwrap();
                left.merge(&decoded).unwrap();
            }
            assert_eq!(left.serialize(), expected_bytes);
            assert_eq!(
                left.quantiles(&query_ranks).unwrap().unwrap(),
                expected_quantiles
            );

            // Right-fold merge.
            let mut right = UddSketch::new(128, 0.01).unwrap();
            for part in partials.iter().rev() {
                right.merge(part).unwrap();
            }
            assert_eq!(right.serialize(), expected_bytes);

            // Balanced binary reduction tree.
            let mut layer = partials;
            while layer.len() > 1 {
                let mut next = Vec::with_capacity(layer.len().div_ceil(2));
                let mut iter = layer.into_iter();
                while let Some(mut a) = iter.next() {
                    if let Some(b) = iter.next() {
                        a.merge(&b).unwrap();
                    }
                    next.push(a);
                }
                layer = next;
            }
            assert_eq!(layer[0].serialize(), expected_bytes);
        }
    }
}

#[test]
fn merging_mismatched_collapse_levels_matches_single_stream() {
    let mut coarse = UddSketch::new(8, 0.01).unwrap();
    let mut fine = UddSketch::new(8, 0.01).unwrap();
    let mut combined = UddSketch::new(8, 0.01).unwrap();

    for exp in -40..=40 {
        let v = 2f64.powi(exp);
        coarse.update(v);
        combined.update(v);
    }
    for v in [-3.0, -1.5, 0.0, 2.5] {
        fine.update(v);
        combined.update(v);
    }
    assert!(coarse.num_collapses() > fine.num_collapses());

    let mut fine_into_coarse = coarse.clone();
    fine_into_coarse.merge(&fine).unwrap();

    let mut coarse_into_fine = fine.clone();
    coarse_into_fine.merge(&coarse).unwrap();

    let expected = combined.serialize();
    assert_eq!(fine_into_coarse.serialize(), expected);
    assert_eq!(coarse_into_fine.serialize(), expected);
}

#[test]
fn incompatible_or_overflowing_merge_leaves_receiver_unchanged() {
    let mut empty_base = UddSketch::new(64, 0.01).unwrap();
    let wrong_buckets = UddSketch::new(128, 0.01).unwrap();
    let wrong_alpha = UddSketch::new(64, 0.02).unwrap();

    // Compatibility must be checked even when both sketches are empty.
    assert_eq!(
        empty_base.merge(&wrong_buckets).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        empty_base.merge(&wrong_alpha).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );

    let mut receiver = UddSketch::new(64, 0.01).unwrap();
    receiver.update_with_weight(10.0, u64::MAX - 5);
    let before = receiver.serialize();

    assert_eq!(
        receiver.merge(&wrong_buckets).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(receiver.serialize(), before);

    let mut heavy = UddSketch::new(64, 0.01).unwrap();
    heavy.update_with_weight(-2.0, 6);
    assert_eq!(
        receiver.merge(&heavy).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(receiver.serialize(), before);
}

#[test]
fn merging_unsorted_zero_shift_sketches_preserves_all_buckets() {
    let mut left = UddSketch::new(32, 0.01).unwrap();
    let mut right = left.clone();
    let mut reference = left.clone();
    for exponent in 0..=16 {
        left.update(2f64.powi(exponent));
    }
    left.update(0.5);
    right.update(1.0);
    assert_eq!(left.num_collapses(), 0);
    assert_eq!(right.num_collapses(), 0);

    reference.update(0.5);
    reference.update(1.0);
    for exponent in 0..=16 {
        reference.update(2f64.powi(exponent));
    }
    left.merge(&right).unwrap();
    assert_eq!(left.serialize(), reference.serialize());
}

#[test]
fn merging_full_sketches_collapses_multiple_levels_and_avoids_capacity_doubling() {
    let mut left = UddSketch::new(16, 0.01).unwrap();
    let mut right = left.clone();
    let mut combined = left.clone();
    let mut doubled = left.clone();
    for exponent in 0..16 {
        let value = 2f64.powi(exponent);
        left.update(value);
        right.update(-value);
        combined.update(value);
        combined.update(-value);
        doubled.update_with_weight(value, 2);
    }
    assert_eq!(left.num_retained(), 16);
    assert_eq!(right.num_retained(), 16);

    let mut identical = left.clone();
    let size = identical.estimated_size();
    identical.merge(&left).unwrap();
    assert_eq!(identical.estimated_size(), size);
    assert_eq!(identical.serialize(), doubled.serialize());

    let initial_level = left.num_collapses().max(right.num_collapses());
    left.merge(&right).unwrap();
    assert!(left.num_collapses() >= initial_level + 2);
    assert_eq!(left.serialize(), combined.serialize());
}
