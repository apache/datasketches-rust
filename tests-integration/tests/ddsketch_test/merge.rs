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

use datasketches::ddsketch::DdSketch;
use datasketches::ddsketch::DdSketchBuilder;
use datasketches::ddsketch::MappingKind;
use datasketches::ddsketch::StoreKind;
use datasketches::error::ErrorKind;

#[test]
fn merge_rejects_incompatible_mappings_even_when_empty_and_preserves_self_on_error() {
    let mut cubic_001 = DdSketchBuilder::new(0.01)
        .mapping(MappingKind::Cubic)
        .build()
        .unwrap();
    let cubic_002 = DdSketchBuilder::new(0.02)
        .mapping(MappingKind::Cubic)
        .build()
        .unwrap();
    let log_001 = DdSketchBuilder::new(0.01)
        .mapping(MappingKind::Logarithmic)
        .build()
        .unwrap();

    // Compatibility is checked before taking any empty-sketch fast path.
    assert_eq!(
        cubic_001.merge(&cubic_002).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        cubic_001.merge(&log_001).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );

    // Near-equal `gamma` values exceeding either the bucket-drift (`1e-4`) or value-drift (`1e-10`)
    // merge bound across the `f64` range must be rejected.
    for (alpha_a, alpha_b) in [
        (1e-6, 1e-6 + 4e-13),
        (0.01, 0.010_000_05),
        (0.01, 0.01 + 1e-11),
    ] {
        let mut left = DdSketchBuilder::new(alpha_a)
            .mapping(MappingKind::Logarithmic)
            .build::<u64>()
            .unwrap();
        let mut right = DdSketchBuilder::new(alpha_b)
            .mapping(MappingKind::Logarithmic)
            .build::<u64>()
            .unwrap();
        right.update(1e100);
        assert_eq!(
            left.merge(&right).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert!(left.is_empty());
    }

    cubic_001.update_with_weight(10.0, u64::MAX - 5);
    let mut right = DdSketch::new(0.01).unwrap();
    right.update_with_weight(20.0, 6);

    let snapshot: Vec<_> = cubic_001.iter().collect();
    let err = cubic_001.merge(&right).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidArgument);
    assert_eq!(cubic_001.total_weight(), u64::MAX - 5);
    assert_eq!(cubic_001.iter().collect::<Vec<_>>(), snapshot);
}

#[test]
fn merge_is_order_and_partition_independent_across_stores() {
    let values: Vec<f64> = (-200_i32..=200)
        .map(|i| {
            if i % 17 == 0 {
                0.0
            } else {
                let sign = if i < 0 { -1.0 } else { 1.0 };
                sign * (1.0 + f64::from(i.abs()) * 3.7).powi(2)
            }
        })
        .collect();

    let mut sequential = DdSketch::new(0.01).unwrap();
    for &v in &values {
        sequential.update(v);
    }

    // Partition across heterogeneous store strategies sharing the same index mapping, then fold
    // in two opposite merge orders.
    let stores = [
        StoreKind::CollapsingLowestDense { max_num_bins: 2048 },
        StoreKind::UnboundedDense,
        StoreKind::Sparse,
        StoreKind::CollapsingHighestDense { max_num_bins: 2048 },
    ];
    let mut partials: Vec<DdSketch> = stores
        .iter()
        .map(|&store| DdSketchBuilder::new(0.01).store(store).build().unwrap())
        .collect();
    let num_partials = partials.len();
    for (idx, &v) in values.iter().enumerate() {
        partials[idx % num_partials].update(v);
    }

    let mut forward = DdSketch::new(0.01).unwrap();
    for partial in &partials {
        forward.merge(partial).unwrap();
    }

    let mut reverse = DdSketch::new(0.01).unwrap();
    for partial in partials.iter().rev() {
        reverse.merge(partial).unwrap();
    }

    assert_eq!(
        forward.store_kind(),
        StoreKind::CollapsingLowestDense { max_num_bins: 2048 }
    );
    assert_eq!(forward.total_weight(), sequential.total_weight());
    assert_eq!(forward.zero_weight(), sequential.zero_weight());
    assert_eq!(
        forward.iter().collect::<Vec<_>>(),
        sequential.iter().collect::<Vec<_>>()
    );
    assert_eq!(
        reverse.iter().collect::<Vec<_>>(),
        sequential.iter().collect::<Vec<_>>()
    );
}

#[test]
fn merge_under_tight_collapse_budget_preserves_tail_invariants() {
    let store = StoreKind::CollapsingLowestDense { max_num_bins: 8 };
    let mut left = DdSketchBuilder::new(0.01)
        .store(store)
        .build::<u64>()
        .unwrap();
    let mut right = DdSketchBuilder::new(0.01)
        .store(store)
        .build::<u64>()
        .unwrap();

    for v in [1.0, 2.0, 4.0] {
        left.update(v);
    }
    for v in [1e6, 2e6, 4e6] {
        right.update(v);
    }

    let mut left_then_right = left.clone();
    left_then_right.merge(&right).unwrap();

    let mut right_then_left = right.clone();
    right_then_left.merge(&left).unwrap();

    assert_eq!(left_then_right.total_weight(), 6);
    assert_eq!(
        left_then_right.iter().collect::<Vec<_>>(),
        right_then_left.iter().collect::<Vec<_>>()
    );
    assert_eq!(left_then_right.serialize(), right_then_left.serialize());
    assert_eq!(left_then_right.max_value(), Some(4e6));

    // Merging a collapsed store into an uncollapsed same-kind store whose active span already
    // covers the collapsed window must preserve `is_collapsed` symmetrically across merge orders.
    for (one_bin_store, anchor) in [
        (StoreKind::CollapsingLowestDense { max_num_bins: 1 }, 100.0),
        (StoreKind::CollapsingHighestDense { max_num_bins: 1 }, 1.0),
    ] {
        let mut uncollapsed = DdSketchBuilder::new(0.01)
            .store(one_bin_store)
            .build::<u64>()
            .unwrap();
        uncollapsed.update(anchor);

        let mut collapsed = DdSketchBuilder::new(0.01)
            .store(one_bin_store)
            .build::<u64>()
            .unwrap();
        collapsed.update(1.0);
        collapsed.update(100.0);

        let mut a_then_b = uncollapsed.clone();
        a_then_b.merge(&collapsed).unwrap();
        let mut b_then_a = collapsed.clone();
        b_then_a.merge(&uncollapsed).unwrap();

        let bytes = a_then_b.serialize();
        assert_eq!(bytes, b_then_a.serialize());
        assert_eq!(
            DdSketch::<u64>::deserialize(&bytes).unwrap().total_weight(),
            3
        );
    }
}
#[test]
fn merge_wide_sparse_and_unbounded_into_empty_narrow_collapsing_matches_incremental() {
    let samples = [
        (1e-6, 3_u64),
        (1e-3, 1),
        (0.5, 5),
        (1.0, 2),
        (7.5, 4),
        (190.0, 1),
        (25_000.0, 6),
    ];
    let ranks = [0.0, 0.25, 0.5, 0.75, 1.0];

    let mut wide_sparse = DdSketchBuilder::new(0.01)
        .store(StoreKind::Sparse)
        .build()
        .unwrap();
    let mut wide_unbounded = DdSketchBuilder::new(0.01)
        .store(StoreKind::UnboundedDense)
        .build()
        .unwrap();
    for &(v, w) in &samples {
        wide_sparse.update_with_weight(v, w);
        wide_unbounded.update_with_weight(v, w);
    }

    for kind in [
        StoreKind::CollapsingLowestDense { max_num_bins: 8 },
        StoreKind::CollapsingHighestDense { max_num_bins: 8 },
    ] {
        let mut incremental = DdSketchBuilder::new(0.01).store(kind).build().unwrap();
        for &(v, w) in &samples {
            incremental.update_with_weight(v, w);
        }

        for src in [&wide_sparse, &wide_unbounded] {
            let mut merged = DdSketchBuilder::new(0.01).store(kind).build().unwrap();
            merged.merge(src).unwrap();
            assert_eq!(merged.total_weight(), incremental.total_weight());
            assert_eq!(merged.min_value(), incremental.min_value());
            assert_eq!(merged.max_value(), incremental.max_value());
            assert_eq!(
                merged.quantiles(&ranks).unwrap(),
                incremental.quantiles(&ranks).unwrap()
            );
            assert_eq!(
                merged.iter().collect::<Vec<_>>(),
                incremental.iter().collect::<Vec<_>>()
            );
            assert_eq!(merged.serialize(), incremental.serialize());
        }
    }
}

#[test]
fn fractional_merge_combines_stores_and_rejects_f64_overflow_atomically() {
    let mut left = DdSketchBuilder::new(0.01)
        .store(StoreKind::CollapsingLowestDense { max_num_bins: 128 })
        .build::<f64>()
        .unwrap();
    let mut right = DdSketchBuilder::new(0.01)
        .store(StoreKind::Sparse)
        .build::<f64>()
        .unwrap();

    left.update_with_weight(-25.0, 0.75);
    left.update_with_weight(0.0, 0.25);
    left.update_with_weight(10.0, 1.5);
    right.update_with_weight(-25.0, 0.5);
    right.update_with_weight(0.0, 0.5);
    right.update_with_weight(100.0, 2.25);

    left.merge(&right).unwrap();
    assert_eq!(left.total_weight(), 5.75);
    assert_eq!(left.zero_weight(), 0.75);
    assert_eq!(left.min_value(), Some(-25.0));
    assert_eq!(left.max_value(), Some(100.0));

    // Overflowing `f64::MAX` returns `InvalidArgument` without modifying `left`.
    let mut huge_left = DdSketch::<f64>::new(0.01).unwrap();
    huge_left.update_with_weight(10.0, 0.75 * f64::MAX);
    let mut huge_right = DdSketch::<f64>::new(0.01).unwrap();
    huge_right.update_with_weight(-10.0, 0.5 * f64::MAX);
    let snapshot: Vec<_> = huge_left.iter().collect();
    assert_eq!(
        huge_left.merge(&huge_right).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(huge_left.total_weight(), 0.75 * f64::MAX);
    assert_eq!(huge_left.iter().collect::<Vec<_>>(), snapshot);
}
