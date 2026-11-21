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

    cubic_001.update_with_weight(10.0, u64::MAX - 5);
    let mut right = DdSketch::new(0.01).unwrap();
    right.update_with_weight(20.0, 6);

    let snapshot = cubic_001.serialize();
    let err = cubic_001.merge(&right).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidArgument);
    assert_eq!(cubic_001.total_weight(), u64::MAX - 5);
    assert_eq!(cubic_001.serialize(), snapshot);
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
    let mut left = DdSketchBuilder::new(0.01).store(store).build().unwrap();
    let mut right = DdSketchBuilder::new(0.01).store(store).build().unwrap();

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
    let max = left_then_right.max_value().unwrap();
    assert!((max - 4e6).abs() <= 0.01 * 4e6);
}
