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

use datasketches::ddsketch::DdSketch;
use datasketches::ddsketch::DdSketchBuilder;
use datasketches::ddsketch::MappingKind;
use datasketches::ddsketch::StoreKind;
use datasketches::error::ErrorKind;

#[test]
fn construction_validates_accuracy_and_store_bounds() {
    for bad_alpha in [
        f64::NAN,
        f64::NEG_INFINITY,
        f64::INFINITY,
        -0.01,
        0.0,
        9.99e-7,
        1.0,
        1.5,
    ] {
        let err = DdSketch::new(bad_alpha).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidArgument, "alpha={bad_alpha}");
    }

    for valid_alpha in [1e-6, 0.01, 0.5, 0.999_999, 1.0_f64.next_down()] {
        for kind in [
            MappingKind::Cubic,
            MappingKind::Linear,
            MappingKind::Logarithmic,
        ] {
            let sketch = DdSketchBuilder::new(valid_alpha)
                .mapping(kind)
                .build()
                .unwrap();
            assert_eq!(sketch.mapping_kind(), kind);
            assert!((sketch.relative_accuracy() - valid_alpha).abs() <= 1e-12);
        }
    }

    for bad_bins in [0, (1 << 26) + 1] {
        for store in [
            StoreKind::CollapsingLowestDense {
                max_num_bins: bad_bins,
            },
            StoreKind::CollapsingHighestDense {
                max_num_bins: bad_bins,
            },
        ] {
            let err = DdSketchBuilder::new(0.01).store(store).build().unwrap_err();
            assert_eq!(err.kind(), ErrorKind::InvalidArgument);
        }
    }

    let one_bin = DdSketchBuilder::new(0.01)
        .store(StoreKind::CollapsingLowestDense { max_num_bins: 1 })
        .build()
        .unwrap();
    assert_eq!(
        one_bin.store_kind(),
        StoreKind::CollapsingLowestDense { max_num_bins: 1 }
    );
}

#[test]
fn empty_sketch_validates_ranks_before_returning_none() {
    let sketch = DdSketch::default();
    assert!(sketch.is_empty());
    assert_eq!(sketch.total_weight(), 0);
    assert_eq!(sketch.zero_weight(), 0);
    assert_eq!(sketch.num_retained(), 0);
    assert_eq!(sketch.min_value(), None);
    assert_eq!(sketch.max_value(), None);
    assert_eq!(sketch.sum(), None);
    assert_eq!(sketch.iter().len(), 0);
    assert_eq!(sketch.iter().next(), None);

    assert_eq!(sketch.quantile(0.0).unwrap(), None);
    assert_eq!(sketch.quantile(0.5).unwrap(), None);
    assert_eq!(sketch.quantile(1.0).unwrap(), None);
    assert_eq!(sketch.quantiles(&[]).unwrap(), None);
    assert_eq!(sketch.quantiles(&[0.25, 0.75]).unwrap(), None);

    for bad_rank in [f64::NAN, f64::NEG_INFINITY, f64::INFINITY, -0.001, 1.001] {
        assert_eq!(
            sketch.quantile(bad_rank).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(
            sketch.quantiles(&[0.5, bad_rank]).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
    }
}

#[test]
fn non_finite_inputs_and_zero_weights_are_ignored_even_at_max_weight() {
    let mut sketch = DdSketch::new(0.01).unwrap();
    sketch.update(f64::NAN);
    sketch.update(f64::INFINITY);
    sketch.update(f64::NEG_INFINITY);
    sketch.update_with_weight(10.0, 0);
    assert!(sketch.is_empty());

    sketch.update_with_weight(10.0, u64::MAX - 2);
    sketch.update_with_weight(20.0, 2);
    assert_eq!(sketch.total_weight(), u64::MAX);

    // Ignored updates must remain no-ops at capacity.
    sketch.update(f64::NAN);
    sketch.update(f64::INFINITY);
    sketch.update_with_weight(30.0, 0);
    assert_eq!(sketch.total_weight(), u64::MAX);

    // Overflowing update panics without mutating observable sketch state.
    let before_min = sketch.min_value();
    let before_max = sketch.max_value();
    let before_retained = sketch.num_retained();
    let panicked = catch_unwind(AssertUnwindSafe(|| {
        sketch.update(5.0);
    }));
    assert!(panicked.is_err());
    assert_eq!(sketch.total_weight(), u64::MAX);
    assert_eq!(sketch.min_value(), before_min);
    assert_eq!(sketch.max_value(), before_max);
    assert_eq!(sketch.num_retained(), before_retained);
}

#[test]
fn zero_threshold_subnormals_and_extreme_finite_values_are_tracked() {
    for kind in [
        MappingKind::Cubic,
        MappingKind::Linear,
        MappingKind::Logarithmic,
    ] {
        let mut sketch = DdSketchBuilder::new(0.01).mapping(kind).build().unwrap();
        let threshold = sketch.zero_threshold();
        assert!(threshold > 0.0 && threshold.is_finite());

        let subnormal = f64::from_bits(1);
        for v in [0.0, -0.0, subnormal, -subnormal, threshold, -threshold] {
            sketch.update(v);
        }
        assert_eq!(sketch.total_weight(), 6);
        assert_eq!(sketch.zero_weight(), 6);
        assert_eq!(sketch.num_retained(), 1);
        assert_eq!(sketch.min_value(), Some(-threshold));
        assert_eq!(sketch.max_value(), Some(threshold));
        assert_eq!(sketch.sum(), Some(0.0));
        assert_eq!(sketch.quantile(0.0).unwrap(), Some(-threshold));
        assert_eq!(sketch.quantile(0.5).unwrap(), Some(0.0));
        assert_eq!(sketch.quantile(1.0).unwrap(), Some(threshold));

        sketch.update(-f64::MAX);
        sketch.update(f64::MAX);
        assert_eq!(sketch.total_weight(), 8);
        assert_eq!(sketch.num_retained(), 3);
        assert_eq!(sketch.min_value(), Some(-f64::MAX));
        assert_eq!(sketch.max_value(), Some(f64::MAX));
        assert_eq!(sketch.sum(), Some(0.0));
        assert_eq!(sketch.quantile(0.0).unwrap(), Some(-f64::MAX));
        assert_eq!(sketch.quantile(1.0).unwrap(), Some(f64::MAX));
    }

    // Single-sample and negative-zero exact summary and clamping invariants.
    let mut single = DdSketch::new(0.01).unwrap();
    single.update(1.0);
    assert_eq!(single.min_value(), Some(1.0));
    assert_eq!(single.max_value(), Some(1.0));
    assert_eq!(single.sum(), Some(1.0));
    assert_eq!(
        single.quantiles(&[0.0, 0.5, 1.0]).unwrap(),
        Some(vec![1.0, 1.0, 1.0])
    );

    let mut neg_zero = DdSketch::new(0.01).unwrap();
    neg_zero.update(-0.0);
    assert_eq!(neg_zero.min_value().unwrap().to_bits(), 0.0_f64.to_bits());
    assert_eq!(neg_zero.max_value().unwrap().to_bits(), 0.0_f64.to_bits());
    assert_eq!(neg_zero.sum().unwrap().to_bits(), 0.0_f64.to_bits());
}

#[test]
fn reset_reuses_dense_capacity_and_iter_supports_double_ended_traversal() {
    let mut sketch = DdSketch::new(0.01).unwrap();
    for v in [-1000.0, -10.0, 0.0, 0.0, 5.0, 500.0] {
        sketch.update(v);
    }
    let cap_before = sketch.estimated_size();
    assert!(cap_before > 0);
    assert_eq!(sketch.num_retained(), 5);

    let mut iter = sketch.iter();
    assert_eq!(iter.len(), 5);
    let first = iter.next().unwrap();
    let last = iter.next_back().unwrap();
    assert!(first.0 < -900.0 && first.1 == 1);
    assert!(last.0 > 490.0 && last.1 == 1);
    assert_eq!(iter.len(), 3);
    let middle_back = iter.next_back().unwrap();
    assert!(middle_back.0 > 4.9 && middle_back.0 < 5.1 && middle_back.1 == 1);
    let zero_bin = iter.next_back().unwrap();
    assert_eq!(zero_bin, (0.0, 2));
    let neg_ten = iter.next().unwrap();
    assert!(neg_ten.0 > -10.2 && neg_ten.0 < -9.8 && neg_ten.1 == 1);
    assert_eq!(iter.len(), 0);
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next_back(), None);

    let full_forward: Vec<_> = sketch.iter().collect();
    let mut full_reverse: Vec<_> = sketch.iter().rev().collect();
    full_reverse.reverse();
    assert_eq!(full_forward, full_reverse);
    assert!(full_forward.windows(2).all(|pair| pair[0].0 < pair[1].0));

    sketch.reset();
    assert!(sketch.is_empty());
    assert_eq!(sketch.num_retained(), 0);
    assert_eq!(sketch.estimated_size(), cap_before);

    sketch.update(42.0);
    let q = sketch.quantile(0.5).unwrap().unwrap();
    assert!((q - 42.0).abs() <= 0.01 * 42.0);
}
