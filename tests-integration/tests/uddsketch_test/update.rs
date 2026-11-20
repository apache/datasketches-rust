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

use datasketches::error::ErrorKind;
use datasketches::uddsketch::UddSketch;

fn assert_same_summary(left: &UddSketch, right: &UddSketch) {
    assert_eq!(left.max_buckets(), right.max_buckets());
    assert_eq!(
        left.initial_alpha().to_bits(),
        right.initial_alpha().to_bits()
    );
    assert_eq!(left.num_collapses(), right.num_collapses());
    assert_eq!(left.total_weight(), right.total_weight());
    assert_eq!(left.zero_weight(), right.zero_weight());
    assert_eq!(left.num_retained(), right.num_retained());
    assert_eq!(
        left.min_value().map(f64::to_bits),
        right.min_value().map(f64::to_bits)
    );
    assert_eq!(
        left.max_value().map(f64::to_bits),
        right.max_value().map(f64::to_bits)
    );
    let probe_ranks: Vec<f64> = (0..=100).map(|i| i as f64 / 100.0).collect();
    let lq = left.quantiles(&probe_ranks).unwrap();
    let rq = right.quantiles(&probe_ranks).unwrap();
    assert_eq!(lq, rq);
}

#[test]
fn constructor_validates_parameters() {
    for bad_buckets in [0, 1, 2, 3] {
        let err = UddSketch::new(bad_buckets, 0.01).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidArgument);
    }
    for bad_alpha in [
        -0.1,
        0.0,
        1e-13,
        1.0,
        1.5,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ] {
        let err = UddSketch::new(16, bad_alpha).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidArgument);
    }

    assert!(UddSketch::new(4, 1e-12).is_ok());
    assert!(UddSketch::new(4, 1.0 - f64::EPSILON).is_ok());
}

#[test]
fn empty_sketch_validates_query_ranks_before_empty_fast_path() {
    let sketch = UddSketch::default();
    assert!(sketch.is_empty());
    assert_eq!(sketch.quantile(0.5).unwrap(), None);
    assert_eq!(sketch.quantiles(&[]).unwrap(), None);
    assert_eq!(sketch.quantiles(&[0.0, 0.5, 1.0]).unwrap(), None);

    for bad_rank in [-0.01, 1.01, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
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
fn non_finite_values_and_zero_weights_are_ignored_and_signed_zeros_canonicalize() {
    let mut left = UddSketch::new(64, 0.01).unwrap();
    for ignored in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        left.update(ignored);
        left.update_with_weight(ignored, 100);
    }
    left.update_with_weight(42.0, 0);
    assert!(left.is_empty());

    left.update(-0.0);
    left.update(0.0);

    let mut right = UddSketch::new(64, 0.01).unwrap();
    right.update(0.0);
    right.update(-0.0);

    assert_eq!(left.min_value().unwrap().to_bits(), 0.0f64.to_bits());
    assert_eq!(left.max_value().unwrap().to_bits(), 0.0f64.to_bits());
    assert_eq!(left.zero_weight(), 2);
    assert_eq!(left.num_retained(), 0);
    assert_same_summary(&left, &right);
}

#[test]
fn weighted_updates_match_repeated_unit_updates_across_index_and_collapse() {
    let mut unit = UddSketch::new(48, 0.005).unwrap();
    let mut weighted = UddSketch::new(48, 0.005).unwrap();

    for i in 0..96 {
        let magnitude = 1.03_f64.powi(i - 40);
        let value = if i % 3 == 0 {
            -magnitude
        } else if i % 11 == 0 {
            0.0
        } else {
            magnitude
        };
        let weight = ((i % 7) + 1) as u64;
        for _ in 0..weight {
            unit.update(value);
        }
        weighted.update_with_weight(value, weight);
    }

    assert!(weighted.num_collapses() > 0);
    assert!(weighted.num_retained() <= 48);
    assert_same_summary(&weighted, &unit);
}

#[test]
fn reset_restores_initial_level_and_produces_identical_subsequent_builds() {
    let mut reused = UddSketch::new(16, 0.001).unwrap();
    for exp in -100..=100 {
        reused.update(2f64.powi(exp));
        reused.update(-2f64.powi(exp));
    }
    assert!(reused.num_collapses() > 0);

    reused.reset();
    assert!(reused.is_empty());
    assert_eq!(reused.num_collapses(), 0);
    assert_eq!(reused.alpha(), reused.initial_alpha());

    let mut fresh = UddSketch::new(16, 0.001).unwrap();
    for v in [-3.5, 0.0, 1.25, 9.75] {
        reused.update(v);
        fresh.update(v);
    }
    assert_same_summary(&reused, &fresh);
}

#[test]
fn weight_overflow_panics_without_modifying_sketch() {
    let mut sketch = UddSketch::new(32, 0.01).unwrap();
    sketch.update_with_weight(-5.0, u64::MAX - 2);
    sketch.update_with_weight(10.0, 2);
    let snapshot = sketch.clone();

    let panic_unit = catch_unwind(AssertUnwindSafe(|| {
        sketch.update(1.0);
    }));
    assert!(panic_unit.is_err());
    assert_same_summary(&sketch, &snapshot);

    let panic_weighted = catch_unwind(AssertUnwindSafe(|| {
        sketch.update_with_weight(0.0, 1);
    }));
    assert!(panic_weighted.is_err());
    assert_same_summary(&sketch, &snapshot);

    // Non-finite and zero-weight calls remain no-ops even at u64::MAX.
    sketch.update(f64::NAN);
    sketch.update_with_weight(1.0, 0);
    assert_eq!(sketch.total_weight(), u64::MAX);
}
