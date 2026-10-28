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

use datasketches::ddsketch::CollapseDirection;
use datasketches::ddsketch::DdSketch;
use datasketches::error::ErrorKind;

#[test]
fn empty_and_invalid_queries() {
    let sketch = DdSketch::default();
    assert!(sketch.is_empty());
    assert_eq!(sketch.min_value(), None);
    assert_eq!(sketch.max_value(), None);
    assert_eq!(sketch.quantile(0.5).unwrap(), None);
    assert_eq!(sketch.quantiles(&[]).unwrap(), None);
    assert_eq!(sketch.quantile_is_collapsed(0.5).unwrap(), None);
    for rank in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.1, 1.1] {
        assert_eq!(
            sketch.quantile(rank).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(
            sketch.quantiles(&[0.5, rank]).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(
            sketch.quantile_is_collapsed(rank).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
    }
}

#[test]
fn invalid_configuration() {
    for accuracy in [
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        -0.1,
        0.0,
        1e-10,
        1.0,
    ] {
        assert_eq!(
            DdSketch::new(2048, accuracy).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
    }
    assert!(DdSketch::new(0, 0.01).is_err());
}

#[test]
fn invalid_updates_and_overflow_are_atomic() {
    let mut sketch = DdSketch::default();
    sketch.update_weighted(5.0, u64::MAX).unwrap();
    let before = sketch.serialize();
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        for weight in [0, 1] {
            assert_eq!(
                sketch.update_weighted(value, weight).unwrap_err().kind(),
                ErrorKind::InvalidArgument
            );
            assert_eq!(sketch.serialize(), before);
        }
    }
    assert!(sketch.update(-10.0).is_err());
    assert_eq!(sketch.serialize(), before);
    sketch.update_weighted(-10.0, 0).unwrap();
    assert_eq!(sketch.serialize(), before);
    assert_eq!(sketch.total_weight(), u64::MAX);
}

#[test]
fn weighted_updates_match_repeated_values() {
    let mut weighted = DdSketch::new(16, 0.01).unwrap();
    let mut repeated = weighted.clone();
    for (value, weight) in [(-500.0, 7), (-2.0, 3), (0.0, 5), (3.0, 11), (1000.0, 4)] {
        weighted.update_weighted(value, weight).unwrap();
        for _ in 0..weight {
            repeated.update(value).unwrap();
        }
    }
    assert_eq!(weighted.serialize(), repeated.serialize());
    let ranks = [1.0, 0.5, 0.0, 0.1, 0.5];
    let scalar: Vec<_> = ranks
        .iter()
        .map(|&q| weighted.quantile(q).unwrap().unwrap())
        .collect();
    assert_eq!(weighted.quantiles(&ranks).unwrap(), Some(scalar));
    assert_eq!(weighted.quantiles(&[]).unwrap(), Some(vec![]));
}

#[test]
fn signed_zero_and_subnormal_inputs() {
    let mut sketch = DdSketch::default();
    for value in [-0.0, 0.0, -f64::from_bits(1), f64::from_bits(1)] {
        sketch.update(value).unwrap();
    }
    assert_eq!(sketch.zero_count(), 4);
    assert_eq!(sketch.quantile(0.5).unwrap(), Some(0.0));
    assert_eq!(sketch.min_value(), Some(-f64::from_bits(1)));
    assert_eq!(sketch.max_value(), Some(f64::from_bits(1)));
    assert_eq!(sketch.quantile(0.0).unwrap(), sketch.min_value());
    assert_eq!(sketch.quantile(1.0).unwrap(), sketch.max_value());
    sketch.update(f64::MIN_POSITIVE).unwrap();
    sketch.update(-f64::MIN_POSITIVE).unwrap();
    assert_eq!(sketch.zero_count(), 4);

    let mut zero = DdSketch::default();
    zero.update(-0.0).unwrap();
    assert_eq!(zero.min_value().unwrap().to_bits(), 0.0f64.to_bits());
    assert_eq!(zero.max_value().unwrap().to_bits(), 0.0f64.to_bits());
}

#[test]
fn full_u64_weight_selects_the_exact_discrete_rank() {
    let mut sketch = DdSketch::default();
    sketch.update_weighted(1.0, 1 << 63).unwrap();
    sketch.update_weighted(100.0, (1 << 63) - 1).unwrap();
    assert_eq!(sketch.total_weight(), u64::MAX);
    assert_eq!(sketch.quantile(0.5).unwrap(), Some(1.0));
    assert!(
        sketch
            .quantile(f64::from_bits(0.5f64.to_bits() + 1))
            .unwrap()
            .unwrap()
            > 99.0
    );
    assert_eq!(sketch.quantile(f64::from_bits(1)).unwrap(), Some(1.0));
    assert_eq!(sketch.quantile(1.0).unwrap(), Some(100.0));
}

#[test]
fn reset_reuses_storage_and_clears_collapse() {
    let mut sketch =
        DdSketch::with_collapse_direction(8, 0.01, CollapseDirection::Highest).unwrap();
    for value in [-1e200, -1e-200, 0.0, 1e-200, 1e200] {
        sketch.update(value).unwrap();
    }
    assert!(sketch.is_collapsed());
    assert_eq!(sketch.num_buckets(), 17);
    let size = sketch.estimated_size();
    sketch.reset();
    assert!(sketch.is_empty());
    assert!(!sketch.is_collapsed());
    assert_eq!(sketch.estimated_size(), size);
    let mut fresh = DdSketch::with_collapse_direction(8, 0.01, CollapseDirection::Highest).unwrap();
    for value in [-20.0, 0.0, 10.0] {
        sketch.update(value).unwrap();
        fresh.update(value).unwrap();
    }
    assert_eq!(sketch.serialize(), fresh.serialize());
}
