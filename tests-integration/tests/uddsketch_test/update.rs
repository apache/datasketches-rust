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

use super::build;

#[test]
fn invalid_updates_and_merges_are_atomic() {
    let mut sketch = build(&[-2.0, 0.0, 3.0], 3, 0.01);
    let before = sketch.serialize();
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_eq!(
            sketch.update(value).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert!(sketch.update_weighted(value, 0).is_err());
    }
    for other in [
        UddSketch::new(4, 0.01).unwrap(),
        UddSketch::new(3, 0.02).unwrap(),
    ] {
        assert!(sketch.merge(&other).is_err());
    }
    assert_eq!(sketch.serialize(), before);
    sketch.update_weighted(8.0, 0).unwrap();
    assert_eq!(sketch.serialize(), before);
    let mut full = UddSketch::new(3, 0.01).unwrap();
    full.update_weighted(1.0, u64::MAX - 1).unwrap();
    full.update(2.0).unwrap();
    let before = full.serialize();
    assert!(full.update(3.0).is_err());
    assert!(full.merge(&sketch).is_err());
    assert_eq!(full.serialize(), before);
    assert_eq!(UddSketch::deserialize(&before).unwrap().serialize(), before);
    assert_eq!(full.quantile(1.0).unwrap(), Some(2.0));
    assert_eq!(
        full.quantile(f64::from_bits(1.0f64.to_bits() - 1)).unwrap(),
        Some(1.0)
    );
}

#[test]
fn empty_and_discrete_semantics() {
    let empty = UddSketch::default();
    assert_eq!(empty.quantile(0.5).unwrap(), None);
    assert_eq!(empty.quantiles(&[]).unwrap(), None);
    for rank in [f64::NAN, f64::NEG_INFINITY, -0.01, 1.01] {
        assert!(empty.quantile(rank).is_err());
        assert!(empty.quantiles(&[0.5, rank]).is_err());
    }
    let sketch = build(&[-100.0, 0.0, 10.0, 20.0], 512, 0.01);
    assert_eq!(sketch.quantile(0.5).unwrap(), Some(0.0));
    assert_eq!(sketch.quantile(0.0).unwrap(), Some(-100.0));
    assert_eq!(sketch.quantile(1.0).unwrap(), Some(20.0));
    assert_eq!(sketch.quantiles(&[]).unwrap(), Some(vec![]));
    assert_eq!(
        build(&[-0.0, 0.0], 3, 0.01).serialize(),
        build(&[0.0, 0.0], 3, 0.01).serialize()
    );
    for alpha in [0.0, 1e-10, 1.0, f64::NAN] {
        assert!(UddSketch::new(512, alpha).is_err());
    }
    assert!(UddSketch::new(2, 0.01).is_err());
}

#[test]
fn reset_starts_an_independent_stream() {
    for limit in [3, 16, 512] {
        let mut sketch = build(
            &(-2000..2000).map(f64::from).collect::<Vec<_>>(),
            limit,
            0.001,
        );
        assert!(sketch.compaction_level() > 0);
        for values in [
            &[][..],
            &[-f64::MAX, -0.0, f64::from_bits(1), f64::MAX],
            &[1.0, 1.001, 1.005, 2.0],
        ] {
            sketch.reset();
            let fresh = UddSketch::new(limit, 0.001).unwrap();
            assert_eq!(sketch.serialize(), fresh.serialize());
            assert_eq!(sketch.quantile(0.5).unwrap(), None);
            for &value in values {
                sketch.update(value).unwrap();
            }
            let expected = build(values, limit, 0.001);
            assert_eq!(sketch.serialize(), expected.serialize());
            assert_eq!(sketch.relative_error(), expected.relative_error());
        }
        sketch.reset();
        sketch.update_weighted(1.0, u64::MAX).unwrap();
        sketch.reset();
        sketch.update(2.0).unwrap();
        assert_eq!(sketch.total_weight(), 1);
        assert_eq!(sketch.quantile(0.5).unwrap(), Some(2.0));
    }
}

#[test]
fn exact_weighted_rank_above_float_integer_precision() {
    let mut sketch = UddSketch::default();
    sketch.update_weighted(1.0, 1u64 << 63).unwrap();
    sketch.update_weighted(2.0, (1u64 << 63) - 1).unwrap();
    assert_eq!(sketch.quantile(0.5).unwrap(), Some(1.0));
    assert_eq!(
        sketch
            .quantile(f64::from_bits(0.5f64.to_bits() + 1))
            .unwrap(),
        Some(2.0)
    );
}

#[test]
fn weighted_intervals_and_gaps_match_singleton_merges() {
    let values = [
        1.001,
        1.009,
        1.005,
        1.002,
        1.0095,
        1.0005,
        2.0,
        0.5,
        -1.009,
        -1.001,
        -1.005,
        -1.0095,
        -1.0005,
        -2.0,
        0.0,
        -0.0,
        f64::from_bits(1),
        f64::from_bits(5),
        f64::from_bits(3),
        f64::MAX,
        -f64::MAX,
    ];
    for limit in [3, 16, 512] {
        for accuracy in [1e-9, 0.01, 0.9] {
            let mut updated = UddSketch::new(limit, accuracy).unwrap();
            let mut merged = UddSketch::new(limit, accuracy).unwrap();
            for (i, value) in values.into_iter().enumerate() {
                let weight = i as u64 + 1;
                updated.update_weighted(value, weight).unwrap();
                let mut singleton = UddSketch::new(limit, accuracy).unwrap();
                singleton.update_weighted(value, weight).unwrap();
                merged.merge(&singleton).unwrap();
                assert_eq!(updated.serialize(), merged.serialize());
                assert_eq!(updated.relative_error(), merged.relative_error());
            }
        }
    }
}

#[test]
fn retained_boundaries_and_saturated_weights_match_merges() {
    let values = [
        -f64::MAX,
        -3.0,
        -2.0,
        -1.0,
        -f64::from_bits(5),
        -f64::from_bits(1),
        0.0,
        f64::from_bits(1),
        f64::from_bits(5),
        1.0,
        2.0,
        3.0,
        f64::MAX,
    ];
    // Every retained extremum comes from this input. Probe either side of each one,
    // along with gaps, interiors, and both zero encodings.
    let mut probes = vec![-2.5, -1.5, -0.0, 0.0, 1.5, 2.5];
    for value in values {
        probes.push(value);
        for bits in [
            value.to_bits().saturating_sub(1),
            value.to_bits().saturating_add(1),
        ] {
            let neighbor = f64::from_bits(bits);
            if neighbor.is_finite() {
                probes.push(neighbor);
            }
        }
    }
    for limit in [3, 16, 512] {
        for accuracy in [1e-9, 0.01, 0.9, f64::from_bits(1.0f64.to_bits() - 1)] {
            for input in [&[][..], &values[..]] {
                let base = build(input, limit, accuracy);
                for &value in &probes {
                    for weight in [0, 1, u64::MAX - base.total_weight()] {
                        let mut updated = base.clone();
                        updated.update_weighted(value, weight).unwrap();
                        // A singleton merge bypasses the interval-based insertion path.
                        let mut singleton = UddSketch::new(limit, accuracy).unwrap();
                        singleton.update_weighted(value, weight).unwrap();
                        let mut merged = base.clone();
                        merged.merge(&singleton).unwrap();
                        assert_eq!(updated.serialize(), merged.serialize());
                        assert_eq!(updated.relative_error(), merged.relative_error());
                        assert_eq!(
                            updated.quantiles(&[0.0, 0.5, 0.99, 1.0]).unwrap(),
                            merged.quantiles(&[0.0, 0.5, 0.99, 1.0]).unwrap()
                        );
                        if updated.total_weight() == u64::MAX {
                            let bytes = updated.serialize();
                            assert!(updated.update(value).is_err());
                            updated.update_weighted(value, 0).unwrap();
                            assert_eq!(updated.serialize(), bytes);
                        }
                    }
                }
            }
        }
    }
}
