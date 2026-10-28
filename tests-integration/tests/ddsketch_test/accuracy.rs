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

use super::assert_relative_close;
use super::next_random;

fn check_quantiles(
    mut values: Vec<f64>,
    limit: u32,
    accuracy: f64,
    direction: CollapseDirection,
) -> DdSketch {
    let mut sketch = DdSketch::with_collapse_direction(limit, accuracy, direction).unwrap();
    for &value in &values {
        sketch.update(value).unwrap();
        assert!(sketch.num_buckets() <= 2 * limit as usize + 1);
    }
    values.sort_by(f64::total_cmp);
    let mut previous = f64::NEG_INFINITY;
    for i in 0..=1024 {
        let q = i as f64 / 1024.0;
        let exact = values[i * (values.len() - 1) / 1024];
        let estimate = sketch.quantile(q).unwrap().unwrap();
        assert!(
            estimate >= previous && estimate >= values[0] && estimate <= *values.last().unwrap()
        );
        previous = estimate;
        if !sketch.quantile_is_collapsed(q).unwrap().unwrap() {
            if exact.abs() < sketch.zero_threshold() {
                assert!((estimate - exact).abs() < sketch.zero_threshold());
            } else {
                assert_relative_close(estimate, exact, accuracy);
            }
        }
    }
    assert_eq!(sketch.quantile(0.0).unwrap(), values.first().copied());
    assert_eq!(sketch.quantile(1.0).unwrap(), values.last().copied());
    sketch
}

#[test]
fn value_accuracy_across_distributions() {
    let mut state = 123;
    let mut uniform = Vec::new();
    let mut log_normal = Vec::new();
    let mut pareto = Vec::new();
    let mut repeated = Vec::new();
    for i in 0..4097 {
        let u = ((next_random(&mut state) >> 11) as f64 + 0.5) / (1u64 << 53) as f64;
        let v = ((next_random(&mut state) >> 11) as f64 + 0.5) / (1u64 << 53) as f64;
        uniform.push(1000.0 * (u - 0.5));
        log_normal.push((3.0 * (-2.0 * u.ln()).sqrt() * (std::f64::consts::TAU * v).cos()).exp());
        pareto.push(u.powf(-2.0));
        repeated.push([0.0, 0.001, 10.0, 10000.0][i % 4]);
    }
    for values in [uniform, log_normal, pareto, repeated] {
        for direction in [CollapseDirection::Lowest, CollapseDirection::Highest] {
            assert!(!check_quantiles(values.clone(), 2048, 0.01, direction).is_collapsed());
            assert!(check_quantiles(values.clone(), 32, 0.01, direction).is_collapsed());
        }
    }
}

#[test]
fn floating_point_domain_extremes_and_sparse_gaps() {
    let mut state = 42;
    let mut values = vec![
        f64::MAX,
        -f64::MAX,
        f64::MIN_POSITIVE,
        -f64::MIN_POSITIVE,
        0.0,
    ];
    for _ in 0..4092 {
        let bits = next_random(&mut state);
        let value = f64::from_bits(bits);
        values.push(if value.is_finite() { value } else { 0.0 });
    }
    for direction in [CollapseDirection::Lowest, CollapseDirection::Highest] {
        assert!(!check_quantiles(values.clone(), 100_000, 0.01, direction).is_collapsed());
        assert!(check_quantiles(values.clone(), 8, 1e-9, direction).is_collapsed());
    }
}

#[test]
fn logarithmic_boundaries_and_supported_accuracy_extremes() {
    for accuracy in [
        1e-9,
        0.001,
        0.01,
        1.0 / 3.0,
        0.9,
        f64::from_bits(1.0f64.to_bits() - 1),
    ] {
        for exponent in [-1022, -100, -1, 0, 1, 100, 1023] {
            let center = 2.0f64.powi(exponent);
            let mut values = vec![
                center,
                f64::from_bits(center.to_bits() - 1),
                f64::from_bits(center.to_bits() + 1),
            ];
            for factor in [1.0 - accuracy, 1.0 + accuracy] {
                let value = center * factor;
                if value.is_finite() {
                    values.push(value);
                }
            }
            for direction in [CollapseDirection::Lowest, CollapseDirection::Highest] {
                check_quantiles(values.clone(), 64, accuracy, direction);
                check_quantiles(values.iter().map(|v| -v).collect(), 64, accuracy, direction);
            }
        }
    }
}

#[test]
fn collapse_direction_follows_numeric_order_for_each_sign() {
    for sign in [-1.0, 1.0] {
        for direction in [CollapseDirection::Lowest, CollapseDirection::Highest] {
            let mut sketch = DdSketch::with_collapse_direction(4, 0.1, direction).unwrap();
            let mut values = Vec::new();
            for i in 0..17 {
                let value = sign * 1.3f64.powi(i);
                values.push(value);
                sketch.update(value).unwrap();
            }
            values.sort_by(f64::total_cmp);
            assert!(sketch.is_collapsed());
            let (lost, retained) = match direction {
                CollapseDirection::Lowest => (0.125, 0.9375),
                CollapseDirection::Highest => (0.9375, 0.0625),
            };
            assert_eq!(sketch.quantile_is_collapsed(lost).unwrap(), Some(true));
            assert_eq!(sketch.quantile_is_collapsed(retained).unwrap(), Some(false));
            assert_relative_close(
                sketch.quantile(retained).unwrap().unwrap(),
                values[(retained * 16.0) as usize],
                0.1,
            );
            for q in [0.0, 1.0] {
                assert_eq!(sketch.quantile_is_collapsed(q).unwrap(), Some(false));
            }
        }
    }
}

#[test]
fn single_bucket_preserves_zeros_and_exact_extrema() {
    for direction in [CollapseDirection::Lowest, CollapseDirection::Highest] {
        let mut sketch = DdSketch::with_collapse_direction(1, 0.01, direction).unwrap();
        for value in [-1e300, -10.0, 0.0, 10.0, 1e300] {
            sketch.update(value).unwrap();
        }
        assert_eq!(sketch.num_buckets(), 3);
        assert_eq!(sketch.quantile(0.5).unwrap(), Some(0.0));
        assert_eq!(sketch.quantile_is_collapsed(0.5).unwrap(), Some(false));
        assert_eq!(sketch.min_value(), Some(-1e300));
        assert_eq!(sketch.max_value(), Some(1e300));
        assert_eq!(sketch.quantile_is_collapsed(0.25).unwrap(), Some(true));
        assert_eq!(sketch.quantile_is_collapsed(0.75).unwrap(), Some(true));
    }
}
