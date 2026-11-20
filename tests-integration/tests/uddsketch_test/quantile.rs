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

use datasketches::uddsketch::UddSketch;

fn assert_within_bound(estimate: f64, exact: f64, alpha: f64) {
    if exact == 0.0 {
        assert_eq!(estimate, 0.0);
        return;
    }
    assert_eq!(
        estimate.is_sign_positive(),
        exact.is_sign_positive(),
        "sign mismatch: estimate={estimate:e}, exact={exact:e}"
    );
    // Allow 0.5 ULP of the estimate (and 0.5 * 2^-1074 in the subnormal range).
    let subnormal_half_ulp = 0.5 * f64::from_bits(1);
    let normal_ulp_slack = 2.0 * f64::EPSILON * exact.abs();
    let allowed = alpha * exact.abs() + normal_ulp_slack.max(subnormal_half_ulp);
    let actual_err = (estimate - exact).abs();
    assert!(
        actual_err <= allowed,
        "estimate={estimate:e}, exact={exact:e}, err={actual_err:e}, allowed={allowed:e}, alpha={alpha}"
    );
}

#[test]
fn quantile_satisfies_relative_error_bound_across_distributions_and_minimum_budget() {
    let configs = [(4u32, 0.05), (32, 0.01), (512, 0.005)];
    let ranks = [0.0, 0.001, 0.05, 0.25, 0.5, 0.75, 0.95, 0.999, 1.0];

    let mut datasets: Vec<Vec<f64>> = Vec::new();

    // Heavy-tailed signed Pareto-like + zeros.
    let mut pareto = Vec::with_capacity(5000);
    for i in 1..=5000 {
        let u = (i as f64) / 5001.0;
        let mag = (1.0 - u).powf(-2.5);
        pareto.push(match i % 7 {
            0 => 0.0,
            1 | 2 => -mag,
            _ => mag,
        });
    }
    datasets.push(pareto);

    // Extreme dynamic range spanning smallest subnormals through f64::MAX on both signs.
    let mut extreme = vec![
        -f64::MAX,
        -1e200,
        -1.0,
        -f64::MIN_POSITIVE,
        -f64::from_bits(2),
        -f64::from_bits(1),
        0.0,
        f64::from_bits(1),
        f64::from_bits(2),
        f64::MIN_POSITIVE,
        1.0,
        1e200,
        f64::MAX,
    ];
    for exp in (-1074..=1023).step_by(19) {
        let mag = if exp < -1022 {
            f64::from_bits(1u64 << (exp + 1074))
        } else {
            f64::from_bits(((exp + 1023) as u64) << 52)
        };
        extreme.push(-mag);
        extreme.push(mag);
    }
    datasets.push(extreme);

    for (max_buckets, initial_alpha) in configs {
        for data in &datasets {
            let mut sketch = UddSketch::new(max_buckets, initial_alpha).unwrap();
            for &v in data {
                sketch.update(v);
            }
            assert!(sketch.num_retained() <= max_buckets as usize);

            let mut sorted = data.clone();
            sorted.sort_by(f64::total_cmp);
            assert_eq!(sketch.quantile(0.0).unwrap(), Some(sorted[0]));
            assert_eq!(sketch.quantile(1.0).unwrap(), Some(*sorted.last().unwrap()));

            let alpha = sketch.alpha();
            let batch = sketch.quantiles(&ranks).unwrap().unwrap();
            for (idx, &q) in ranks.iter().enumerate() {
                let single = sketch.quantile(q).unwrap().unwrap();
                assert_eq!(single.to_bits(), batch[idx].to_bits());
                let exact_idx = (q * (sorted.len() - 1) as f64).floor() as usize;
                assert_within_bound(single, sorted[exact_idx], alpha);
            }
        }
    }
}

#[test]
fn lower_discrete_quantile_handles_weights_above_f64_precision() {
    let mut sketch = UddSketch::new(64, 0.01).unwrap();
    let half = 1u64 << 62;
    sketch.update_with_weight(-10.0, half);
    sketch.update_with_weight(0.0, 1);
    sketch.update_with_weight(10.0, half);

    // Total weight is `2^63 + 1`, so `total_weight - 1 = 2^63`; at `rank = 0.5`, exact index is
    // `floor(0.5 * 2^63) = 2^62 = half`, which lands on the single `0.0` item.
    assert_eq!(sketch.quantile(0.5).unwrap(), Some(0.0));
    assert_eq!(sketch.quantiles(&[]).unwrap(), Some(vec![]));
}

#[test]
fn constant_stream_returns_exact_value_for_all_ranks() {
    for value in [-f64::MAX, -42.5, 0.0, f64::from_bits(1), 123.456, f64::MAX] {
        let mut sketch = UddSketch::new(16, 0.05).unwrap();
        sketch.update_with_weight(value, 100);
        for rank in [0.0, 0.1, 0.5, 0.9, 1.0] {
            assert_eq!(sketch.quantile(rank).unwrap(), Some(value));
        }
    }
}
