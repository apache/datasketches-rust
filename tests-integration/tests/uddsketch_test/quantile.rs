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

fn exact_lower_quantile_index(rank: f64, len: usize) -> usize {
    const MAX_EXACT_F64_INT: u64 = (1_u64 << 53) - 1;

    let last = (len - 1) as u64;
    if rank <= 0.0 || last == 0 {
        return 0;
    }
    if rank >= 1.0 {
        return last as usize;
    }
    if last <= MAX_EXACT_F64_INT {
        return ((rank * (last as f64)).floor() as u64).min(last) as usize;
    }

    let bits = rank.to_bits();
    let biased_exp = ((bits >> 52) & 0x7ff) as u32;
    if biased_exp == 0 {
        return 0;
    }
    let significand = (bits & 0x000f_ffff_ffff_ffff) | (1_u64 << 52);
    let product = u128::from(significand) * u128::from(last);
    let shift = 1075 - biased_exp;
    if shift >= 128 {
        0
    } else {
        ((product >> shift) as u64).min(last) as usize
    }
}

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
    // Allow a few ULPs of IEEE-754 evaluation rounding and 1 subnormal ULP near zero.
    let subnormal_ulp = f64::from_bits(1);
    let normal_ulp_slack = 4.0 * f64::EPSILON * exact.abs().max(estimate.abs());
    let allowed = alpha * exact.abs() + normal_ulp_slack.max(subnormal_ulp);
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
                let exact_idx = exact_lower_quantile_index(q, sorted.len());
                assert_within_bound(single, sorted[exact_idx], alpha);
            }
        }
    }
}

#[test]
fn lower_discrete_quantile_handles_weights_above_f64_precision() {
    let mut weighted = UddSketch::default();
    weighted.update_with_weight(0.0, 1_u64 << 62);
    weighted.update_with_weight(100.0, 1_u64 << 62);
    assert_eq!(weighted.quantile(0.5).unwrap(), Some(0.0));
    assert_eq!(weighted.quantiles(&[]).unwrap(), Some(vec![]));
    let next_rank = f64::from_bits(0.5_f64.to_bits() + 1);
    assert_within_bound(
        weighted.quantile(next_rank).unwrap().unwrap(),
        100.0,
        weighted.alpha(),
    );
    assert_eq!(weighted.quantile(f64::from_bits(1)).unwrap(), Some(0.0));

    let mut mostly_zeros = UddSketch::default();
    mostly_zeros.update_with_weight(0.0, 19);
    mostly_zeros.update(1.0);
    mostly_zeros.update(2.0);
    assert_within_bound(
        mostly_zeros.quantile(0.95).unwrap().unwrap(),
        1.0,
        mostly_zeros.alpha(),
    );

    let mut geometric = UddSketch::new(512, 0.001).unwrap();
    for i in 0..=100 {
        geometric.update(1.1_f64.powi(i));
    }
    assert_within_bound(
        geometric.quantile(0.95).unwrap().unwrap(),
        1.1_f64.powi(95),
        geometric.alpha(),
    );
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

#[test]
fn boundary_rounding_cases_stay_within_floating_point_slack() {
    let mut subnormal = UddSketch::default();
    for v in [0.0, f64::from_bits(31), 1.0] {
        subnormal.update(v);
    }
    assert_within_bound(
        subnormal.quantile(0.5).unwrap().unwrap(),
        f64::from_bits(31),
        subnormal.alpha(),
    );

    let mut wide_alpha = UddSketch::new(512, 0.935).unwrap();
    for v in [0.0, 886.207_100_591_715_4, 2000.0] {
        wide_alpha.update(v);
    }
    assert_within_bound(
        wide_alpha.quantile(0.5).unwrap().unwrap(),
        886.207_100_591_715_4,
        wide_alpha.alpha(),
    );

    let mut multi_collapse = UddSketch::new(4, 0.005).unwrap();
    for v in [
        2f64.powi(-32),
        2f64.powi(-16),
        1.0,
        2f64.powi(16),
        2f64.powi(32),
    ] {
        multi_collapse.update(v);
    }
    assert_within_bound(
        multi_collapse.quantile(0.5).unwrap().unwrap(),
        1.0,
        multi_collapse.alpha(),
    );
}

#[test]
fn unsorted_uncollapsed_stores_match_sorted_across_queries_and_subsequent_updates() {
    let ranks = [0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0];
    for count in [10usize, 40, 200, 600] {
        let budget = 2048;
        let mut ascending = UddSketch::new(budget, 0.01).unwrap();
        let mut descending = UddSketch::new(budget, 0.01).unwrap();
        for i in 0..count {
            let v = 2f64.powi((i as i32) - 300);
            ascending.update(-v);
            ascending.update(v);
        }
        for i in (0..count).rev() {
            let v = 2f64.powi((i as i32) - 300);
            descending.update(v);
            descending.update(-v);
        }
        assert_eq!(descending.num_collapses(), 0);
        let size_before_extrema = descending.estimated_size();
        assert_eq!(
            descending.quantiles(&[0.0, 1.0]).unwrap(),
            ascending.quantiles(&[0.0, 1.0]).unwrap()
        );
        assert_eq!(descending.estimated_size(), size_before_extrema);
        assert_eq!(descending.serialize(), ascending.serialize());
        assert_eq!(
            descending.quantiles(&ranks).unwrap(),
            ascending.quantiles(&ranks).unwrap()
        );
        for &r in &ranks {
            assert_eq!(
                descending.quantile(r).unwrap().unwrap().to_bits(),
                ascending.quantile(r).unwrap().unwrap().to_bits()
            );
        }
        // Mutating after populating the lazy sorted cache must invalidate it cleanly.
        ascending.update(-0.375);
        ascending.update(0.375);
        descending.update(0.375);
        descending.update(-0.375);
        assert_eq!(descending.serialize(), ascending.serialize());
        assert_eq!(
            descending.quantiles(&ranks).unwrap(),
            ascending.quantiles(&ranks).unwrap()
        );
    }
}
