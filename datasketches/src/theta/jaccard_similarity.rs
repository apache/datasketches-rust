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

//! Jaccard similarity for Theta sketches.

use std::collections::BTreeSet;

use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::compute_seed_hash;
use crate::theta::CompactThetaSketch;
use crate::theta::ThetaIntersection;
use crate::theta::ThetaSketchView;

const NUM_STD_DEVS: f64 = 2.0;

/// Jaccard similarity result for two Theta sketches.
///
/// The entries are lower bound, estimate, and upper bound, matching the C++
/// `theta_jaccard_similarity::jaccard` result order.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct JaccardSimilarity {
    /// Approximate lower bound for the Jaccard index.
    pub lower_bound: f64,
    /// Estimate of the Jaccard index.
    pub estimate: f64,
    /// Approximate upper bound for the Jaccard index.
    pub upper_bound: f64,
}

impl JaccardSimilarity {
    fn exact(value: f64) -> Self {
        Self {
            lower_bound: value,
            estimate: value,
            upper_bound: value,
        }
    }
}

/// Computes Jaccard similarity between Theta sketches.
pub struct ThetaJaccardSimilarity;

impl ThetaJaccardSimilarity {
    /// Computes the Jaccard similarity index with the default update seed.
    ///
    /// The Jaccard index is `|A intersection B| / |A union B|`. The returned value contains
    /// lower bound, estimate, and upper bound.
    pub fn jaccard<A: ThetaSketchView, B: ThetaSketchView>(
        sketch_a: &A,
        sketch_b: &B,
    ) -> Result<JaccardSimilarity, Error> {
        Self::jaccard_with_seed(sketch_a, sketch_b, DEFAULT_UPDATE_SEED)
    }

    /// Computes the Jaccard similarity index with an explicit update seed.
    ///
    /// Returns an error if a non-empty sketch was built with a different seed.
    pub fn jaccard_with_seed<A: ThetaSketchView, B: ThetaSketchView>(
        sketch_a: &A,
        sketch_b: &B,
        seed: u64,
    ) -> Result<JaccardSimilarity, Error> {
        if sketch_a.is_empty() && sketch_b.is_empty() {
            return Ok(JaccardSimilarity::exact(1.0));
        }
        if sketch_a.is_empty() || sketch_b.is_empty() {
            return Ok(JaccardSimilarity::exact(0.0));
        }

        let union = compute_union(sketch_a, sketch_b, seed)?;
        if identical_sets(sketch_a, sketch_b, &union) {
            return Ok(JaccardSimilarity::exact(1.0));
        }

        let mut intersection = ThetaIntersection::new(seed);
        intersection.update(sketch_a)?;
        intersection.update(sketch_b)?;
        intersection.update(&union)?;
        let intersection = intersection.result_with_ordered(false);

        ratio_bounds(&union, &intersection)
    }
}

fn compute_union<A: ThetaSketchView, B: ThetaSketchView>(
    sketch_a: &A,
    sketch_b: &B,
    seed: u64,
) -> Result<CompactThetaSketch, Error> {
    let expected_seed_hash = compute_seed_hash(seed);
    if !sketch_a.is_empty() && sketch_a.seed_hash() != expected_seed_hash {
        return Err(Error::invalid_argument(format!(
            "incompatible seed hash for sketch A: expected {}, got {}",
            expected_seed_hash,
            sketch_a.seed_hash()
        )));
    }
    if !sketch_b.is_empty() && sketch_b.seed_hash() != expected_seed_hash {
        return Err(Error::invalid_argument(format!(
            "incompatible seed hash for sketch B: expected {}, got {}",
            expected_seed_hash,
            sketch_b.seed_hash()
        )));
    }

    let theta = sketch_a.theta64().min(sketch_b.theta64());
    let mut entries = BTreeSet::new();
    entries.extend(sketch_a.iter().filter(|&hash| hash < theta));
    entries.extend(sketch_b.iter().filter(|&hash| hash < theta));

    Ok(CompactThetaSketch::from_parts(
        entries.into_iter().collect(),
        theta,
        expected_seed_hash,
        false,
        false,
    ))
}

fn identical_sets<A: ThetaSketchView, B: ThetaSketchView>(
    sketch_a: &A,
    sketch_b: &B,
    union: &CompactThetaSketch,
) -> bool {
    union.num_retained() == sketch_a.num_retained()
        && union.num_retained() == sketch_b.num_retained()
        && union.theta64() == sketch_a.theta64()
        && union.theta64() == sketch_b.theta64()
}

fn ratio_bounds(
    sketch_a: &CompactThetaSketch,
    sketch_b: &CompactThetaSketch,
) -> Result<JaccardSimilarity, Error> {
    let theta_a = sketch_a.theta64();
    let theta_b = sketch_b.theta64();
    if theta_b > theta_a {
        return Err(Error::invalid_argument(format!(
            "theta_a must be <= theta_b: theta_a={theta_a}, theta_b={theta_b}"
        )));
    }

    let count_b = sketch_b.num_retained() as u64;
    let count_a = if theta_a == theta_b {
        sketch_a.num_retained() as u64
    } else {
        sketch_a.iter().filter(|&hash| hash < theta_b).count() as u64
    };

    if count_a == 0 {
        return Ok(JaccardSimilarity {
            lower_bound: 0.0,
            estimate: 0.5,
            upper_bound: 1.0,
        });
    }

    let f = sketch_b.theta();
    Ok(JaccardSimilarity {
        lower_bound: lower_bound_for_b_over_a(count_a, count_b, f)?,
        estimate: count_b as f64 / count_a as f64,
        upper_bound: upper_bound_for_b_over_a(count_a, count_b, f)?,
    })
}

fn lower_bound_for_b_over_a(a: u64, b: u64, f: f64) -> Result<f64, Error> {
    check_ratio_inputs(a, b, f)?;
    if a == 0 {
        return Ok(0.0);
    }
    if f == 1.0 {
        return Ok(b as f64 / a as f64);
    }
    Ok(approximate_lower_bound_on_p(
        a,
        b,
        NUM_STD_DEVS * hacky_adjuster(f),
    ))
}

fn upper_bound_for_b_over_a(a: u64, b: u64, f: f64) -> Result<f64, Error> {
    check_ratio_inputs(a, b, f)?;
    if a == 0 {
        return Ok(1.0);
    }
    if f == 1.0 {
        return Ok(b as f64 / a as f64);
    }
    Ok(approximate_upper_bound_on_p(
        a,
        b,
        NUM_STD_DEVS * hacky_adjuster(f),
    ))
}

fn check_ratio_inputs(a: u64, b: u64, f: f64) -> Result<(), Error> {
    if a < b {
        return Err(Error::invalid_argument(format!(
            "a must be >= b: a = {a}, b = {b}"
        )));
    }
    if !(0.0..=1.0).contains(&f) || f == 0.0 {
        return Err(Error::invalid_argument(format!(
            "f must be in the range (0.0, 1.0], got {f}"
        )));
    }
    Ok(())
}

fn hacky_adjuster(f: f64) -> f64 {
    let tmp = (1.0 - f).sqrt();
    if f <= 0.5 {
        tmp
    } else {
        tmp + (0.01 * (f - 0.5))
    }
}

fn approximate_lower_bound_on_p(n: u64, k: u64, num_std_devs: f64) -> f64 {
    if n == 0 || k == 0 {
        0.0
    } else if k == 1 {
        exact_lower_bound_on_p_k_eq_1(n, delta_of_num_stdevs(num_std_devs))
    } else if k == n {
        exact_lower_bound_on_p_k_eq_n(n, delta_of_num_stdevs(num_std_devs))
    } else {
        let x = abramowitz_stegun_formula_26p5p22((n - k) as f64 + 1.0, k as f64, -num_std_devs);
        1.0 - x
    }
}

fn approximate_upper_bound_on_p(n: u64, k: u64, num_std_devs: f64) -> f64 {
    if n == 0 || k == n {
        1.0
    } else if k == n - 1 {
        exact_upper_bound_on_p_k_eq_minusone(n, delta_of_num_stdevs(num_std_devs))
    } else if k == 0 {
        exact_upper_bound_on_p_k_eq_zero(n, delta_of_num_stdevs(num_std_devs))
    } else {
        let x = abramowitz_stegun_formula_26p5p22((n - k) as f64, k as f64 + 1.0, num_std_devs);
        1.0 - x
    }
}

fn delta_of_num_stdevs(kappa: f64) -> f64 {
    normal_cdf(-kappa)
}

fn normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / 2.0_f64.sqrt()))
}

fn erf(x: f64) -> f64 {
    if x < 0.0 {
        -erf_of_nonneg(-x)
    } else {
        erf_of_nonneg(x)
    }
}

fn erf_of_nonneg(x: f64) -> f64 {
    let a1 = 0.0705230784;
    let a2 = 0.0422820123;
    let a3 = 0.0092705272;
    let a4 = 0.0001520143;
    let a5 = 0.0002765672;
    let a6 = 0.0000430638;
    let x2 = x * x;
    let x3 = x2 * x;
    let x4 = x2 * x2;
    let x5 = x2 * x3;
    let x6 = x3 * x3;
    let sum = 1.0 + (a1 * x) + (a2 * x2) + (a3 * x3) + (a4 * x4) + (a5 * x5) + (a6 * x6);
    let sum2 = sum * sum;
    let sum4 = sum2 * sum2;
    let sum8 = sum4 * sum4;
    let sum16 = sum8 * sum8;
    1.0 - (1.0 / sum16)
}

fn abramowitz_stegun_formula_26p5p22(a: f64, b: f64, yp: f64) -> f64 {
    let b2m1 = (2.0 * b) - 1.0;
    let a2m1 = (2.0 * a) - 1.0;
    let lambda = ((yp * yp) - 3.0) / 6.0;
    let htmp = (1.0 / a2m1) + (1.0 / b2m1);
    let h = 2.0 / htmp;
    let term1 = (yp * (h + lambda).sqrt()) / h;
    let term2 = (1.0 / b2m1) - (1.0 / a2m1);
    let term3 = (lambda + (5.0 / 6.0)) - (2.0 / (3.0 * h));
    let w = term1 - (term2 * term3);
    a / (a + (b * (2.0 * w).exp()))
}

fn exact_upper_bound_on_p_k_eq_zero(n: u64, delta: f64) -> f64 {
    1.0 - delta.powf(1.0 / n as f64)
}

fn exact_lower_bound_on_p_k_eq_n(n: u64, delta: f64) -> f64 {
    delta.powf(1.0 / n as f64)
}

fn exact_lower_bound_on_p_k_eq_1(n: u64, delta: f64) -> f64 {
    1.0 - (1.0 - delta).powf(1.0 / n as f64)
}

fn exact_upper_bound_on_p_k_eq_minusone(n: u64, delta: f64) -> f64 {
    (1.0 - delta).powf(1.0 / n as f64)
}
