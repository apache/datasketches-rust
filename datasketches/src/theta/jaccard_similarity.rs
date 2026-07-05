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
//!
//! The Jaccard similarity index is `J(A, B) = |A intersection B| / |A union B|`.
//! It measures how similar two sketches are: `1.0` means they are considered equal,
//! `0.0` means they are disjoint, and `0.95` means the overlap is 95% of the union.

use crate::common::bounds_binomial_proportions;
use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::theta::CompactThetaSketch;
use crate::theta::ThetaIntersection;
use crate::theta::ThetaSketchView;
use crate::theta::union::ThetaUnion;

const NUM_STD_DEVS: f64 = 2.0;

/// Jaccard similarity result for two Theta sketches.
///
/// The entries are lower bound, estimate, and upper bound, matching the C++
/// `theta_jaccard_similarity::jaccard` result order. The bounds use a 95.4%
/// confidence interval, equivalent to +/- 2 standard deviations.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct JaccardSimilarity {
    lower_bound: f64,
    estimate: f64,
    upper_bound: f64,
}

impl JaccardSimilarity {
    /// Computes the Jaccard similarity index with the default update seed.
    ///
    /// The returned value contains lower bound, estimate, and upper bound. For very large
    /// sketches, where the configured nominal entries are `2^25` or `2^26`, this method may
    /// produce unstable results.
    pub fn between<A: ThetaSketchView, B: ThetaSketchView>(
        sketch_a: &A,
        sketch_b: &B,
    ) -> Result<Self, Error> {
        Self::between_with_seed(sketch_a, sketch_b, DEFAULT_UPDATE_SEED)
    }

    /// Computes the Jaccard similarity index with an explicit update seed.
    ///
    /// The returned value contains lower bound, estimate, and upper bound. For very large
    /// sketches, where the configured nominal entries are `2^25` or `2^26`, this method may
    /// produce unstable results.
    ///
    /// Returns an error if a non-empty sketch was built with a different seed.
    pub fn between_with_seed<A: ThetaSketchView, B: ThetaSketchView>(
        sketch_a: &A,
        sketch_b: &B,
        seed: u64,
    ) -> Result<Self, Error> {
        if sketch_a.is_empty() && sketch_b.is_empty() {
            return Ok(Self::exact(1.0));
        }
        if sketch_a.is_empty() || sketch_b.is_empty() {
            return Ok(Self::exact(0.0));
        }

        let union = ThetaUnion::compute(sketch_a, sketch_b, seed)?;
        if identical_sets(sketch_a, sketch_b, &union) {
            return Ok(Self::exact(1.0));
        }

        let mut intersection = ThetaIntersection::new(seed);
        intersection.update(sketch_a)?;
        intersection.update(sketch_b)?;
        // Ensure the numerator sketch is a subset of the denominator sketch used by
        // the ratio bounds calculation.
        intersection.update(&union)?;
        let intersection = intersection.result_with_ordered(false);

        ratio_bounds(&union, &intersection)
    }

    /// Returns the approximate lower bound for the Jaccard index.
    pub fn lower_bound(&self) -> f64 {
        self.lower_bound
    }

    /// Returns the estimate of the Jaccard index.
    pub fn estimate(&self) -> f64 {
        self.estimate
    }

    /// Returns the approximate upper bound for the Jaccard index.
    pub fn upper_bound(&self) -> f64 {
        self.upper_bound
    }

    fn exact(value: f64) -> Self {
        Self {
            lower_bound: value,
            estimate: value,
            upper_bound: value,
        }
    }
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
    bounds_binomial_proportions::approximate_lower_bound_on_p(
        a,
        b,
        NUM_STD_DEVS * hacky_adjuster(f),
    )
}

fn upper_bound_for_b_over_a(a: u64, b: u64, f: f64) -> Result<f64, Error> {
    check_ratio_inputs(a, b, f)?;
    if a == 0 {
        return Ok(1.0);
    }
    if f == 1.0 {
        return Ok(b as f64 / a as f64);
    }
    bounds_binomial_proportions::approximate_upper_bound_on_p(
        a,
        b,
        NUM_STD_DEVS * hacky_adjuster(f),
    )
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
