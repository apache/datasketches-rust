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

use std::f64::consts::LN_2;

use super::MIN_RELATIVE_ACCURACY;
use crate::error::Error;

const SIGNIFICAND_MASK: u64 = 0x000f_ffff_ffff_ffff;
const EXPONENT_MASK: u64 = 0x7ff0_0000_0000_0000;
const EXPONENT_SHIFT: u32 = 52;
const EXPONENT_BIAS: i32 = 1023;
const ONE_BITS: u64 = 0x3ff0_0000_0000_0000;

// Optimal cubic polynomial coefficients on s in [0, 1) minimizing worst-case bucket overhead
// while preserving monotonicity and C^0 continuity across powers of two:
// P(s) = (6/35) s^3 - (3/5) s^2 + (10/7) s, with inf_{s in [0, 1)} (1 + s) P'(s) = 10/7.
const CUBIC_A: f64 = 6.0 / 35.0;
const CUBIC_B: f64 = -3.0 / 5.0;
const CUBIC_C: f64 = 10.0 / 7.0;
const CUBIC_D0: f64 = CUBIC_B * CUBIC_B - 3.0 * CUBIC_A * CUBIC_C;
const MAPPING_EQUALITY_REL_TOL: f64 = 1e-12;

/// Index mapping curve used by a [`DdSketch`](super::DdSketch) to map positive magnitudes to
/// integer bucket indices.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum MappingKind {
    /// Exact logarithmic mapping (`floor(ln(x) / ln(gamma))`).
    ///
    /// Uses the fewest buckets for a target relative accuracy at the cost of evaluating a natural
    /// logarithm on each update.
    Logarithmic,
    /// Base-2 exponent extraction with linear significand interpolation.
    ///
    /// Avoids transcendental math on updates and queries, using about 44% more buckets than
    /// [`MappingKind::Logarithmic`] for the same relative accuracy.
    Linear,
    /// Base-2 exponent extraction with cubic significand interpolation (default).
    ///
    /// Evaluates a degree-3 polynomial on the IEEE 754 significand, using under 1% more buckets
    /// than [`MappingKind::Logarithmic`] with fast, platform-independent updates.
    #[default]
    Cubic,
}

#[derive(Debug, Clone, Copy)]
pub struct IndexMapping {
    kind: MappingKind,
    gamma: f64,
    index_offset: f64,
    multiplier: f64,
    relative_accuracy: f64,
    min_indexable_value: f64,
    min_index: i32,
    max_index: i32,
}

impl IndexMapping {
    pub fn from_accuracy(kind: MappingKind, relative_accuracy: f64) -> Result<Self, Error> {
        if !(MIN_RELATIVE_ACCURACY..1.0).contains(&relative_accuracy) {
            return Err(Error::invalid_argument(format!(
                "relative_accuracy must be in [{MIN_RELATIVE_ACCURACY}, 1.0), got {relative_accuracy}"
            )));
        }

        let exact_gamma = (1.0 + relative_accuracy) / (1.0 - relative_accuracy);
        let (gamma, index_offset) = match kind {
            MappingKind::Logarithmic => (exact_gamma, 0.0),
            MappingKind::Linear => {
                let gamma = exact_gamma.powf(LN_2);
                // Matches the historical offset convention of Datadog's
                // LinearlyInterpolatedMapping.
                let index_offset =
                    1.0 / (2.0 * relative_accuracy / (1.0 - relative_accuracy)).ln_1p();
                (gamma, index_offset)
            }
            MappingKind::Cubic => (exact_gamma.powf(10.0 * LN_2 / 7.0), 0.0),
        };

        Self::build(kind, gamma, index_offset).map_err(Error::invalid_argument)
    }

    pub fn from_gamma_offset(
        kind: MappingKind,
        gamma: f64,
        index_offset: f64,
    ) -> Result<Self, Error> {
        Self::build(kind, gamma, index_offset).map_err(Error::deserial)
    }

    fn build(kind: MappingKind, gamma: f64, index_offset: f64) -> Result<Self, String> {
        if !gamma.is_finite() || gamma <= 1.0 {
            return Err(format!("gamma must be finite and > 1.0, got {gamma}"));
        }
        if !index_offset.is_finite() {
            return Err(format!("index_offset must be finite, got {index_offset}"));
        }
        // Canonicalize `-0.0` so equivalent zero offsets serialize and compare identically.
        let index_offset = if index_offset == 0.0 {
            0.0
        } else {
            index_offset
        };

        let ln_gamma = (gamma - 1.0).ln_1p();
        if !ln_gamma.is_finite() || ln_gamma <= 0.0 {
            return Err(format!("gamma is too close to 1.0: {gamma}"));
        }

        let (multiplier, exact_gamma, max_log) = match kind {
            MappingKind::Logarithmic => (1.0 / ln_gamma, gamma, f64::MAX.ln()),
            MappingKind::Linear => (LN_2 / ln_gamma, gamma.powf(1.0 / LN_2), 1024.0),
            MappingKind::Cubic => (LN_2 / ln_gamma, gamma.powf(7.0 / (10.0 * LN_2)), 1024.0),
        };

        if !multiplier.is_finite() || multiplier <= 0.0 || !exact_gamma.is_finite() {
            return Err(format!(
                "invalid mapping parameters: gamma={gamma}, index_offset={index_offset}"
            ));
        }

        let relative_accuracy = if exact_gamma <= 2.0 {
            (exact_gamma - 1.0) / (exact_gamma + 1.0)
        } else {
            1.0 - 2.0 / (1.0 + exact_gamma)
        };
        if !relative_accuracy.is_finite() || relative_accuracy <= 0.0 || relative_accuracy >= 1.0 {
            return Err(format!(
                "mapping relative_accuracy must be in (0.0, 1.0), got {relative_accuracy}"
            ));
        }

        // Ensure every finite normal value up to f64::MAX maps within [i32::MIN, i32::MAX].
        let max_raw_index = max_log * multiplier + index_offset;
        if !max_raw_index.is_finite()
            || max_raw_index > f64::from(i32::MAX)
            || max_raw_index <= f64::from(i32::MIN) + 1.0
        {
            return Err(format!(
                "mapping parameters overflow 32-bit bucket indices at f64::MAX: gamma={gamma}, index_offset={index_offset}"
            ));
        }

        let min_exp_arg = (f64::from(i32::MIN) - index_offset) / multiplier + 1.0;
        let min_from_i32 = match kind {
            MappingKind::Logarithmic => min_exp_arg.exp(),
            MappingKind::Linear | MappingKind::Cubic => min_exp_arg.exp2(),
        };
        let min_from_normal = f64::MIN_POSITIVE * exact_gamma;
        let min_indexable_value = min_from_i32.max(min_from_normal);
        if !min_indexable_value.is_finite() || min_indexable_value >= f64::MAX {
            return Err(format!(
                "mapping has empty indexable domain: min_indexable_value={min_indexable_value}"
            ));
        }

        let first_raw =
            approx_log_of(kind, min_indexable_value.next_up()) * multiplier + index_offset;
        let last_raw = approx_log_of(kind, f64::MAX) * multiplier + index_offset;
        if !first_raw.is_finite()
            || first_raw < f64::from(i32::MIN)
            || !last_raw.is_finite()
            || last_raw > f64::from(i32::MAX)
        {
            return Err(format!(
                "mapping parameters overflow 32-bit bucket indices: gamma={gamma}, index_offset={index_offset}"
            ));
        }
        // Allow the 1-bin shift when `first_raw` lands on an exact negative integer in reference
        // Java/Go encoders (`(int) raw - 1`).
        let min_index = if first_raw < 0.0 && first_raw.fract() == 0.0 {
            (first_raw as i32).saturating_sub(1)
        } else {
            first_raw.floor() as i32
        };
        let max_index = last_raw.floor() as i32;
        if min_index > max_index {
            return Err(format!(
                "mapping has inverted index bounds [{min_index}, {max_index}]"
            ));
        }

        Ok(Self {
            kind,
            gamma,
            index_offset,
            multiplier,
            relative_accuracy,
            min_indexable_value,
            min_index,
            max_index,
        })
    }

    #[inline]
    pub const fn kind(&self) -> MappingKind {
        self.kind
    }

    #[inline]
    pub const fn gamma(&self) -> f64 {
        self.gamma
    }

    #[inline]
    pub const fn index_offset(&self) -> f64 {
        self.index_offset
    }

    #[inline]
    pub const fn relative_accuracy(&self) -> f64 {
        self.relative_accuracy
    }

    #[inline]
    pub const fn min_indexable_value(&self) -> f64 {
        self.min_indexable_value
    }

    #[inline]
    pub fn is_compatible(&self, other: &Self) -> bool {
        self.kind == other.kind
            && within_tolerance(self.gamma, other.gamma, MAPPING_EQUALITY_REL_TOL)
            && within_tolerance(
                self.index_offset,
                other.index_offset,
                MAPPING_EQUALITY_REL_TOL,
            )
    }

    #[inline]
    pub fn index(&self, value: f64) -> i32 {
        debug_assert!(value > self.min_indexable_value && value.is_finite());
        let approx_log = approx_log_of(self.kind, value);
        let raw = (approx_log * self.multiplier + self.index_offset).floor();
        debug_assert!(raw >= f64::from(i32::MIN) && raw <= f64::from(i32::MAX));
        raw as i32
    }

    #[inline]
    pub fn value(&self, index: i32) -> f64 {
        let lower = self.lower_bound(index);
        if !lower.is_finite() {
            return f64::MAX;
        }
        (lower * (1.0 + self.relative_accuracy)).min(f64::MAX)
    }

    #[inline]
    pub fn lower_bound(&self, index: i32) -> f64 {
        let scaled = (f64::from(index) - self.index_offset) / self.multiplier;
        match self.kind {
            MappingKind::Logarithmic => scaled.exp(),
            MappingKind::Linear => linear_exp(scaled),
            MappingKind::Cubic => cubic_exp(scaled),
        }
    }

    #[inline]
    pub const fn max_indexable_index(&self) -> i32 {
        self.max_index
    }

    #[inline]
    pub const fn contains_valid_index(&self, index: i32) -> bool {
        index >= self.min_index && index <= self.max_index
    }
}

#[inline]
fn within_tolerance(a: f64, b: f64, tolerance: f64) -> bool {
    if a == b {
        return true;
    }
    if a == 0.0 || b == 0.0 {
        return a.abs() <= tolerance && b.abs() <= tolerance;
    }
    (a - b).abs() <= tolerance * a.abs().max(b.abs())
}

#[inline]
fn approx_log_of(kind: MappingKind, value: f64) -> f64 {
    match kind {
        MappingKind::Logarithmic => value.ln(),
        MappingKind::Linear => linear_log(value),
        MappingKind::Cubic => cubic_log(value),
    }
}

#[inline]
fn unpack_normal_f64(value: f64) -> (f64, f64) {
    let bits = value.to_bits();
    debug_assert!((bits & EXPONENT_MASK) != 0, "expected normal positive f64");
    let exponent = (((bits & EXPONENT_MASK) >> EXPONENT_SHIFT) as i32 - EXPONENT_BIAS) as f64;
    let significand = f64::from_bits((bits & SIGNIFICAND_MASK) | ONE_BITS) - 1.0;
    (exponent, significand)
}

#[inline]
fn linear_log(value: f64) -> f64 {
    let (e, s) = unpack_normal_f64(value);
    e + s
}

#[inline]
fn cubic_log(value: f64) -> f64 {
    let (e, s) = unpack_normal_f64(value);
    ((CUBIC_A * s + CUBIC_B) * s + CUBIC_C) * s + e
}

#[inline]
fn linear_exp(x: f64) -> f64 {
    let exp_f = x.floor();
    if exp_f > 1024.0 {
        return f64::INFINITY;
    }
    if exp_f < -1075.0 {
        return 0.0;
    }
    let s = (x - exp_f).clamp(0.0, 1.0);
    ldexp_f64(1.0 + s, exp_f as i32)
}

#[inline]
fn cubic_exp(x: f64) -> f64 {
    let exp_f = x.floor();
    if exp_f > 1024.0 {
        return f64::INFINITY;
    }
    if exp_f < -1075.0 {
        return 0.0;
    }
    let frac = x - exp_f;
    // Exact real root of A*s^3 + B*s^2 + C*s - frac = 0 on [0, 1] via Cardano's formula.
    let d1 = 2.0 * CUBIC_B * CUBIC_B * CUBIC_B
        - 9.0 * CUBIC_A * CUBIC_B * CUBIC_C
        - 27.0 * CUBIC_A * CUBIC_A * frac;
    let disc = (d1 * d1 - 4.0 * CUBIC_D0 * CUBIC_D0 * CUBIC_D0).max(0.0);
    let p = ((d1 - disc.sqrt()) * 0.5).cbrt();
    let s = (-(CUBIC_B + p + CUBIC_D0 / p) / (3.0 * CUBIC_A)).clamp(0.0, 1.0);
    ldexp_f64(1.0 + s, exp_f as i32)
}

#[inline]
fn ldexp_f64(x: f64, exp: i32) -> f64 {
    let step1 = exp.clamp(-1022, 1023);
    let step2 = (exp - step1).clamp(-1022, 1023);
    let step3 = exp - step1 - step2;
    let pow2 = |e: i32| f64::from_bits(((e + EXPONENT_BIAS) as u64) << EXPONENT_SHIFT);
    ((x * pow2(step1)) * pow2(step2)) * pow2(step3)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mappings_respect_relative_accuracy_at_extremes_and_powers_of_two() {
        for kind in [
            MappingKind::Logarithmic,
            MappingKind::Linear,
            MappingKind::Cubic,
        ] {
            for alpha in [1e-4, 0.005, 0.01, 0.05, 0.25] {
                let mapping = IndexMapping::from_accuracy(kind, alpha).unwrap();
                let min_v = mapping.min_indexable_value() * 1.000_001;
                let samples = [
                    min_v,
                    1e-300,
                    1e-12,
                    0.5,
                    1.0 - f64::EPSILON,
                    1.0,
                    1.0 + f64::EPSILON,
                    1.5,
                    2.0 - f64::EPSILON,
                    2.0,
                    std::f64::consts::PI,
                    1e12,
                    1e300,
                    f64::MAX * 0.999,
                    f64::MAX,
                ];
                for x in samples {
                    let idx = mapping.index(x);
                    let rep = mapping.value(idx);
                    assert!(rep.is_finite() && rep > 0.0, "{kind:?} alpha={alpha} x={x}");
                    let rel_err = (rep - x).abs() / x;
                    // Allow a tiny slack for transcendental rounding near bucket boundaries.
                    assert!(
                        rel_err <= alpha + 1e-12,
                        "{kind:?} alpha={alpha} x={x}: rep={rep} rel_err={rel_err}"
                    );
                }
            }
        }
    }

    #[test]
    fn cubic_and_linear_indices_are_monotone_across_significand_boundaries() {
        for kind in [MappingKind::Linear, MappingKind::Cubic] {
            let mapping = IndexMapping::from_accuracy(kind, 0.01).unwrap();
            let mut prev_idx = i32::MIN;
            for exp in -10..=10 {
                let base = 2.0_f64.powi(exp);
                for step in 0..256 {
                    let x = base * (1.0 + f64::from(step) / 256.0);
                    let idx = mapping.index(x);
                    assert!(idx >= prev_idx, "{kind:?} non-monotone at x={x}");
                    prev_idx = idx;
                }
            }
        }
    }
}
