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
use std::f64::consts::LOG2_E;

/// Fixed-point binary logarithm scale (`2^52` units per octave).
const LOG_UNIT: i64 = 1 << 52;
const LOG_UNIT_F64: f64 = (1u64 << 52) as f64;
const INV_LOG_UNIT_F64: f64 = f64::from_bits((1023 - 52) << 52);

/// Exact Q52 coordinates `round(2^52 * log2(1 + s / 16))` for `s in 0..=16`.
const ANCHOR_LOG2_Q52: [i64; 17] = [
    0,
    393_897_619_263_793,
    765_274_173_176_529,
    1_116_566_257_159_425,
    1_449_835_248_174_819,
    1_766_840_599_037_378,
    2_069_096_066_497_156,
    2_357_913_430_203_731,
    2_634_436_900_273_512,
    2_899_670_496_349_637,
    3_154_500_053_615_718,
    3_399_711_073_450_041,
    3_636_003_326_134_362,
    3_864_002_889_947_501,
    4_084_272_148_448_331,
    4_297_318_147_896_633,
    LOG_UNIT,
];

/// IEEE-754 round-to-nearest-even magic bias (`3 * 2^51`) for values in `[-2^51, 2^51]`.
const ROUND_MAGIC: f64 = 6_755_399_441_055_744.0;

/// Exact Q52 coordinate of the smallest positive `f64` (`2^-1074`).
const MIN_LOG2_Q52: i64 = -1074 * LOG_UNIT;

/// Exact Q52 coordinate of `f64::MAX` (`(2 - 2^-52) * 2^1023`).
const MAX_LOG2_Q52: i64 = 1024 * LOG_UNIT - 1;

/// Safety margin subtracted from raw Q52 step width so rounding in `log2_q52` and `exp2_q52`
/// never causes a bucket's multiplicative span to exceed `gamma_0`.
const STEP_MARGIN_Q52: i64 = 8;

const TWO_LOG2_E_Q52: f64 = 2.0 * LOG2_E * LOG_UNIT_F64;

/// Exact unsigned division by a fixed positive `u64` divisor for dividends `< 2^63`.
#[derive(Clone, Copy, Debug)]
struct FastDiv {
    divisor: u64,
    mul: u64,
    shift: u32,
    is_pow2: bool,
}

impl FastDiv {
    fn new(divisor: u64) -> Self {
        debug_assert!(divisor >= 1);
        let shift = 63 - divisor.leading_zeros();
        if divisor.is_power_of_two() {
            Self {
                divisor,
                mul: 0,
                shift,
                is_pow2: true,
            }
        } else {
            let mul = ((1u128 << (64 + shift)) / u128::from(divisor)) as u64;
            Self {
                divisor,
                mul,
                shift,
                is_pow2: false,
            }
        }
    }

    #[inline(always)]
    fn div(self, n: u64) -> u64 {
        debug_assert!(n < (1u64 << 63));
        if self.is_pow2 {
            return n >> self.shift;
        }
        let approx = ((u128::from(n) * u128::from(self.mul)) >> (64 + self.shift)) as u64;
        approx + u64::from(n - approx * self.divisor >= self.divisor)
    }
}

/// Deterministic logarithmic bucket mapping parameterized by `initial_alpha`.
#[derive(Clone, Copy, Debug)]
pub struct Mapping {
    initial_alpha: f64,
    step: i64,
    div: FastDiv,
}

impl Mapping {
    pub fn new(initial_alpha: f64) -> Self {
        let raw_step = if initial_alpha < 0.25 {
            atanh_log2_q52_wide(initial_alpha)
        } else {
            let gamma = (1.0 + initial_alpha) / (1.0 - initial_alpha);
            log2_q52(gamma)
        };
        let step = (raw_step - STEP_MARGIN_Q52).max(1);
        Self {
            initial_alpha,
            step,
            div: FastDiv::new(step as u64),
        }
    }

    #[inline]
    pub fn initial_alpha(self) -> f64 {
        self.initial_alpha
    }

    /// Computes the effective relative error bound `alpha_c` after `num_collapses` uniform
    /// collapses using the exact recurrence `alpha' = 2 * alpha / (1 + alpha^2)`.
    pub fn alpha_at_level(self, num_collapses: u8) -> f64 {
        let mut alpha = self.initial_alpha;
        for _ in 0..num_collapses {
            alpha = (2.0 * alpha) / (1.0 + alpha * alpha);
        }
        alpha
    }

    /// Maps a finite positive magnitude `value > 0.0` to its bucket key at `num_collapses`.
    #[inline(always)]
    pub fn key_at_level(self, value: f64, num_collapses: u8) -> i64 {
        let coord = log2_q52(value);
        let key0 = if coord > 0 {
            self.div.div((coord - 1) as u64) as i64 + 1
        } else {
            -(self.div.div((-coord) as u64) as i64)
        };
        collapse_key(key0, num_collapses)
    }

    /// Returns the harmonic-mean representative magnitude for positive bucket `key` at
    /// `num_collapses`.
    pub fn value_at_level(self, key: i64, num_collapses: u8) -> f64 {
        let level_step = i128::from(self.step) << num_collapses;
        let lo_raw = i128::from(key - 1).saturating_mul(level_step);
        let hi_raw = i128::from(key).saturating_mul(level_step).saturating_add(1);
        let lo_q52 = lo_raw.clamp(i128::from(MIN_LOG2_Q52), i128::from(MAX_LOG2_Q52)) as i64;
        let hi_q52 = hi_raw.clamp(i128::from(MIN_LOG2_Q52), i128::from(MAX_LOG2_Q52)) as i64;

        let a = exp2_q52(lo_q52);
        let b = exp2_q52(hi_q52);
        if a >= b {
            return a;
        }
        (a / (0.5 + 0.5 * (a / b))).clamp(a, b)
    }
}

/// Collapses a bucket key `shift` levels (`ceil(key / 2^shift)`).
#[inline(always)]
pub fn collapse_key(key: i64, shift: u8) -> i64 {
    if shift == 0 {
        key
    } else {
        ((key - 1) >> shift) + 1
    }
}

/// Deterministic Q52 binary logarithm for finite positive `value`.
///
/// Uses only IEEE-754 basic arithmetic (`+`, `-`, `*`, `/`) and bit operations so bucket
/// boundaries are bit-for-bit identical across platforms and standard libraries.
#[inline(always)]
fn log2_q52(value: f64) -> i64 {
    debug_assert!(value > 0.0 && value.is_finite());
    let bits = value.to_bits();
    let exp_bits = (bits >> 52) as i32;
    let (exp, mant_bits) = if exp_bits == 0 {
        let shift = bits.leading_zeros() - 11;
        (-1022 - shift as i32, bits << shift)
    } else {
        (exp_bits - 1023, (bits & ((1u64 << 52) - 1)) | (1u64 << 52))
    };

    let frac_bits = mant_bits - (1u64 << 52);
    let seg = ((frac_bits + (1u64 << 47)) >> 48) as usize;
    let m = f64::from_bits((1023u64 << 52) | frac_bits);
    let anchor = 1.0 + (seg as f64) * (1.0 / 16.0);

    // `anchor` is exact in `[1.0, 2.0]` and within `1/32` of `m`, so `m - anchor` is exact by
    // Sterbenz's lemma and `|z| <= 1 / 65`.
    let z = (m - anchor) / (m + anchor);
    let z2 = z * z;
    let z4 = z2 * z2;
    const C0: f64 = TWO_LOG2_E_Q52;
    const C1: f64 = TWO_LOG2_E_Q52 / 3.0;
    const C2: f64 = TWO_LOG2_E_Q52 / 5.0;
    const C3: f64 = TWO_LOG2_E_Q52 / 7.0;
    let poly = (C0 + z2 * C1) + z4 * (C2 + z2 * C3);
    let delta_q52 = ((z * poly + ROUND_MAGIC) - ROUND_MAGIC) as i64;
    i64::from(exp) * LOG_UNIT + ANCHOR_LOG2_Q52[seg] + delta_q52
}

/// Evaluates `floor(2 * atanh(alpha) * log2(e) * 2^52)` for `0 < alpha < 0.25` without
/// `(1 + alpha) / (1 - alpha)` cancellation at tiny `alpha`.
fn atanh_log2_q52_wide(alpha: f64) -> i64 {
    let z2 = alpha * alpha;
    let series = 1.0
        + z2 * ((1.0 / 3.0)
            + z2 * ((1.0 / 5.0)
                + z2 * ((1.0 / 7.0)
                    + z2 * ((1.0 / 9.0)
                        + z2 * ((1.0 / 11.0)
                            + z2 * ((1.0 / 13.0)
                                + z2 * ((1.0 / 15.0)
                                    + z2 * ((1.0 / 17.0)
                                        + z2 * ((1.0 / 19.0)
                                            + z2 * ((1.0 / 21.0)
                                                + z2 * ((1.0 / 23.0) + z2 * (1.0 / 25.0))))))))))));
    (alpha * TWO_LOG2_E_Q52 * series) as i64
}

/// Deterministic inverse of `log2_q52` over `[MIN_LOG2_Q52, MAX_LOG2_Q52]`.
fn exp2_q52(q52: i64) -> f64 {
    debug_assert!((MIN_LOG2_Q52..=MAX_LOG2_Q52).contains(&q52));
    let mut exp = (q52 >> 52) as i32;
    let rem = q52 & (LOG_UNIT - 1);
    let signed_rem = if rem > (LOG_UNIT >> 1) {
        exp += 1;
        rem - LOG_UNIT
    } else {
        rem
    };

    let t = (signed_rem as f64) * INV_LOG_UNIT_F64 * LN_2;
    let mut poly = 1.0 / 6_227_020_800.0; // 1 / 13!
    for k in (1..=12).rev() {
        poly = poly * t + 1.0 / factorial(k);
    }
    let mantissa = 1.0 + t * poly;

    scale_exp2(mantissa, exp)
}

const fn factorial(n: u32) -> f64 {
    let mut acc = 1u64;
    let mut i = 2;
    while i <= n {
        acc *= i as u64;
        i += 1;
    }
    acc as f64
}

/// Multiplies `value` by `2^exp` using exact IEEE-754 power-of-two factors, handling subnormal
/// and `exp == 1024` (`mantissa < 1.0`) ranges without `libm::ldexp`.
fn scale_exp2(value: f64, exp: i32) -> f64 {
    if exp == 1024 {
        // `exp == 1024` occurs only when `signed_rem < 0`, so `value < 1.0` and `value * 2^1023`
        // is strictly below `2^1023` before doubling into `[2^1023, f64::MAX]`.
        return ((value * f64::from_bits(2046u64 << 52)) * 2.0).min(f64::MAX);
    }
    if exp >= -1022 {
        return value * f64::from_bits(((exp + 1023) as u64) << 52);
    }
    // `exp` is in `[-1074, -1023]`; split as `(exp + 54) + (-54)` so the first factor is normal.
    let normal_scale = f64::from_bits(((exp + 54 + 1023) as u64) << 52);
    let subnormal_scale = f64::from_bits((1023 - 54) << 52);
    (value * normal_scale * subnormal_scale).max(f64::from_bits(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anchor_table_matches_binary_logarithm() {
        for (seg, &anchor_q52) in ANCHOR_LOG2_Q52.iter().enumerate() {
            let exact = (1.0 + (seg as f64) / 16.0).log2() * LOG_UNIT_F64;
            assert!(
                (anchor_q52 as f64 - exact).abs() <= 1.0,
                "seg={seg}: table={anchor_q52}, exact={exact}"
            );
        }
    }

    #[test]
    fn log2_q52_is_exact_on_all_powers_of_two() {
        for exp in -1074..=1023 {
            let bits = if exp < -1022 {
                1u64 << (exp + 1074)
            } else {
                ((exp + 1023) as u64) << 52
            };
            assert_eq!(log2_q52(f64::from_bits(bits)), i64::from(exp) * LOG_UNIT);
        }
    }

    #[test]
    fn log2_q52_is_monotone_across_critical_boundaries_and_samples() {
        let mut critical_centers = vec![
            1u64,
            (1u64 << 52) - 1,
            1u64 << 52,
            1023u64 << 52,
            (2046u64 << 52) | ((1u64 << 52) - 1),
        ];
        for seg in 0..16u64 {
            // Segment midpoint transitions `(seg + 0.5) / 16`.
            critical_centers.push((1023u64 << 52) | (seg << 48) | (1u64 << 47));
        }
        for center in critical_centers {
            let start = center.saturating_sub(4096).max(1);
            let end = center.saturating_add(4096).min(f64::MAX.to_bits());
            let mut prev = log2_q52(f64::from_bits(start));
            for bits in (start + 1)..=end {
                let curr = log2_q52(f64::from_bits(bits));
                assert!(
                    curr >= prev,
                    "non-monotone at bits={bits:#x}: {curr} < {prev}"
                );
                prev = curr;
            }
        }

        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        for _ in 0..100_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let bits = (state % (f64::MAX.to_bits() - 1)) + 1;
            let lo = log2_q52(f64::from_bits(bits));
            let hi = log2_q52(f64::from_bits(bits + 1));
            assert!(hi >= lo, "non-monotone at random bits={bits:#x}");

            let val = f64::from_bits(bits);
            let norm = if (bits >> 52) == 0 {
                val * f64::from_bits((1023 + 54) << 52)
            } else {
                val
            };
            let mantissa = f64::from_bits((1023u64 << 52) | (norm.to_bits() & ((1u64 << 52) - 1)));
            let frac_q52 = lo.rem_euclid(LOG_UNIT) as f64 * INV_LOG_UNIT_F64;
            let exact_frac = mantissa.log2();
            assert!(
                (frac_q52 - exact_frac).abs() <= 2.0 * INV_LOG_UNIT_F64,
                "error too large at {val:e}: frac={frac_q52}, exact={exact_frac}"
            );
        }
    }

    #[test]
    fn exp2_q52_inverts_log2_q52_across_domain() {
        for bits in [
            1u64,
            2,
            3,
            (1u64 << 52) - 1,
            1u64 << 52,
            1023u64 << 52,
            (1023u64 << 52) | 0x0006_a09e_667f_3bcc,
            f64::MAX.to_bits() - 1,
            f64::MAX.to_bits(),
        ] {
            let x = f64::from_bits(bits);
            let y = exp2_q52(log2_q52(x));
            assert!(y.is_finite() && y > 0.0);
            let rel = (y - x).abs() / x;
            assert!(rel <= 4.0 * f64::EPSILON, "x={x:e}, y={y:e}, rel={rel:e}");
        }
    }

    #[test]
    fn fast_div_matches_exact_division() {
        for &alpha in &[1e-12, 1e-9, 1e-6, 1e-3, 0.01, 0.25, 0.5, 0.999999999] {
            let mapping = Mapping::new(alpha);
            let d = mapping.div.divisor;
            for mult in [0u64, 1, 2, 7, 1024, (1u64 << 62) / d] {
                for delta in 0..=4 {
                    let base = mult.saturating_mul(d);
                    for n in [base.saturating_sub(delta), base.saturating_add(delta)] {
                        if n < (1u64 << 63) {
                            assert_eq!(mapping.div.div(n), n / d, "n={n}, d={d}");
                        }
                    }
                }
            }
        }
    }
}
