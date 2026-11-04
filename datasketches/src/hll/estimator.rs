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

//! Cardinality estimation from HLL register statistics and update history.
//!
//! Estimator formulas and crossover constants follow datasketches-cpp:
//! <https://github.com/apache/datasketches-cpp/blob/5a055521/hll/include/HllArray-internal.hpp>

use crate::common::NumStdDev;
use crate::common::inv_pow2::inv_pow2;
use crate::hll::composite_interpolation;
use crate::hll::cubic_interpolation;
use crate::hll::harmonic_numbers;

/// Selects the estimate that is valid for the current register history.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EstimateState {
    /// The register updates have a known order, so the HIP accumulator is valid.
    Hip(f64),
    /// A bulk merge lost the update order, so the estimate must come from the registers.
    Composite,
}

impl EstimateState {
    pub fn from_serialized(hip_accum: f64, out_of_order: bool) -> Self {
        if out_of_order {
            Self::Composite
        } else {
            Self::Hip(hip_accum)
        }
    }

    pub fn hip_accum(self) -> f64 {
        match self {
            Self::Hip(value) => value,
            Self::Composite => 0.0,
        }
    }

    pub fn uses_composite_estimate(self) -> bool {
        matches!(self, Self::Composite)
    }

    #[inline]
    pub fn update(&mut self, lg_config_k: u8, kxq: &mut Kxq, old_value: u8, new_value: u8) {
        // HIP uses the sums from before this register increase.
        if let Self::Hip(hip_accum) = self {
            *hip_accum += (1 << lg_config_k) as f64 / (kxq.low + kxq.high);
        }
        kxq.update(old_value, new_value);
    }
}

/// Sums of `2^-register`, split at register value 32 to preserve precision.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Kxq {
    pub low: f64,
    pub high: f64,
}

impl Kxq {
    #[inline]
    fn update(&mut self, old_value: u8, new_value: u8) {
        if old_value < 32 {
            self.low -= inv_pow2(old_value);
        } else {
            self.high -= inv_pow2(old_value);
        }
        if new_value < 32 {
            self.low += inv_pow2(new_value);
        } else {
            self.high += inv_pow2(new_value);
        }
    }
}

/// Representation-independent inputs to composite estimation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RegisterStatistics {
    pub num_zeros: u32,
    pub kxq: Kxq,
}

impl RegisterStatistics {
    pub fn from_registers(registers: impl Iterator<Item = u8>) -> Self {
        let mut statistics = Self {
            num_zeros: 0,
            kxq: Kxq {
                low: 0.0,
                high: 0.0,
            },
        };
        for value in registers {
            if value == 0 {
                statistics.num_zeros += 1;
                statistics.kxq.low += 1.0;
            } else if value < 32 {
                statistics.kxq.low += 1.0 / (1u64 << value) as f64;
            } else {
                statistics.kxq.high += 1.0 / (1u64 << value) as f64;
            }
        }
        statistics
    }

    fn raw_estimate(&self, lg_config_k: u8) -> f64 {
        let k = (1 << lg_config_k) as f64;

        // Correction factors from empirical analysis
        let correction_factor = match lg_config_k {
            4 => 0.673,
            5 => 0.697,
            6 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / k),
        };

        (correction_factor * k * k) / (self.kxq.low + self.kxq.high)
    }

    fn bitmap_estimate(&self, lg_config_k: u8) -> f64 {
        let k = 1 << lg_config_k;

        let num_unhit = self.num_zeros;

        // Preserve the upstream fallback for a bitmap with no empty registers.
        if num_unhit == 0 {
            return (k as f64) * (k as f64 / 0.5).ln();
        }

        let num_hit = k - num_unhit;
        harmonic_numbers::bitmap_estimate(k, num_hit)
    }

    /// Selects between bias-corrected HLL and bitmap estimates using their empirical crossover.
    pub fn composite_estimate(&self, lg_config_k: u8) -> f64 {
        let raw_estimate = self.raw_estimate(lg_config_k);

        let x_arr = composite_interpolation::get_x_arr(lg_config_k);
        let x_arr_len = composite_interpolation::get_x_arr_length();
        let y_stride = composite_interpolation::get_y_stride(lg_config_k) as f64;

        if raw_estimate < x_arr[0] {
            return 0.0;
        }

        let x_arr_len_m1 = x_arr_len - 1;

        // Above interpolation range: extrapolate linearly
        if raw_estimate > x_arr[x_arr_len_m1] {
            let final_y = y_stride * (x_arr_len_m1 as f64);
            let factor = final_y / x_arr[x_arr_len_m1];
            return raw_estimate * factor;
        }

        let adjusted_estimate =
            cubic_interpolation::using_x_arr_and_y_stride(x_arr, y_stride, raw_estimate);

        // Avoid linear counting if estimate is high
        // (threshold: 3*k ensures we're above potential linear counting instability)
        let k = 1 << lg_config_k;
        if adjusted_estimate > (3 * k) as f64 {
            return adjusted_estimate;
        }

        let linear_estimate = self.bitmap_estimate(lg_config_k);

        // Comparing the average to the threshold reduces estimator-selection bias.
        let average_estimate = (adjusted_estimate + linear_estimate) / 2.0;

        // Crossover thresholds (empirically determined)
        let crossover = match lg_config_k {
            4 => 0.718,
            5 => 0.672,
            _ => 0.64,
        };

        let threshold = crossover * (k as f64);

        if average_estimate > threshold {
            adjusted_estimate
        } else {
            linear_estimate
        }
    }
}

/// Relative error used as `estimate / (1 + error)`: negative for upper bounds.
/// Follows datasketches-cpp's `HllUtil::getRelErr`:
/// <https://github.com/apache/datasketches-cpp/blob/5a055521/hll/include/HllUtil.hpp>
pub fn relative_error(
    lg_config_k: u8,
    upper_bound: bool,
    composite: bool,
    num_std_dev: NumStdDev,
) -> f64 {
    if lg_config_k > 12 {
        // RSE factors from datasketches-cpp.
        // HLL_HIP_RSE_FACTOR = sqrt(ln(2)) ≈ 0.8325546
        // HLL_NON_HIP_RSE_FACTOR = sqrt((3 * ln(2)) - 1) ≈ 1.03896
        let rse_factor = if composite {
            1.03896 // Composite
        } else {
            0.8325546 // HIP
        };

        let k = (1 << lg_config_k) as f64;
        let sign = if upper_bound { -1.0 } else { 1.0 };

        return sign * (num_std_dev as u8 as f64) * rse_factor / k.sqrt();
    }

    // Small sketches use measured error quantiles rather than the asymptotic formula.
    let idx = ((lg_config_k as usize) - 4) * 3 + ((num_std_dev as usize) - 1);

    match (composite, upper_bound) {
        (false, false) => HIP_LB[idx],
        (false, true) => HIP_UB[idx],
        (true, false) => NON_HIP_LB[idx],
        (true, true) => NON_HIP_UB[idx],
    }
}

// Measured quantiles, grouped by lg_k 4–12 with three standard deviations per group.
// Source: https://github.com/apache/datasketches-cpp/blob/5a055521/hll/include/RelativeErrorTables-internal.hpp

/// HIP (in-order) Lower Bound errors for lg_k 4-12, std_dev 1-3
/// Q(.84134), Q(.97725), Q(.99865) quantiles
static HIP_LB: [f64; 27] = [
    0.207316195,
    0.502865572,
    0.882303765, //4
    0.146981579,
    0.335426881,
    0.557052, //5
    0.104026721,
    0.227683872,
    0.365888317, //6
    0.073614601,
    0.156781585,
    0.245740374, //7
    0.05205248,
    0.108783763,
    0.168030442, //8
    0.036770852,
    0.075727545,
    0.11593785, //9
    0.025990219,
    0.053145536,
    0.080772263, //10
    0.018373987,
    0.037266176,
    0.056271814, //11
    0.012936253,
    0.02613829,
    0.039387631, //12
];

/// HIP (in-order) Upper Bound errors for lg_k 4-12, std_dev 1-3
/// Q(.15866), Q(.02275), Q(.00135) quantiles
static HIP_UB: [f64; 27] = [
    -0.207805347,
    -0.355574279,
    -0.475535095, //4
    -0.146988328,
    -0.262390832,
    -0.360864026, //5
    -0.103877775,
    -0.191503663,
    -0.269311582, //6
    -0.073452978,
    -0.138513438,
    -0.198487447, //7
    -0.051982806,
    -0.099703123,
    -0.144128618, //8
    -0.036768609,
    -0.07138158,
    -0.104430324, //9
    -0.025991325,
    -0.050854296,
    -0.0748143, //10
    -0.01834533,
    -0.036121138,
    -0.05327616, //11
    -0.012920332,
    -0.025572893,
    -0.037896952, //12
];

/// Composite (non-HIP) lower-bound errors for lg_k 4-12, std_dev 1-3.
/// Q(.84134), Q(.97725), Q(.99865) quantiles
static NON_HIP_LB: [f64; 27] = [
    0.254409839,
    0.682266712,
    1.304022158, //4
    0.181817353,
    0.443389054,
    0.778776219, //5
    0.129432281,
    0.295782195,
    0.49252279, //6
    0.091640655,
    0.201175925,
    0.323664385, //7
    0.064858051,
    0.138523393,
    0.218805328, //8
    0.045851855,
    0.095925072,
    0.148635751, //9
    0.032454144,
    0.067009668,
    0.102660669, //10
    0.022921382,
    0.046868565,
    0.071307398, //11
    0.016155679,
    0.032825719,
    0.049677541, //12
];

/// Composite (non-HIP) upper-bound errors for lg_k 4-12, std_dev 1-3.
/// Q(.15866), Q(.02275), Q(.00135) quantiles
static NON_HIP_UB: [f64; 27] = [
    -0.256980172,
    -0.411905944,
    -0.52651057, // lg_k=4
    -0.182332109,
    -0.310275547,
    -0.412660505, // lg_k=5
    -0.129314228,
    -0.230142294,
    -0.315636197, // lg_k=6
    -0.091584836,
    -0.16834013,
    -0.236346847, // lg_k=7
    -0.06487411,
    -0.122045231,
    -0.174112107, // lg_k=8
    -0.04591465,
    -0.08784505,
    -0.126917615, // lg_k=9
    -0.032433119,
    -0.062897613,
    -0.091862929, // lg_k=10
    -0.022960633,
    -0.044875401,
    -0.065736049, // lg_k=11
    -0.016186662,
    -0.031827816,
    -0.046973459, // lg_k=12
];
