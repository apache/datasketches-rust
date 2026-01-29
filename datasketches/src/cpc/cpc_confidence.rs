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

use crate::common::NumStdDev;
use crate::cpc::icon_estimator::icon_estimate;

const ICON_ERROR_CONSTANT: f64 = LN_2;

const ICON_LOW_SIDE_DATA: [u16; 33] = [
    //1,    2,    3,   kappa
    //                 lgK num trials
    6037, 5720, 5328, // 4 1000000
    6411, 6262, 5682, // 5 1000000
    6724, 6403, 6127, // 6 1000000
    6665, 6411, 6208, // 7 1000000
    6959, 6525, 6427, // 8 1000000
    6892, 6665, 6619, // 9 1000000
    6792, 6752, 6690, // 10 1000000
    6899, 6818, 6708, // 11 1000000
    6871, 6845, 6812, // 12 1046369
    6909, 6861, 6828, // 13 1043411
    6919, 6897, 6842, // 14 1000297
];

const ICON_HIGH_SIDE_DATA: [u16; 33] = [
    //1,    2,    3,   kappa
    //                 lgK num trials
    8031, 8559, 9309, // 4 1000000
    7084, 7959, 8660, // 5 1000000
    7141, 7514, 7876, // 6 1000000
    7458, 7430, 7572, // 7 1000000
    6892, 7141, 7497, // 8 1000000
    6889, 7132, 7290, // 9 1000000
    7075, 7118, 7185, // 10 1000000
    7040, 7047, 7085, // 11 1000000
    6993, 7019, 7053, // 12 1046369
    6953, 7001, 6983, // 13 1043411
    6944, 6966, 7004, // 14 1000297
];

#[allow(clippy::excessive_precision)]
const HIP_ERROR_CONSTANT: f64 = 0.588705011257737332; // (LN_2 / 2.0).sqrt()

const HIP_LOW_SIDE_DATA: [u16; 33] = [
    //1,    2,    3,   kappa
    //                 lgK num trials
    5871, 5247, 4826, // 4 1000000
    5877, 5403, 5070, // 5 1000000
    5873, 5533, 5304, // 6 1000000
    5878, 5632, 5464, // 7 1000000
    5874, 5690, 5564, // 8 1000000
    5880, 5745, 5619, // 9 1000000
    5875, 5784, 5701, // 10 1000000
    5866, 5789, 5742, // 11 1000000
    5869, 5827, 5784, // 12 1046369
    5876, 5860, 5827, // 13 1043411
    5881, 5853, 5842, // 14 1000297
];

const HIP_HIGH_SIDE_DATA: [u16; 33] = [
    //1,    2,    3,   kappa
    //                 lgK num trials
    5855, 6688, 7391, // 4 1000000
    5886, 6444, 6923, // 5 1000000
    5885, 6254, 6594, // 6 1000000
    5889, 6134, 6326, // 7 1000000
    5900, 6072, 6203, // 8 1000000
    5875, 6005, 6089, // 9 1000000
    5871, 5980, 6040, // 10 1000000
    5889, 5941, 6015, // 11 1000000
    5871, 5926, 5973, // 12 1046369
    5866, 5901, 5915, // 13 1043411
    5880, 5914, 5953, // 14 1000297
];

pub(super) fn icon_confidence_lb(lg_k: u8, num_coupons: u32, kappa: NumStdDev) -> f64 {
    if num_coupons == 0 {
        return 0.0;
    }

    let k = (1 << lg_k) as f64;
    let kappa = kappa.as_u8();

    let mut x = ICON_ERROR_CONSTANT;
    if lg_k <= 14 {
        let idx = (3 * (lg_k - 4) + (kappa - 1)) as usize;
        x = (ICON_HIGH_SIDE_DATA[idx] as f64) / 10000.0;
    }
    let rel = x / k.sqrt();
    let eps = (kappa as f64) * rel;
    let est = icon_estimate(lg_k, num_coupons);
    let result = est / (1.0 + eps);
    if result < (num_coupons as f64) {
        num_coupons as f64
    } else {
        result
    }
}

pub(super) fn icon_confidence_ub(lg_k: u8, num_coupons: u32, kappa: NumStdDev) -> f64 {
    if num_coupons == 0 {
        return 0.0;
    }

    let k = (1 << lg_k) as f64;
    let kappa = kappa.as_u8();

    let mut x = ICON_ERROR_CONSTANT;
    if lg_k <= 14 {
        let idx = (3 * (lg_k - 4) + (kappa - 1)) as usize;
        x = (ICON_LOW_SIDE_DATA[idx] as f64) / 10000.0;
    }
    let rel = x / k.sqrt();
    let eps = (kappa as f64) * rel;
    let est = icon_estimate(lg_k, num_coupons);
    let result = est / (1.0 - eps);
    result.ceil() // slight widening of interval to be conservative
}

// merge_flag must already be checked as false
pub(super) fn hip_confidence_lb(
    lg_k: u8,
    num_coupons: u32,
    hip_estimate: f64,
    kappa: NumStdDev,
) -> f64 {
    if num_coupons == 0 {
        return 0.0;
    }

    let k = (1 << lg_k) as f64;
    let kappa = kappa.as_u8();

    let mut x = HIP_ERROR_CONSTANT;
    if lg_k <= 14 {
        let idx = (3 * (lg_k - 4) + (kappa - 1)) as usize;
        x = (HIP_HIGH_SIDE_DATA[idx] as f64) / 10000.0;
    }
    let rel = x / k.sqrt();
    let eps = (kappa as f64) * rel;
    let result = hip_estimate / (1.0 + eps);
    if result < (num_coupons as f64) {
        num_coupons as f64
    } else {
        result
    }
}

// merge_flag must already be checked as false
pub(super) fn get_hip_confidence_ub(
    lg_k: u8,
    num_coupons: u32,
    hip_estimate: f64,
    kappa: NumStdDev,
) -> f64 {
    if num_coupons == 0 {
        return 0.0;
    }

    let k = (1 << lg_k) as f64;
    let kappa = kappa.as_u8();

    let mut x = HIP_ERROR_CONSTANT;
    if lg_k <= 14 {
        let idx = (3 * (lg_k - 4) + (kappa - 1)) as usize;
        x = (HIP_LOW_SIDE_DATA[idx] as f64) / 10000.0;
    }
    let rel = x / k.sqrt();
    let eps = (kappa as f64) * rel;
    let result = hip_estimate / (1.0 - eps);
    result.ceil() // widening for coverage
}
