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

const POWERS_OF_THREE: [u64; 31] = [
    1,
    3,
    9,
    27,
    81,
    243,
    729,
    2187,
    6561,
    19683,
    59049,
    177147,
    531441,
    1594323,
    4782969,
    14348907,
    43046721,
    129140163,
    387420489,
    1162261467,
    3486784401,
    10460353203,
    31381059609,
    94143178827,
    282429536481,
    847288609443,
    2541865828329,
    7625597484987,
    22876792454961,
    68630377364883,
    205891132094649,
];

pub fn total_capacity(k: u16, m: u8, num_levels: usize) -> u32 {
    let mut total: u32 = 0;
    for level in 0..num_levels {
        total += level_capacity(k, num_levels, level, m);
    }
    total
}

pub fn level_capacity(k: u16, num_levels: usize, height: usize, min_width: u8) -> u32 {
    assert!(height < num_levels, "height must be < num_levels");
    let depth = num_levels - height - 1;
    let cap = capacity_at_depth(k, depth as u8);
    std::cmp::max(min_width as u32, cap as u32)
}

fn capacity_at_depth(k: u16, depth: u8) -> u16 {
    if depth > 60 {
        panic!("depth must be <= 60");
    }
    if depth <= 30 {
        return capacity_at_shallow_depth(k, depth);
    }
    let half = depth / 2;
    let rest = depth - half;
    let tmp = capacity_at_shallow_depth(k, half);
    capacity_at_shallow_depth(tmp, rest)
}

fn capacity_at_shallow_depth(k: u16, depth: u8) -> u16 {
    if depth > 30 {
        panic!("depth must be <= 30");
    }
    let twok = (k as u64) << 1;
    let tmp = (twok << depth) / POWERS_OF_THREE[depth as usize];
    let result = (tmp + 1) >> 1;
    assert!(result <= k as u64, "capacity result exceeds k");
    result as u16
}
