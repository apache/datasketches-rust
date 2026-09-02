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

const MAX_DEPTH: usize = 60;
const MAX_SHALLOW_DEPTH: usize = POWERS_OF_THREE.len() - 1;

pub const fn total_capacity(k: u16, minimum_capacity: u8, num_levels: usize) -> u32 {
    let mut total: u32 = 0;
    let mut level = 0;
    while level < num_levels {
        total += level_capacity(k, num_levels, level, minimum_capacity);
        level += 1;
    }
    total
}

pub const fn level_capacity(k: u16, num_levels: usize, level: usize, minimum_capacity: u8) -> u32 {
    assert!(
        level < num_levels,
        "level index must be less than the number of levels"
    );
    let depth = num_levels - level - 1;
    let capacity = capacity_at_depth(k, depth) as u32;
    if capacity < minimum_capacity as u32 {
        minimum_capacity as u32
    } else {
        capacity
    }
}

const fn capacity_at_depth(k: u16, depth: usize) -> u16 {
    assert!(depth <= MAX_DEPTH, "KLL capacity depth must be at most 60");
    if depth <= MAX_SHALLOW_DEPTH {
        return capacity_at_shallow_depth(k, depth);
    }
    let first_depth = depth / 2;
    let remaining_depth = depth - first_depth;
    let intermediate_capacity = capacity_at_shallow_depth(k, first_depth);
    capacity_at_shallow_depth(intermediate_capacity, remaining_depth)
}

const fn capacity_at_shallow_depth(k: u16, depth: usize) -> u16 {
    assert!(
        depth <= MAX_SHALLOW_DEPTH,
        "shallow KLL capacity depth must be at most 30"
    );
    let twice_k = (k as u64) << 1;
    let scaled_capacity = (twice_k << depth) / POWERS_OF_THREE[depth];
    let result = (scaled_capacity + 1) >> 1;
    assert!(
        result <= k as u64,
        "computed level capacity must not exceed k"
    );
    result as u16
}
