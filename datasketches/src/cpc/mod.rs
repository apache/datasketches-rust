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

//! Compressed Probabilistic Counting sketch.
//!
//! This is a unique-counting sketch that implements the Compressed Probabilistic Counting (CPC,
//! a.k.a. FM85) algorithms developed by Kevin Lang in his paper [Back to the Future: an Even More
//! Nearly Optimal Cardinality Estimation Algorithm](https://arxiv.org/abs/1708.06839).
//!
//! This sketch is extremely space-efficient when serialized. In an apples-to-apples empirical
//! comparison against compressed HyperLogLog sketches, this new algorithm simultaneously wins on
//! the two dimensions of the space/accuracy tradeoff and produces sketches that are smaller than
//! the entropy of HLL, so no possible implementation of compressed HLL can match its space
//! efficiency for a given accuracy. As described in the paper this sketch implements a newly
//! developed ICON estimator algorithm that survives union operations, another well-known estimator,
//! the [Historical Inverse Probability (HIP)](https://arxiv.org/abs/1306.3284) estimator does not.
//!
//! The update speed performance of this sketch is quite fast and is comparable to the speed of HLL.
//! The union (merging) capability of this sketch also allows for merging of sketches with different
//! configurations of K.
//!
//! For additional security this sketch can be configured with a user-specified hash seed.

mod compression;
mod compression_data;
mod estimator;
mod kxp_byte_lookup;
mod pair_table;
mod sketch;
mod union;

pub use self::sketch::CpcSketch;
pub use self::union::CpcUnion;

/// Default log2 of K.
const DEFAULT_LG_K: u8 = 11;
/// Min log2 of K.
const MIN_LG_K: usize = 4;
/// Max log2 of K.
const MAX_LG_K: usize = 26;

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
#[expect(clippy::upper_case_acronyms)]
enum Flavor {
    EMPTY,   //    0  == C <    1
    SPARSE,  //    1  <= C <   3K/32
    HYBRID,  // 3K/32 <= C <   K/2
    PINNED,  //   K/2 <= C < 27K/8  [NB: 27/8 = 3 + 3/8]
    SLIDING, // 27K/8 <= C
}

fn count_bits_set_in_matrix(matrix: &[u64]) -> u32 {
    let mut count = 0;
    for word in matrix {
        count += word.count_ones();
    }
    count
}

fn determine_flavor(lg_k: u8, num_coupons: u32) -> Flavor {
    let k = 1 << lg_k;
    let c2 = num_coupons << 1;
    let c8 = num_coupons << 3;
    let c32 = num_coupons << 5;
    if num_coupons == 0 {
        Flavor::EMPTY
    } else if c32 < (3 * k) {
        Flavor::SPARSE
    } else if c2 < k {
        Flavor::HYBRID
    } else if c8 < (27 * k) {
        Flavor::PINNED
    } else {
        Flavor::SLIDING
    }
}

fn determine_correct_offset(lg_k: u8, num_coupons: u32) -> u8 {
    let k = 1 << lg_k;
    let tmp = ((num_coupons as i64) << 3) - (19 * k); // 8C - 19K
    if tmp < 0 {
        0
    } else {
        (tmp >> (lg_k + 3)) as u8 // tmp / 8K
    }
}
