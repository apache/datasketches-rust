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

use crate::common::NumStdDev;
use crate::cpc::estimator::Estimator;
use crate::cpc::pair_table::PairTable;
use crate::hash::DEFAULT_UPDATE_SEED;

/// Default log2 of K.
const DEFAULT_LG_K: u8 = 11;
/// Min log2 of K.
const MIN_LG_K: usize = 4;
/// Max log2 of K.
const MAX_LG_K: usize = 26;

/// A Compressed Probabilistic Counting sketch.
#[derive(Debug, Clone)]
pub struct CpcSketch {
    // immutable config variables
    lg_k: u8,
    seed: u64,

    // sketch state
    /// The number of coupons collected so far.
    num_coupons: u32,
    /// This is part of a speed optimization.
    first_interesting_column: u8,
    /// Physical storage for the sketch data.
    storage: PhysicalStorage,
    /// The current estimator type and associated data.
    estimator: Estimator,
}

impl Default for CpcSketch {
    fn default() -> Self {
        Self::new(DEFAULT_LG_K)
    }
}

impl CpcSketch {
    /// Creates a new `CpcSketch` with the given `lg_k` and default seed.
    pub fn new(lg_k: u8) -> Self {
        Self::with_seed(lg_k, DEFAULT_UPDATE_SEED)
    }

    /// Creates a new `CpcSketch` with the given `lg_k` and `seed`.
    pub fn with_seed(lg_k: u8, seed: u64) -> Self {
        assert!(
            (MIN_LG_K..=MAX_LG_K).contains(&(lg_k as usize)),
            "lg_k out of range; got {lg_k}",
        );

        Self {
            lg_k,
            seed,
            num_coupons: 0,
            first_interesting_column: 0,
            storage: PhysicalStorage::Empty,
            estimator: Estimator::Hip {
                kxp: (1 << lg_k) as f64,
                hip_estimate: 0.0,
            },
        }
    }

    /// Return the parameter lg_k.
    pub fn lg_k(&self) -> u8 {
        self.lg_k
    }

    /// Returns the best estimate of the cardinality of the sketch.
    pub fn estimate(&self) -> f64 {
        let (lg_k, num_coupons) = (self.lg_k, self.num_coupons);
        self.estimator.estimate(lg_k, num_coupons)
    }

    /// Returns the best estimate of the lower bound of the confidence interval given `kappa`.
    pub fn lower_bound(&self, kappa: NumStdDev) -> f64 {
        let (lg_k, num_coupons) = (self.lg_k, self.num_coupons);
        self.estimator.lower_bound(lg_k, num_coupons, kappa)
    }

    /// Returns the best estimate of the upper bound of the confidence interval given `kappa`.
    pub fn upper_bound(&self, kappa: NumStdDev) -> f64 {
        let (lg_k, num_coupons) = (self.lg_k, self.num_coupons);
        self.estimator.upper_bound(lg_k, num_coupons, kappa)
    }
}

#[derive(Debug, Clone)]
enum PhysicalStorage {
    /// Empty storage state for EMPTY state.
    Empty,
    /// Sparse storage state for SPARSE state.
    Sparse { surprising_value_table: PairTable },
    /// Dense storage state for HYBRID/PINNED/SLIDING state.
    Dense {
        window_offset: u8,
        sliding_window: Vec<u8>,
    },
}

impl CpcSketch {
    /// Returns the estimated maximum compressed serialized size of a sketch.
    ///
    /// The actual size of a compressed CPC sketch has a small random variance, but the following
    /// empirically measured size should be large enough for at least 99.9 percent of sketches.
    ///
    /// For small values of `n` the size can be much smaller.
    pub fn max_serialized_bytes(lg_k: u8) -> usize {
        let lg_k = lg_k as usize;
        assert!(
            (MIN_LG_K..=MAX_LG_K).contains(&lg_k),
            "lg_k out of range; got {lg_k}",
        );

        // These empirical values for the 99.9th percentile of size in bytes were measured using 100,000
        // trials. The value for each trial is the maximum of 5*16=80 measurements that were equally
        // spaced over values of the quantity C/K between 3.0 and 8.0. This table does not include the
        // worst-case space for the preamble, which is added by the function.
        const CPC_EMPIRICAL_SIZE_MAX_LGK: usize = 19;
        const CPC_EMPIRICAL_MAX_SIZE_BYTES: [usize; 16] = [
            24,     // lg_k = 4
            36,     // lg_k = 5
            56,     // lg_k = 6
            100,    // lg_k = 7
            180,    // lg_k = 8
            344,    // lg_k = 9
            660,    // lg_k = 10
            1292,   // lg_k = 11
            2540,   // lg_k = 12
            5020,   // lg_k = 13
            9968,   // lg_k = 14
            19836,  // lg_k = 15
            39532,  // lg_k = 16
            78880,  // lg_k = 17
            157516, // lg_k = 18
            314656, // lg_k = 19
        ];
        const CPC_EMPIRICAL_MAX_SIZE_FACTOR: f64 = 0.6; // 0.6 = 4.8 / 8.0
        const CPC_MAX_PREAMBLE_SIZE_BYTES: usize = 40;

        if lg_k <= CPC_EMPIRICAL_SIZE_MAX_LGK {
            return CPC_EMPIRICAL_MAX_SIZE_BYTES[lg_k - MIN_LG_K] + CPC_MAX_PREAMBLE_SIZE_BYTES;
        }
        let k = 1usize << lg_k;
        ((CPC_EMPIRICAL_MAX_SIZE_FACTOR * k as f64) as usize) + CPC_MAX_PREAMBLE_SIZE_BYTES
    }
}
