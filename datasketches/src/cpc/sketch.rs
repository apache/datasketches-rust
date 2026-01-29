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
