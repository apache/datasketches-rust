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

use std::hash::Hash;

use crate::common::NumStdDev;
use crate::common::canonical_double;
use crate::common::inv_pow2_table::INVERSE_POWERS_OF_2;
use crate::cpc::estimator::hip_confidence_lb;
use crate::cpc::estimator::hip_confidence_ub;
use crate::cpc::estimator::icon_confidence_lb;
use crate::cpc::estimator::icon_confidence_ub;
use crate::cpc::estimator::icon_estimate;
use crate::cpc::pair_table::PairTable;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::MurmurHash3X64128;

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
    /// Part of a speed optimization.
    first_interesting_column: u8,
    /// The number of coupons collected so far.
    num_coupons: u32,
    /// Surprising values table in sparse mode.
    surprising_value_table: Option<PairTable>,
    /// Derivable from num_coupons, but made explicit for speed.
    window_offset: u8,
    /// Size K bytes in dense mode.
    sliding_window: Vec<u8>,

    // estimator state
    /// Whether the sketch is a result of merging.
    ///
    /// If `false`, the HIP (Historical Inverse Probability) estimator is used.
    /// If `true`, the ICON (Inter-Column Optimal) Estimator is fallback in use.
    merge_flag: bool,
    // the following variables are only valid in HIP estimator
    /// A pre-calculated probability factor (`k * p`) used to compute the increment delta.
    kxp: f64,
    /// The accumulated cardinality estimate.
    hip_est_accum: f64,
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
            first_interesting_column: 0,
            num_coupons: 0,
            surprising_value_table: None,
            window_offset: 0,
            sliding_window: vec![],
            merge_flag: false,
            kxp: (1 << lg_k) as f64,
            hip_est_accum: 0.0,
        }
    }

    /// Return the parameter lg_k.
    pub fn lg_k(&self) -> u8 {
        self.lg_k
    }

    /// Returns the best estimate of the cardinality of the sketch.
    pub fn estimate(&self) -> f64 {
        if !self.merge_flag {
            self.hip_est_accum
        } else {
            icon_estimate(self.lg_k, self.num_coupons)
        }
    }

    /// Returns the best estimate of the lower bound of the confidence interval given `kappa`.
    pub fn lower_bound(&self, kappa: NumStdDev) -> f64 {
        if !self.merge_flag {
            hip_confidence_lb(self.lg_k, self.num_coupons, self.hip_est_accum, kappa)
        } else {
            icon_confidence_lb(self.lg_k, self.num_coupons, kappa)
        }
    }

    /// Returns the best estimate of the upper bound of the confidence interval given `kappa`.
    pub fn upper_bound(&self, kappa: NumStdDev) -> f64 {
        if !self.merge_flag {
            hip_confidence_ub(self.lg_k, self.num_coupons, self.hip_est_accum, kappa)
        } else {
            icon_confidence_ub(self.lg_k, self.num_coupons, kappa)
        }
    }

    /// Returns true if the sketch is empty.
    pub fn is_empty(&self) -> bool {
        self.num_coupons == 0
    }

    /// Update the sketch with a hashable value.
    ///
    /// For `f32`/`f64` values, use `update_f32`/`update_f64` instead.
    pub fn update<T: Hash>(&mut self, value: T) {
        let mut hasher = MurmurHash3X64128::with_seed(self.seed);
        value.hash(&mut hasher);
        let (h1, h2) = hasher.finish128();

        let k = 1 << self.lg_k;
        let col = h2.leading_zeros(); // 0 <= col <= 64
        let col = if col > 63 { 63 } else { col as u8 }; // clip so that 0 <= col <= 63
        let row = (h1 & (k - 1)) as u32;
        let mut row_col = (row << 6) | (col as u32);
        // To avoid the hash table's "empty" value, we change the row of the following pair.
        // This case is extremely unlikely, but we might as well handle it.
        if row_col == u32::MAX {
            row_col ^= 1 << 6;
        }
        self.row_col_update(row_col);
    }

    /// Update the sketch with a f64 value.
    pub fn update_f64(&mut self, value: f64) {
        // Canonicalize double for compatibility with Java
        let canonical = canonical_double(value);
        self.update(canonical);
    }

    /// Update the sketch with a f32 value.
    pub fn update_f32(&mut self, value: f32) {
        self.update_f64(value as f64);
    }

    fn row_col_update(&mut self, row_col: u32) {
        let col = (row_col & 63) as u8;
        if col < self.first_interesting_column {
            // important speed optimization
            return;
        }

        if self.num_coupons == 0 {
            // promote EMPTY to SPARSE
            self.surprising_value_table = Some(PairTable::new(2, 6 + self.lg_k));
        }

        if self.sliding_window.is_empty() {
            self.update_sparse(row_col);
        } else {
            self.update_windowed(row_col);
        }
    }

    fn mut_surprising_value_table(&mut self) -> &mut PairTable {
        self.surprising_value_table
            .as_mut()
            .expect("surprising value table must be initialized")
    }

    fn update_hip(&mut self, row_col: u32) {
        let k = 1 << self.lg_k;
        let col = (row_col & 63) as usize;
        let one_over_p = (k as f64) / self.kxp;
        self.hip_est_accum += one_over_p;
        self.kxp -= INVERSE_POWERS_OF_2[col + 1] // notice the "+1"
    }

    fn update_sparse(&mut self, row_col: u32) {
        let k = 1 << self.lg_k;
        let c32pre = (self.num_coupons as u64) << 5;
        assert!(c32pre < 3 * k); // C < 3K/32, in other words, flavor == SPARSE
        let is_novel = self.mut_surprising_value_table().maybe_insert(row_col);
        if is_novel {
            self.num_coupons += 1;
            self.update_hip(row_col);
            let c32post = (self.num_coupons as u64) << 5;
            if c32post >= 3 * k {
                self.promote_sparse_to_windowed();
            }
        }
    }

    fn promote_sparse_to_windowed(&mut self) {
        assert_eq!(self.window_offset, 0);

        let k = 1 << self.lg_k;
        let c32 = (self.num_coupons as u64) << 5;
        assert!((c32 == (3 * k)) || ((self.lg_k == 4) && (c32 > (3 * k))));

        self.sliding_window.resize(k as usize, 0);

        let old_table = self
            .surprising_value_table
            .replace(PairTable::new(2, 6 + self.lg_k))
            .expect("surprising value table must be initialized");
        let old_slots = old_table.slots();
        for &row_col in old_slots {
            if row_col != u32::MAX {
                let col = (row_col & 63) as u8;
                if col < 8 {
                    let row = (row_col >> 6) as usize;
                    self.sliding_window[row] |= 1 << col;
                } else {
                    // cannot use must_insert(), because it doesn't provide for growth
                    let is_novel = self.mut_surprising_value_table().maybe_insert(row_col);
                    assert!(is_novel);
                }
            }
        }
    }

    fn update_windowed(&mut self, row_col: u32) {
        assert!(self.window_offset <= 56);
        let k = 1 << self.lg_k;
        let c32pre = (self.num_coupons as u64) << 5;
        assert!(c32pre >= 3 * k); // C >= 3K/32, in other words flavor >= HYBRID
        let c8pre = (self.num_coupons as u64) << 3;
        let w8pre = (self.window_offset as u64) << 3;
        assert!(c8pre < (27 + w8pre) * k); // C < (K * 27/8) + (K * windowOffset)

        let mut is_novel = false; // novel if new coupon;
        let col = (row_col & 63) as u8;
        if col < self.window_offset {
            // track the surprising 0's "before" the window
            is_novel = self.mut_surprising_value_table().maybe_delete(row_col); // inverted logic
        } else if col < self.window_offset + 8 {
            // track the 8 bits inside the window
            let row = (row_col >> 6) as usize;
            let old_bits = self.sliding_window[row];
            let new_bits = old_bits | (1 << (col - self.window_offset));
            if old_bits != new_bits {
                self.sliding_window[row] = new_bits;
                is_novel = true;
            }
        } else {
            // track the surprising 1's "after" the window
            is_novel = self.mut_surprising_value_table().maybe_insert(row_col); // normal logic
        }

        if is_novel {
            self.num_coupons += 1;
            self.update_hip(row_col);
            let c8post = (self.num_coupons as u64) << 3;
            if c8post >= (27 + w8pre) * k {
                self.move_window();
                assert!((1..=56).contains(&self.window_offset));
                let w8post = (self.window_offset as u64) << 3;
                assert!(c8post < ((27 + w8post) * k)); // C < (K * 27/8) + (K * windowOffset)
            }
        }
    }

    fn move_window(&mut self) {
        todo!()
    }
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

        // These empirical values for the 99.9th percentile of size in bytes were measured using
        // 100,000 trials. The value for each trial is the maximum of 5*16=80 measurements
        // that were equally spaced over values of the quantity C/K between 3.0 and 8.0.
        // This table does not include the worst-case space for the preamble, which is added
        // by the function.
        const EMPIRICAL_SIZE_MAX_LGK: usize = 19;
        const EMPIRICAL_MAX_SIZE_BYTES: [usize; 16] = [
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
        const EMPIRICAL_MAX_SIZE_FACTOR: f64 = 0.6; // 0.6 = 4.8 / 8.0
        const MAX_PREAMBLE_SIZE_BYTES: usize = 40;

        if lg_k <= EMPIRICAL_SIZE_MAX_LGK {
            return EMPIRICAL_MAX_SIZE_BYTES[lg_k - MIN_LG_K] + MAX_PREAMBLE_SIZE_BYTES;
        }
        let k = 1usize << lg_k;
        ((EMPIRICAL_MAX_SIZE_FACTOR * k as f64) as usize) + MAX_PREAMBLE_SIZE_BYTES
    }
}
