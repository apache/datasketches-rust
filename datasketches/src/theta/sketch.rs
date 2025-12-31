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

//! Theta sketch implementation
//!
//! This module provides ThetaSketch (mutable) and CompactThetaSketch (immutable)
//! for cardinality estimation.

use std::hash::Hash;

use crate::ResizeFactor;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::theta::hash_table::DEFAULT_LG_K;
use crate::theta::hash_table::MAX_LG_K;
use crate::theta::hash_table::MAX_THETA;
use crate::theta::hash_table::MIN_LG_K;
use crate::theta::hash_table::ThetaHashTable;

/// Mutable theta sketch for building from input data
#[derive(Debug)]
pub struct ThetaSketch {
    table: ThetaHashTable,
}

impl ThetaSketch {
    /// Create a new builder for ThetaSketch
    pub fn builder() -> ThetaSketchBuilder {
        ThetaSketchBuilder::default()
    }

    /// Update the sketch with a hashable value
    pub fn update<T: Hash>(&mut self, value: T) {
        let hash = self.table.hash_and_screen(value);
        if hash != 0 {
            self.table.try_insert(hash);
        }
    }

    /// Update the sketch with a f64 value
    pub fn update_f64(&mut self, value: f64) {
        // Canonicalize double for compatibility with Java
        let canonical = canonical_double(value);
        self.update(canonical);
    }

    /// Update the sketch with a f32 value
    pub fn update_f32(&mut self, value: f32) {
        self.update_f64(value as f64);
    }

    /// Return cardinality estimate
    pub fn estimate(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let num_retained = self.table.num_entries() as f64;
        let theta = self.table.theta() as f64 / MAX_THETA as f64;
        num_retained / theta
    }

    /// Return theta as a fraction (0.0 to 1.0)
    pub fn theta(&self) -> f64 {
        self.table.theta() as f64 / MAX_THETA as f64
    }

    /// Return theta as u64
    pub fn theta64(&self) -> u64 {
        self.table.theta()
    }

    /// Check if sketch is empty
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Check if sketch is in estimation mode
    pub fn is_estimation_mode(&self) -> bool {
        self.table.theta() < MAX_THETA
    }

    /// Return number of retained entries
    pub fn num_retained(&self) -> usize {
        self.table.num_entries()
    }

    /// Return lg_k
    pub fn lg_k(&self) -> u8 {
        self.table.lg_nom_size()
    }

    /// Trim the sketch to nominal size k
    pub fn trim(&mut self) {
        self.table.trim();
    }

    /// Reset the sketch to empty state
    pub fn reset(&mut self) {
        self.table.reset();
    }

    /// Return iterator over hash values
    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.table.iter()
    }
}

/// Builder for ThetaSketch
#[derive(Debug)]
pub struct ThetaSketchBuilder {
    lg_k: u8,
    resize_factor: ResizeFactor,
    sampling_probability: f32,
    seed: u64,
}

impl Default for ThetaSketchBuilder {
    fn default() -> Self {
        Self {
            lg_k: DEFAULT_LG_K,
            resize_factor: ResizeFactor::X8,
            sampling_probability: 1.0,
            seed: DEFAULT_UPDATE_SEED,
        }
    }
}

impl ThetaSketchBuilder {
    /// Set lg_k (log2 of nominal size k).
    ///
    /// # Panics
    ///
    /// If lg_k is not in range [5, 26]
    pub fn lg_k(mut self, lg_k: u8) -> Self {
        assert!(
            (MIN_LG_K..=MAX_LG_K).contains(&lg_k),
            "lg_k must be in [{}, {}], got {}",
            MIN_LG_K,
            MAX_LG_K,
            lg_k
        );
        self.lg_k = lg_k;
        self
    }

    /// Set resize factor.
    pub fn resize_factor(mut self, factor: ResizeFactor) -> Self {
        self.resize_factor = factor;
        self
    }

    /// Set sampling probability p.
    ///
    /// # Panics
    ///
    /// If p is not in range [0.0, 1.0]
    pub fn sampling_probability(mut self, probability: f32) -> Self {
        assert!(
            (0.0..=1.0).contains(&probability),
            "p must be in [0.0, 1.0], got {probability}"
        );
        self.sampling_probability = probability;
        self
    }

    /// Set hash seed.
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Build the ThetaSketch.
    pub fn build(self) -> ThetaSketch {
        let table = ThetaHashTable::new(
            self.lg_k,
            self.resize_factor,
            self.sampling_probability,
            self.seed,
        );

        ThetaSketch { table }
    }
}

/// Canonicalize double value for compatibility with Java
fn canonical_double(value: f64) -> i64 {
    if value.is_nan() {
        // Java's Double.doubleToLongBits() NaN value
        0x7ff8000000000000i64
    } else {
        // -0.0 + 0.0 == +0.0 under IEEE754 roundTiesToEven rounding mode,
        // which Rust guarantees. Thus, by adding a positive zero we
        // canonicalize signed zero without any branches in one instruction.
        (value + 0.0).to_bits() as i64
    }
}
