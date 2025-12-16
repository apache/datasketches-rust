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

use crate::tdigest::DEFAULT_K;

const BUFFER_MULTIPLIER: usize = 4;

/// T-Digest sketch for estimating quantiles and ranks.
///
/// See the [module documentation](super) for more details.
#[derive(Debug, Clone, PartialEq)]
pub struct TDigest {
    k: usize,

    reverse_merge: bool,
    min: f64,
    max: f64,

    centroids: Vec<Centroid>,
    centroids_weight: u64,
    centroids_capacity: usize,
    buffer: Vec<f64>,
}

impl Default for TDigest {
    fn default() -> Self {
        TDigest::new(DEFAULT_K)
    }
}

impl TDigest {
    /// Creates a tdigest instance with the given value of k.
    ///
    /// # Panics
    ///
    /// If k is less than 10
    pub fn new(k: usize) -> Self {
        assert!(k >= 10, "k must be at least 10");

        let fudge = if k < 30 { 30 } else { 10 };
        let centroids_capacity = (k * 2) + fudge;

        let centroids = Vec::with_capacity(centroids_capacity);
        let buffer = Vec::with_capacity(centroids_capacity * BUFFER_MULTIPLIER);

        TDigest {
            k,
            reverse_merge: false,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            centroids,
            centroids_weight: 0,
            centroids_capacity,
            buffer,
        }
    }

    /// Update this TDigest with the given value.
    pub fn update(&mut self, value: f64) {
        if value.is_nan() {
            return;
        }

        if self.buffer.len() == self.centroids_capacity * BUFFER_MULTIPLIER {
            todo!("implement compress()");
        }

        self.buffer.push(value);
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }

    /// Returns true if TDigest has not seen any data.
    pub fn is_empty(&self) -> bool {
        self.centroids.is_empty() && self.buffer.is_empty()
    }

    /// Returns minimum value seen by TDigest.
    pub fn min_value(&self) -> Option<f64> {
        if self.is_empty() {
            None
        } else {
            Some(self.min)
        }
    }

    /// Returns maximum value seen by TDigest.
    pub fn max_value(&self) -> Option<f64> {
        if self.is_empty() {
            None
        } else {
            Some(self.max)
        }
    }

    /// Returns total weight.
    pub fn total_weight(&self) -> u64 {
        self.centroids_weight + (self.buffer.len() as u64)
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Centroid {
    mean: f64,
    weight: u64,
}

impl Centroid {
    fn add(&mut self, other: &Centroid) {
        if self.weight != 0 {
            let total_weight = self.weight + other.weight;
            self.mean += (other.weight as f64) * (other.mean - self.mean) / (total_weight as f64);
            self.weight = total_weight;
        } else {
            self.mean = other.mean;
            self.weight = other.weight;
        }
    }
}
