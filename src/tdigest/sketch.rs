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
        TDigest::new(Self::DEFAULT_K)
    }
}

impl TDigest {
    /// The default value of K if one is not specified.
    pub const DEFAULT_K: usize = 200;

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

    /// Update this TDigest with the given value (`NaN` values are ignored).
    pub fn update(&mut self, value: f64) {
        if value.is_nan() {
            return;
        }

        if self.buffer.len() == self.centroids_capacity * BUFFER_MULTIPLIER {
            self.compress();
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

    /// Compute approximate quantile value corresponding to the given normalized rank
    ///
    /// # Panics
    ///
    /// If rank is not in [0.0, 1.0], or if TDigest is empty.
    pub fn get_quantile(&mut self, rank: f64) -> f64 {
        assert!(rank >= 0.0 && rank <= 1.0, "rank must be in [0.0, 1.0]");
        assert!(!self.is_empty(), "TDigest is empty");
        self.compress();
        if self.centroids.len() == 1 {
            return self.centroids[0].mean;
        }

        // at least 2 centroids
        let centroids_weight = self.centroids_weight as f64;
        let num_centroids = self.centroids.len();
        let weight = rank * centroids_weight;
        if weight < 1. {
            return self.min;
        }
        if weight > centroids_weight - 1. {
            return self.max;
        }
        let first_weight = self.centroids[0].weight as f64;
        if first_weight > 1. && weight < first_weight / 2. {
            return self.min
                + (((weight - 1.) / ((first_weight / 2.) - 1.))
                    * (self.centroids[0].mean - self.min));
        }
        let last_weight = self.centroids[num_centroids - 1].weight as f64;
        if last_weight > 1. && (centroids_weight - weight <= last_weight / 2.) {
            return self.max
                + (((centroids_weight - weight - 1.) / ((last_weight / 2.) - 1.))
                    * (self.max - self.centroids[num_centroids - 1].mean));
        }

        // interpolate between extremes
        let mut weight_so_far = first_weight / 2.;
        for i in 0..(num_centroids - 1) {
            let dw = (self.centroids[i].weight + self.centroids[i + 1].weight) as f64 / 2.;
            if weight_so_far + dw > weight {
                // the target weight is between centroids i and i+1
                let mut left_weight = 0.;
                if self.centroids[i].weight == 1 {
                    if weight - weight_so_far < 0.5 {
                        return self.centroids[i].mean;
                    }
                    left_weight = 0.5;
                }
                let mut right_weight = 0.;
                if self.centroids[i + 1].weight == 1 {
                    if weight_so_far + dw - weight < 0.5 {
                        return self.centroids[i + 1].mean;
                    }
                    right_weight = 0.5;
                }
                let w1 = weight - weight_so_far - left_weight;
                let w2 = weight_so_far + dw - weight - right_weight;
                return weighted_average(
                    self.centroids[i].mean,
                    w1,
                    self.centroids[i + 1].mean,
                    w2,
                );
            }
            weight_so_far += dw;
        }

        let w1 = weight
            - (self.centroids_weight as f64)
            - ((self.centroids[num_centroids - 1].weight as f64) / 2.);
        let w2 = (self.centroids[num_centroids - 1].weight as f64 / 2.) - w1;
        weighted_average(self.centroids[num_centroids - 1].mean, w1, self.max, w2)
    }

    /// Process buffered values and merge centroids if needed.
    fn compress(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        let mut tmp = Vec::with_capacity(self.buffer.len() + self.centroids.len());
        for &v in &self.buffer {
            tmp.push(Centroid { mean: v, weight: 1 });
        }
        self.merge(tmp, self.buffer.len() as u64)
    }

    /// Merges the given buffer of centroids into this TDigest.
    ///
    /// # Contract
    ///
    /// * `buffer` must have at least one centroid.
    /// * `buffer` is generated from `self.buffer`, and thus:
    ///     * No `NAN` values are present in `buffer`.
    ///     * We should clear `self.buffer` after merging.
    fn merge(&mut self, mut buffer: Vec<Centroid>, weight: u64) {
        buffer.extend(std::mem::take(&mut self.centroids));
        buffer.sort_by(centroid_cmp);
        if self.reverse_merge {
            buffer.reverse();
        }
        self.centroids_weight += weight;

        let mut num_centroids = 0;
        let len = buffer.len();
        self.centroids.push(buffer[0]);
        num_centroids += 1;
        let mut current = 1;
        let mut weight_so_far = 0.;
        while current < len {
            let c = buffer[current];
            let proposed_weight = (self.centroids[num_centroids - 1].weight + c.weight) as f64;
            let mut add_this = false;
            if (current != 1) && (current != (len - 1)) {
                let centroids_weight = self.centroids_weight as f64;
                let q0 = weight_so_far / centroids_weight;
                let q2 = (weight_so_far + proposed_weight) / centroids_weight;
                let normalizer = scale_function::normalizer((2 * self.k) as f64, centroids_weight);
                add_this = proposed_weight
                    <= (centroids_weight
                        * scale_function::max(q0, normalizer)
                            .min(scale_function::max(q2, normalizer)));
            }
            if add_this {
                // merge into existing centroid
                self.centroids[num_centroids - 1].add(c);
            } else {
                // copy to a new centroid
                weight_so_far += self.centroids[num_centroids - 1].weight as f64;
                self.centroids.push(c);
                num_centroids += 1;
            }
            current += 1;
        }

        if self.reverse_merge {
            self.centroids.reverse();
        }
        self.min = self.min.min(self.centroids[0].mean);
        self.max = self.max.max(self.centroids[num_centroids - 1].mean);
        self.reverse_merge = !self.reverse_merge;
        self.buffer.clear();
    }
}

fn centroid_cmp(a: &Centroid, b: &Centroid) -> std::cmp::Ordering {
    match a.mean.partial_cmp(&b.mean) {
        Some(order) => order,
        None => unreachable!("NaN values should never be present in centroids"),
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Centroid {
    mean: f64,
    weight: u64,
}

impl Centroid {
    fn add(&mut self, other: Centroid) {
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

/// Generates cluster sizes proportional to `q*(1-q)`.
///
/// The use of a normalizing function results in a strictly bounded number of clusters no matter
/// how many samples.
///
/// Corresponds to K_2 in the reference implementation
mod scale_function {
    pub(super) fn max(q: f64, normalizer: f64) -> f64 {
        q * (1. - q) / normalizer
    }

    pub(super) fn normalizer(compression: f64, n: f64) -> f64 {
        compression / z(compression, n)
    }

    pub(super) fn z(compression: f64, n: f64) -> f64 {
        4. * (n / compression).ln() + 24.
    }
}

const fn weighted_average(x1: f64, w1: f64, x2: f64, w2: f64) -> f64 {
    (x1 * w1 + x2 * w2) / (w1 + w2)
}
