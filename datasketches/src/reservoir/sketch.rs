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

use std::fmt;
use std::mem::size_of;
use std::slice;
use std::vec;

use super::DEFAULT_KAPPA;
use super::MAX_ITEMS_SEEN;
use super::MAX_K;
use super::MIN_K;
use super::MIN_LG_ARR_ITEMS;
use super::ReservoirValue;
use super::serialization::deserialize_sketch;
use super::serialization::serialize_sketch;
use crate::common::ResizeFactor;
use crate::common::binomial_bounds::approximate_lower_bound_on_p;
use crate::common::binomial_bounds::approximate_upper_bound_on_p;
use crate::common::random::random_below_u64;
use crate::error::Error;

/// Estimated subset sum and confidence bounds for items matching a predicate in a
/// [`ReservoirSketch`].
///
/// Confidence bounds are computed heuristically at approximately +/- 2 standard deviations using
/// a finite-population-adjusted binomial proportion interval, or returned exactly when the sketch
/// is in exact mode (`total_weight <= k`).
#[derive(Clone, Copy, Debug)]
pub struct SampleSubsetSummary {
    lower_bound: f64,
    estimate: f64,
    upper_bound: f64,
    total_weight: f64,
}

impl SampleSubsetSummary {
    pub(super) const fn new(
        lower_bound: f64,
        estimate: f64,
        upper_bound: f64,
        total_weight: f64,
    ) -> Self {
        Self {
            lower_bound,
            estimate,
            upper_bound,
            total_weight,
        }
    }

    /// Returns the approximate lower bound on the matching subset count.
    pub fn lower_bound(&self) -> f64 {
        self.lower_bound
    }

    /// Returns the estimated count of matching items across the stream.
    pub fn estimate(&self) -> f64 {
        self.estimate
    }

    /// Returns the approximate upper bound on the matching subset count.
    pub fn upper_bound(&self) -> f64 {
        self.upper_bound
    }

    /// Returns the total stream weight represented by the sketch (`0.0` when empty).
    pub fn total_weight(&self) -> f64 {
        self.total_weight
    }
}

/// Reservoir sampling sketch over unweighted stream items.
///
/// Retains up to `k` uniformly sampled items from an input stream. See the
/// [reservoir module documentation](crate::reservoir) for algorithm and compatibility details.
#[derive(Clone)]
pub struct ReservoirSketch<T> {
    k: usize,
    resize_factor: ResizeFactor,
    total_weight: u64,
    curr_items_alloc: usize,
    data: Vec<T>,
}

impl<T> fmt::Debug for ReservoirSketch<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReservoirSketch")
            .field("k", &self.k)
            .field("resize_factor", &self.resize_factor)
            .field("total_weight", &self.total_weight)
            .field("is_empty", &self.is_empty())
            .field("is_estimation_mode", &self.is_estimation_mode())
            .field("num_retained", &self.num_retained())
            .finish()
    }
}

impl<T> ReservoirSketch<T> {
    /// Creates an empty reservoir sketch with maximum sample capacity `k` and
    /// [`ResizeFactor::X8`].
    ///
    /// Unlike power-of-two sketches, `k` may be any integer in `2..=2_147_483_647`.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if `k` is outside
    /// `2..=2_147_483_647` (`i32::MAX as usize`).
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::reservoir::ReservoirSketch;
    ///
    /// let mut sketch = ReservoirSketch::<i64>::new(128)?;
    /// sketch.update(42);
    /// assert_eq!(sketch.k(), 128);
    /// assert_eq!(sketch.num_retained(), 1);
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn new(k: usize) -> Result<Self, Error> {
        Self::with_resize_factor(k, ResizeFactor::X8)
    }

    /// Creates an empty reservoir sketch with maximum sample capacity `k` and the specified
    /// [`ResizeFactor`].
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if `k` is outside
    /// `2..=2_147_483_647` (`i32::MAX as usize`).
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::common::ResizeFactor;
    /// use datasketches::reservoir::ReservoirSketch;
    ///
    /// let sketch = ReservoirSketch::<String>::with_resize_factor(64, ResizeFactor::X2)?;
    /// assert_eq!(sketch.resize_factor(), ResizeFactor::X2);
    /// assert!(sketch.is_empty());
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn with_resize_factor(k: usize, resize_factor: ResizeFactor) -> Result<Self, Error> {
        if !(MIN_K..=MAX_K).contains(&k) {
            return Err(Error::invalid_argument(format!(
                "k must be in [{MIN_K}, {MAX_K}], got {k}"
            )));
        }
        Ok(Self {
            k,
            resize_factor,
            total_weight: 0,
            curr_items_alloc: 0,
            data: Vec::new(),
        })
    }

    /// Returns the maximum number of samples `k` retained in the reservoir.
    pub fn k(&self) -> usize {
        self.k
    }

    /// Returns the configured [`ResizeFactor`] for internal buffer growth.
    pub fn resize_factor(&self) -> ResizeFactor {
        self.resize_factor
    }

    /// Returns the total number of stream items presented to the sketch.
    pub fn total_weight(&self) -> u64 {
        self.total_weight
    }

    /// Returns the number of samples currently retained in the reservoir (`min(k, total_weight)`).
    pub fn num_retained(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if the sketch has observed no items (`total_weight() == 0`).
    pub fn is_empty(&self) -> bool {
        self.total_weight == 0
    }

    /// Returns `true` if the sketch is in sampling mode (`total_weight() > k()`).
    pub fn is_estimation_mode(&self) -> bool {
        self.total_weight > self.k as u64
    }

    /// Returns a slice of the items currently retained in the reservoir.
    ///
    /// While `total_weight() <= k()`, items appear in insertion order. Once sampling begins, slot
    /// order reflects randomized replacements.
    pub fn samples(&self) -> &[T] {
        &self.data
    }

    /// Consumes the sketch and returns its retained sample vector without cloning.
    pub fn into_samples(self) -> Vec<T> {
        self.data
    }

    /// Returns an iterator over the retained samples.
    pub fn iter(&self) -> slice::Iter<'_, T> {
        self.data.iter()
    }

    /// Updates the sketch with a single unweighted item.
    ///
    /// # Panics
    ///
    /// Panics without modifying `self` if [`total_weight`](Self::total_weight) has reached
    /// `2^48 - 1` (`281_474_976_710_655`).
    pub fn update(&mut self, item: T) {
        assert!(
            self.total_weight < MAX_ITEMS_SEEN,
            "total stream weight exceeds maximum ({MAX_ITEMS_SEEN})"
        );

        if self.total_weight < self.k as u64 {
            if self.data.len() == self.curr_items_alloc {
                self.grow_reservoir();
            }
            self.data.push(item);
            self.total_weight += 1;
        } else {
            let new_weight = self.total_weight + 1;
            let slot = random_below_u64(new_weight);
            if slot < self.k as u64 {
                self.data[slot as usize] = item;
            }
            self.total_weight = new_weight;
        }
    }

    /// Estimates the number of stream items matching `predicate`, returning the point estimate and
    /// heuristic +/- 2 standard deviation bounds.
    ///
    /// Returns exact counts (`lower_bound == estimate == upper_bound`) when the sketch is empty or
    /// in exact mode (`total_weight() <= k()`).
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::reservoir::ReservoirSketch;
    ///
    /// let mut sketch = ReservoirSketch::<i64>::new(128)?;
    /// for i in 0..1_000 {
    ///     sketch.update(i);
    /// }
    /// let summary = sketch.estimate_subset_sum(|&x| x % 2 == 0);
    /// assert_eq!(summary.total_weight(), 1_000.0);
    /// assert!(summary.lower_bound() <= summary.estimate());
    /// assert!(summary.estimate() <= summary.upper_bound());
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn estimate_subset_sum<F>(&self, mut predicate: F) -> SampleSubsetSummary
    where
        F: FnMut(&T) -> bool,
    {
        if self.total_weight == 0 {
            return SampleSubsetSummary::new(0.0, 0.0, 0.0, 0.0);
        }

        let num_samples = self.data.len();
        let mut true_count = 0_usize;
        for item in &self.data {
            if predicate(item) {
                true_count += 1;
            }
        }

        if self.total_weight <= self.k as u64 {
            let exact = true_count as f64;
            return SampleSubsetSummary::new(exact, exact, exact, num_samples as f64);
        }

        let total = self.total_weight as f64;
        let num_samples_u64 = num_samples as u64;
        let true_count_u64 = true_count as u64;
        let sampling_rate = (num_samples as f64) / total;
        let adjusted_kappa = DEFAULT_KAPPA * (1.0 - sampling_rate).max(0.0).sqrt();

        let lb_fraction =
            approximate_lower_bound_on_p(num_samples_u64, true_count_u64, adjusted_kappa)
                .expect("true_count cannot exceed num_samples");
        let ub_fraction =
            approximate_upper_bound_on_p(num_samples_u64, true_count_u64, adjusted_kappa)
                .expect("true_count cannot exceed num_samples");
        let est_fraction = (true_count as f64) / (num_samples as f64);

        SampleSubsetSummary::new(
            total * lb_fraction,
            total * est_fraction,
            total * ub_fraction,
            total,
        )
    }

    /// Resets the sketch to its empty state while preserving `k` and `resize_factor`.
    pub fn reset(&mut self) {
        self.total_weight = 0;
        self.curr_items_alloc = 0;
        self.data = Vec::new();
    }

    /// Returns an estimate of the sketch's heap memory usage in bytes.
    pub fn estimated_size(&self) -> usize {
        self.data.capacity() * size_of::<T>()
    }

    /// Serializes this sketch into the DataSketches reservoir binary format (version 2).
    ///
    /// # Cross-language compatibility
    ///
    /// The serialized data can be read by other DataSketches implementations when using a
    /// compatible item encoding (`i64`, `f64`, or UTF-8 `String`).
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::reservoir::ReservoirSketch;
    ///
    /// let mut sketch = ReservoirSketch::<i64>::new(32)?;
    /// sketch.update(10);
    /// sketch.update(20);
    ///
    /// let bytes = sketch.serialize();
    /// let decoded = ReservoirSketch::<i64>::deserialize(&bytes)?;
    /// assert_eq!(decoded.samples(), &[10, 20]);
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn serialize(&self) -> Vec<u8>
    where
        T: ReservoirValue,
    {
        serialize_sketch(self)
    }

    /// Deserializes a sketch from bytes in the DataSketches reservoir binary format.
    ///
    /// Also accepts data serialized by other DataSketches implementations (including legacy serial
    /// version 1 images) when using a compatible item encoding (`i64`, `f64`, or UTF-8 `String`).
    ///
    /// # Errors
    ///
    /// Returns [`InvalidData`](crate::error::ErrorKind::InvalidData) if the input is truncated,
    /// has an unsupported family ID, serial version, preamble length, or flag bit, violates
    /// reservoir parameter or count invariants, or has trailing bytes.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error>
    where
        T: ReservoirValue,
    {
        deserialize_sketch(bytes)
    }

    pub(super) fn from_parts(
        k: usize,
        resize_factor: ResizeFactor,
        total_weight: u64,
        data: Vec<T>,
    ) -> Self {
        let curr_items_alloc = data.capacity().min(k).max(data.len());
        Self {
            k,
            resize_factor,
            total_weight,
            curr_items_alloc,
            data,
        }
    }

    #[inline]
    pub(super) fn update_cloned(&mut self, item: &T)
    where
        T: Clone,
    {
        assert!(
            self.total_weight < MAX_ITEMS_SEEN,
            "total stream weight exceeds maximum ({MAX_ITEMS_SEEN})"
        );

        if self.total_weight < self.k as u64 {
            if self.data.len() == self.curr_items_alloc {
                self.grow_reservoir();
            }
            self.data.push(item.clone());
            self.total_weight += 1;
        } else {
            let new_weight = self.total_weight + 1;
            let slot = random_below_u64(new_weight);
            if slot < self.k as u64 {
                self.data[slot as usize] = item.clone();
            }
            self.total_weight = new_weight;
        }
    }

    pub(super) fn downsampled_copy(&self, new_k: usize) -> Self
    where
        T: Clone,
    {
        debug_assert!((MIN_K..self.k).contains(&new_k));
        let mut result = Self::with_resize_factor(new_k, self.resize_factor)
            .expect("new_k is validated to [MIN_K, MAX_K]");
        for item in &self.data {
            result.update_cloned(item);
        }
        if result.total_weight < self.total_weight {
            result.force_increment_total_weight(self.total_weight - result.total_weight);
        }
        result
    }

    pub(super) fn downsample_owned(self, new_k: usize) -> Self {
        debug_assert!((MIN_K..self.k).contains(&new_k));
        let original_weight = self.total_weight;
        let mut result = Self::with_resize_factor(new_k, self.resize_factor)
            .expect("new_k is validated to [MIN_K, MAX_K]");
        for item in self.data {
            result.update(item);
        }
        if result.total_weight < original_weight {
            result.force_increment_total_weight(original_weight - result.total_weight);
        }
        result
    }

    #[inline]
    pub(super) fn replace_sample(&mut self, pos: usize, item: T) {
        self.data[pos] = item;
    }

    #[inline]
    pub(super) fn force_increment_total_weight(&mut self, delta: u64) {
        assert!(
            delta <= MAX_ITEMS_SEEN - self.total_weight,
            "total stream weight exceeds maximum ({MAX_ITEMS_SEEN})"
        );
        self.total_weight += delta;
    }

    fn grow_reservoir(&mut self) {
        let next_alloc = if self.resize_factor == ResizeFactor::X1 {
            self.k
        } else if self.curr_items_alloc == 0 {
            let lg_k = ceiling_lg_k(self.k);
            let init_lg = self
                .resize_factor
                .starting_sub_multiple(lg_k, MIN_LG_ARR_ITEMS);
            adjusted_size(self.k, 1_usize << init_lg)
        } else {
            let lg_k = ceiling_lg_k(self.k);
            let min_lg = ceiling_lg_k(self.curr_items_alloc).max(MIN_LG_ARR_ITEMS);
            let aligned_lg = self.resize_factor.starting_sub_multiple(lg_k, min_lg);
            let aligned = 1_usize << aligned_lg;
            let target = if aligned > self.curr_items_alloc {
                aligned
            } else {
                aligned.saturating_mul(self.resize_factor.value())
            };
            adjusted_size(self.k, target.max(self.data.len() + 1))
        };

        if size_of::<T>() > 0 && next_alloc > self.data.capacity() {
            self.data.reserve_exact(next_alloc - self.data.len());
        }
        self.curr_items_alloc = next_alloc;
    }
}

impl<'a, T> IntoIterator for &'a ReservoirSketch<T> {
    type Item = &'a T;
    type IntoIter = slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T> IntoIterator for ReservoirSketch<T> {
    type Item = T;
    type IntoIter = vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}

#[inline]
fn ceiling_lg_k(n: usize) -> u8 {
    const TOP_PWR_OF_2: usize = 1 << 30;
    if n <= 1 {
        0
    } else if n >= TOP_PWR_OF_2 {
        30
    } else {
        n.next_power_of_two().trailing_zeros() as u8
    }
}

#[inline]
fn adjusted_size(max_size: usize, resize_target: usize) -> usize {
    if (resize_target as u64) * 2 > max_size as u64 {
        max_size
    } else {
        resize_target
    }
}
