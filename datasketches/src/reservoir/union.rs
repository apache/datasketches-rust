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

use super::MAX_ITEMS_SEEN;
use super::MAX_K;
use super::MIN_K;
use super::ReservoirSketch;
use super::ReservoirValue;
use super::serialization::deserialize_union;
use super::serialization::serialize_union;
use crate::common::random::random_below;
use crate::common::random::random_f64;
use crate::error::Error;

/// Stateful union operator for [`ReservoirSketch`] instances.
///
/// Combines multiple reservoir samples (and individual items) up to a maximum sample size `max_k`
/// while preserving uniform marginal inclusion probability across the combined stream.
///
/// See the [reservoir module documentation](crate::reservoir#union-semantics) for details.
#[derive(Clone)]
pub struct ReservoirUnion<T> {
    max_k: usize,
    // Invariant: `gadget` is `Some(sketch)` if and only if `!sketch.is_empty()`.
    gadget: Option<ReservoirSketch<T>>,
}

impl<T> fmt::Debug for ReservoirUnion<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReservoirUnion")
            .field("max_k", &self.max_k)
            .field("is_empty", &self.is_empty())
            .field("gadget", &self.gadget)
            .finish()
    }
}

impl<T> ReservoirUnion<T> {
    /// Creates an empty reservoir union with maximum reservoir capacity `max_k`.
    ///
    /// Incoming sketches with `k > max_k` are downsampled to `max_k` during updates.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if `max_k` is outside
    /// `2..=2_147_483_647` (`i32::MAX as usize`).
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::reservoir::ReservoirUnion;
    ///
    /// let union = ReservoirUnion::<i64>::new(128)?;
    /// assert_eq!(union.max_k(), 128);
    /// assert!(union.is_empty());
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn new(max_k: usize) -> Result<Self, Error> {
        if !(MIN_K..=MAX_K).contains(&max_k) {
            return Err(Error::invalid_argument(format!(
                "max_k must be in [{MIN_K}, {MAX_K}], got {max_k}"
            )));
        }
        Ok(Self {
            max_k,
            gadget: None,
        })
    }

    /// Returns the maximum reservoir capacity `max_k` configured for this union.
    pub fn max_k(&self) -> usize {
        self.max_k
    }

    /// Returns `true` if the union has observed no items (`total_weight == 0`).
    pub fn is_empty(&self) -> bool {
        self.gadget.is_none()
    }

    /// Updates the union with a single unweighted item.
    ///
    /// # Panics
    ///
    /// Panics without modifying `self` if the union's total stream weight has reached `2^48 - 1`
    /// (`281_474_976_710_655`).
    pub fn update_value(&mut self, item: T) {
        let current_weight = self
            .gadget
            .as_ref()
            .map_or(0, ReservoirSketch::total_weight);
        assert!(
            current_weight < MAX_ITEMS_SEEN,
            "total stream weight exceeds maximum ({MAX_ITEMS_SEEN})"
        );

        let gadget = self.gadget.get_or_insert_with(|| {
            ReservoirSketch::new(self.max_k).expect("max_k is validated to [MIN_K, MAX_K]")
        });
        gadget.update(item);
    }

    /// Merges `sketch` into this union.
    ///
    /// Empty input sketches are ignored. If `sketch.k() > self.max_k()`, `sketch` is downsampled
    /// to `max_k` without modifying `sketch`.
    ///
    /// # Panics
    ///
    /// Panics without modifying `self` if the combined stream weight would exceed `2^48 - 1`
    /// (`281_474_976_710_655`).
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::reservoir::ReservoirSketch;
    /// use datasketches::reservoir::ReservoirUnion;
    ///
    /// let mut left = ReservoirSketch::<i64>::new(32)?;
    /// let mut right = ReservoirSketch::<i64>::new(32)?;
    /// for i in 0..50 {
    ///     left.update(i);
    ///     right.update(100 + i);
    /// }
    ///
    /// let mut union = ReservoirUnion::<i64>::new(32)?;
    /// union.update(&left);
    /// union.update(&right);
    ///
    /// let result = union.to_sketch();
    /// assert_eq!(result.total_weight(), 100);
    /// assert_eq!(result.num_retained(), 32);
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn update(&mut self, sketch: &ReservoirSketch<T>)
    where
        T: Clone,
    {
        if sketch.is_empty() {
            return;
        }

        let current_weight = self
            .gadget
            .as_ref()
            .map_or(0, ReservoirSketch::total_weight);
        assert!(
            sketch.total_weight() <= MAX_ITEMS_SEEN - current_weight,
            "combined total stream weight exceeds maximum ({MAX_ITEMS_SEEN})"
        );

        if sketch.k() > self.max_k {
            let downsampled = sketch.downsampled_copy(self.max_k);
            self.merge_owned(downsampled);
            return;
        }

        let Some(gadget) = self.gadget.take() else {
            self.gadget = Some(self.init_gadget_from_borrowed(sketch));
            return;
        };

        self.gadget = Some(two_way_merge_borrowed(gadget, sketch));
    }

    /// Returns an independent [`ReservoirSketch`] representing the union's current state.
    ///
    /// When the union is empty, returns an empty sketch configured with `max_k()`.
    pub fn to_sketch(&self) -> ReservoirSketch<T>
    where
        T: Clone,
    {
        match &self.gadget {
            Some(gadget) => gadget.clone(),
            None => ReservoirSketch::new(self.max_k).expect("max_k is validated to [MIN_K, MAX_K]"),
        }
    }

    /// Consumes this union and returns its underlying [`ReservoirSketch`] without cloning.
    ///
    /// When the union is empty, returns an empty sketch configured with `max_k()`.
    pub fn into_sketch(self) -> ReservoirSketch<T> {
        self.gadget.unwrap_or_else(|| {
            ReservoirSketch::new(self.max_k).expect("max_k is validated to [MIN_K, MAX_K]")
        })
    }

    /// Resets the union to its empty state while preserving `max_k()`.
    pub fn reset(&mut self) {
        self.gadget = None;
    }

    /// Returns an estimate of the union's heap memory usage in bytes.
    pub fn estimated_size(&self) -> usize {
        self.gadget
            .as_ref()
            .map_or(0, ReservoirSketch::estimated_size)
    }

    /// Serializes this union into the DataSketches reservoir union binary format (version 2).
    ///
    /// # Cross-language compatibility
    ///
    /// The serialized data can be read by other DataSketches implementations when using a
    /// compatible item encoding (`i64`, `f64`, or UTF-8 `String`).
    pub fn serialize(&self) -> Vec<u8>
    where
        T: ReservoirValue,
    {
        serialize_union(self)
    }

    /// Deserializes a union from bytes in the DataSketches reservoir union binary format.
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
        deserialize_union(bytes)
    }

    pub(super) fn gadget(&self) -> Option<&ReservoirSketch<T>> {
        self.gadget.as_ref()
    }

    pub(super) fn merge_owned(&mut self, sketch: ReservoirSketch<T>) {
        if sketch.is_empty() {
            return;
        }

        let current_weight = self
            .gadget
            .as_ref()
            .map_or(0, ReservoirSketch::total_weight);
        assert!(
            sketch.total_weight() <= MAX_ITEMS_SEEN - current_weight,
            "combined total stream weight exceeds maximum ({MAX_ITEMS_SEEN})"
        );

        let sketch = if sketch.k() > self.max_k {
            sketch.downsample_owned(self.max_k)
        } else {
            sketch
        };

        let Some(gadget) = self.gadget.take() else {
            self.gadget = Some(self.init_gadget_from_owned(sketch));
            return;
        };

        self.gadget = Some(two_way_merge_owned(gadget, sketch));
    }

    fn init_gadget_from_borrowed(&self, source: &ReservoirSketch<T>) -> ReservoirSketch<T>
    where
        T: Clone,
    {
        if source.k() < self.max_k && source.total_weight() <= source.k() as u64 {
            let mut gadget =
                ReservoirSketch::new(self.max_k).expect("max_k is validated to [MIN_K, MAX_K]");
            merge_standard_borrowed(&mut gadget, source);
            gadget
        } else {
            source.clone()
        }
    }

    fn init_gadget_from_owned(&self, source: ReservoirSketch<T>) -> ReservoirSketch<T> {
        if source.k() < self.max_k && source.total_weight() <= source.k() as u64 {
            let mut gadget =
                ReservoirSketch::new(self.max_k).expect("max_k is validated to [MIN_K, MAX_K]");
            merge_standard_owned(&mut gadget, source);
            gadget
        } else {
            source
        }
    }
}

/// Three-way sketch state classification from `datasketches-java`:
/// - `uni` (`n < k`): exact unit-weight source; can only accept unit weights directly.
/// - `mid` (`n == k`): exact unit-weight source; can also accept light general weights.
/// - `gen` (`n > k`): general-weight source (`n / k > 1`); can accept light general weights.
///
/// In the `(gen, gen)` case, at least one direction satisfies strict lightness
/// `n_s / k_s < n_t / (k_t - 1)` for `k_t >= 2`: if `n_s / k_s <= n_t / k_t`, then
/// `n_s / k_s < n_t / (k_t - 1)`; otherwise `n_t / k_t < n_s / k_s < n_s / (k_s - 1)`.
#[inline]
fn is_source_light_for_target(
    source_n: u64,
    source_k: usize,
    target_n: u64,
    target_k: usize,
) -> bool {
    debug_assert!(target_k >= MIN_K);
    u128::from(source_n) * ((target_k - 1) as u128) < u128::from(target_n) * (source_k as u128)
}

fn two_way_merge_borrowed<T: Clone>(
    mut gadget: ReservoirSketch<T>,
    source: &ReservoirSketch<T>,
) -> ReservoirSketch<T> {
    if source.total_weight() <= source.k() as u64 {
        merge_standard_borrowed(&mut gadget, source);
        gadget
    } else if gadget.total_weight() < gadget.k() as u64 {
        let mut next = source.clone();
        merge_standard_owned(&mut next, gadget);
        next
    } else if is_source_light_for_target(
        source.total_weight(),
        source.k(),
        gadget.total_weight(),
        gadget.k(),
    ) {
        merge_weighted_borrowed(&mut gadget, source);
        gadget
    } else {
        let mut next = source.clone();
        merge_weighted_owned(&mut next, gadget);
        next
    }
}

fn two_way_merge_owned<T>(
    mut gadget: ReservoirSketch<T>,
    mut source: ReservoirSketch<T>,
) -> ReservoirSketch<T> {
    if source.total_weight() <= source.k() as u64 {
        merge_standard_owned(&mut gadget, source);
        gadget
    } else if gadget.total_weight() < gadget.k() as u64 {
        merge_standard_owned(&mut source, gadget);
        source
    } else if is_source_light_for_target(
        source.total_weight(),
        source.k(),
        gadget.total_weight(),
        gadget.k(),
    ) {
        merge_weighted_owned(&mut gadget, source);
        gadget
    } else {
        merge_weighted_owned(&mut source, gadget);
        source
    }
}

fn merge_standard_borrowed<T: Clone>(target: &mut ReservoirSketch<T>, source: &ReservoirSketch<T>) {
    debug_assert!(source.total_weight() <= source.k() as u64);
    for item in source.samples() {
        target.update_cloned(item);
    }
}

fn merge_standard_owned<T>(target: &mut ReservoirSketch<T>, source: ReservoirSketch<T>) {
    debug_assert!(source.total_weight() <= source.k() as u64);
    for item in source.into_samples() {
        target.update(item);
    }
}

fn merge_weighted_borrowed<T: Clone>(target: &mut ReservoirSketch<T>, source: &ReservoirSketch<T>) {
    debug_assert!(target.total_weight() >= target.k() as u64);
    let num_source_samples = source.k();
    let source_n = source.total_weight();
    let source_item_weight = (source_n as f64) / (num_source_samples as f64);
    let target_k = target.k();
    let base_target_n = target.total_weight() as f64;
    let rescaled_prob = (target_k as f64) * source_item_weight;

    for (idx, item) in source.samples().iter().enumerate() {
        let target_total = base_target_n + ((idx + 1) as f64) * source_item_weight;
        debug_assert!(rescaled_prob < target_total);
        let rescaled_flip = target_total * random_f64();
        if rescaled_flip < rescaled_prob {
            let slot_no = random_below(target_k);
            target.replace_sample(slot_no, item.clone());
        }
    }

    target.force_increment_total_weight(source_n);
}

fn merge_weighted_owned<T>(target: &mut ReservoirSketch<T>, source: ReservoirSketch<T>) {
    debug_assert!(target.total_weight() >= target.k() as u64);
    let num_source_samples = source.k();
    let source_n = source.total_weight();
    let source_item_weight = (source_n as f64) / (num_source_samples as f64);
    let target_k = target.k();
    let base_target_n = target.total_weight() as f64;
    let rescaled_prob = (target_k as f64) * source_item_weight;

    for (idx, item) in source.into_samples().into_iter().enumerate() {
        let target_total = base_target_n + ((idx + 1) as f64) * source_item_weight;
        debug_assert!(rescaled_prob < target_total);
        let rescaled_flip = target_total * random_f64();
        if rescaled_flip < rescaled_prob {
            let slot_no = random_below(target_k);
            target.replace_sample(slot_no, item);
        }
    }

    target.force_increment_total_weight(source_n);
}
