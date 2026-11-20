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

use super::DEFAULT_ALPHA;
use super::DEFAULT_MAX_BUCKETS;
use super::MIN_ALPHA;
use super::MIN_MAX_BUCKETS;
use super::mapping::Mapping;
use super::serialization::deserialize_sketch;
use super::serialization::serialize_sketch;
use super::store::BucketStore;
use crate::error::Error;

/// Uniform DDSketch (`UDDSketch`) for reproducible quantile estimation with a relative value error
/// bound.
///
/// See the [module-level documentation](super) for algorithm details and reproducibility
/// guarantees.
#[derive(Clone)]
pub struct UddSketch {
    max_buckets: u32,
    mapping: Mapping,
    num_collapses: u8,
    total_weight: u64,
    zero_weight: u64,
    min: f64,
    max: f64,
    neg: BucketStore,
    pos: BucketStore,
}

impl fmt::Debug for UddSketch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UddSketch")
            .field("max_buckets", &self.max_buckets)
            .field("initial_alpha", &self.initial_alpha())
            .field("alpha", &self.alpha())
            .field("num_collapses", &self.num_collapses)
            .field("is_empty", &self.is_empty())
            .field("total_weight", &self.total_weight)
            .field("zero_weight", &self.zero_weight)
            .field("num_retained", &self.num_retained())
            .field("min_value", &self.min_value())
            .field("max_value", &self.max_value())
            .finish()
    }
}

impl Default for UddSketch {
    /// Creates a sketch with `max_buckets = 512` and `initial_alpha = 0.01`.
    fn default() -> Self {
        Self::new(DEFAULT_MAX_BUCKETS, DEFAULT_ALPHA).unwrap()
    }
}

impl UddSketch {
    /// Creates an empty sketch with the given bucket budget and initial relative accuracy target.
    ///
    /// `max_buckets` bounds the combined number of retained negative and positive buckets; exact
    /// zeros are tracked in a dedicated counter without consuming a bucket slot.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if `max_buckets < 4`
    /// or `initial_alpha` is NaN or outside `[1e-12, 1.0)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::uddsketch::UddSketch;
    ///
    /// let sketch = UddSketch::new(512, 0.01)?;
    /// assert_eq!(sketch.max_buckets(), 512);
    /// assert_eq!(sketch.initial_alpha(), 0.01);
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn new(max_buckets: u32, initial_alpha: f64) -> Result<Self, Error> {
        if max_buckets < MIN_MAX_BUCKETS {
            return Err(Error::invalid_argument(format!(
                "max_buckets must be at least {MIN_MAX_BUCKETS}, got {max_buckets}"
            )));
        }
        if !(MIN_ALPHA..1.0).contains(&initial_alpha) {
            return Err(Error::invalid_argument(format!(
                "initial_alpha must be in [{MIN_ALPHA}, 1.0), got {initial_alpha}"
            )));
        }

        Ok(Self::from_parts(
            max_buckets,
            Mapping::new(initial_alpha),
            0,
            0,
            0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            BucketStore::default(),
            BucketStore::default(),
        ))
    }

    /// Returns the maximum number of nonzero buckets retained by this sketch.
    pub fn max_buckets(&self) -> u32 {
        self.max_buckets
    }

    /// Returns the configured initial relative error parameter before any uniform collapses.
    pub fn initial_alpha(&self) -> f64 {
        self.mapping.initial_alpha()
    }

    /// Returns the effective relative value error bound at the current collapse level.
    ///
    /// Equal to [`initial_alpha`](Self::initial_alpha) until a uniform collapse occurs, after which
    /// each collapse updates `alpha` to `2 * alpha / (1 + alpha^2)`. For nonzero quantile results,
    /// `abs(estimate - value) <= sketch.alpha() * abs(value)` up to final `f64` representation
    /// rounding (`0.5` ULP, or `0.5 * f64::from_bits(1)` in the subnormal range).
    pub fn alpha(&self) -> f64 {
        self.mapping.alpha_at_level(self.num_collapses)
    }

    /// Returns the number of uniform collapses performed on this sketch.
    pub fn num_collapses(&self) -> u8 {
        self.num_collapses
    }

    /// Returns `true` if this sketch has not observed any finite values.
    pub fn is_empty(&self) -> bool {
        self.total_weight == 0
    }

    /// Returns the total accumulated stream weight across negative, zero, and positive values.
    pub fn total_weight(&self) -> u64 {
        self.total_weight
    }

    /// Returns the accumulated stream weight of exact zero (`0.0` and `-0.0`) values.
    pub fn zero_weight(&self) -> u64 {
        self.zero_weight
    }

    /// Returns the number of nonzero buckets currently retained (`<= max_buckets()`).
    pub fn num_retained(&self) -> usize {
        self.neg.len() + self.pos.len()
    }

    /// Returns the minimum finite value seen by this sketch, or `None` if it is empty.
    pub fn min_value(&self) -> Option<f64> {
        if self.is_empty() {
            None
        } else {
            Some(self.min)
        }
    }

    /// Returns the maximum finite value seen by this sketch, or `None` if it is empty.
    pub fn max_value(&self) -> Option<f64> {
        if self.is_empty() {
            None
        } else {
            Some(self.max)
        }
    }

    /// Returns an estimate of heap memory currently allocated by this sketch in bytes.
    pub fn estimated_size(&self) -> usize {
        self.neg.estimated_size() + self.pos.estimated_size()
    }

    /// Resets this sketch to its empty state while retaining configuration and allocated capacity.
    pub fn reset(&mut self) {
        self.num_collapses = 0;
        self.total_weight = 0;
        self.zero_weight = 0;
        self.min = f64::INFINITY;
        self.max = f64::NEG_INFINITY;
        self.neg.reset();
        self.pos.reset();
    }

    /// Updates this sketch with a single observation of `value`.
    ///
    /// [`f64::NAN`], [`f64::INFINITY`], and [`f64::NEG_INFINITY`] values are ignored. Negative zero
    /// (`-0.0`) is canonicalized to `0.0`.
    ///
    /// # Panics
    ///
    /// Panics without modifying the sketch if the total stream weight would exceed [`u64::MAX`].
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::uddsketch::UddSketch;
    ///
    /// let mut sketch = UddSketch::default();
    /// sketch.update(42.0);
    /// assert_eq!(sketch.total_weight(), 1);
    /// ```
    #[inline]
    pub fn update(&mut self, value: f64) {
        self.update_with_weight(value, 1);
    }

    /// Updates this sketch with `value` repeated `weight` times.
    ///
    /// [`f64::NAN`], [`f64::INFINITY`], and [`f64::NEG_INFINITY`] values are ignored, and a
    /// `weight` of `0` is a no-op. Negative zero (`-0.0`) is canonicalized to `0.0`.
    ///
    /// # Panics
    ///
    /// Panics without modifying the sketch if the total stream weight would exceed [`u64::MAX`].
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::uddsketch::UddSketch;
    ///
    /// let mut sketch = UddSketch::default();
    /// sketch.update_with_weight(10.0, 5);
    /// assert_eq!(sketch.total_weight(), 5);
    /// ```
    pub fn update_with_weight(&mut self, value: f64, weight: u64) {
        if !value.is_finite() || weight == 0 {
            return;
        }
        let next_total = self
            .total_weight
            .checked_add(weight)
            .expect("total stream weight overflow");

        let canonical = if value == 0.0 { 0.0 } else { value };
        self.total_weight = next_total;
        self.min = self.min.min(canonical);
        self.max = self.max.max(canonical);

        if canonical == 0.0 {
            self.zero_weight += weight;
            return;
        }

        if canonical > 0.0 {
            let key = self.mapping.key_at_level(canonical, self.num_collapses);
            self.pos.insert(key, weight);
        } else {
            let key = self.mapping.key_at_level(-canonical, self.num_collapses);
            self.neg.insert(key, weight);
        }

        self.enforce_budget();
    }

    /// Merges `other` into `self`.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) without modifying
    /// `self` if `self` and `other` have different `max_buckets` or `initial_alpha`, or if the
    /// combined stream weight would exceed [`u64::MAX`].
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::uddsketch::UddSketch;
    ///
    /// let mut left = UddSketch::new(256, 0.01)?;
    /// let mut right = UddSketch::new(256, 0.01)?;
    /// left.update(1.0);
    /// right.update(100.0);
    /// left.merge(&right)?;
    /// assert_eq!(left.total_weight(), 2);
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn merge(&mut self, other: &Self) -> Result<(), Error> {
        if self.max_buckets != other.max_buckets {
            return Err(Error::invalid_argument(format!(
                "cannot merge UDDSketch instances with different max_buckets: expected {}, got {}",
                self.max_buckets, other.max_buckets
            )));
        }
        if self.initial_alpha().to_bits() != other.initial_alpha().to_bits() {
            return Err(Error::invalid_argument(format!(
                "cannot merge UDDSketch instances with different initial_alpha: expected {}, got {}",
                self.initial_alpha(),
                other.initial_alpha()
            )));
        }

        if other.is_empty() {
            return Ok(());
        }
        let next_total = self
            .total_weight
            .checked_add(other.total_weight)
            .ok_or_else(|| Error::invalid_argument("total stream weight overflow"))?;

        if self.is_empty() {
            self.num_collapses = other.num_collapses;
            self.total_weight = other.total_weight;
            self.zero_weight = other.zero_weight;
            self.min = other.min;
            self.max = other.max;
            self.neg.clone_from_store(&other.neg);
            self.pos.clone_from_store(&other.pos);
            return Ok(());
        }

        let target_level = self.num_collapses.max(other.num_collapses);
        let self_shift = target_level - self.num_collapses;
        let other_shift = target_level - other.num_collapses;

        self.neg
            .merge_collapsed(self_shift, &other.neg, other_shift);
        self.pos
            .merge_collapsed(self_shift, &other.pos, other_shift);
        self.num_collapses = target_level;
        self.total_weight = next_total;
        self.zero_weight += other.zero_weight;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);

        self.enforce_budget();
        Ok(())
    }

    /// Returns the approximate lower discrete quantile for `rank` in `[0.0, 1.0]`.
    ///
    /// Ranks `0.0` and `1.0` return the exact minimum and maximum values of the stream, and ranks
    /// falling within the zero bucket return `0.0`. Returns `Ok(None)` if the sketch is empty.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if `rank` is NaN or
    /// outside `[0.0, 1.0]`, even if the sketch is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::uddsketch::UddSketch;
    ///
    /// let mut sketch = UddSketch::default();
    /// for v in [-10.0, 0.0, 20.0] {
    ///     sketch.update(v);
    /// }
    /// assert_eq!(sketch.quantile(0.0)?, Some(-10.0));
    /// assert_eq!(sketch.quantile(0.5)?, Some(0.0));
    /// assert_eq!(sketch.quantile(1.0)?, Some(20.0));
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn quantile(&self, rank: f64) -> Result<Option<f64>, Error> {
        check_rank(rank)?;
        if self.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.quantile_unchecked(rank)))
    }

    /// Returns approximate lower discrete quantiles for each normalized rank in `ranks`.
    ///
    /// Returns `Ok(None)` if the sketch is empty. Otherwise, an empty `ranks` slice returns
    /// `Ok(Some(vec![]))`.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if any rank is NaN or
    /// outside `[0.0, 1.0]`, even if the sketch is empty.
    pub fn quantiles(&self, ranks: &[f64]) -> Result<Option<Vec<f64>>, Error> {
        for &rank in ranks {
            check_rank(rank)?;
        }
        if self.is_empty() {
            return Ok(None);
        }
        let mut out = Vec::with_capacity(ranks.len());
        for &rank in ranks {
            out.push(self.quantile_unchecked(rank));
        }
        Ok(Some(out))
    }

    /// Serializes this sketch into the DataSketches UDDSketch binary format.
    ///
    /// See the [module documentation](crate::uddsketch#cross-language-compatibility) for format
    /// compatibility notes.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::uddsketch::UddSketch;
    ///
    /// let mut sketch = UddSketch::new(256, 0.01)?;
    /// sketch.update(3.5);
    /// let bytes = sketch.serialize();
    /// let decoded = UddSketch::deserialize(&bytes)?;
    /// assert_eq!(decoded.total_weight(), 1);
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn serialize(&self) -> Vec<u8> {
        serialize_sketch(self)
    }

    /// Deserializes a sketch from the DataSketches UDDSketch binary format.
    ///
    /// See the [module documentation](crate::uddsketch#cross-language-compatibility) for format
    /// compatibility notes.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidData`](crate::error::ErrorKind::InvalidData) if the image is truncated, has
    /// an unsupported header, or violates bucket ordering, count, budget, or extrema invariants.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        deserialize_sketch(bytes)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn from_parts(
        max_buckets: u32,
        mapping: Mapping,
        num_collapses: u8,
        total_weight: u64,
        zero_weight: u64,
        min: f64,
        max: f64,
        neg: BucketStore,
        pos: BucketStore,
    ) -> Self {
        Self {
            max_buckets,
            mapping,
            num_collapses,
            total_weight,
            zero_weight,
            min,
            max,
            neg,
            pos,
        }
    }

    pub(super) fn neg_store(&self) -> &BucketStore {
        &self.neg
    }

    pub(super) fn pos_store(&self) -> &BucketStore {
        &self.pos
    }

    fn enforce_budget(&mut self) {
        let budget = self.max_buckets as usize;
        while self.neg.len() + self.pos.len() > budget {
            self.neg.collapse(1);
            self.pos.collapse(1);
            self.num_collapses += 1;
        }
    }

    fn quantile_unchecked(&self, rank: f64) -> f64 {
        if rank == 0.0 || self.min == self.max {
            return self.min;
        }
        if rank == 1.0 {
            return self.max;
        }

        let target = lower_quantile_index(rank, self.total_weight);
        let neg_weight = self.neg.total_weight();
        if target < neg_weight {
            // Negative magnitudes are stored in ascending `abs(x)` order, which is descending
            // signed order; invert the zero-based rank within the negative partition.
            let mag_rank = neg_weight - 1 - target;
            let key = self.neg.key_at_rank(mag_rank);
            let estimate = -self.mapping.value_at_level(key, self.num_collapses);
            estimate.clamp(self.min, self.max)
        } else if target < neg_weight + self.zero_weight {
            0.0
        } else {
            let pos_rank = target - neg_weight - self.zero_weight;
            let key = self.pos.key_at_rank(pos_rank);
            let estimate = self.mapping.value_at_level(key, self.num_collapses);
            estimate.clamp(self.min, self.max)
        }
    }
}

#[inline]
fn check_rank(rank: f64) -> Result<(), Error> {
    if !(0.0..=1.0).contains(&rank) {
        return Err(Error::invalid_argument(format!(
            "rank must be in [0.0, 1.0], got {rank}"
        )));
    }
    Ok(())
}

/// Computes `floor(rank * (total_weight - 1))` exactly for `rank in (0.0, 1.0)` and
/// `total_weight >= 1` using exact IEEE-754 significand arithmetic so weights above `2^53` never
/// suffer `f64` rounding.
fn lower_quantile_index(rank: f64, total_weight: u64) -> u64 {
    debug_assert!(rank > 0.0 && rank < 1.0 && total_weight >= 1);
    let bits = rank.to_bits();
    let exp_bits = ((bits >> 52) & 0x7ff) as i32;
    let (mantissa, shift) = if exp_bits == 0 {
        (bits & ((1u64 << 52) - 1), 1074u32)
    } else {
        (
            (bits & ((1u64 << 52) - 1)) | (1u64 << 52),
            (1075 - exp_bits) as u32,
        )
    };
    if shift >= 128 {
        0
    } else {
        ((u128::from(mantissa) * u128::from(total_weight - 1)) >> shift) as u64
    }
}
