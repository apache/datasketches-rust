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

use super::mapping::Mapping;
use super::store::Store;
use crate::error::Error;

#[path = "serialization.rs"]
mod serialization;

/// The numerical end of each signed store to collapse when its bucket limit is exceeded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CollapseDirection {
    /// Collapses the smallest values, prioritizing high quantiles.
    Lowest,
    /// Collapses the largest values, prioritizing low quantiles.
    Highest,
}

/// A bounded, mergeable logarithmic histogram for relative-value quantile estimates.
///
/// See the [module documentation](crate::ddsketch) for accuracy and memory tradeoffs.
#[derive(Clone, Debug)]
pub struct DdSketch {
    max_buckets: u32,
    accuracy: f64,
    direction: CollapseDirection,
    mapping: Mapping,
    negative: Store,
    positive: Store,
    zero_count: u64,
    count: u64,
    min: f64,
    max: f64,
}

impl Default for DdSketch {
    /// Creates a sketch with `2048` buckets per sign, `0.01` accuracy, and lowest-value collapse.
    fn default() -> Self {
        Self::new(2048, 0.01).unwrap()
    }
}

impl DdSketch {
    /// Creates a sketch that collapses the smallest values when either store exceeds its limit.
    ///
    /// `max_buckets` limits each sign separately and includes empty bins between retained values.
    /// The zero counter is additional. With `2048` buckets and `0.01` relative accuracy, a positive
    /// range from approximately `80` microseconds to one year fits without collapse.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if `max_buckets` is zero or `relative_accuracy` is outside
    /// `[1e-9, 1)`, including NaN. The lower accuracy limit keeps the full normal `f64` domain
    /// indexable and limits the effect of floating-point rounding on the requested precision.
    pub fn new(max_buckets: u32, relative_accuracy: f64) -> Result<Self, Error> {
        Self::with_collapse_direction(max_buckets, relative_accuracy, CollapseDirection::Lowest)
    }

    /// Creates a sketch with the specified collapse direction.
    ///
    /// Both the budget and direction apply to each sign separately. Directions are in numerical
    /// order, so `Lowest` collapses large negative magnitudes and small positive magnitudes.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` for the same invalid parameters as [`Self::new`].
    pub fn with_collapse_direction(
        max_buckets: u32,
        relative_accuracy: f64,
        direction: CollapseDirection,
    ) -> Result<Self, Error> {
        if max_buckets == 0 || !(1e-9..1.0).contains(&relative_accuracy) {
            return Err(Error::invalid_argument(
                "max_buckets must be positive and relative_accuracy must be in [1e-9, 1)",
            ));
        }
        Ok(Self {
            max_buckets,
            accuracy: relative_accuracy,
            direction,
            mapping: Mapping::new(relative_accuracy),
            negative: Store::default(),
            positive: Store::default(),
            zero_count: 0,
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        })
    }

    /// Removes all values and collapse state, retaining configuration and allocated capacity.
    pub fn reset(&mut self) {
        self.negative.reset();
        self.positive.reset();
        self.zero_count = 0;
        self.count = 0;
        self.min = f64::INFINITY;
        self.max = f64::NEG_INFINITY;
    }

    /// Inserts one finite value, normalizing signed zeros to positive zero.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` for a non-finite value or total weight overflow, leaving the
    /// sketch unchanged.
    pub fn update(&mut self, value: f64) -> Result<(), Error> {
        self.update_weighted(value, 1)
    }

    /// Inserts `weight` copies of a finite value.
    ///
    /// A zero weight has no effect, but the value is still validated. Subnormal values share
    /// the zero counter; exact extrema still include them.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` for a non-finite value or total weight overflow, leaving the
    /// sketch unchanged.
    pub fn update_weighted(&mut self, value: f64, weight: u64) -> Result<(), Error> {
        if !value.is_finite() {
            return Err(Error::invalid_argument("value must be finite"));
        }
        let count = self
            .count
            .checked_add(weight)
            .ok_or_else(|| Error::invalid_argument("total weight overflow"))?;
        if weight == 0 {
            return Ok(());
        }
        let value = if value == 0.0 { 0.0 } else { value };
        if value.abs() < self.zero_threshold() {
            self.zero_count += weight;
        } else {
            let index = self.mapping.index(value.abs());
            if value < 0.0 {
                self.negative
                    .add(-index, weight, self.max_buckets, self.direction);
            } else {
                self.positive
                    .add(index, weight, self.max_buckets, self.direction);
            }
        }
        self.count = count;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        Ok(())
    }

    /// Merges a compatible sketch without modifying the input.
    ///
    /// Accuracy, bucket limit, and collapse direction must match exactly, even for empty inputs.
    /// Previously collapsed buckets retain their loss of accuracy after merging.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` for incompatible parameters or total weight overflow, leaving
    /// the receiver unchanged.
    pub fn merge(&mut self, other: &Self) -> Result<(), Error> {
        if self.max_buckets != other.max_buckets
            || self.accuracy.to_bits() != other.accuracy.to_bits()
            || self.direction != other.direction
        {
            return Err(Error::invalid_argument("incompatible DDSketch parameters"));
        }
        let count = self
            .count
            .checked_add(other.count)
            .ok_or_else(|| Error::invalid_argument("total weight overflow"))?;
        self.negative
            .merge(&other.negative, self.max_buckets, self.direction);
        self.positive
            .merge(&other.positive, self.max_buckets, self.direction);
        self.zero_count += other.zero_count;
        self.count = count;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        Ok(())
    }

    /// Returns the total number of inserted values, including weights.
    pub fn total_weight(&self) -> u64 {
        self.count
    }

    /// Returns whether the sketch contains no values.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns the configured dense bucket limit per sign, excluding the zero counter.
    pub fn max_buckets(&self) -> u32 {
        self.max_buckets
    }

    /// Returns the configured relative accuracy for uncollapsed, nonzero buckets.
    ///
    /// Floating-point rounding and the zero threshold qualify this bound; see the
    /// [module documentation](crate::ddsketch).
    pub fn relative_accuracy(&self) -> f64 {
        self.accuracy
    }

    /// Returns the configured numerical collapse direction.
    pub fn collapse_direction(&self) -> CollapseDirection {
        self.direction
    }

    /// Returns the magnitude below which inputs share the zero counter (`f64::MIN_POSITIVE`).
    ///
    /// Values at the threshold are indexed normally. Exact extrema include smaller inputs.
    pub fn zero_threshold(&self) -> f64 {
        f64::MIN_POSITIVE
    }

    /// Returns the weight of zeros and subnormal values.
    pub fn zero_count(&self) -> u64 {
        self.zero_count
    }

    /// Returns the retained dense bucket count, including empty gaps and a nonempty zero bucket.
    pub fn num_buckets(&self) -> usize {
        self.negative.counts.len() + self.positive.counts.len() + usize::from(self.zero_count != 0)
    }

    /// Returns whether either signed store has collapsed any buckets since the last reset.
    pub fn is_collapsed(&self) -> bool {
        self.negative.collapsed || self.positive.collapsed
    }

    /// Returns the exact minimum, or `None` when empty.
    pub fn min_value(&self) -> Option<f64> {
        (!self.is_empty()).then_some(self.min)
    }

    /// Returns the exact maximum, or `None` when empty.
    pub fn max_value(&self) -> Option<f64> {
        (!self.is_empty()).then_some(self.max)
    }

    /// Returns an estimate of the lower discrete quantile at `rank`, or `None` when empty.
    ///
    /// The target is sorted item `floor(rank * (n - 1))` (zero-based). The multiplication is
    /// evaluated exactly for the supplied binary floating-point rank, even above `2^53` weight.
    /// Ranks `0` and `1` return the exact extrema. Other estimates are clamped to those extrema.
    /// Use [`Self::quantile_is_collapsed`] to detect queries affected by bucket collapse.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if `rank` is outside `[0, 1]`, including NaN, even when empty.
    pub fn quantile(&self, rank: f64) -> Result<Option<f64>, Error> {
        Ok(self.query(rank)?.map(|(value, _)| value))
    }

    /// Returns quantile estimates in input order, or `None` when empty.
    ///
    /// Uses the same semantics as [`Self::quantile`]. An empty rank slice returns an empty vector
    /// for a nonempty sketch.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if any rank is outside `[0, 1]`, including NaN, even when empty.
    pub fn quantiles(&self, ranks: &[f64]) -> Result<Option<Vec<f64>>, Error> {
        for &rank in ranks {
            validate_rank(rank)?;
        }
        if self.is_empty() {
            return Ok(None);
        }
        ranks
            .iter()
            .map(|&rank| self.quantile(rank).map(Option::unwrap))
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }

    /// Returns whether the quantile estimate uses a collapsed bucket, or `None` when empty.
    ///
    /// `true` means that the configured relative accuracy no longer applies to this query.
    /// Exact extrema and constant streams return `false`. A zero-bucket query also returns
    /// `false`, but is subject to the absolute error described by [`Self::zero_threshold`].
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if `rank` is outside `[0, 1]`, including NaN, even when empty.
    pub fn quantile_is_collapsed(&self, rank: f64) -> Result<Option<bool>, Error> {
        Ok(self.query(rank)?.map(|(_, collapsed)| collapsed))
    }

    /// Returns an implementation-dependent heap memory estimate including reserved capacity.
    pub fn estimated_size(&self) -> usize {
        (self.negative.counts.capacity() + self.positive.counts.capacity()) * size_of::<u64>()
    }

    fn query(&self, rank: f64) -> Result<Option<(f64, bool)>, Error> {
        validate_rank(rank)?;
        if self.is_empty() {
            return Ok(None);
        }
        if rank == 0.0 || self.min == self.max {
            return Ok(Some((self.min, false)));
        }
        if rank == 1.0 {
            return Ok(Some((self.max, false)));
        }
        let mut target = quantile_index(rank, self.count);
        for (store, negative) in [(&self.negative, true), (&self.positive, false)] {
            if !negative {
                if target < self.zero_count {
                    return Ok(Some((0.0f64.clamp(self.min, self.max), false)));
                }
                target -= self.zero_count;
            }
            for (position, &weight) in store.counts.iter().enumerate() {
                if target < weight {
                    let index = store.offset + position as i64;
                    let value = if negative {
                        -self.mapping.value(-index)
                    } else {
                        self.mapping.value(index)
                    };
                    return Ok(Some((
                        value.clamp(self.min, self.max),
                        store.is_collapsed_index(index, self.direction),
                    )));
                }
                target -= weight;
            }
        }
        unreachable!("validated counts cover the target rank")
    }
}

fn validate_rank(rank: f64) -> Result<(), Error> {
    if !(0.0..=1.0).contains(&rank) {
        return Err(Error::invalid_argument("rank must be in [0, 1]"));
    }
    Ok(())
}

fn quantile_index(rank: f64, count: u64) -> u64 {
    // Multiply the exact binary significand by the integer weight before shifting.
    // Converting count to f64 first can select a different bucket above 2^53.
    let bits = rank.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as u32;
    if exponent == 0 {
        return 0;
    }
    let significand = (bits & ((1u64 << 52) - 1)) | (1u64 << 52);
    let product = u128::from(significand) * u128::from(count - 1);
    let shift = 1075 - exponent;
    if shift >= 128 {
        0
    } else {
        (product >> shift) as u64
    }
}
