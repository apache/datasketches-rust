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
use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::error::Error;

/// A bounded, mergeable histogram with reproducible relative-value quantile estimates.
///
/// See the [module documentation](crate::uddsketch) for mapping and accuracy contracts.
#[derive(Clone, Debug)]
pub struct UddSketch {
    max_buckets: u32,
    initial_accuracy: f64,
    mapping: Mapping,
    level: u32,
    count: u64,
    buckets: Vec<Bucket>,
}

#[derive(Clone, Copy, Debug)]
struct Bucket {
    key: i64,
    count: u64,
    min: f64,
    max: f64,
}

impl Default for UddSketch {
    /// Creates a sketch with at most `512` buckets and initial accuracy `0.01`.
    fn default() -> Self {
        Self::new(512, 0.01).unwrap()
    }
}

impl UddSketch {
    /// Creates a sketch with a shared budget for negative, zero, and positive buckets.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if `max_buckets < 3` or `initial_accuracy` is outside
    /// `[1e-9, 1)`, including NaN. The lower accuracy limit bounds the resolution
    /// required of the fixed-point logarithmic mapping.
    pub fn new(max_buckets: u32, initial_accuracy: f64) -> Result<Self, Error> {
        if max_buckets < 3 || !(1e-9..1.0).contains(&initial_accuracy) {
            return Err(Error::invalid_argument(
                "max_buckets must be at least 3 and initial_accuracy must be in [1e-9, 1)",
            ));
        }
        Ok(Self {
            max_buckets,
            initial_accuracy,
            mapping: Mapping::new(initial_accuracy),
            level: 0,
            count: 0,
            buckets: Vec::new(),
        })
    }

    /// Inserts one finite value, treating both signed zeros as positive zero.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` for non-finite values or total weight overflow, leaving the
    /// sketch unchanged.
    pub fn update(&mut self, value: f64) -> Result<(), Error> {
        self.update_weighted(value, 1)
    }

    /// Inserts `weight` copies of a finite value.
    ///
    /// A zero weight has no effect, but the value is still validated.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` for non-finite values or total weight overflow, leaving the
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
        // Monotonicity lets observed extrema locate the only possible bucket. A value inside
        // an existing interval needs no mapping; a gap can match only its adjacent buckets.
        // Ascending input stays at the tail, avoiding a binary search for each observation.
        let position = match self.buckets.last() {
            Some(last) if value >= last.min => self.buckets.len() - usize::from(value <= last.max),
            _ => self.buckets.partition_point(|b| b.max < value),
        };
        if let Some(bucket) = self.buckets.get_mut(position).filter(|b| b.min <= value) {
            bucket.count += weight;
        } else {
            let key = self.key(value);
            let bucket = Bucket {
                key,
                count: weight,
                min: value,
                max: value,
            };
            if position < self.buckets.len() && self.buckets[position].key == key {
                self.buckets[position].merge(bucket);
            } else if position > 0 && self.buckets[position - 1].key == key {
                self.buckets[position - 1].merge(bucket);
            } else {
                self.buckets.insert(position, bucket);
            }
        }
        self.count = count;
        self.enforce_budget();
        Ok(())
    }

    /// Merges a compatible sketch without modifying the input.
    ///
    /// Both sketches must have identical bucket limits and initial accuracy, even when empty.
    /// Different compaction levels are aligned automatically.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` for incompatible parameters or total weight overflow, leaving
    /// the receiver unchanged.
    pub fn merge(&mut self, other: &Self) -> Result<(), Error> {
        if self.max_buckets != other.max_buckets
            || self.initial_accuracy.to_bits() != other.initial_accuracy.to_bits()
        {
            return Err(Error::invalid_argument("incompatible UDDSketch parameters"));
        }
        let count = self
            .count
            .checked_add(other.count)
            .ok_or_else(|| Error::invalid_argument("total weight overflow"))?;
        if other.is_empty() {
            return Ok(());
        }
        let level = self.level.max(other.level);
        self.collapse_to(level);
        let mut merged: Vec<Bucket> = Vec::with_capacity(self.buckets.len() + other.buckets.len());
        let mut left = self.buckets.iter().copied().peekable();
        let mut right = other
            .buckets
            .iter()
            .copied()
            .map(|mut b| {
                b.key = parent(b.key, level - other.level);
                b
            })
            .peekable();
        while left.peek().is_some() || right.peek().is_some() {
            let bucket = match (left.peek(), right.peek()) {
                (Some(a), Some(b)) if a.key <= b.key => left.next().unwrap(),
                (Some(_), None) => left.next().unwrap(),
                _ => right.next().unwrap(),
            };
            if let Some(last) = merged.last_mut().filter(|last| last.key == bucket.key) {
                last.merge(bucket);
            } else {
                merged.push(bucket);
            }
        }
        self.buckets = merged;
        self.count = count;
        self.enforce_budget();
        Ok(())
    }

    /// Returns the number of inserted values, including weights.
    pub fn total_weight(&self) -> u64 {
        self.count
    }

    /// Returns whether the sketch contains no values.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns the configured bucket limit.
    pub fn max_buckets(&self) -> u32 {
        self.max_buckets
    }

    /// Returns the initial relative accuracy target.
    pub fn initial_relative_accuracy(&self) -> f64 {
        self.initial_accuracy
    }

    /// Returns the number of retained buckets, including the zero bucket if present.
    pub fn num_buckets(&self) -> usize {
        self.buckets.len()
    }

    /// Returns the number of uniform collapses in the current mapping hierarchy.
    pub fn compaction_level(&self) -> u32 {
        self.level
    }

    /// Returns the exact minimum, or `None` when empty.
    pub fn min_value(&self) -> Option<f64> {
        self.buckets.first().map(|b| b.min)
    }

    /// Returns the exact maximum, or `None` when empty.
    pub fn max_value(&self) -> Option<f64> {
        self.buckets.last().map(|b| b.max)
    }

    /// Returns a conservative bound on relative value error for every quantile of this state.
    ///
    /// For a nonzero lower discrete quantile `x`, the estimate `y` satisfies
    /// `abs(y - x) / abs(x) <= relative_error()`. Zero is returned exactly. Empty sketches and
    /// sketches whose buckets each contain a single distinct value return `0.0`.
    /// The bound may exceed the initial target after uniform collapse. It uses observed extrema
    /// and outward rounding, rather than assuming exact logarithms or representable gamma powers.
    /// This method scans the retained buckets; it does not bound rank error.
    pub fn relative_error(&self) -> f64 {
        self.buckets
            .iter()
            .map(|b| {
                let estimate = b.estimate().abs();
                if b.min == b.max {
                    return 0.0;
                }
                let lo = b.min.abs().min(b.max.abs());
                let hi = b.min.abs().max(b.max.abs());
                // Round each nonnegative operation upward; this also covers subnormal arithmetic.
                let low_error = relative_difference(estimate, lo, lo);
                // The upper-end error cannot exceed one; clamping also handles upward
                // rounding of a difference near f64::MAX to infinity.
                let high_error = relative_difference(hi, estimate, hi).min(1.0);
                low_error.max(high_error)
            })
            .fold(0.0, f64::max)
    }

    /// Returns an estimate of the lower discrete quantile at `rank`, or `None` when empty.
    ///
    /// The target is sorted item `floor(rank * (n - 1))` (zero-based), with the multiplication
    /// evaluated exactly for the supplied binary floating-point rank. Endpoints are exact.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if `rank` is outside `[0, 1]`, including NaN, even when empty.
    pub fn quantile(&self, rank: f64) -> Result<Option<f64>, Error> {
        validate_rank(rank)?;
        if self.is_empty() {
            return Ok(None);
        }
        if rank == 0.0 {
            return Ok(self.min_value());
        }
        if rank == 1.0 {
            return Ok(self.max_value());
        }
        let target = quantile_index(rank, self.count);
        let mut cumulative = 0;
        for bucket in &self.buckets {
            cumulative += bucket.count;
            if cumulative > target {
                return Ok(Some(bucket.estimate()));
            }
        }
        unreachable!("validated counts cover the target rank")
    }

    /// Returns estimates for all ranks in input order, or `None` when empty.
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
            .map(|&q| self.quantile(q).map(Option::unwrap))
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }

    /// Returns an implementation-dependent memory estimate including reserved bucket capacity.
    pub fn estimated_size(&self) -> usize {
        size_of::<Self>() + self.buckets.capacity() * size_of::<Bucket>()
    }

    /// Serializes the sketch in canonical bucket order using the Rust-specific version 1 format.
    ///
    /// The little-endian format starts with `UDD1`, followed by `max_buckets: u32`, initial
    /// accuracy as `f64`, level as `u32`, total weight as `u64`, and bucket count as `u32`.
    /// Each 32-byte bucket contains `key: i64`, `count: u64`, `min: f64`, and `max: f64`.
    /// It has no cross-language compatibility contract.
    pub fn serialize(&self) -> Vec<u8> {
        let mut out = SketchBytes::with_capacity(32 + 32 * self.buckets.len());
        out.write(b"UDD1");
        out.write_u32_le(self.max_buckets);
        out.write_f64_le(self.initial_accuracy);
        out.write_u32_le(self.level);
        out.write_u64_le(self.count);
        out.write_u32_le(self.buckets.len() as u32);
        for b in &self.buckets {
            out.write_i64_le(b.key);
            out.write_u64_le(b.count);
            out.write_f64_le(b.min);
            out.write_f64_le(b.max);
        }
        out.into_bytes()
    }

    /// Deserializes a Rust-specific version 1 image.
    ///
    /// Unmodified images produced by this mapping version preserve estimates, error bounds,
    /// and subsequent update/merge behavior. Validation checks the represented histogram,
    /// including extrema membership, ordering, and counts; it does not authenticate provenance
    /// or prove that the level is the smallest one reachable from an original stream.
    /// An externally constructed, over-compacted image may therefore be accepted and retain
    /// its coarser resolution when merged. Equivalence to replaying original inputs requires
    /// states produced by `new`, updates, compatible merges, and unmodified roundtrips.
    ///
    /// # Errors
    ///
    /// Returns `InvalidData` for unsupported versions, truncation, trailing bytes, invalid
    /// parameters, invalid bucket ordering or extrema, and inconsistent or overflowing counts.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        let bad = || Error::deserial("invalid UDDSketch image");
        if bytes.len() < 32 || &bytes[..4] != b"UDD1" {
            return Err(bad());
        }
        let mut input = SketchSlice::new(&bytes[4..]);
        let limit = input.read_u32_le().map_err(|_| bad())?;
        let accuracy = input.read_f64_le().map_err(|_| bad())?;
        let mut sketch = Self::new(limit, accuracy).map_err(|_| bad())?;
        sketch.level = input.read_u32_le().map_err(|_| bad())?;
        sketch.count = input.read_u64_le().map_err(|_| bad())?;
        let len = input.read_u32_le().map_err(|_| bad())? as usize;
        let max_level = sketch.mapping.max_level();
        if sketch.level > max_level
            || len > limit as usize
            || input.remaining().len() / 32 != len
            || input.remaining().len() % 32 != 0
            || (len == 0 && (sketch.count != 0 || sketch.level != 0))
        {
            return Err(bad());
        }
        // Validate the complete payload length before allocating from an untrusted count.
        sketch.buckets = Vec::with_capacity(len);
        let mut count = 0u64;
        for chunk in input.remaining().chunks_exact(32) {
            let b = Bucket {
                key: i64::from_le_bytes(chunk[..8].try_into().unwrap()),
                count: u64::from_le_bytes(chunk[8..16].try_into().unwrap()),
                min: f64::from_le_bytes(chunk[16..24].try_into().unwrap()),
                max: f64::from_le_bytes(chunk[24..].try_into().unwrap()),
            };
            if b.count == 0
                || !b.min.is_finite()
                || !b.max.is_finite()
                || b.min > b.max
                || (b.count == 1 && b.min != b.max)
                || b.min.to_bits() == (-0.0f64).to_bits()
                || b.max.to_bits() == (-0.0f64).to_bits()
                || sketch.key(b.min) != b.key
                || (b.min != b.max && sketch.key(b.max) != b.key)
                || sketch
                    .buckets
                    .last()
                    .is_some_and(|last| last.key >= b.key || last.max >= b.min)
            {
                return Err(bad());
            }
            count = count.checked_add(b.count).ok_or_else(bad)?;
            sketch.buckets.push(b);
        }
        if count != sketch.count {
            return Err(bad());
        }
        Ok(sketch)
    }

    fn key(&self, value: f64) -> i64 {
        if value == 0.0 {
            return 0;
        }
        let index = (self.mapping.index(value.abs()) >> self.level) as i64 + 1;
        if value < 0.0 { -index } else { index }
    }

    fn enforce_budget(&mut self) {
        while self.buckets.len() > self.max_buckets as usize {
            self.collapse_to(self.level + 1);
        }
    }

    fn collapse_to(&mut self, level: u32) {
        if self.level == level {
            return;
        }
        let mut len = 0;
        for i in 0..self.buckets.len() {
            let mut bucket = self.buckets[i];
            bucket.key = parent(bucket.key, level - self.level);
            if len > 0 && self.buckets[len - 1].key == bucket.key {
                self.buckets[len - 1].merge(bucket);
            } else {
                self.buckets[len] = bucket;
                len += 1;
            }
        }
        self.buckets.truncate(len);
        self.level = level;
    }
}

impl Bucket {
    fn merge(&mut self, other: Self) {
        // The public operations check total weight before mutating any bucket.
        self.count += other.count;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }

    fn estimate(&self) -> f64 {
        if self.min == self.max {
            return self.min;
        }
        let lo = self.min.abs().min(self.max.abs());
        let hi = self.min.abs().max(self.max.abs());
        let magnitude = (lo / (0.5 + 0.5 * (lo / hi))).clamp(lo, hi);
        if self.key < 0 { -magnitude } else { magnitude }
    }
}

fn parent(key: i64, levels: u32) -> i64 {
    if key == 0 {
        return 0;
    }
    let magnitude = (((key.unsigned_abs() - 1) >> levels) + 1) as i64;
    if key < 0 { -magnitude } else { magnitude }
}

fn validate_rank(rank: f64) -> Result<(), Error> {
    if !(0.0..=1.0).contains(&rank) {
        return Err(Error::invalid_argument("rank must be in [0, 1]"));
    }
    Ok(())
}

fn quantile_index(rank: f64, count: u64) -> u64 {
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

fn next_up(value: f64) -> f64 {
    if value == f64::INFINITY {
        value
    } else {
        f64::from_bits(value.to_bits() + 1)
    }
}

fn relative_difference(larger: f64, smaller: f64, denominator: f64) -> f64 {
    if larger == smaller {
        return 0.0;
    }
    // Sterbenz's lemma makes subtraction exact for nearby operands, including subnormals.
    let difference = if larger <= 2.0 * smaller {
        larger - smaller
    } else {
        next_up(larger - smaller)
    };
    next_up(difference / denominator)
}
