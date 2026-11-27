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

use super::DEFAULT_RELATIVE_ACCURACY;
use super::iter::DdSketchIter;
use super::mapping::IndexMapping;
use super::mapping::MappingKind;
use super::serialization::deserialize_compat_sketch;
use super::serialization::deserialize_sketch;
use super::serialization::serialize_compat_sketch;
use super::serialization::serialize_sketch;
use super::store::Store;
use super::store::StoreKind;
use super::store::validate_store_kind;
use super::weight::DdSketchWeight;
use crate::error::Error;

/// Builder for configuring and constructing a [`DdSketch`].
///
/// Defaults to `relative_accuracy = 0.01`, [`MappingKind::Cubic`], and
/// [`StoreKind::CollapsingLowestDense`] with `2048` bins per sign.
///
/// # Examples
///
/// ```
/// use datasketches::ddsketch::DdSketchBuilder;
/// use datasketches::ddsketch::MappingKind;
/// use datasketches::ddsketch::StoreKind;
///
/// let mut sketch = DdSketchBuilder::new(0.005)
///     .mapping(MappingKind::Logarithmic)
///     .store(StoreKind::Sparse)
///     .build::<u64>()?;
/// sketch.update(42.0);
/// assert_eq!(sketch.total_weight(), 1);
/// # Ok::<(), datasketches::error::Error>(())
/// ```
#[derive(Debug, Clone)]
pub struct DdSketchBuilder {
    relative_accuracy: f64,
    mapping_kind: MappingKind,
    store_kind: StoreKind,
}

impl Default for DdSketchBuilder {
    fn default() -> Self {
        Self::new(DEFAULT_RELATIVE_ACCURACY)
    }
}

impl DdSketchBuilder {
    /// Creates a builder with the given `relative_accuracy` and default mapping and store settings.
    pub fn new(relative_accuracy: f64) -> Self {
        Self {
            relative_accuracy,
            mapping_kind: MappingKind::default(),
            store_kind: StoreKind::default(),
        }
    }

    /// Sets the index mapping curve used to assign positive magnitudes to bucket indices.
    #[inline]
    pub fn mapping(mut self, mapping_kind: MappingKind) -> Self {
        self.mapping_kind = mapping_kind;
        self
    }

    /// Sets the bin storage strategy used for both positive and negative stores.
    #[inline]
    pub fn store(mut self, store_kind: StoreKind) -> Self {
        self.store_kind = store_kind;
        self
    }

    /// Validates configuration parameters and creates an empty [`DdSketch`] with weight type `W`.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if
    /// `relative_accuracy` is NaN or outside `[1e-6, 1.0)`, or if `store` specifies a collapsing
    /// dense store with `max_num_bins` outside `1..=67_108_864`.
    pub fn build<W: DdSketchWeight>(self) -> Result<DdSketch<W>, Error> {
        validate_store_kind(self.store_kind)?;
        let mapping = IndexMapping::from_accuracy(self.mapping_kind, self.relative_accuracy)?;
        let negative = Store::new(self.store_kind)?;
        let positive = Store::new(self.store_kind)?;
        Ok(DdSketch::from_parts(
            mapping,
            negative,
            positive,
            W::ZERO,
            SummaryState::Empty,
        ))
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum SummaryState {
    Empty,
    Active { min: f64, max: f64, sum: f64 },
    Missing,
}

impl SummaryState {
    #[inline]
    fn canonical_f64(value: f64) -> f64 {
        if value == 0.0 { 0.0 } else { value }
    }

    #[inline]
    fn record(&mut self, value: f64, weight_f64: f64) {
        let v = Self::canonical_f64(value);
        let weighted = Self::canonical_f64(v * weight_f64);
        match self {
            Self::Empty => {
                *self = Self::Active {
                    min: v,
                    max: v,
                    sum: weighted,
                };
            }
            Self::Active { min, max, sum } => {
                *min = (*min).min(v);
                *max = (*max).max(v);
                *sum = Self::canonical_f64(*sum + weighted);
            }
            Self::Missing => {}
        }
    }

    #[inline]
    fn merge_non_empty(&mut self, other: Self) {
        *self = match (*self, other) {
            (Self::Empty, rhs) => rhs,
            (
                Self::Active { min, max, sum },
                Self::Active {
                    min: omin,
                    max: omax,
                    sum: osum,
                },
            ) => Self::Active {
                min: min.min(omin),
                max: max.max(omax),
                sum: Self::canonical_f64(sum + osum),
            },
            (Self::Missing, _) | (_, Self::Missing) => Self::Missing,
            (lhs, Self::Empty) => lhs,
        };
    }
}

/// Mergeable quantile sketch over finite `f64` values with relative-value error guarantees.
///
/// The weight type `W` defaults to `u64` for exact associative integer weights, and can be set to
/// `f64` for fractional weights and full compatibility with the Java and Go reference
/// implementations. See the [module documentation](super) for accuracy, memory, and compatibility
/// details.
#[derive(Clone)]
pub struct DdSketch<W: DdSketchWeight = u64> {
    mapping: IndexMapping,
    negative: Store<W>,
    positive: Store<W>,
    zero_weight: W,
    summary: SummaryState,
}

impl<W: DdSketchWeight> fmt::Debug for DdSketch<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DdSketch")
            .field("relative_accuracy", &self.mapping.relative_accuracy())
            .field("mapping_kind", &self.mapping.kind())
            .field("store_kind", &self.positive.kind())
            .field("is_empty", &self.is_empty())
            .field("total_weight", &self.total_weight())
            .field("zero_weight", &self.zero_weight)
            .field("num_retained", &self.num_retained())
            .field("min_value", &self.min_value())
            .field("max_value", &self.max_value())
            .field("sum", &self.sum())
            .finish()
    }
}

impl<W: DdSketchWeight> Default for DdSketch<W> {
    fn default() -> Self {
        DdSketchBuilder::default()
            .build()
            .expect("default DDSketch configuration is valid")
    }
}

impl<W: DdSketchWeight> DdSketch<W> {
    /// Creates an empty sketch with the given `relative_accuracy`, [`MappingKind::Cubic`], and
    /// [`StoreKind::default()`].
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if
    /// `relative_accuracy` is NaN or outside `[1e-6, 1.0)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::ddsketch::DdSketch;
    ///
    /// let sketch = DdSketch::<u64>::new(0.01)?;
    /// assert!(sketch.is_empty());
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn new(relative_accuracy: f64) -> Result<Self, Error> {
        DdSketchBuilder::new(relative_accuracy).build()
    }

    pub(super) fn from_parts(
        mapping: IndexMapping,
        negative: Store<W>,
        positive: Store<W>,
        zero_weight: W,
        summary: SummaryState,
    ) -> Self {
        let is_empty = negative.is_empty() && zero_weight.is_zero() && positive.is_empty();
        debug_assert_eq!(is_empty, matches!(summary, SummaryState::Empty));
        debug_assert!(
            checked_combine_weights(
                negative.total_weight(),
                zero_weight,
                positive.total_weight()
            )
            .is_some()
        );
        Self {
            mapping,
            negative,
            positive,
            zero_weight,
            summary,
        }
    }

    #[inline]
    pub(super) const fn summary_state(&self) -> SummaryState {
        self.summary
    }

    #[inline]
    pub(super) const fn mapping(&self) -> IndexMapping {
        self.mapping
    }

    #[inline]
    pub(super) const fn positive_store(&self) -> &Store<W> {
        &self.positive
    }

    #[inline]
    pub(super) const fn negative_store(&self) -> &Store<W> {
        &self.negative
    }

    /// Returns the relative value accuracy guaranteed for non-collapsed buckets.
    #[inline]
    pub fn relative_accuracy(&self) -> f64 {
        self.mapping.relative_accuracy()
    }

    /// Returns the index mapping curve used by this sketch.
    #[inline]
    pub fn mapping_kind(&self) -> MappingKind {
        self.mapping.kind()
    }

    /// Returns the bin storage strategy used by this sketch.
    #[inline]
    pub fn store_kind(&self) -> StoreKind {
        self.positive.kind()
    }

    /// Returns the magnitude threshold below or at which inputs map to the zero bucket.
    ///
    /// Any finite input `x` with `|x| <= self.zero_threshold()` increments
    /// [`zero_weight`](Self::zero_weight) and has a bucket representative value of `0.0` (subject
    /// to `[min_value, max_value]` clamping on quantile queries).
    #[inline]
    pub fn zero_threshold(&self) -> f64 {
        self.mapping.min_indexable_value()
    }

    /// Returns `true` if this sketch has not recorded any finite observations.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.negative.is_empty() && self.zero_weight.is_zero() && self.positive.is_empty()
    }

    /// Returns the accumulated stream weight across all buckets.
    #[inline]
    pub fn total_weight(&self) -> W {
        self.negative
            .total_weight()
            .add_unchecked(self.zero_weight)
            .add_unchecked(self.positive.total_weight())
    }

    /// Returns the accumulated stream weight in the zero bucket.
    #[inline]
    pub fn zero_weight(&self) -> W {
        self.zero_weight
    }

    /// Returns the number of non-empty buckets currently retained by this sketch, including the
    /// zero bucket when [`zero_weight`](Self::zero_weight) is non-zero.
    #[inline]
    pub fn num_retained(&self) -> usize {
        self.negative.num_occupied()
            + usize::from(!self.zero_weight.is_zero())
            + self.positive.num_occupied()
    }

    /// Returns an implementation-dependent estimate of heap memory usage in bytes.
    pub fn estimated_size(&self) -> usize {
        self.negative
            .estimated_size()
            .saturating_add(self.positive.estimated_size())
    }

    /// Updates this sketch with a single observation of unit weight.
    ///
    /// [`f64::NAN`], [`f64::INFINITY`], and [`f64::NEG_INFINITY`] values are ignored.
    ///
    /// # Panics
    ///
    /// Panics without modifying the sketch if [`total_weight`](Self::total_weight) would overflow
    /// `W` ([`u64::MAX`] for `u64`, or non-finite for `f64`), or if [`StoreKind::UnboundedDense`]
    /// would exceed addressable slice capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::ddsketch::DdSketch;
    ///
    /// let mut sketch = DdSketch::<u64>::new(0.01)?;
    /// sketch.update(10.0);
    /// assert_eq!(sketch.total_weight(), 1);
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    #[inline]
    pub fn update(&mut self, value: f64) {
        self.update_with_weight(value, W::ONE);
    }

    /// Updates this sketch with `value` weighted by `weight`.
    ///
    /// [`f64::NAN`], [`f64::INFINITY`], [`f64::NEG_INFINITY`], and a zero `weight` are no-ops.
    ///
    /// # Panics
    ///
    /// Panics without modifying the sketch if `weight` is negative, NaN, or infinite (for `f64`
    /// weights), if [`total_weight`](Self::total_weight) would overflow `W` ([`u64::MAX`] for
    /// `u64`, or non-finite for `f64`), or if [`StoreKind::UnboundedDense`] would exceed
    /// addressable slice capacity.
    ///
    /// # Compatibility with reference implementations
    ///
    /// In the reference DDSketch implementations, Go rejects `NaN`, infinite, and negative-count
    /// inputs with an error, whereas Java rejects out-of-range finite/infinite and negative-count
    /// inputs while counting `NaN` into the zero bucket; both accept fractional `f64` counts. This
    /// method ignores non-finite values and supports both `u64` integer weights (default) and
    /// fractional `f64` weights. For `DdSketch<u64>`, keep per-bucket weights (and
    /// [`total_weight`](Self::total_weight) for Java quantile readers) at or below `2^53 - 1` when
    /// serializing with [`serialize_compat`](Self::serialize_compat).
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::ddsketch::DdSketch;
    ///
    /// let mut sketch = DdSketch::<f64>::new(0.01)?;
    /// sketch.update_with_weight(25.0, 2.5);
    /// assert_eq!(sketch.total_weight(), 2.5);
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    #[inline]
    pub fn update_with_weight(&mut self, value: f64, weight: W) {
        let weight = weight.validate_update_weight();
        if !value.is_finite() || weight.is_zero() {
            return;
        }

        let min_indexable = self.mapping.min_indexable_value();
        if value > min_indexable {
            let next_pos = self
                .positive
                .total_weight()
                .checked_add(weight)
                .expect("total stream weight overflow");
            checked_combine_weights(self.negative.total_weight(), self.zero_weight, next_pos)
                .expect("total stream weight overflow");
            self.positive.add(self.mapping.index(value), weight);
        } else if value < -min_indexable {
            let next_neg = self
                .negative
                .total_weight()
                .checked_add(weight)
                .expect("total stream weight overflow");
            checked_combine_weights(next_neg, self.zero_weight, self.positive.total_weight())
                .expect("total stream weight overflow");
            self.negative.add(self.mapping.index(-value), weight);
        } else {
            let next_zero = self
                .zero_weight
                .checked_add(weight)
                .expect("total stream weight overflow");
            checked_combine_weights(
                self.negative.total_weight(),
                next_zero,
                self.positive.total_weight(),
            )
            .expect("total stream weight overflow");
            self.zero_weight = next_zero;
        }
        self.summary.record(value, weight.to_f64());
    }

    /// Merges `other` into `self`, retaining `self`'s [`StoreKind`].
    ///
    /// Sketches that share an equivalent index mapping can be merged even when they use different
    /// [`StoreKind`] strategies. Merging a non-empty sketch whose exact summary statistics are
    /// unknown (decoded from a bare compatibility image without summary blocks) makes `self`'s
    /// [`min_value`](Self::min_value), [`max_value`](Self::max_value), and [`sum`](Self::sum)
    /// `None` and disables quantile clamping until [`reset`](Self::reset) is called.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) without modifying
    /// `self` if `other` uses an incompatible index mapping (even when either sketch is empty), if
    /// `other` contains a bucket index outside `self`'s valid range, if the combined
    /// [`total_weight`](Self::total_weight) would overflow `W`, or if merging into
    /// [`StoreKind::UnboundedDense`] would exceed addressable slice capacity.
    ///
    /// # Compatibility with reference implementations
    ///
    /// Pairwise compatibility requires the same [`MappingKind`], continuous bucket-coordinate drift
    /// `<= 1e-4` bins, and scaled coordinate drift `drift / min_multiplier <= 1e-10` across the
    /// normal `f64` domain. Merging a distinct compatible mapping adds `other`'s bucket and
    /// zero-bucket counts directly without re-indexing, so quantile accuracy holds up to the
    /// accumulated merge drift and above the largest zero threshold among merged sources.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::ddsketch::DdSketch;
    ///
    /// let mut left = DdSketch::<u64>::new(0.01)?;
    /// let mut right = DdSketch::<u64>::new(0.01)?;
    /// left.update(1.0);
    /// right.update(100.0);
    /// left.merge(&right)?;
    /// assert_eq!(left.total_weight(), 2);
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn merge(&mut self, other: &Self) -> Result<(), Error> {
        if !self.mapping.is_compatible(&other.mapping) {
            return Err(Error::invalid_argument(format!(
                "cannot merge DDSketches with incompatible index mappings: expected ({:?}, gamma={}, offset={}), got ({:?}, gamma={}, offset={})",
                self.mapping.kind(),
                self.mapping.gamma(),
                self.mapping.index_offset(),
                other.mapping.kind(),
                other.mapping.gamma(),
                other.mapping.index_offset(),
            )));
        }

        if other.is_empty() {
            return Ok(());
        }

        let next_neg = self
            .negative
            .total_weight()
            .checked_add(other.negative.total_weight());
        let next_zero = self.zero_weight.checked_add(other.zero_weight);
        let next_pos = self
            .positive
            .total_weight()
            .checked_add(other.positive.total_weight());
        let combined_ok = next_neg
            .zip(next_zero)
            .zip(next_pos)
            .and_then(|((n, z), p)| checked_combine_weights(n, z, p))
            .is_some();
        if !combined_ok {
            return Err(Error::invalid_argument(format!(
                "combined stream weight overflows {}: left {}, right {}",
                W::TYPE_NAME,
                self.total_weight(),
                other.total_weight(),
            )));
        }

        validate_store_domain(&self.mapping, &other.negative)?;
        validate_store_domain(&self.mapping, &other.positive)?;
        self.negative.validate_merge_capacity(&other.negative)?;
        self.positive.validate_merge_capacity(&other.positive)?;

        self.negative.merge_from(&other.negative);
        self.positive.merge_from(&other.positive);
        self.zero_weight = self.zero_weight.add_unchecked(other.zero_weight);
        self.summary.merge_non_empty(other.summary);
        Ok(())
    }

    /// Resets this sketch to its empty state while retaining its configuration and allocated
    /// dense-store capacity.
    pub fn reset(&mut self) {
        self.negative.reset();
        self.positive.reset();
        self.zero_weight = W::ZERO;
        self.summary = SummaryState::Empty;
    }

    /// Returns the exact minimum value recorded by this sketch, or `None` if this sketch is empty
    /// or its exact summary statistics are unknown (after decoding from or merging a compatibility
    /// image without exact summary blocks).
    #[inline]
    pub fn min_value(&self) -> Option<f64> {
        match self.summary {
            SummaryState::Active { min, .. } => Some(min),
            SummaryState::Empty | SummaryState::Missing => None,
        }
    }

    /// Returns the exact maximum value recorded by this sketch, or `None` if this sketch is empty
    /// or its exact summary statistics are unknown (after decoding from or merging a compatibility
    /// image without exact summary blocks).
    #[inline]
    pub fn max_value(&self) -> Option<f64> {
        match self.summary {
            SummaryState::Active { max, .. } => Some(max),
            SummaryState::Empty | SummaryState::Missing => None,
        }
    }

    /// Returns the exact sum of recorded values, or `None` if this sketch is empty or its exact
    /// summary statistics are unknown (after decoding from or merging a compatibility image without
    /// exact summary blocks).
    ///
    /// Accumulated with standard IEEE 754 `f64` addition (`value * weight`); the Java and Go
    /// reference implementations use compensated (Kahan) summation in memory, so `sum` can differ
    /// in the last bits on long or cancelling streams.
    #[inline]
    pub fn sum(&self) -> Option<f64> {
        match self.summary {
            SummaryState::Active { sum, .. } => Some(sum),
            SummaryState::Empty | SummaryState::Missing => None,
        }
    }

    /// Returns the approximate lower discrete quantile for `rank` in `[0.0, 1.0]`, or `Ok(None)`
    /// if this sketch is empty.
    ///
    /// For `u64` weights, selects the bucket containing zero-based sorted item
    /// `floor(rank * (total_weight - 1))` using the standard `f64` product when
    /// `total_weight <= 2^53` (matching the Java reference implementation) and exact integer
    /// significand scaling when `total_weight > 2^53`. For `f64` weights, selects the first bucket
    /// in ascending order whose cumulative weight exceeds `rank * (total_weight - 1.0)` (clamped
    /// to `>= 0.0`, returning the minimum bucket at `rank == 0.0` and the maximum bucket at
    /// `rank == 1.0`). Because `f64` weights use this unit-count offset rather than
    /// `rank * total_weight`, any sketch with `total_weight <= 1.0` returns the minimum bucket for
    /// all interior ranks `0.0 < rank < 1.0`, and small fractional totals above `1.0` skew
    /// interior quantiles downward. When exact summary statistics are present, `rank == 0.0`
    /// returns [`min_value`](Self::min_value), `rank == 1.0` returns [`max_value`](Self::max_value),
    /// and interior estimates are clamped to `[min_value, max_value]`.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if `rank` is NaN or
    /// outside `[0.0, 1.0]`, even if this sketch is empty.
    ///
    /// # Compatibility with reference implementations
    ///
    /// Rank scaling matches `sketches-java` (`rank * (count - 1)`) for both `u64` (up to `2^53`)
    /// and `f64` weights. On negative buckets, `sketches-go` mirrors the floating-point rank
    /// (`KeyAtRank(negative_weight - 1 - rank * (total_weight - 1))`), acting as an upper quantile
    /// on the negative store and differing on any fractional negative-side rank (such as
    /// `quantile(0.5)` on `[-100.0, -1.0]`). `DDSketchWithExactSummaryStatistics` in Java and Go
    /// clamps bucket estimates to `[min, max]` without special-casing `0.0` or `1.0`, so its
    /// endpoint queries can return an inward-rounded bucket value rather than the exact extremum.
    /// Sketches whose exact summary statistics are unknown return unclamped bucket estimates.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::ddsketch::DdSketch;
    ///
    /// let mut sketch = DdSketch::<u64>::new(0.01)?;
    /// for value in 1..=100 {
    ///     sketch.update(f64::from(value));
    /// }
    /// let p95 = sketch.quantile(0.95)?.unwrap();
    /// assert!((p95 - 95.0).abs() <= 0.01 * 95.0);
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn quantile(&self, rank: f64) -> Result<Option<f64>, Error> {
        check_rank(rank)?;
        if self.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.quantile_unchecked(rank)))
    }

    /// Returns approximate quantiles for `ranks` in input order, or `Ok(None)` if this sketch is
    /// empty.
    ///
    /// For a non-empty sketch, an empty `ranks` slice returns `Ok(Some(vec![]))`. Multiple ranks
    /// are answered in a single pass over the retained buckets regardless of input order.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if any rank is NaN or
    /// outside `[0.0, 1.0]`, even if this sketch is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::ddsketch::DdSketch;
    ///
    /// let mut sketch = DdSketch::<u64>::new(0.01)?;
    /// for value in 1..=1000 {
    ///     sketch.update(f64::from(value));
    /// }
    /// let qs = sketch.quantiles(&[0.5, 0.9, 0.99])?.unwrap();
    /// assert_eq!(qs.len(), 3);
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn quantiles(&self, ranks: &[f64]) -> Result<Option<Vec<f64>>, Error> {
        for &rank in ranks {
            check_rank(rank)?;
        }
        if self.is_empty() {
            return Ok(None);
        }
        if ranks.is_empty() {
            return Ok(Some(Vec::new()));
        }
        if ranks.len() == 1 {
            return Ok(Some(vec![self.quantile_unchecked(ranks[0])]));
        }
        if let SummaryState::Active { min, max, .. } = self.summary {
            if min == max {
                return Ok(Some(vec![min; ranks.len()]));
            }
        }

        let total = self.total_weight();
        let is_non_decreasing = ranks.windows(2).all(|pair| pair[0] <= pair[1]);
        let mut out = vec![0.0; ranks.len()];
        if is_non_decreasing {
            self.resolve_non_decreasing_ranks(ranks, total, &mut out);
        } else {
            let mut indexed: Vec<(W, usize)> = ranks
                .iter()
                .enumerate()
                .map(|(pos, &rank)| (W::target_rank(rank, total), pos))
                .collect();
            indexed.sort_unstable_by(|(a, _), (b, _)| a.total_cmp(*b));
            self.resolve_sorted_targets(&indexed, &mut out);
        }

        match self.summary {
            SummaryState::Active { min, max, .. } => {
                for (&rank, estimate) in ranks.iter().zip(&mut out) {
                    if rank == 0.0 {
                        *estimate = min;
                    } else if rank == 1.0 {
                        *estimate = max;
                    } else {
                        *estimate = estimate.clamp(min, max);
                    }
                }
            }
            SummaryState::Missing => {
                for (&rank, estimate) in ranks.iter().zip(&mut out) {
                    if rank == 0.0 {
                        *estimate = self.bucket_min_value();
                    } else if rank == 1.0 {
                        *estimate = self.bucket_max_value();
                    }
                }
            }
            SummaryState::Empty => unreachable!("non-empty sketch checked above"),
        }
        Ok(Some(out))
    }

    /// Returns an iterator over non-empty `(value, weight)` buckets in ascending numerical order.
    #[inline]
    pub fn iter(&self) -> DdSketchIter<'_, W> {
        DdSketchIter::new(
            self.mapping,
            self.negative.iter_asc(),
            self.zero_weight,
            self.positive.iter_asc(),
            self.num_retained(),
        )
    }

    /// Serializes this sketch in this crate's native `DDSKETCH` binary format, preserving
    /// [`MappingKind`], [`StoreKind`], collapse state, exact summary statistics, and bucket
    /// weights.
    ///
    /// Note: the native `Family::DDSKETCH` (`id = 25`) wire format is experimental and may change
    /// in future releases while cross-language DataSketches standardization is underway.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::ddsketch::DdSketch;
    ///
    /// let mut sketch = DdSketch::<u64>::new(0.01)?;
    /// sketch.update(3.5);
    /// let bytes = sketch.serialize();
    /// let decoded = DdSketch::<u64>::deserialize(&bytes)?;
    /// assert_eq!(decoded.total_weight(), 1);
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn serialize(&self) -> Vec<u8> {
        serialize_sketch(self)
    }

    /// Deserializes a sketch from this crate's native `DDSKETCH` binary format, restoring its
    /// encoded [`StoreKind`] and exact summary statistics (when present).
    ///
    /// Note: the native `Family::DDSKETCH` (`id = 25`) wire format is experimental and may change
    /// in future releases while cross-language DataSketches standardization is underway. Use
    /// [`deserialize_compat`](Self::deserialize_compat) to decode compact flag-block images
    /// produced by the Java and Go reference DDSketch implementations.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidData`](crate::error::ErrorKind::InvalidData) if the image is empty,
    /// truncated, has an unsupported family, version, flag, or weight type mismatch, or violates
    /// index, store, or weight bounds.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        deserialize_sketch(bytes)
    }

    /// Serializes this sketch in the compact flag-block binary format of the [Java][java-ref] and
    /// [Go][go-ref] reference DDSketch implementations (distinct from the Protobuf format used by
    /// Python and JavaScript).
    ///
    /// [java-ref]: https://github.com/DataDog/sketches-java
    /// [go-ref]: https://github.com/DataDog/sketches-go
    ///
    /// # Errors
    ///
    /// For `u64` weights, returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if
    /// [`zero_weight`](Self::zero_weight), any retained bucket weight, or (when exact summary
    /// statistics are present) [`total_weight`](Self::total_weight) exceeds `2^53 - 1`
    /// (`9_007_199_254_740_991`), the maximum integer representable without rounding in the
    /// compatibility format's `v + 1.0` floating-point counter encoding. For `f64` weights, counts
    /// are encoded directly with `varfloat64` (`v + 1.0`), which rounds fractional weights to the
    /// precision of `v + 1.0` (weights at or below `2^-53` round to `0.0`, matching the Java and
    /// Go reference implementations), and returns
    /// [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if a non-empty sketch has no
    /// retained bucket or [`zero_weight`](Self::zero_weight) above `2^-53` (so every weight would
    /// encode as `0.0`).
    ///
    /// # Compatibility with reference implementations
    ///
    /// When exact summary statistics are present, emits `count`, `min`, `max`, and non-zero `sum`
    /// feature blocks: `DDSketchWithExactSummaryStatistics` in `sketches-java` and
    /// `DecodeDDSketchWithExactSummaryStatistics` in `sketches-go` decode them, `sketches-java`'s
    /// bare `DDSketch` skips them, and `sketches-go`'s bare `DecodeDDSketch` cannot read them (it
    /// skips a fixed 8 bytes on variable-length `FlagCount`). Sketches whose exact summary
    /// statistics are unknown write no summary blocks; read non-empty summary-less images with bare
    /// `DDSketch` / `DecodeDDSketch`, as the exact-summary decoders reject them.
    ///
    /// [`StoreKind`] and collapse state are in-memory properties in the reference format and are
    /// not recorded in the output bytes; decode with
    /// [`deserialize_compat`](Self::deserialize_compat) and an explicit [`StoreKind`]. For
    /// downstream Java or Go readers, keep integer [`total_weight`](Self::total_weight) at or below
    /// `2^53 - 1` (Java computes `total_weight - 1` in `f64`), per-sign bucket index spans at or
    /// below `2^26` (`MaxDecodeIndexRange` on `sketches-go` `main`; unreleased as of `v1.4.8`), and
    /// input magnitudes within the target reader's `maxIndexableValue` (reference readers do not
    /// clamp near-[`f64::MAX`] buckets).
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::ddsketch::DdSketch;
    /// use datasketches::ddsketch::StoreKind;
    ///
    /// let mut sketch = DdSketch::<f64>::new(0.01)?;
    /// sketch.update_with_weight(42.0, 1.5);
    /// let compat_bytes = sketch.serialize_compat()?;
    /// let decoded = DdSketch::<f64>::deserialize_compat(&compat_bytes, StoreKind::default())?;
    /// assert_eq!(decoded.total_weight(), 1.5);
    /// # Ok::<(), datasketches::error::Error>(())
    /// ```
    pub fn serialize_compat(&self) -> Result<Vec<u8>, Error> {
        serialize_compat_sketch(self)
    }

    /// Deserializes a sketch from the compact flag-block binary format of the [Java][java-ref] and
    /// [Go][go-ref] reference DDSketch implementations into the specified `store` strategy.
    ///
    /// Use `DdSketch::<f64>::deserialize_compat` when images may contain fractional weights, or
    /// `DdSketch::<u64>::deserialize_compat` to enforce exact integer counts.
    ///
    /// [java-ref]: https://github.com/DataDog/sketches-java
    /// [go-ref]: https://github.com/DataDog/sketches-go
    ///
    /// # Errors
    ///
    /// Returns [`InvalidArgument`](crate::error::ErrorKind::InvalidArgument) if `store` has an
    /// invalid `max_num_bins` bound, or [`InvalidData`](crate::error::ErrorKind::InvalidData) if
    /// the image is empty, truncated, missing an index mapping, encodes a mapping that cannot index
    /// up to [`f64::MAX`] within 32-bit bucket bounds, contains unsupported flags or invalid
    /// counts (non-integer or `>= 2^64` for `u64`, negative or non-finite for `f64`), overflows
    /// index or weight bounds, or spans more than `67_108_864` total bins across negative and
    /// positive stores when `store` is [`StoreKind::UnboundedDense`].
    ///
    /// # Compatibility with reference implementations
    ///
    /// Preserves optional exact summary-statistic blocks (`count`, `sum`, `min`, `max`) emitted by
    /// `DDSketchWithExactSummaryStatistics` in Java and Go; bare `DDSketch` images without summary
    /// blocks decode with [`min_value`](Self::min_value), [`max_value`](Self::max_value), and
    /// [`sum`](Self::sum) set to `None` and re-encode without summary blocks. Exact summary extrema
    /// and finite sums must be sign-consistent with occupied buckets. Custom `minIndexedValue`
    /// thresholds are not stored in the reference format, so non-empty zero buckets accept any
    /// sign-consistent summary bounds while subsequent updates use the mapping-derived
    /// [`zero_threshold`](Self::zero_threshold). Extreme bucket index bounds allow `0.25` bins of
    /// slack before flooring to accept `sketches-go` `1 / math.Log2(gamma)` rounding near domain
    /// limits.
    ///
    /// Because reference images do not record [`StoreKind`] and encode sparse bins as compact index
    /// deltas, a small compatibility payload can span up to `67_108_864` bins (`512 MiB` of
    /// counters) when decoded into [`StoreKind::UnboundedDense`]; decode untrusted inputs into a
    /// bounded collapsing store (`StoreKind::default()`) or [`StoreKind::Sparse`].
    pub fn deserialize_compat(bytes: &[u8], store: StoreKind) -> Result<Self, Error> {
        validate_store_kind(store)?;
        deserialize_compat_sketch(bytes, store)
    }

    fn bucket_min_value(&self) -> f64 {
        if let Some(max_neg_idx) = self.negative.max_index() {
            -self.mapping.value(max_neg_idx)
        } else if !self.zero_weight.is_zero() {
            0.0
        } else {
            let min_pos_idx = self
                .positive
                .min_index()
                .expect("non-empty sketch has an occupied bucket");
            self.mapping.value(min_pos_idx)
        }
    }

    fn bucket_max_value(&self) -> f64 {
        if let Some(max_pos_idx) = self.positive.max_index() {
            self.mapping.value(max_pos_idx)
        } else if !self.zero_weight.is_zero() {
            0.0
        } else {
            let min_neg_idx = self
                .negative
                .min_index()
                .expect("non-empty sketch has an occupied bucket");
            -self.mapping.value(min_neg_idx)
        }
    }

    fn bucket_quantile_at_rank(&self, rank: f64, total: W) -> f64 {
        if rank == 0.0 {
            return self.bucket_min_value();
        }
        if rank == 1.0 {
            return self.bucket_max_value();
        }

        let target = W::target_rank(rank, total);
        let neg_weight = self.negative.total_weight();
        if target < neg_weight {
            return -self.mapping.value(self.negative.key_at_rank_desc(target));
        }
        let non_pos_weight = neg_weight.add_unchecked(self.zero_weight);
        if target < non_pos_weight {
            return 0.0;
        }
        if !self.positive.is_empty() {
            let pos_rank = target.sub_saturating(non_pos_weight);
            return self.mapping.value(self.positive.key_at_rank_asc(pos_rank));
        }
        self.bucket_max_value()
    }

    fn quantile_unchecked(&self, rank: f64) -> f64 {
        debug_assert!(!self.is_empty());
        let total = self.total_weight();
        if let SummaryState::Active { min, max, .. } = self.summary {
            if rank == 0.0 || min == max {
                return min;
            }
            if rank == 1.0 {
                return max;
            }
            return self.bucket_quantile_at_rank(rank, total).clamp(min, max);
        }
        self.bucket_quantile_at_rank(rank, total)
    }

    fn resolve_non_decreasing_ranks(&self, ranks: &[f64], total: W, out: &mut [f64]) {
        let neg_weight = self.negative.total_weight();
        let non_pos_weight = neg_weight.add_unchecked(self.zero_weight);

        let neg_end = ranks.partition_point(|&r| W::target_rank(r, total) < neg_weight);
        let zero_end = ranks[neg_end..]
            .partition_point(|&r| W::target_rank(r, total) < non_pos_weight)
            + neg_end;

        if neg_end > 0 {
            let mut last_idx: Option<(i32, f64)> = None;
            let iter = ranks[..neg_end]
                .iter()
                .enumerate()
                .map(|(pos, &r)| (W::target_rank(r, total), pos));
            self.negative.scan_ranks_desc(iter, |pos, idx| {
                let val = match last_idx {
                    Some((cached_idx, cached_val)) if cached_idx == idx => cached_val,
                    _ => {
                        let v = -self.mapping.value(idx);
                        last_idx = Some((idx, v));
                        v
                    }
                };
                out[pos] = val;
            });
        }

        if zero_end < ranks.len() {
            if self.positive.is_empty() {
                let max_val = self.bucket_max_value();
                out[zero_end..].fill(max_val);
            } else {
                let mut last_idx: Option<(i32, f64)> = None;
                let iter = ranks[zero_end..].iter().enumerate().map(|(offset, &r)| {
                    (
                        W::target_rank(r, total).sub_saturating(non_pos_weight),
                        zero_end + offset,
                    )
                });
                self.positive.scan_ranks_asc(iter, |pos, idx| {
                    let val = match last_idx {
                        Some((cached_idx, cached_val)) if cached_idx == idx => cached_val,
                        _ => {
                            let v = self.mapping.value(idx);
                            last_idx = Some((idx, v));
                            v
                        }
                    };
                    out[pos] = val;
                });
            }
        }
    }

    fn resolve_sorted_targets(&self, sorted: &[(W, usize)], out: &mut [f64]) {
        let neg_weight = self.negative.total_weight();
        let non_pos_weight = neg_weight.add_unchecked(self.zero_weight);

        let neg_end = sorted.partition_point(|&(target, _)| target < neg_weight);
        let zero_end =
            sorted[neg_end..].partition_point(|&(target, _)| target < non_pos_weight) + neg_end;

        if neg_end > 0 {
            let mut last_idx: Option<(i32, f64)> = None;
            self.negative
                .scan_ranks_desc(sorted[..neg_end].iter().copied(), |pos, idx| {
                    let val = match last_idx {
                        Some((cached_idx, cached_val)) if cached_idx == idx => cached_val,
                        _ => {
                            let v = -self.mapping.value(idx);
                            last_idx = Some((idx, v));
                            v
                        }
                    };
                    out[pos] = val;
                });
        }

        if zero_end < sorted.len() {
            if self.positive.is_empty() {
                let max_val = self.bucket_max_value();
                for &(_, pos) in &sorted[zero_end..] {
                    out[pos] = max_val;
                }
            } else {
                let mut last_idx: Option<(i32, f64)> = None;
                let iter = sorted[zero_end..]
                    .iter()
                    .map(|&(target, pos)| (target.sub_saturating(non_pos_weight), pos));
                self.positive.scan_ranks_asc(iter, |pos, idx| {
                    let val = match last_idx {
                        Some((cached_idx, cached_val)) if cached_idx == idx => cached_val,
                        _ => {
                            let v = self.mapping.value(idx);
                            last_idx = Some((idx, v));
                            v
                        }
                    };
                    out[pos] = val;
                });
            }
        }
    }
}

impl<'a, W: DdSketchWeight> IntoIterator for &'a DdSketch<W> {
    type Item = (f64, W);
    type IntoIter = DdSketchIter<'a, W>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[inline]
fn checked_combine_weights<W: DdSketchWeight>(neg: W, zero: W, pos: W) -> Option<W> {
    neg.checked_add(zero)?.checked_add(pos)
}

#[inline]
fn validate_store_domain<W: DdSketchWeight>(
    mapping: &IndexMapping,
    store: &Store<W>,
) -> Result<(), Error> {
    if let (Some(lo), Some(hi)) = (store.min_index(), store.max_index()) {
        if !mapping.contains_valid_index(lo) || !mapping.contains_valid_index(hi) {
            return Err(Error::invalid_argument(format!(
                "cannot merge DDSketch bucket range [{lo}, {hi}] outside destination mapping domain"
            )));
        }
    }
    Ok(())
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
