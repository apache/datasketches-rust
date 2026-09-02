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

//! Tuple sketch types.
//!
//! This module provides [`TupleSketch`] (mutable) and [`CompactTupleSketch`] (immutable),
//! the Tuple sketch analogues of the Theta sketch. Each retained key carries a user-defined summary
//! created by a [`SummaryPolicy`] and updated through one or more [`SummaryUpdatePolicy`]
//! implementations.

use std::hash::Hash;
use std::slice;

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::ensure_preamble_longs_in_range;
use crate::codec::assert::insufficient_data;
use crate::codec::family::Family;
use crate::common::NumStdDev;
use crate::common::ResizeFactor;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::check_seed_hash;
use crate::hash::compute_seed_hash;
use crate::thetacommon::EntrySketch;
use crate::thetacommon::KeySketch;
use crate::thetacommon::binomial_bounds;
use crate::thetacommon::constants::DEFAULT_LG_K;
use crate::thetacommon::constants::FLAGS_IS_COMPACT;
use crate::thetacommon::constants::FLAGS_IS_EMPTY;
use crate::thetacommon::constants::FLAGS_IS_ORDERED;
use crate::thetacommon::constants::FLAGS_IS_READ_ONLY;
use crate::thetacommon::constants::MAX_THETA;
use crate::thetacommon::hash_table::SketchHashTableIter;
use crate::thetacommon::sketch_state::CompactSketchState;
use crate::thetacommon::sketch_state::ThetaFamilySketchMetadata;
use crate::tuple::hash_table::TupleEntry;
use crate::tuple::hash_table::TupleHashTable;
use crate::tuple::policy::SummaryPolicy;
use crate::tuple::policy::SummaryUpdatePolicy;
use crate::tuple::serialization::SERIAL_VERSION;
use crate::tuple::serialization::SERIAL_VERSION_LEGACY;
use crate::tuple::serialization::SKETCH_TYPE;
use crate::tuple::serialization::SKETCH_TYPE_LEGACY;
use crate::tuple::serialization::TupleSummaryValue;

/// Read-only view of a mutable or compact Tuple sketch.
///
/// The view borrows the sketch without exposing its update policy. It can inspect keys without
/// requiring `S: Clone`; set operations that retain summaries require `S: Clone` when invoked.
///
/// # Examples
///
/// ```
/// use datasketches::tuple::DefaultUpdatePolicy;
/// use datasketches::tuple::TupleSketchBuilder;
///
/// let mut sketch = TupleSketchBuilder::new(DefaultUpdatePolicy::<u64>::default())
///     .build()
///     .unwrap();
/// sketch.update("apple", 1);
/// let view = sketch.as_view();
/// assert_eq!(view.iter().next().unwrap().summary(), &1);
/// ```
#[derive(Debug)]
pub struct TupleSketchView<'a, S>(TupleSketchViewState<'a, S>);

#[derive(Debug)]
enum TupleSketchViewState<'a, S> {
    Mutable {
        table: &'a TupleHashTable<S>,
        is_empty: bool,
    },
    Compact(&'a CompactTupleSketch<S>),
}

enum TupleSketchIter<'a, S> {
    Mutable(SketchHashTableIter<'a, TupleEntry<S>>),
    Compact(slice::Iter<'a, TupleEntry<S>>),
}

impl<'a, S> Iterator for TupleSketchIter<'a, S> {
    type Item = &'a TupleEntry<S>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Mutable(iter) => iter.next(),
            Self::Compact(iter) => iter.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Mutable(iter) => iter.size_hint(),
            Self::Compact(iter) => iter.size_hint(),
        }
    }
}

impl<S> Clone for TupleSketchView<'_, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for TupleSketchView<'_, S> {}

impl<S> Clone for TupleSketchViewState<'_, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for TupleSketchViewState<'_, S> {}

impl<'a, S> TupleSketchView<'a, S> {
    /// Returns the 16-bit seed hash.
    pub fn seed_hash(&self) -> u16 {
        match self.0 {
            TupleSketchViewState::Mutable { table, .. } => table.seed_hash(),
            TupleSketchViewState::Compact(sketch) => sketch.seed_hash(),
        }
    }

    /// Returns theta as a `u64` threshold.
    pub fn theta64(&self) -> u64 {
        match self.0 {
            TupleSketchViewState::Mutable { table, is_empty } => {
                if is_empty {
                    MAX_THETA
                } else {
                    table.retention_theta()
                }
            }
            TupleSketchViewState::Compact(sketch) => sketch.theta64(),
        }
    }

    /// Returns `true` if the viewed sketch is empty.
    pub fn is_empty(&self) -> bool {
        match self.0 {
            TupleSketchViewState::Mutable { is_empty, .. } => is_empty,
            TupleSketchViewState::Compact(sketch) => sketch.is_empty(),
        }
    }

    /// Returns whether retained entries are ordered by ascending hash.
    pub fn is_ordered(&self) -> bool {
        match self.0 {
            TupleSketchViewState::Mutable { .. } => false,
            TupleSketchViewState::Compact(sketch) => sketch.is_ordered(),
        }
    }

    /// Returns an iterator over retained entries.
    pub fn iter(self) -> impl Iterator<Item = &'a TupleEntry<S>> + 'a {
        match self.0 {
            TupleSketchViewState::Mutable { table, .. } => {
                TupleSketchIter::Mutable(table.iter_entries())
            }
            TupleSketchViewState::Compact(sketch) => {
                TupleSketchIter::Compact(sketch.compact_state.retained_entries().iter())
            }
        }
    }

    /// Returns the number of retained entries.
    pub fn num_retained(&self) -> usize {
        match self.0 {
            TupleSketchViewState::Mutable { table, .. } => table.num_retained(),
            TupleSketchViewState::Compact(sketch) => sketch.num_retained(),
        }
    }
}

impl<S> KeySketch for TupleSketchView<'_, S> {
    fn metadata(self) -> ThetaFamilySketchMetadata {
        if self.is_empty() {
            ThetaFamilySketchMetadata::Empty {
                seed_hash: self.seed_hash(),
            }
        } else {
            ThetaFamilySketchMetadata::NonEmpty {
                seed_hash: self.seed_hash(),
                theta: self.theta64(),
                ordered: self.is_ordered(),
                num_retained: self.num_retained(),
            }
        }
    }

    fn hashes(self) -> impl Iterator<Item = u64> {
        self.iter().map(TupleEntry::hash)
    }
}

impl<'a, S> EntrySketch for TupleSketchView<'a, S>
where
    S: Clone + 'a,
{
    type Entry = TupleEntry<S>;

    fn entries(self) -> impl Iterator<Item = Self::Entry> {
        self.iter().cloned()
    }
}

impl<'a, P> From<&'a TupleSketch<P>> for TupleSketchView<'a, P::Summary>
where
    P: SummaryPolicy,
{
    fn from(sketch: &'a TupleSketch<P>) -> Self {
        Self(TupleSketchViewState::Mutable {
            table: &sketch.table,
            is_empty: sketch.is_empty,
        })
    }
}

impl<'a, S> From<&'a CompactTupleSketch<S>> for TupleSketchView<'a, S> {
    fn from(sketch: &'a CompactTupleSketch<S>) -> Self {
        Self(TupleSketchViewState::Compact(sketch))
    }
}

/// Mutable Tuple sketch for building from input data.
///
/// `P` defines how summaries are created. The summary retained alongside each key is
/// [`P::Summary`](SummaryPolicy::Summary), while each accepted update type is selected by a
/// [`SummaryUpdatePolicy<U>`] implementation.
///
/// # Examples
///
/// ```
/// use datasketches::tuple::DefaultUpdatePolicy;
/// use datasketches::tuple::TupleSketchBuilder;
///
/// let policy = DefaultUpdatePolicy::<u64>::default();
/// let mut sketch = TupleSketchBuilder::new(policy).build().unwrap();
/// sketch.update("apple", 1);
/// sketch.update("apple", 1);
/// assert!(sketch.estimate() >= 1.0);
/// assert_eq!(sketch.num_retained(), 1);
/// ```
#[derive(Debug)]
pub struct TupleSketch<P>
where
    P: SummaryPolicy,
{
    table: TupleHashTable<P::Summary>,
    // Public emptiness tracks update calls, not retained entries: theta may screen every update.
    is_empty: bool,
    policy: P,
}

impl<P> TupleSketch<P>
where
    P: SummaryPolicy,
{
    /// Returns a read-only view accepted by Tuple set operations.
    pub fn as_view(&self) -> TupleSketchView<'_, P::Summary> {
        self.into()
    }

    /// Updates the sketch with a key and a value accepted by the policy.
    ///
    /// If the key is new, the policy creates a summary and folds in `value`; if the key already
    /// exists, `value` is folded into the retained summary. Updates screened out by theta do not
    /// change any summary.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tuple::DefaultUpdatePolicy;
    /// use datasketches::tuple::TupleSketchBuilder;
    ///
    /// let policy = DefaultUpdatePolicy::<u64>::default();
    /// let mut sketch = TupleSketchBuilder::new(policy).build().unwrap();
    /// sketch.update(42, 5);
    /// ```
    pub fn update<U>(&mut self, key: impl Hash, value: U)
    where
        P: SummaryUpdatePolicy<U>,
    {
        self.is_empty = false;
        let policy = &self.policy;
        self.table.try_insert(key, |existing| match existing {
            Some(summary) => {
                policy.update(summary, value);
                None
            }
            None => {
                let mut summary = policy.create();
                policy.update(&mut summary, value);
                Some(summary)
            }
        });
    }

    /// Returns the cardinality (distinct key count) estimate.
    pub fn estimate(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let num_retained = self.table.num_retained() as f64;
        let theta = self.theta64() as f64 / MAX_THETA as f64;
        num_retained / theta
    }

    /// Returns theta as a fraction in `[0.0, 1.0]`.
    pub fn theta(&self) -> f64 {
        self.theta64() as f64 / MAX_THETA as f64
    }

    /// Returns theta as a `u64`.
    ///
    /// An empty sketch reports `MAX_THETA` even when it was built with a sampling probability
    /// below `1.0`, matching the other DataSketches implementations.
    pub fn theta64(&self) -> u64 {
        if self.is_empty {
            MAX_THETA
        } else {
            self.table.retention_theta()
        }
    }

    /// Returns the 16-bit seed hash.
    pub fn seed_hash(&self) -> u16 {
        self.table.seed_hash()
    }

    /// Returns `true` if the sketch is empty.
    pub fn is_empty(&self) -> bool {
        self.is_empty
    }

    /// Returns `true` if the sketch is in estimation mode.
    pub fn is_estimation_mode(&self) -> bool {
        !self.is_empty && self.table.retention_theta() < MAX_THETA
    }

    /// Returns the number of retained entries.
    pub fn num_retained(&self) -> usize {
        self.table.num_retained()
    }

    /// Returns the configured `lg_k`.
    pub fn lg_k(&self) -> u8 {
        self.table.lg_nom_size()
    }

    /// Trims the sketch to the capacity configured by `lg_k`.
    pub fn trim(&mut self) {
        self.table.trim();
    }

    /// Resets the sketch to the empty state.
    pub fn reset(&mut self) {
        self.table.reset();
        self.is_empty = true;
    }

    /// Returns an iterator over retained entries.
    pub fn iter(&self) -> impl Iterator<Item = &TupleEntry<P::Summary>> + '_ {
        self.table.iter()
    }

    /// Returns the approximate lower error bound given the number of standard deviations.
    pub fn lower_bound(&self, num_std_dev: NumStdDev) -> f64 {
        if !self.is_estimation_mode() {
            return self.num_retained() as f64;
        }
        binomial_bounds::lower_bound(self.num_retained() as u64, self.theta(), num_std_dev)
            .expect("theta should always be valid")
    }

    /// Returns the approximate upper error bound given the number of standard deviations.
    pub fn upper_bound(&self, num_std_dev: NumStdDev) -> f64 {
        if !self.is_estimation_mode() {
            return self.num_retained() as f64;
        }
        binomial_bounds::upper_bound(
            self.num_retained() as u64,
            self.theta(),
            num_std_dev,
            self.is_empty(),
        )
        .expect("theta should always be valid")
    }

    /// Returns the estimated size of the sketch in bytes.
    pub fn estimated_size(&self) -> usize {
        size_of::<Self>() + self.table.estimated_size()
    }
}

impl<P> TupleSketch<P>
where
    P: SummaryPolicy,
    P::Summary: Clone,
{
    /// Returns this sketch in compact, immutable form.
    ///
    /// If `ordered` is `true`, retained entries are sorted by hash in ascending order.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tuple::DefaultUpdatePolicy;
    /// use datasketches::tuple::TupleSketchBuilder;
    ///
    /// let policy = DefaultUpdatePolicy::<u64>::default();
    /// let mut sketch = TupleSketchBuilder::new(policy).build().unwrap();
    /// sketch.update("apple", 1);
    /// let compact = sketch.compact(true);
    /// assert_eq!(compact.num_retained(), 1);
    /// ```
    pub fn compact(&self, ordered: bool) -> CompactTupleSketch<P::Summary> {
        let compact_state = if self.is_empty() {
            debug_assert_eq!(self.num_retained(), 0);
            CompactSketchState::empty(self.seed_hash())
        } else {
            self.table.to_non_empty_compact_state(ordered)
        };
        CompactTupleSketch::from_compact_state(compact_state)
    }
}

/// Compact (immutable) Tuple sketch.
///
/// This is the serialization-friendly form: a compact array of retained hash-summary pairs plus
/// theta and a 16-bit seed hash. It can be ordered (sorted ascending by hash) or unordered.
#[derive(Clone, Debug)]
pub struct CompactTupleSketch<S> {
    compact_state: CompactSketchState<TupleEntry<S>>,
}

impl<S> CompactTupleSketch<S> {
    pub(super) fn from_compact_state(compact_state: CompactSketchState<TupleEntry<S>>) -> Self {
        Self { compact_state }
    }

    /// Returns a read-only view accepted by Tuple set operations.
    pub fn as_view(&self) -> TupleSketchView<'_, S> {
        self.into()
    }

    /// Returns the cardinality (distinct key count) estimate.
    pub fn estimate(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let num_retained = self.num_retained() as f64;
        if self.theta64() == MAX_THETA {
            return num_retained;
        }
        let theta = self.theta();
        num_retained / theta
    }

    /// Returns theta as a fraction (0.0 to 1.0).
    pub fn theta(&self) -> f64 {
        self.theta64() as f64 / MAX_THETA as f64
    }

    /// Returns theta as `u64`.
    pub fn theta64(&self) -> u64 {
        self.compact_state.theta()
    }

    /// Returns `true` if the sketch is empty.
    pub fn is_empty(&self) -> bool {
        self.compact_state.is_empty()
    }

    /// Returns `true` if the sketch is in estimation mode.
    pub fn is_estimation_mode(&self) -> bool {
        self.compact_state.is_estimation_mode()
    }

    /// Returns the number of retained entries.
    pub fn num_retained(&self) -> usize {
        self.retained_entries().len()
    }

    /// Returns `true` if retained entries are ordered (sorted ascending by hash).
    pub fn is_ordered(&self) -> bool {
        self.compact_state.is_ordered()
    }

    /// Returns the 16-bit seed hash.
    pub fn seed_hash(&self) -> u16 {
        self.compact_state.seed_hash()
    }

    /// Returns an iterator over retained entries.
    pub fn iter(&self) -> impl Iterator<Item = &TupleEntry<S>> + '_ {
        self.retained_entries().iter()
    }

    fn retained_entries(&self) -> &[TupleEntry<S>] {
        self.compact_state.retained_entries()
    }

    /// Returns the approximate lower error bound given the number of standard deviations.
    pub fn lower_bound(&self, num_std_dev: NumStdDev) -> f64 {
        if !self.is_estimation_mode() {
            return self.num_retained() as f64;
        }
        binomial_bounds::lower_bound(self.num_retained() as u64, self.theta(), num_std_dev)
            .expect("compact theta should always be valid")
    }

    /// Returns the approximate upper error bound given the number of standard deviations.
    pub fn upper_bound(&self, num_std_dev: NumStdDev) -> f64 {
        if !self.is_estimation_mode() {
            return self.num_retained() as f64;
        }
        binomial_bounds::upper_bound(
            self.num_retained() as u64,
            self.theta(),
            num_std_dev,
            self.is_empty(),
        )
        .expect("compact theta should always be valid")
    }

    /// Returns the estimated size of the sketch in bytes.
    pub fn estimated_size(&self) -> usize {
        size_of::<Self>()
            + self.compact_state.retained_entries_capacity() * size_of::<TupleEntry<S>>()
    }

    fn preamble_longs(&self) -> u8 {
        if self.is_estimation_mode() {
            3
        } else if self.is_empty() || self.num_retained() == 1 {
            1
        } else {
            2
        }
    }

    /// Serializes this sketch into the compact Tuple binary format.
    ///
    /// Each summary is encoded by its [`TupleSummaryValue`] implementation. The layout matches the
    /// Java/C++ Tuple sketches, so the output can be read by those implementations given a
    /// compatible summary encoding.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tuple::DefaultUpdatePolicy;
    /// use datasketches::tuple::TupleSketchBuilder;
    ///
    /// let policy = DefaultUpdatePolicy::<u64>::default();
    /// let mut sketch = TupleSketchBuilder::new(policy).build().unwrap();
    /// sketch.update("apple", 1);
    /// let bytes = sketch.compact(true).serialize();
    /// assert!(!bytes.is_empty());
    /// ```
    pub fn serialize(&self) -> Vec<u8>
    where
        S: TupleSummaryValue,
    {
        let retained_entries = self.retained_entries();
        let pre_longs = self.preamble_longs();
        let entries_size: usize = retained_entries
            .iter()
            .map(|entry| 8 + entry.summary().serialize_size())
            .sum();
        let mut bytes = SketchBytes::with_capacity(8 * pre_longs as usize + entries_size);

        bytes.write_u8(pre_longs);
        bytes.write_u8(SERIAL_VERSION);
        bytes.write_u8(Family::TUPLE.id);
        bytes.write_u8(SKETCH_TYPE);
        bytes.write_u8(0); // unused

        let mut flags = FLAGS_IS_READ_ONLY | FLAGS_IS_COMPACT;
        if self.is_empty() {
            flags |= FLAGS_IS_EMPTY;
        }
        if self.is_ordered() {
            flags |= FLAGS_IS_ORDERED;
        }
        bytes.write_u8(flags);
        bytes.write_u16_le(self.seed_hash());

        if pre_longs > 1 {
            bytes.write_u32_le(retained_entries.len() as u32);
            bytes.write_u32_le(0); // unused
        }
        if self.is_estimation_mode() {
            bytes.write_u64_le(self.theta64());
        }

        for entry in retained_entries {
            bytes.write_u64_le(entry.hash());
            entry.summary().serialize_value(&mut bytes);
        }
        bytes.into_bytes()
    }

    /// Deserializes a compact Tuple sketch using the default seed.
    ///
    /// # Errors
    ///
    /// Returns `InvalidData` if the image is malformed, its seed hash does not match the default
    /// seed, or a summary cannot be decoded by `S`.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error>
    where
        S: TupleSummaryValue,
    {
        Self::deserialize_with_seed(bytes, DEFAULT_UPDATE_SEED)
    }

    /// Deserializes a compact Tuple sketch using the provided expected `seed`.
    ///
    /// # Errors
    ///
    /// Returns `InvalidData` if the bytes are truncated, the family/serial version/sketch type are
    /// unexpected, the seed hash does not match, the supplied seed computes to the reserved zero
    /// seed hash, or an entry is corrupted.
    pub fn deserialize_with_seed(bytes: &[u8], seed: u64) -> Result<Self, Error>
    where
        S: TupleSummaryValue,
    {
        let expected_seed_hash = compute_seed_hash(seed, ErrorKind::InvalidData)?;
        let mut cursor = SketchSlice::new(bytes);
        let pre_longs = cursor
            .read_u8()
            .map_err(insufficient_data("preamble_longs"))?;
        let ser_ver = cursor
            .read_u8()
            .map_err(insufficient_data("serial_version"))?;
        let family_id = cursor.read_u8().map_err(insufficient_data("family_id"))?;
        let sketch_type = cursor.read_u8().map_err(insufficient_data("sketch_type"))?;
        cursor.read_u8().map_err(insufficient_data("<unused>"))?;
        let flags = cursor.read_u8().map_err(insufficient_data("flags"))?;
        let seed_hash = cursor
            .read_u16_le()
            .map_err(insufficient_data("seed_hash"))?;

        Family::TUPLE.validate_id(family_id)?;
        ensure_preamble_longs_in_range(
            Family::TUPLE.min_pre_longs..=Family::TUPLE.max_pre_longs,
            pre_longs,
        )?;
        if ser_ver != SERIAL_VERSION && ser_ver != SERIAL_VERSION_LEGACY {
            return Err(Error::deserial(format!(
                "unsupported serial version: expected {} or {}, got {ser_ver}",
                SERIAL_VERSION, SERIAL_VERSION_LEGACY,
            )));
        }
        if sketch_type != SKETCH_TYPE && sketch_type != SKETCH_TYPE_LEGACY {
            return Err(Error::deserial(format!(
                "unsupported sketch type: expected {} or {}, got {sketch_type}",
                SKETCH_TYPE, SKETCH_TYPE_LEGACY,
            )));
        }

        let empty = (flags & FLAGS_IS_EMPTY) != 0;
        let ordered = (flags & FLAGS_IS_ORDERED) != 0;

        if empty {
            return Ok(Self::from_compact_state(CompactSketchState::empty(
                seed_hash,
            )));
        }

        check_seed_hash(
            expected_seed_hash,
            seed_hash,
            "deserialized CompactTupleSketch",
            ErrorKind::InvalidData,
        )?;

        let mut theta = MAX_THETA;
        let num_entries = if pre_longs == 1 {
            1
        } else {
            let n = cursor
                .read_u32_le()
                .map_err(insufficient_data("num_entries"))? as usize;
            cursor
                .read_u32_le()
                .map_err(insufficient_data("<unused_u32>"))?;
            if pre_longs > 2 {
                let value = cursor.read_u64_le().map_err(insufficient_data("theta"))?;
                if !(1..=MAX_THETA).contains(&value) {
                    return Err(Error::deserial(format!(
                        "corrupted: theta must be in [1, {MAX_THETA}], got {value}"
                    )));
                }
                theta = value;
            }
            n
        };

        let required_hash_bytes = num_entries
            .checked_mul(size_of::<u64>())
            .ok_or_else(|| Error::deserial("Tuple entry payload length overflows"))?;
        cursor
            .ensure_remaining(required_hash_bytes)
            .map_err(insufficient_data("Tuple entry hashes"))?;
        let mut retained_entries = Vec::with_capacity(num_entries);
        for _ in 0..num_entries {
            let hash = cursor
                .read_u64_le()
                .map_err(insufficient_data("entry_hash"))?;
            if hash == 0 || hash >= theta {
                return Err(Error::deserial("corrupted: invalid retained hash value"));
            }
            let summary = S::deserialize_value(&mut cursor)?;
            retained_entries.push(TupleEntry::new(hash, summary));
        }

        Ok(Self::from_compact_state(CompactSketchState::non_empty(
            retained_entries,
            theta,
            seed_hash,
            ordered,
        )))
    }
}

/// Builder for [`TupleSketch`].
///
/// Every builder carries a concrete [`SummaryPolicy`]. Use
/// [`DefaultUpdatePolicy`](crate::tuple::DefaultUpdatePolicy) for default-constructed additive
/// summaries, or supply a custom policy.
///
/// Configuration is stored without validation and checked when [`build()`](Self::build) is called.
#[derive(Debug)]
pub struct TupleSketchBuilder<P>
where
    P: SummaryPolicy,
{
    lg_k: u8,
    resize_factor: ResizeFactor,
    sampling_probability: f32,
    seed: u64,
    policy: P,
}

impl<P> TupleSketchBuilder<P>
where
    P: SummaryPolicy,
{
    /// Creates a builder with the given summary policy.
    ///
    /// # Examples
    ///
    /// ```
    /// use datasketches::tuple::SummaryPolicy;
    /// use datasketches::tuple::SummaryUpdatePolicy;
    /// use datasketches::tuple::TupleSketchBuilder;
    ///
    /// struct MaxPolicy;
    ///
    /// impl SummaryPolicy for MaxPolicy {
    ///     type Summary = u64;
    ///
    ///     fn create(&self) -> Self::Summary {
    ///         0
    ///     }
    /// }
    ///
    /// impl SummaryUpdatePolicy<u64> for MaxPolicy {
    ///     fn update(&self, summary: &mut Self::Summary, value: u64) {
    ///         *summary = (*summary).max(value);
    ///     }
    /// }
    ///
    /// let mut sketch = TupleSketchBuilder::new(MaxPolicy).build().unwrap();
    /// sketch.update("k", 3);
    /// sketch.update("k", 7);
    /// ```
    pub fn new(policy: P) -> Self {
        Self {
            lg_k: DEFAULT_LG_K,
            resize_factor: ResizeFactor::X8,
            sampling_probability: 1.0,
            seed: DEFAULT_UPDATE_SEED,
            policy,
        }
    }

    /// Sets `lg_k`, the base-2 logarithm of the nominal capacity.
    pub fn lg_k(mut self, lg_k: u8) -> Self {
        self.lg_k = lg_k;
        self
    }

    /// Sets the resize factor.
    pub fn resize_factor(mut self, factor: ResizeFactor) -> Self {
        self.resize_factor = factor;
        self
    }

    /// Sets the sampling probability.
    pub fn sampling_probability(mut self, probability: f32) -> Self {
        self.sampling_probability = probability;
        self
    }

    /// Sets the hash seed.
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Builds a [`TupleSketch`] using the supplied policy.
    ///
    /// # Errors
    ///
    /// Returns an error if `lg_k` is outside `[5, 26]`, `sampling_probability` is outside
    /// `(0.0, 1.0]`, or the computed seed hash is zero.
    pub fn build(self) -> Result<TupleSketch<P>, Error> {
        Ok(TupleSketch {
            table: TupleHashTable::new(
                self.lg_k,
                self.resize_factor,
                self.sampling_probability,
                self.seed,
            )?,
            is_empty: true,
            policy: self.policy,
        })
    }
}
