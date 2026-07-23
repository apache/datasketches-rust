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

use crate::codec::SketchBytes;
use crate::codec::SketchSlice;
use crate::codec::assert::ensure_preamble_longs_in_range;
use crate::codec::assert::insufficient_data;
use crate::codec::family::Family;
use crate::common::NumStdDev;
use crate::common::ResizeFactor;
use crate::error::Error;
use crate::hash::DEFAULT_UPDATE_SEED;
use crate::hash::compute_seed_hash;
use crate::thetacommon::RawThetaSketchView;
use crate::thetacommon::binomial_bounds;
use crate::thetacommon::constants::DEFAULT_LG_K;
use crate::thetacommon::constants::FLAGS_IS_COMPACT;
use crate::thetacommon::constants::FLAGS_IS_EMPTY;
use crate::thetacommon::constants::FLAGS_IS_ORDERED;
use crate::thetacommon::constants::FLAGS_IS_READ_ONLY;
use crate::thetacommon::constants::MAX_LG_K;
use crate::thetacommon::constants::MAX_THETA;
use crate::thetacommon::constants::MIN_LG_K;
use crate::tuple::hash_table::TupleEntry;
use crate::tuple::hash_table::TupleHashTable;
use crate::tuple::policy::SummaryPolicy;
use crate::tuple::policy::SummaryUpdatePolicy;
use crate::tuple::serialization::SERIAL_VERSION;
use crate::tuple::serialization::SERIAL_VERSION_LEGACY;
use crate::tuple::serialization::SKETCH_TYPE;
use crate::tuple::serialization::SKETCH_TYPE_LEGACY;
use crate::tuple::serialization::TupleSummaryValue;

/// Read-only view for Tuple sketches.
///
/// This trait is the input abstraction for APIs (such as union and intersection) that accept
/// either a mutable [`TupleSketch`] or an immutable [`CompactTupleSketch`]. `S` is the
/// summary type retained by the sketch.
///
/// It is blanket-implemented for every [`RawThetaSketchView`] over [`TupleEntry`], so custom
/// sketch-like inputs can be supplied by implementing that trait.
pub trait TupleSketchView<S>: RawThetaSketchView<TupleEntry<S>> {}

impl<S, T> TupleSketchView<S> for T where T: RawThetaSketchView<TupleEntry<S>> {}

/// Mutable Tuple sketch for building from input data.
///
/// `P` defines how summaries are created. The summary retained alongside each key is
/// [`P::Summary`](SummaryPolicy::Summary), while each accepted update type is selected by a
/// [`SummaryUpdatePolicy<U>`] implementation.
///
/// # Examples
///
/// ```
/// # use datasketches::tuple::{DefaultUpdatePolicy, TupleSketchBuilder};
/// let policy = DefaultUpdatePolicy::<u64>::default();
/// let mut sketch = TupleSketchBuilder::new(policy).build();
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
    policy: P,
}

impl<P> TupleSketch<P>
where
    P: SummaryPolicy,
{
    /// Updates the sketch with a key and a value accepted by the policy.
    ///
    /// If the key is new, the policy creates a summary and folds in `value`; if the key already
    /// exists, `value` is folded into the retained summary. Updates screened out by theta do not
    /// change any summary.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::tuple::{DefaultUpdatePolicy, TupleSketchBuilder};
    /// let policy = DefaultUpdatePolicy::<u64>::default();
    /// let mut sketch = TupleSketchBuilder::new(policy).build();
    /// sketch.update(42, 5);
    /// ```
    pub fn update<U>(&mut self, key: impl Hash, value: U)
    where
        P: SummaryUpdatePolicy<U>,
    {
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
        let theta = self.table.theta() as f64 / MAX_THETA as f64;
        num_retained / theta
    }

    /// Returns theta as a fraction (0.0 to 1.0).
    pub fn theta(&self) -> f64 {
        self.table.theta() as f64 / MAX_THETA as f64
    }

    /// Returns theta as `u64`.
    pub fn theta64(&self) -> u64 {
        self.table.theta()
    }

    /// Returns the 16-bit seed hash.
    pub fn seed_hash(&self) -> u16 {
        self.table.seed_hash()
    }

    /// Returns true if the sketch is empty.
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Returns true if the sketch is in estimation mode.
    pub fn is_estimation_mode(&self) -> bool {
        self.table.theta() < MAX_THETA
    }

    /// Returns the number of retained entries.
    pub fn num_retained(&self) -> usize {
        self.table.num_retained()
    }

    /// Returns lg_k (log2 of the nominal size k).
    pub fn lg_k(&self) -> u8 {
        self.table.lg_nom_size()
    }

    /// Trims the sketch to the nominal size k.
    pub fn trim(&mut self) {
        self.table.trim();
    }

    /// Resets the sketch to the empty state.
    pub fn reset(&mut self) {
        self.table.reset();
    }

    /// Returns an iterator over retained entries as `(hash, &summary)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (u64, &P::Summary)> + '_ {
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
    /// Returns this sketch in compact (immutable) form.
    ///
    /// If `ordered` is true, retained entries are sorted by hash in ascending order.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datasketches::tuple::{DefaultUpdatePolicy, TupleSketchBuilder};
    /// let policy = DefaultUpdatePolicy::<u64>::default();
    /// let mut sketch = TupleSketchBuilder::new(policy).build();
    /// sketch.update("apple", 1);
    /// let compact = sketch.compact(true);
    /// assert_eq!(compact.num_retained(), 1);
    /// ```
    pub fn compact(&self, ordered: bool) -> CompactTupleSketch<P::Summary> {
        let parts = self.table.to_compact_parts(ordered);
        CompactTupleSketch::from_parts(
            parts.entries,
            parts.theta,
            parts.seed_hash,
            parts.ordered,
            parts.empty,
        )
    }
}

impl<P> RawThetaSketchView<TupleEntry<P::Summary>> for TupleSketch<P>
where
    P: SummaryPolicy,
    P::Summary: Clone,
{
    fn seed_hash(&self) -> u16 {
        self.table.seed_hash()
    }

    fn theta(&self) -> u64 {
        self.table.theta()
    }

    fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    fn is_ordered(&self) -> bool {
        false
    }

    fn iter(&self) -> impl Iterator<Item = TupleEntry<P::Summary>> + '_ {
        self.table
            .iter()
            .map(|(hash, summary)| TupleEntry::new(hash, summary.clone()))
    }

    fn num_retained(&self) -> usize {
        self.table.num_retained()
    }
}

/// Compact (immutable) Tuple sketch.
///
/// This is the serialization-friendly form: a compact array of retained [`TupleEntry`] values
/// (hash plus summary) plus theta and a 16-bit seed hash. It can be ordered (sorted ascending by
/// hash) or unordered.
#[derive(Clone, Debug)]
pub struct CompactTupleSketch<S> {
    entries: Vec<TupleEntry<S>>,
    theta: u64,
    seed_hash: u16,
    ordered: bool,
    empty: bool,
}

impl<S> CompactTupleSketch<S> {
    pub(super) fn from_parts(
        entries: Vec<TupleEntry<S>>,
        theta: u64,
        seed_hash: u16,
        ordered: bool,
        empty: bool,
    ) -> Self {
        Self {
            entries,
            theta,
            seed_hash,
            ordered,
            empty,
        }
    }

    /// Returns the cardinality (distinct key count) estimate.
    pub fn estimate(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let num_retained = self.num_retained() as f64;
        if self.theta == MAX_THETA {
            return num_retained;
        }
        let theta = self.theta as f64 / MAX_THETA as f64;
        num_retained / theta
    }

    /// Returns theta as a fraction (0.0 to 1.0).
    pub fn theta(&self) -> f64 {
        self.theta as f64 / MAX_THETA as f64
    }

    /// Returns theta as `u64`.
    pub fn theta64(&self) -> u64 {
        self.theta
    }

    /// Returns true if the sketch is empty.
    pub fn is_empty(&self) -> bool {
        self.empty
    }

    /// Returns true if the sketch is in estimation mode.
    pub fn is_estimation_mode(&self) -> bool {
        self.theta < MAX_THETA
    }

    /// Returns the number of retained entries.
    pub fn num_retained(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if retained entries are ordered (sorted ascending by hash).
    pub fn is_ordered(&self) -> bool {
        self.ordered
    }

    /// Returns the 16-bit seed hash.
    pub fn seed_hash(&self) -> u16 {
        self.seed_hash
    }

    /// Returns an iterator over retained entries as `(hash, &summary)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (u64, &S)> + '_ {
        self.entries
            .iter()
            .map(|entry| (entry.hash(), entry.summary()))
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
        size_of::<Self>() + self.entries.capacity() * size_of::<TupleEntry<S>>()
    }

    fn preamble_longs(&self) -> u8 {
        if self.is_estimation_mode() {
            3
        } else if self.is_empty() || self.entries.len() == 1 {
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
    /// # use datasketches::tuple::{DefaultUpdatePolicy, TupleSketchBuilder};
    /// let policy = DefaultUpdatePolicy::<u64>::default();
    /// let mut sketch = TupleSketchBuilder::new(policy).build();
    /// sketch.update("apple", 1);
    /// let bytes = sketch.compact(true).serialize();
    /// assert!(!bytes.is_empty());
    /// ```
    pub fn serialize(&self) -> Vec<u8>
    where
        S: TupleSummaryValue,
    {
        let pre_longs = self.preamble_longs();
        let entries_size: usize = self
            .entries
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
        bytes.write_u16_le(self.seed_hash);

        if pre_longs > 1 {
            bytes.write_u32_le(self.entries.len() as u32);
            bytes.write_u32_le(0); // unused
        }
        if self.is_estimation_mode() {
            bytes.write_u64_le(self.theta);
        }

        for entry in &self.entries {
            bytes.write_u64_le(entry.hash());
            entry.summary().serialize_value(&mut bytes);
        }
        bytes.into_bytes()
    }

    /// Deserializes a compact Tuple sketch using the default seed.
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
    /// Returns an error if the bytes are truncated, the family/serial version/sketch type are
    /// unexpected, the seed hash does not match (for non-empty sketches), or an entry is corrupted.
    pub fn deserialize_with_seed(bytes: &[u8], seed: u64) -> Result<Self, Error>
    where
        S: TupleSummaryValue,
    {
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
            return Ok(Self::from_parts(
                vec![],
                MAX_THETA,
                seed_hash,
                ordered,
                true,
            ));
        }

        let expected_seed_hash = compute_seed_hash(seed);
        if seed_hash != expected_seed_hash {
            return Err(Error::deserial(format!(
                "incompatible seed hash: expected {expected_seed_hash}, got {seed_hash}",
            )));
        }

        let mut theta = MAX_THETA;
        let num_entries = if pre_longs == 1 {
            1usize
        } else {
            let n = cursor
                .read_u32_le()
                .map_err(insufficient_data("num_entries"))? as usize;
            cursor
                .read_u32_le()
                .map_err(insufficient_data("<unused_u32>"))?;
            if pre_longs > 2 {
                theta = cursor.read_u64_le().map_err(insufficient_data("theta"))?;
            }
            n
        };

        let mut entries = Vec::with_capacity(num_entries);
        for _ in 0..num_entries {
            let hash = cursor
                .read_u64_le()
                .map_err(insufficient_data("entry_hash"))?;
            if hash == 0 || hash >= theta {
                return Err(Error::deserial("corrupted: invalid retained hash value"));
            }
            let summary = S::deserialize_value(&mut cursor)?;
            entries.push(TupleEntry::new(hash, summary));
        }

        Ok(Self::from_parts(entries, theta, seed_hash, ordered, false))
    }
}

impl<S: Clone> RawThetaSketchView<TupleEntry<S>> for CompactTupleSketch<S> {
    fn seed_hash(&self) -> u16 {
        self.seed_hash
    }

    fn theta(&self) -> u64 {
        self.theta
    }

    fn is_empty(&self) -> bool {
        self.empty
    }

    fn is_ordered(&self) -> bool {
        self.ordered
    }

    fn iter(&self) -> impl Iterator<Item = TupleEntry<S>> + '_ {
        self.entries.iter().cloned()
    }

    fn num_retained(&self) -> usize {
        self.entries.len()
    }
}

/// Builder for [`TupleSketch`].
///
/// Every builder carries a concrete [`SummaryPolicy`]. Use
/// [`DefaultUpdatePolicy`](crate::tuple::DefaultUpdatePolicy) for default-constructed additive
/// summaries, or supply a custom policy.
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
    /// let mut sketch = TupleSketchBuilder::new(MaxPolicy).build();
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

    /// Sets lg_k (log2 of the nominal size k).
    ///
    /// # Panics
    ///
    /// Panics if lg_k is not in range [5, 26].
    pub fn lg_k(mut self, lg_k: u8) -> Self {
        assert!(
            (MIN_LG_K..=MAX_LG_K).contains(&lg_k),
            "lg_k must be in [{MIN_LG_K}, {MAX_LG_K}], got {lg_k}"
        );
        self.lg_k = lg_k;
        self
    }

    /// Sets the resize factor.
    pub fn resize_factor(mut self, factor: ResizeFactor) -> Self {
        self.resize_factor = factor;
        self
    }

    /// Sets the sampling probability p.
    ///
    /// # Panics
    ///
    /// Panics if p is not in range `(0.0, 1.0]`.
    pub fn sampling_probability(mut self, probability: f32) -> Self {
        assert!(
            (0.0..=1.0).contains(&probability) && probability > 0.0,
            "sampling_probability must be in (0.0, 1.0], got {probability}"
        );
        self.sampling_probability = probability;
        self
    }

    /// Sets the hash seed.
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Builds a [`TupleSketch`] using the supplied policy.
    pub fn build(self) -> TupleSketch<P> {
        TupleSketch {
            table: TupleHashTable::new(
                self.lg_k,
                self.resize_factor,
                self.sampling_probability,
                self.seed,
            ),
            policy: self.policy,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;
    use crate::tuple::policy::DefaultUpdatePolicy;
    use crate::tuple::policy::SummaryPolicy;
    use crate::tuple::policy::SummaryUpdatePolicy;

    fn sorted_updatable_entries<P>(sketch: &TupleSketch<P>) -> Vec<(u64, u64)>
    where
        P: SummaryPolicy<Summary = u64>,
    {
        let mut entries: Vec<(u64, u64)> = sketch.iter().map(|(h, &s)| (h, s)).collect();
        entries.sort_unstable();
        entries
    }

    fn sorted_compact_entries(sketch: &CompactTupleSketch<u64>) -> Vec<(u64, u64)> {
        let mut entries: Vec<(u64, u64)> = sketch.iter().map(|(h, &s)| (h, s)).collect();
        entries.sort_unstable();
        entries
    }

    fn assert_updatable_and_compact_equivalent<P>(
        updatable: &TupleSketch<P>,
        compact: &CompactTupleSketch<u64>,
    ) where
        P: SummaryPolicy<Summary = u64>,
    {
        assert_eq!(updatable.is_empty(), compact.is_empty());
        assert_eq!(updatable.is_estimation_mode(), compact.is_estimation_mode());
        assert_eq!(updatable.num_retained(), compact.num_retained());
        assert_eq!(updatable.theta64(), compact.theta64());
        assert_eq!(updatable.seed_hash(), compact.seed_hash());
        assert_eq!(
            sorted_updatable_entries(updatable),
            sorted_compact_entries(compact)
        );
        assert!((updatable.estimate() - compact.estimate()).abs() <= 1e-9);
    }

    #[test]
    fn exact_mode_updatable_and_compact_equivalent() {
        let policy = DefaultUpdatePolicy::<u64>::default();
        let mut sketch = TupleSketchBuilder::new(policy).lg_k(12).build();
        for i in 0..2000 {
            sketch.update(i, 1u64);
        }
        assert!(!sketch.is_estimation_mode());

        for ordered in [false, true] {
            let compact = sketch.compact(ordered);
            assert_updatable_and_compact_equivalent(&sketch, &compact);
            if compact.num_retained() > 1 {
                assert_eq!(compact.is_ordered(), ordered);
            }
        }
    }

    #[test]
    fn estimation_mode_updatable_and_compact_equivalent() {
        let policy = DefaultUpdatePolicy::<u64>::default();
        let mut sketch = TupleSketchBuilder::new(policy).lg_k(5).build();
        for i in 0..5000 {
            sketch.update(i, 1u64);
        }
        assert!(sketch.is_estimation_mode());

        for ordered in [false, true] {
            let compact = sketch.compact(ordered);
            assert_updatable_and_compact_equivalent(&sketch, &compact);
        }
    }

    #[test]
    fn summaries_accumulate_with_default_policy() {
        let policy = DefaultUpdatePolicy::<u64>::default();
        let mut sketch = TupleSketchBuilder::new(policy).build();
        for _ in 0..5 {
            sketch.update("same_key", 2u64);
        }
        assert_eq!(sketch.num_retained(), 1);
        let entries = sorted_updatable_entries(&sketch);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, 10); // 5 updates of 2

        // Summaries survive the compaction.
        let compact = sketch.compact(true);
        assert_eq!(sorted_compact_entries(&compact)[0].1, 10);
    }

    #[test]
    fn default_policy_accepts_any_add_assign_rhs() {
        let policy = DefaultUpdatePolicy::<String>::default();
        let mut sketch = TupleSketchBuilder::new(policy).build();
        sketch.update("k", "hello");
        sketch.update("k", " world");

        assert_eq!(sketch.iter().next().unwrap().1, "hello world");
    }

    #[test]
    fn empty_sketch_is_ordered_and_zero_estimate() {
        let policy = DefaultUpdatePolicy::<u64>::default();
        let sketch = TupleSketchBuilder::new(policy).build();
        assert!(sketch.is_empty());
        assert_eq!(sketch.estimate(), 0.0);

        let compact = sketch.compact(false);
        assert!(compact.is_empty());
        assert!(compact.is_ordered());
        assert_eq!(compact.estimate(), 0.0);
        assert_eq!(compact.theta64(), MAX_THETA);
    }

    #[test]
    fn bounds_bracket_estimate_in_estimation_mode() {
        let policy = DefaultUpdatePolicy::<u64>::default();
        let mut sketch = TupleSketchBuilder::new(policy).lg_k(12).build();
        for i in 0..10000 {
            sketch.update(i, 1u64);
        }
        let estimate = sketch.estimate();
        let lower = sketch.lower_bound(NumStdDev::Two);
        let upper = sketch.upper_bound(NumStdDev::Two);
        assert!(lower <= estimate);
        assert!(estimate <= upper);
    }

    #[derive(Default)]
    struct MaxPolicy;

    impl SummaryPolicy for MaxPolicy {
        type Summary = u64;

        fn create(&self) -> Self::Summary {
            0
        }
    }

    impl SummaryUpdatePolicy<u64> for MaxPolicy {
        fn update(&self, summary: &mut Self::Summary, value: u64) {
            *summary = (*summary).max(value);
        }
    }

    #[test]
    fn custom_policy_drives_summary_behavior() {
        let mut sketch = TupleSketchBuilder::new(MaxPolicy).build();
        sketch.update("k", 3u64);
        sketch.update("k", 7u64);
        sketch.update("k", 2u64);

        assert_eq!(sketch.num_retained(), 1);
        let entries = sorted_updatable_entries(&sketch);
        assert_eq!(entries[0].1, 7);
    }

    struct ArraySumPolicy {
        num_values: usize,
    }

    impl SummaryPolicy for ArraySumPolicy {
        type Summary = Vec<f64>;

        fn create(&self) -> Self::Summary {
            vec![0.0; self.num_values]
        }
    }

    impl<U> SummaryUpdatePolicy<U> for ArraySumPolicy
    where
        U: AsRef<[f64]>,
    {
        fn update(&self, summary: &mut Self::Summary, value: U) {
            let value = value.as_ref();
            assert_eq!(value.len(), self.num_values);
            for (summary, value) in summary.iter_mut().zip(value) {
                *summary += value;
            }
        }
    }

    #[test]
    fn custom_policy_accepts_multiple_update_representations() {
        let mut sketch = TupleSketchBuilder::new(ArraySumPolicy { num_values: 2 }).build();
        sketch.update("k", &[1.0, 2.0]);
        sketch.update("k", vec![3.0, 4.0]);

        assert_eq!(sketch.iter().next().unwrap().1, &vec![4.0, 6.0]);
    }

    fn view_num_retained<V: TupleSketchView<u64>>(view: &V) -> usize {
        view.num_retained()
    }

    fn view_summary_sum<V: TupleSketchView<u64>>(view: &V) -> u64 {
        view.iter().map(|entry| *entry.summary()).sum()
    }

    fn view_is_ordered<V: TupleSketchView<u64>>(view: &V) -> bool {
        view.is_ordered()
    }

    #[test]
    fn view_trait_accepts_both_sketch_types() {
        let policy = DefaultUpdatePolicy::<u64>::default();
        let mut sketch = TupleSketchBuilder::new(policy).build();
        for i in 0..100 {
            sketch.update(i, 2u64);
        }
        let compact = sketch.compact(true);

        // Both sketch types are accepted through the shared view trait.
        assert_eq!(view_num_retained(&sketch), 100);
        assert_eq!(view_num_retained(&compact), 100);
        assert_eq!(view_summary_sum(&sketch), view_summary_sum(&compact));
        assert_eq!(view_summary_sum(&compact), 200); // 100 keys * 2

        // Updatable is unordered by default; compact(true) reports ordered.
        assert!(!view_is_ordered(&sketch));
        assert!(view_is_ordered(&compact));
    }

    fn assert_compact_round_trip(original: &CompactTupleSketch<u64>) {
        let bytes = original.serialize();
        let restored = CompactTupleSketch::<u64>::deserialize(&bytes).unwrap();
        assert_eq!(original.is_empty(), restored.is_empty());
        assert_eq!(original.is_ordered(), restored.is_ordered());
        assert_eq!(original.theta64(), restored.theta64());
        assert_eq!(original.seed_hash(), restored.seed_hash());
        assert_eq!(original.num_retained(), restored.num_retained());
        assert_eq!(
            sorted_compact_entries(original),
            sorted_compact_entries(&restored)
        );
    }

    #[test]
    fn serialize_round_trip_exact_mode() {
        let policy = DefaultUpdatePolicy::<u64>::default();
        let mut sketch = TupleSketchBuilder::new(policy).lg_k(12).build();
        for i in 0..2000 {
            sketch.update(i, 1u64);
        }
        assert!(!sketch.is_estimation_mode());
        assert_compact_round_trip(&sketch.compact(true));
        assert_compact_round_trip(&sketch.compact(false));
    }

    #[test]
    fn serialize_round_trip_estimation_mode() {
        let policy = DefaultUpdatePolicy::<u64>::default();
        let mut sketch = TupleSketchBuilder::new(policy).lg_k(5).build();
        for i in 0..5000 {
            sketch.update(i, 3u64);
        }
        let compact = sketch.compact(true);
        assert!(compact.is_estimation_mode());
        assert_compact_round_trip(&compact);
        assert_compact_round_trip(&sketch.compact(false));
    }

    #[test]
    fn serialize_round_trip_empty() {
        let policy = DefaultUpdatePolicy::<u64>::default();
        let sketch = TupleSketchBuilder::new(policy).build();
        let compact = sketch.compact(true);
        assert!(compact.is_empty());
        assert_compact_round_trip(&compact);
    }

    #[test]
    fn serialize_round_trip_single_entry() {
        let policy = DefaultUpdatePolicy::<u64>::default();
        let mut sketch = TupleSketchBuilder::new(policy).build();
        sketch.update("only", 42u64);
        let compact = sketch.compact(true);
        assert_eq!(compact.num_retained(), 1);

        let bytes = compact.serialize();
        // A single-entry exact sketch uses a 1-long preamble.
        assert_eq!(bytes[0], 1);

        let restored = CompactTupleSketch::<u64>::deserialize(&bytes).unwrap();
        assert_eq!(restored.num_retained(), 1);
        assert_eq!(restored.iter().next().unwrap().1, &42);
    }

    #[test]
    fn serialize_header_fields_match_tuple_format() {
        let policy = DefaultUpdatePolicy::<u64>::default();
        let mut sketch = TupleSketchBuilder::new(policy).build();
        for i in 0..100 {
            sketch.update(i, 1u64);
        }
        let bytes = sketch.compact(true).serialize();
        assert_eq!(bytes[0], 2); // preamble longs (exact, multi-entry)
        assert_eq!(bytes[1], 3); // serial version
        assert_eq!(bytes[2], 9); // TUPLE family id
        assert_eq!(bytes[3], 1); // sketch type
    }

    #[test]
    fn serialize_preserves_summaries() {
        let policy = DefaultUpdatePolicy::<u64>::default();
        let mut sketch = TupleSketchBuilder::new(policy).build();
        for i in 0..50 {
            sketch.update(i, 1u64);
            sketch.update(i, 1u64); // each summary accumulates to 2
        }
        let bytes = sketch.compact(true).serialize();
        let restored = CompactTupleSketch::<u64>::deserialize(&bytes).unwrap();
        assert_eq!(restored.num_retained(), 50);
        assert!(restored.iter().all(|(_, &s)| s == 2));
    }

    #[test]
    fn deserialize_rejects_wrong_family() {
        let policy = DefaultUpdatePolicy::<u64>::default();
        let mut sketch = TupleSketchBuilder::new(policy).build();
        for i in 0..10 {
            sketch.update(i, 1u64);
        }
        let mut bytes = sketch.compact(true).serialize();
        bytes[2] = 3; // pretend it is a THETA sketch
        let err = CompactTupleSketch::<u64>::deserialize(&bytes).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn deserialize_rejects_seed_mismatch() {
        let policy = DefaultUpdatePolicy::<u64>::default();
        let mut sketch = TupleSketchBuilder::new(policy).build();
        for i in 0..10 {
            sketch.update(i, 1u64);
        }
        let bytes = sketch.compact(true).serialize();
        let err = CompactTupleSketch::<u64>::deserialize_with_seed(&bytes, 999).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn deserialize_rejects_truncated_summary() {
        let policy = DefaultUpdatePolicy::<u64>::default();
        let mut sketch = TupleSketchBuilder::new(policy).build();
        for i in 0..100 {
            sketch.update(i, 1u64);
        }
        let bytes = sketch.compact(true).serialize();
        let truncated = &bytes[..bytes.len() - 4]; // cut the last summary in half
        let err = CompactTupleSketch::<u64>::deserialize(truncated).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }
}
