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

use std::collections::BTreeMap;
use std::collections::btree_map;
use std::iter::FusedIterator;

use super::DEFAULT_MAX_NUM_BINS;
use super::MAX_DENSE_BINS;
use crate::error::Error;

const INITIAL_DENSE_OVERHEAD: usize = 64;
const MAX_I32_SPAN: u64 = (i32::MAX as i64 - i32::MIN as i64 + 1) as u64;
const MAX_VEC_U64_LEN: u64 = ((isize::MAX as usize) / size_of::<u64>()) as u64;
/// Maximum number of `u64` slots a `DenseStore` can hold: bounded both by addressable `Vec<u64>`
/// capacity (`isize::MAX / 8` on 32-bit targets) and by the 32-bit bucket index domain (`2^32` on
/// 64-bit targets).
pub const MAX_DENSE_LEN: usize = if MAX_VEC_U64_LEN < MAX_I32_SPAN {
    MAX_VEC_U64_LEN as usize
} else {
    MAX_I32_SPAN as usize
};

/// Bin storage strategy used by the positive and negative stores of a
/// [`DdSketch`](super::DdSketch).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StoreKind {
    /// Contiguous bin array capped at `max_num_bins` per sign, collapsing lowest-magnitude bins
    /// when the observed index span exceeds `max_num_bins` (default: `2048`).
    ///
    /// Preserves relative accuracy for large magnitudes (high quantiles of positive streams and
    /// extreme positive/negative tails of signed streams).
    CollapsingLowestDense {
        /// Maximum number of contiguous bins tracked per sign (`1..=67_108_864`).
        max_num_bins: usize,
    },
    /// Contiguous bin array capped at `max_num_bins` per sign, collapsing highest-magnitude bins
    /// when the observed index span exceeds `max_num_bins`.
    ///
    /// Preserves relative accuracy for magnitudes closest to zero.
    CollapsingHighestDense {
        /// Maximum number of contiguous bins tracked per sign (`1..=67_108_864`).
        max_num_bins: usize,
    },
    /// Dynamically growing contiguous bin array without bin collapsing.
    ///
    /// Offers constant-time updates inside the observed index span while preserving relative
    /// accuracy across all indexed values. Memory scales with the logarithmic span of observed
    /// magnitudes; when decoding untrusted compatibility inputs with
    /// [`DdSketch::deserialize_compat`](super::DdSketch::deserialize_compat), prefer a bounded
    /// collapsing store or [`StoreKind::Sparse`] because a compact two-bin delta stream can span up
    /// to `67_108_864` bins (`512 MiB`).
    UnboundedDense,
    /// Ordered map of non-empty bins without bin collapsing.
    ///
    /// Retains only occupied bins, bounding memory by the number of distinct occupied buckets on
    /// sparse or multimodal distributions at the cost of logarithmic-time updates.
    Sparse,
}

impl Default for StoreKind {
    fn default() -> Self {
        Self::CollapsingLowestDense {
            max_num_bins: DEFAULT_MAX_NUM_BINS,
        }
    }
}

pub(super) fn validate_store_kind(kind: StoreKind) -> Result<(), Error> {
    match kind {
        StoreKind::CollapsingLowestDense { max_num_bins }
        | StoreKind::CollapsingHighestDense { max_num_bins } => {
            if !(1..=MAX_DENSE_BINS).contains(&max_num_bins) {
                return Err(Error::invalid_argument(format!(
                    "max_num_bins must be in [1, {MAX_DENSE_BINS}], got {max_num_bins}"
                )));
            }
            Ok(())
        }
        StoreKind::UnboundedDense | StoreKind::Sparse => Ok(()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CollapseMode {
    None,
    Lowest(usize),
    Highest(usize),
}

#[derive(Debug, Clone)]
pub enum Store {
    Dense(DenseStore),
    Sparse(SparseStore),
}

impl Store {
    pub fn new(kind: StoreKind) -> Result<Self, Error> {
        validate_store_kind(kind)?;
        Ok(match kind {
            StoreKind::CollapsingLowestDense { max_num_bins } => {
                Self::Dense(DenseStore::new(CollapseMode::Lowest(max_num_bins)))
            }
            StoreKind::CollapsingHighestDense { max_num_bins } => {
                Self::Dense(DenseStore::new(CollapseMode::Highest(max_num_bins)))
            }
            StoreKind::UnboundedDense => Self::Dense(DenseStore::new(CollapseMode::None)),
            StoreKind::Sparse => Self::Sparse(SparseStore::default()),
        })
    }

    /// Creates a store pre-windowed to the non-zero wire index range `bounds`, avoiding
    /// intermediate reallocations or window shifts when populating bins via
    /// [`add_in_bounds`](Self::add_in_bounds).
    pub fn with_wire_bounds(kind: StoreKind, bounds: Option<(i32, i32)>) -> Result<Self, Error> {
        let mut store = Self::new(kind)?;
        if let (Self::Dense(dense), Some((wire_min, wire_max))) = (&mut store, bounds) {
            dense.init_for_wire_bounds(wire_min, wire_max);
        }
        Ok(store)
    }

    #[inline]
    pub const fn kind(&self) -> StoreKind {
        match self {
            Self::Dense(dense) => match dense.mode {
                CollapseMode::None => StoreKind::UnboundedDense,
                CollapseMode::Lowest(max_num_bins) => {
                    StoreKind::CollapsingLowestDense { max_num_bins }
                }
                CollapseMode::Highest(max_num_bins) => {
                    StoreKind::CollapsingHighestDense { max_num_bins }
                }
            },
            Self::Sparse(_) => StoreKind::Sparse,
        }
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.total_weight() == 0
    }

    #[inline]
    pub const fn total_weight(&self) -> u64 {
        match self {
            Self::Dense(dense) => dense.count,
            Self::Sparse(sparse) => sparse.count,
        }
    }

    #[inline]
    pub fn num_occupied(&self) -> usize {
        match self {
            Self::Dense(dense) => dense.occupied,
            Self::Sparse(sparse) => sparse.bins.len(),
        }
    }

    #[inline]
    pub fn min_index(&self) -> Option<i32> {
        match self {
            Self::Dense(dense) => dense.min_index(),
            Self::Sparse(sparse) => sparse.min_index(),
        }
    }

    #[inline]
    pub fn max_index(&self) -> Option<i32> {
        match self {
            Self::Dense(dense) => dense.max_index(),
            Self::Sparse(sparse) => sparse.max_index(),
        }
    }

    #[inline]
    pub fn ensure_index_capacity(&self, index: i32) {
        if let Self::Dense(dense) = self {
            if matches!(dense.mode, CollapseMode::None) && dense.count > 0 {
                let span = span_u64(dense.min_index.min(index), dense.max_index.max(index));
                assert!(
                    span <= MAX_DENSE_LEN as u64,
                    "unbounded dense store span ({span}) exceeds maximum dense capacity ({MAX_DENSE_LEN})"
                );
            }
        }
    }

    pub fn validate_merge_capacity(&self, other: &Self) -> Result<(), Error> {
        if other.is_empty() {
            return Ok(());
        }
        if let Self::Dense(dst) = self {
            if matches!(dst.mode, CollapseMode::None) {
                let other_min = other.min_index().expect("non-empty store has min_index");
                let other_max = other.max_index().expect("non-empty store has max_index");
                let (new_min, new_max) = if dst.count == 0 {
                    (other_min, other_max)
                } else {
                    (dst.min_index.min(other_min), dst.max_index.max(other_max))
                };
                let span = span_u64(new_min, new_max);
                if span > MAX_DENSE_LEN as u64 {
                    return Err(Error::invalid_argument(format!(
                        "merged unbounded dense store span ({span}) exceeds maximum dense capacity ({MAX_DENSE_LEN})"
                    )));
                }
            }
        }
        Ok(())
    }

    #[inline]
    pub fn add(&mut self, index: i32, weight: u64) {
        if weight == 0 {
            return;
        }
        match self {
            Self::Dense(dense) => dense.add(index, weight),
            Self::Sparse(sparse) => sparse.add(index, weight),
        }
    }

    /// Adds `weight` at `index` into a store initialized via
    /// [`with_wire_bounds`](Self::with_wire_bounds) covering `index`.
    #[inline]
    pub fn add_in_bounds(&mut self, index: i32, weight: u64) {
        if weight == 0 {
            return;
        }
        match self {
            Self::Dense(dense) => dense.add_in_bounds(index, weight),
            Self::Sparse(sparse) => sparse.add(index, weight),
        }
    }

    pub fn merge_from(&mut self, other: &Self) {
        if other.is_empty() {
            return;
        }
        match (self, other) {
            (Self::Dense(dst), Self::Dense(src)) => dst.merge_dense(src),
            (Self::Dense(dst), Self::Sparse(src)) => dst.merge_sparse(src),
            (Self::Sparse(dst), src) => {
                for (index, weight) in src.iter_asc() {
                    dst.add(index, weight);
                }
            }
        }
    }

    pub fn key_at_rank(&self, rank: u64) -> i32 {
        match self {
            Self::Dense(dense) => dense.key_at_rank(rank),
            Self::Sparse(sparse) => sparse.key_at_rank(rank),
        }
    }

    /// Resolves non-decreasing 0-based `ranks` in ascending index order in a single scan, invoking
    /// `emit(pos, index)` for each `(rank, pos)` entry.
    pub fn scan_ranks_asc(
        &self,
        ranks: impl IntoIterator<Item = (u64, usize)>,
        mut emit: impl FnMut(usize, i32),
    ) {
        let mut ranks = ranks.into_iter().peekable();
        if ranks.peek().is_none() {
            return;
        }
        match self {
            Self::Dense(dense) => {
                let start = dense.pos(dense.min_index);
                let end = dense.pos(dense.max_index);
                let slice = &dense.bins[start..=end];
                let mut offset = 0_usize;
                let mut cumul_end = slice[0];
                for (target, pos) in ranks {
                    while target >= cumul_end {
                        offset += 1;
                        cumul_end += slice[offset];
                    }
                    let idx = (i64::from(dense.min_index) + (offset as i64)) as i32;
                    emit(pos, idx);
                }
            }
            Self::Sparse(sparse) => {
                let mut iter = sparse.bins.iter();
                let (&first_idx, &first_w) = iter.next().expect("non-empty sparse store");
                let mut cur_idx = first_idx;
                let mut cumul_end = first_w;
                for (target, pos) in ranks {
                    while target >= cumul_end {
                        let (&idx, &w) =
                            iter.next().expect("rank within sparse store total_weight");
                        cur_idx = idx;
                        cumul_end += w;
                    }
                    emit(pos, cur_idx);
                }
            }
        }
    }

    /// Resolves non-decreasing 0-based `ranks` in descending index order in a single scan,
    /// invoking `emit(pos, index)` for each `(rank, pos)` entry.
    pub fn scan_ranks_desc(
        &self,
        ranks: impl IntoIterator<Item = (u64, usize)>,
        mut emit: impl FnMut(usize, i32),
    ) {
        let mut ranks = ranks.into_iter().peekable();
        if ranks.peek().is_none() {
            return;
        }
        match self {
            Self::Dense(dense) => {
                let start = dense.pos(dense.min_index);
                let end = dense.pos(dense.max_index);
                let slice = &dense.bins[start..=end];
                let mut offset = slice.len() - 1;
                let mut cumul_end = slice[offset];
                for (target, pos) in ranks {
                    while target >= cumul_end {
                        offset -= 1;
                        cumul_end += slice[offset];
                    }
                    let idx = (i64::from(dense.min_index) + (offset as i64)) as i32;
                    emit(pos, idx);
                }
            }
            Self::Sparse(sparse) => {
                let mut iter = sparse.bins.iter().rev();
                let (&first_idx, &first_w) = iter.next().expect("non-empty sparse store");
                let mut cur_idx = first_idx;
                let mut cumul_end = first_w;
                for (target, pos) in ranks {
                    while target >= cumul_end {
                        let (&idx, &w) =
                            iter.next().expect("rank within sparse store total_weight");
                        cur_idx = idx;
                        cumul_end += w;
                    }
                    emit(pos, cur_idx);
                }
            }
        }
    }

    pub fn reset(&mut self) {
        match self {
            Self::Dense(dense) => dense.reset(),
            Self::Sparse(sparse) => sparse.reset(),
        }
    }

    pub fn estimated_size(&self) -> usize {
        match self {
            Self::Dense(dense) => dense.bins.capacity().saturating_mul(size_of::<u64>()),
            Self::Sparse(sparse) => sparse
                .bins
                .len()
                .saturating_mul(size_of::<i32>() + size_of::<u64>() + 16),
        }
    }

    #[inline]
    pub fn iter_asc(&self) -> StoreIter<'_> {
        match self {
            Self::Dense(dense) => StoreIter::Dense(dense.iter_asc()),
            Self::Sparse(sparse) => StoreIter::Sparse(sparse.bins.iter()),
        }
    }

    #[inline]
    pub fn dense_contiguous_slice(&self) -> Option<(i32, &[u64])> {
        match self {
            Self::Dense(dense) => dense.active_slice(),
            Self::Sparse(_) => None,
        }
    }

    #[inline]
    pub const fn is_collapsed(&self) -> bool {
        match self {
            Self::Dense(dense) => dense.is_collapsed,
            Self::Sparse(_) => false,
        }
    }

    #[inline]
    pub fn set_collapsed(&mut self, collapsed: bool) {
        if let Self::Dense(dense) = self {
            if let CollapseMode::Lowest(max_bins) | CollapseMode::Highest(max_bins) = dense.mode {
                debug_assert!(
                    !collapsed
                        || (dense.count > 0
                            && dense.bins.len() == max_bins
                            && span_u64(dense.min_index, dense.max_index) == max_bins as u64
                            && dense.offset == dense.min_index)
                );
                dense.is_collapsed = collapsed;
            }
        }
    }

    /// Decodes `num_bins` contiguous weights starting at `first_index` (step `+1`) into an empty
    /// store whose bin budget fits `num_bins`, returning `(weight_sum, occupied_wire_bounds)`.
    pub fn decode_contiguous_unit_step<F>(
        &mut self,
        first_index: i32,
        num_bins: usize,
        mut next_weight: F,
    ) -> Result<(u64, Option<(i32, i32)>), Error>
    where
        F: FnMut() -> Result<u64, Error>,
    {
        debug_assert!(self.is_empty());
        if num_bins == 0 {
            return Ok((0, None));
        }
        let last_index = (i64::from(first_index) + (num_bins as i64) - 1) as i32;
        match self {
            Self::Dense(dense) => {
                let cap = dense.exact_len(num_bins as u64);
                debug_assert!(dense.count == 0 && num_bins <= cap);
                dense.grow_bins(cap);
                let offset = centered_offset(first_index, last_index, dense.bins.len());
                let dst_start = (i64::from(first_index) - i64::from(offset)) as usize;
                let dst = &mut dense.bins[dst_start..(dst_start + num_bins)];
                let mut sum = 0_u64;
                let mut occupied = 0_usize;
                let mut first_nz: Option<usize> = None;
                let mut last_nz = 0_usize;
                for (i, slot) in dst.iter_mut().enumerate() {
                    let w = next_weight()?;
                    if w != 0 {
                        sum = sum
                            .checked_add(w)
                            .ok_or_else(|| Error::deserial("total_weight overflows u64"))?;
                        *slot = w;
                        occupied += 1;
                        if first_nz.is_none() {
                            first_nz = Some(i);
                        }
                        last_nz = i;
                    }
                }
                let wire_bounds = first_nz.map(|first_pos| {
                    let lo = (i64::from(first_index) + (first_pos as i64)) as i32;
                    let hi = (i64::from(first_index) + (last_nz as i64)) as i32;
                    dense.offset = offset;
                    dense.min_index = lo;
                    dense.max_index = hi;
                    dense.occupied = occupied;
                    dense.count = sum;
                    (lo, hi)
                });
                Ok((sum, wire_bounds))
            }
            Self::Sparse(sparse) => {
                let mut sum = 0_u64;
                let mut first_nz: Option<usize> = None;
                let mut last_nz = 0_usize;
                for i in 0..num_bins {
                    let w = next_weight()?;
                    if w != 0 {
                        sum = sum
                            .checked_add(w)
                            .ok_or_else(|| Error::deserial("total_weight overflows u64"))?;
                        if first_nz.is_none() {
                            first_nz = Some(i);
                        }
                        last_nz = i;
                        let idx = (i64::from(first_index) + (i as i64)) as i32;
                        sparse.add(idx, w);
                    }
                }
                let wire_bounds = first_nz.map(|first_pos| {
                    (
                        (i64::from(first_index) + (first_pos as i64)) as i32,
                        (i64::from(first_index) + (last_nz as i64)) as i32,
                    )
                });
                Ok((sum, wire_bounds))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct DenseStore {
    bins: Vec<u64>,
    count: u64,
    occupied: usize,
    offset: i32,
    min_index: i32,
    max_index: i32,
    mode: CollapseMode,
    is_collapsed: bool,
}

impl DenseStore {
    const fn new(mode: CollapseMode) -> Self {
        Self {
            bins: Vec::new(),
            count: 0,
            occupied: 0,
            offset: 0,
            min_index: 0,
            max_index: 0,
            mode,
            is_collapsed: false,
        }
    }

    #[inline]
    const fn min_index(&self) -> Option<i32> {
        if self.count == 0 {
            None
        } else {
            Some(self.min_index)
        }
    }

    #[inline]
    const fn max_index(&self) -> Option<i32> {
        if self.count == 0 {
            None
        } else {
            Some(self.max_index)
        }
    }

    #[inline]
    fn active_slice(&self) -> Option<(i32, &[u64])> {
        if self.count == 0 {
            return None;
        }
        let start = self.pos(self.min_index);
        let end = self.pos(self.max_index);
        Some((self.min_index, &self.bins[start..=end]))
    }

    #[inline]
    fn pos(&self, index: i32) -> usize {
        (i64::from(index) - i64::from(self.offset)) as usize
    }

    #[inline]
    fn record_at(&mut self, pos: usize, weight: u64) {
        if self.bins[pos] == 0 {
            self.occupied += 1;
        }
        self.bins[pos] += weight;
        self.count += weight;
    }

    #[inline]
    fn add(&mut self, index: i32, weight: u64) {
        if self.count > 0 && index >= self.min_index && index <= self.max_index {
            let pos = self.pos(index);
            self.record_at(pos, weight);
            return;
        }
        self.add_cold(index, weight);
    }

    #[cold]
    fn add_cold(&mut self, index: i32, weight: u64) {
        if self.count == 0 {
            let len = self.desired_len(1);
            self.grow_bins(len);
            let offset = centered_offset(index, index, self.bins.len());
            self.offset = offset;
            self.min_index = index;
            self.max_index = index;
            let pos = self.pos(index);
            self.record_at(pos, weight);
            return;
        }

        if index < self.min_index {
            if matches!(self.mode, CollapseMode::Lowest(_)) && self.is_collapsed {
                self.record_at(0, weight);
                return;
            }
            self.extend_range(index, self.max_index);
            if matches!(self.mode, CollapseMode::Lowest(_)) && self.is_collapsed {
                self.record_at(0, weight);
                return;
            }
        } else if index > self.max_index {
            if matches!(self.mode, CollapseMode::Highest(_)) && self.is_collapsed {
                let last = self.bins.len() - 1;
                self.record_at(last, weight);
                return;
            }
            self.extend_range(self.min_index, index);
            if matches!(self.mode, CollapseMode::Highest(_)) && self.is_collapsed {
                let last = self.bins.len() - 1;
                self.record_at(last, weight);
                return;
            }
        }

        let pos = self.pos(index);
        self.record_at(pos, weight);
    }

    fn init_for_wire_bounds(&mut self, wire_min: i32, wire_max: i32) {
        debug_assert_eq!(self.count, 0);
        debug_assert!(wire_min <= wire_max);
        let span = span_u64(wire_min, wire_max);
        let target_len = self.exact_len(span);
        self.grow_bins(target_len);
        let cap = self.bins.len();
        if span <= cap as u64 {
            self.offset = centered_offset(wire_min, wire_max, cap);
            self.min_index = wire_min;
            self.max_index = wire_max;
            self.is_collapsed = false;
            return;
        }
        match self.mode {
            CollapseMode::None => unreachable!("unbounded dense store always grows to span"),
            CollapseMode::Lowest(max_bins) => {
                debug_assert_eq!(cap, max_bins);
                let collapsed_min = (i64::from(wire_max) - (max_bins as i64 - 1)) as i32;
                self.offset = collapsed_min;
                self.min_index = collapsed_min;
                self.max_index = wire_max;
                self.is_collapsed = true;
            }
            CollapseMode::Highest(max_bins) => {
                debug_assert_eq!(cap, max_bins);
                let collapsed_max = (i64::from(wire_min) + (max_bins as i64 - 1)) as i32;
                self.offset = wire_min;
                self.min_index = wire_min;
                self.max_index = collapsed_max;
                self.is_collapsed = true;
            }
        }
    }

    #[inline]
    fn add_in_bounds(&mut self, index: i32, weight: u64) {
        let pos = if index <= self.min_index {
            debug_assert!(
                index == self.min_index
                    || (matches!(self.mode, CollapseMode::Lowest(_)) && self.is_collapsed)
            );
            self.pos(self.min_index)
        } else if index >= self.max_index {
            debug_assert!(
                index == self.max_index
                    || (matches!(self.mode, CollapseMode::Highest(_)) && self.is_collapsed)
            );
            self.pos(self.max_index)
        } else {
            self.pos(index)
        };
        self.record_at(pos, weight);
    }

    fn grow_bins(&mut self, target_len: usize) {
        if target_len > self.bins.len() {
            let additional = target_len - self.bins.len();
            self.bins.reserve_exact(additional);
            self.bins.resize(target_len, 0);
        }
    }

    fn exact_len(&self, span: u64) -> usize {
        let clamped = span.max(1);
        match self.mode {
            CollapseMode::None => {
                assert!(
                    clamped <= MAX_DENSE_LEN as u64,
                    "unbounded dense store span ({span}) exceeds maximum dense capacity ({MAX_DENSE_LEN})"
                );
                clamped as usize
            }
            CollapseMode::Lowest(max_bins) | CollapseMode::Highest(max_bins) => {
                clamped.min(max_bins as u64) as usize
            }
        }
    }

    fn desired_len(&self, span: u64) -> usize {
        // Match the 10% growth + 64-bin slack curve used by Datadog's DenseStore.
        let padded = span.saturating_add(INITIAL_DENSE_OVERHEAD as u64 - 1);
        let grown = padded.saturating_add(padded / 10).max(span).max(1);
        match self.mode {
            CollapseMode::None => {
                assert!(
                    span <= MAX_DENSE_LEN as u64,
                    "unbounded dense store span ({span}) exceeds maximum dense capacity ({MAX_DENSE_LEN})"
                );
                grown.min(MAX_DENSE_LEN as u64) as usize
            }
            CollapseMode::Lowest(max_bins) | CollapseMode::Highest(max_bins) => {
                grown.min(max_bins as u64) as usize
            }
        }
    }

    fn extend_range(&mut self, req_min: i32, req_max: i32) {
        debug_assert!(self.count > 0);
        let new_min = self.min_index.min(req_min);
        let new_max = self.max_index.max(req_max);

        let buf_end = i64::from(self.offset) + (self.bins.len() as i64) - 1;
        if i64::from(new_min) >= i64::from(self.offset) && i64::from(new_max) <= buf_end {
            self.min_index = new_min;
            self.max_index = new_max;
            return;
        }

        let span = span_u64(new_min, new_max);
        let target_len = self.desired_len(span);
        self.grow_bins(target_len);

        let cap = self.bins.len();
        if span <= cap as u64 {
            self.center_counts(new_min, new_max);
            return;
        }

        match self.mode {
            CollapseMode::None => unreachable!("unbounded dense store always grows to span"),
            CollapseMode::Lowest(max_bins) => {
                debug_assert_eq!(cap, max_bins);
                let collapsed_min = (i64::from(new_max) - (max_bins as i64 - 1)) as i32;
                let old_start = self.pos(self.min_index);
                let old_end = self.pos(self.max_index);

                if collapsed_min >= self.max_index {
                    self.bins[old_start..=old_end].fill(0);
                    self.bins[0] = self.count;
                    self.occupied = 1;
                    self.offset = collapsed_min;
                    self.min_index = collapsed_min;
                    self.max_index = new_max;
                    self.is_collapsed = true;
                    return;
                }

                let keep_min = if self.min_index < collapsed_min {
                    let collapse_end = self.pos(collapsed_min);
                    let mut collapsed_weight = 0_u64;
                    for slot in &mut self.bins[old_start..collapse_end] {
                        if *slot != 0 {
                            collapsed_weight += *slot;
                            *slot = 0;
                            self.occupied -= 1;
                        }
                    }
                    if collapsed_weight != 0 {
                        if self.bins[collapse_end] == 0 {
                            self.occupied += 1;
                        }
                        self.bins[collapse_end] += collapsed_weight;
                    }
                    collapsed_min
                } else {
                    self.min_index
                };

                self.relocate_active_slice(keep_min, self.max_index, collapsed_min);
                self.min_index = collapsed_min;
                self.max_index = new_max;
                self.is_collapsed = true;
            }
            CollapseMode::Highest(max_bins) => {
                debug_assert_eq!(cap, max_bins);
                let collapsed_max = (i64::from(new_min) + (max_bins as i64 - 1)) as i32;
                let old_start = self.pos(self.min_index);
                let old_end = self.pos(self.max_index);

                if collapsed_max <= self.min_index {
                    self.bins[old_start..=old_end].fill(0);
                    self.bins[max_bins - 1] = self.count;
                    self.occupied = 1;
                    self.offset = new_min;
                    self.min_index = new_min;
                    self.max_index = collapsed_max;
                    self.is_collapsed = true;
                    return;
                }

                let keep_max = if self.max_index > collapsed_max {
                    let collapse_pos = self.pos(collapsed_max);
                    let mut collapsed_weight = 0_u64;
                    for slot in &mut self.bins[(collapse_pos + 1)..=old_end] {
                        if *slot != 0 {
                            collapsed_weight += *slot;
                            *slot = 0;
                            self.occupied -= 1;
                        }
                    }
                    if collapsed_weight != 0 {
                        if self.bins[collapse_pos] == 0 {
                            self.occupied += 1;
                        }
                        self.bins[collapse_pos] += collapsed_weight;
                    }
                    collapsed_max
                } else {
                    self.max_index
                };

                self.relocate_active_slice(self.min_index, keep_max, new_min);
                self.min_index = new_min;
                self.max_index = collapsed_max;
                self.is_collapsed = true;
            }
        }
    }

    fn center_counts(&mut self, new_min: i32, new_max: i32) {
        let new_offset = centered_offset(new_min, new_max, self.bins.len());
        self.relocate_active_slice(self.min_index, self.max_index, new_offset);
        self.min_index = new_min;
        self.max_index = new_max;
    }

    fn relocate_active_slice(&mut self, active_min: i32, active_max: i32, new_offset: i32) {
        let src_start = self.pos(active_min);
        let len = span_u64(active_min, active_max) as usize;
        let dst_start = (i64::from(active_min) - i64::from(new_offset)) as usize;
        if src_start != dst_start {
            self.bins
                .copy_within(src_start..(src_start + len), dst_start);
            if dst_start > src_start {
                let clear_end = dst_start.min(src_start + len);
                self.bins[src_start..clear_end].fill(0);
            } else {
                let clear_start = (dst_start + len).max(src_start);
                self.bins[clear_start..(src_start + len)].fill(0);
            }
        }
        self.offset = new_offset;
    }

    fn merge_dense(&mut self, other: &Self) {
        debug_assert!(other.count > 0);
        if self.count == 0 {
            let src_start = other.pos(other.min_index);
            let src_end = other.pos(other.max_index);
            let src_slice = &other.bins[src_start..=src_end];
            let span = src_slice.len();
            let cap = self.exact_len(span as u64);
            self.grow_bins(cap);

            if span <= self.bins.len() {
                let offset = centered_offset(other.min_index, other.max_index, self.bins.len());
                let dst_start = (i64::from(other.min_index) - i64::from(offset)) as usize;
                self.bins[dst_start..(dst_start + span)].copy_from_slice(src_slice);
                self.offset = offset;
                self.min_index = other.min_index;
                self.max_index = other.max_index;
                self.count = other.count;
                self.occupied = other.occupied;
                self.is_collapsed = other.mode == self.mode && other.is_collapsed;
                return;
            }

            self.init_for_wire_bounds(other.min_index, other.max_index);
            for (i, &w) in src_slice.iter().enumerate() {
                if w != 0 {
                    let idx = (i64::from(other.min_index) + (i as i64)) as i32;
                    self.add_in_bounds(idx, w);
                }
            }
            return;
        }

        if other.min_index < self.min_index || other.max_index > self.max_index {
            let skip_extend = match self.mode {
                CollapseMode::Lowest(_) => self.is_collapsed && other.max_index <= self.max_index,
                CollapseMode::Highest(_) => self.is_collapsed && other.min_index >= self.min_index,
                CollapseMode::None => false,
            };
            if !skip_extend {
                self.extend_range(other.min_index, other.max_index);
            }
        }

        let Some((other_min, src_slice)) = other.active_slice() else {
            return;
        };

        // Fast contiguous overlap path when the entire source span lies within `[min_index,
        // max_index]`.
        if other_min >= self.min_index && other.max_index <= self.max_index {
            let dst_start = self.pos(other_min);
            let dst_slice = &mut self.bins[dst_start..(dst_start + src_slice.len())];
            let mut newly_occupied = 0_usize;
            for (dst, &w) in dst_slice.iter_mut().zip(src_slice) {
                if w != 0 {
                    if *dst == 0 {
                        newly_occupied += 1;
                    }
                    *dst += w;
                }
            }
            self.occupied += newly_occupied;
            self.count += other.count;
        } else {
            for (i, &w) in src_slice.iter().enumerate() {
                if w != 0 {
                    let idx = (i64::from(other_min) + (i as i64)) as i32;
                    self.add(idx, w);
                }
            }
        }
        if other.mode == self.mode && other.is_collapsed {
            self.is_collapsed = true;
        }
    }

    fn merge_sparse(&mut self, src: &SparseStore) {
        debug_assert!(src.count > 0);
        let src_min = src.min_index().expect("non-empty sparse store");
        let src_max = src.max_index().expect("non-empty sparse store");
        if self.count == 0 {
            self.init_for_wire_bounds(src_min, src_max);
            for (&idx, &w) in &src.bins {
                self.add_in_bounds(idx, w);
            }
            return;
        }

        if src_min < self.min_index || src_max > self.max_index {
            let skip_extend = match self.mode {
                CollapseMode::Lowest(_) => self.is_collapsed && src_max <= self.max_index,
                CollapseMode::Highest(_) => self.is_collapsed && src_min >= self.min_index,
                CollapseMode::None => false,
            };
            if !skip_extend {
                self.extend_range(src_min, src_max);
            }
        }
        for (&idx, &w) in &src.bins {
            self.add(idx, w);
        }
    }

    fn key_at_rank(&self, mut rank: u64) -> i32 {
        debug_assert!(self.count > 0 && rank < self.count);
        let start = self.pos(self.min_index);
        let end = self.pos(self.max_index);
        for (i, &w) in self.bins[start..=end].iter().enumerate() {
            if rank < w {
                return (i64::from(self.min_index) + (i as i64)) as i32;
            }
            rank -= w;
        }
        self.max_index
    }

    fn reset(&mut self) {
        if self.count > 0 {
            let start = self.pos(self.min_index);
            let end = self.pos(self.max_index);
            self.bins[start..=end].fill(0);
        }
        self.count = 0;
        self.occupied = 0;
        self.offset = 0;
        self.min_index = 0;
        self.max_index = 0;
        self.is_collapsed = false;
    }

    #[inline]
    fn iter_asc(&self) -> DenseStoreIter<'_> {
        match self.active_slice() {
            Some((min_index, slice)) => DenseStoreIter {
                slice,
                front_index: min_index,
            },
            None => DenseStoreIter {
                slice: &[],
                front_index: 0,
            },
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SparseStore {
    bins: BTreeMap<i32, u64>,
    count: u64,
}

impl SparseStore {
    #[inline]
    fn min_index(&self) -> Option<i32> {
        self.bins.first_key_value().map(|(&idx, _)| idx)
    }

    #[inline]
    fn max_index(&self) -> Option<i32> {
        self.bins.last_key_value().map(|(&idx, _)| idx)
    }

    #[inline]
    fn add(&mut self, index: i32, weight: u64) {
        *self.bins.entry(index).or_insert(0) += weight;
        self.count += weight;
    }

    fn key_at_rank(&self, mut rank: u64) -> i32 {
        debug_assert!(self.count > 0 && rank < self.count);
        for (&idx, &w) in &self.bins {
            if rank < w {
                return idx;
            }
            rank -= w;
        }
        *self
            .bins
            .last_key_value()
            .expect("non-empty sparse store")
            .0
    }

    fn reset(&mut self) {
        self.bins.clear();
        self.count = 0;
    }
}

#[derive(Debug, Clone)]
pub enum StoreIter<'a> {
    Dense(DenseStoreIter<'a>),
    Sparse(btree_map::Iter<'a, i32, u64>),
}

impl Iterator for StoreIter<'_> {
    type Item = (i32, u64);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dense(iter) => iter.next(),
            Self::Sparse(iter) => iter.next().map(|(&idx, &w)| (idx, w)),
        }
    }
}

impl DoubleEndedIterator for StoreIter<'_> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dense(iter) => iter.next_back(),
            Self::Sparse(iter) => iter.next_back().map(|(&idx, &w)| (idx, w)),
        }
    }
}

impl FusedIterator for StoreIter<'_> {}

#[derive(Debug, Clone)]
pub struct DenseStoreIter<'a> {
    slice: &'a [u64],
    front_index: i32,
}

impl Iterator for DenseStoreIter<'_> {
    type Item = (i32, u64);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while let Some((&first, rest)) = self.slice.split_first() {
            let idx = self.front_index;
            self.slice = rest;
            self.front_index = idx.wrapping_add(1);
            if first != 0 {
                return Some((idx, first));
            }
        }
        None
    }
}

impl DoubleEndedIterator for DenseStoreIter<'_> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        while let Some((&last, rest)) = self.slice.split_last() {
            let back_index = (i64::from(self.front_index) + (rest.len() as i64)) as i32;
            self.slice = rest;
            if last != 0 {
                return Some((back_index, last));
            }
        }
        None
    }
}

impl FusedIterator for DenseStoreIter<'_> {}

#[inline]
pub fn span_u64(min_index: i32, max_index: i32) -> u64 {
    debug_assert!(min_index <= max_index);
    ((i64::from(max_index) - i64::from(min_index)) + 1) as u64
}

#[inline]
fn centered_offset(min_index: i32, max_index: i32, len: usize) -> i32 {
    let span_u = span_u64(min_index, max_index);
    debug_assert!(span_u <= len as u64 && (len as u64) <= MAX_I32_SPAN);
    let slack = ((len as u64) - span_u) / 2;
    let ideal = i64::from(min_index) - (slack as i64);
    let max_offset = i64::from(i32::MAX) - ((len as i64) - 1);
    ideal.clamp(i64::from(i32::MIN), max_offset) as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_dense_invariants(store: &Store) {
        let Store::Dense(dense) = store else {
            return;
        };
        assert_eq!(dense.bins.capacity(), dense.bins.len());
        let nonzero: Vec<u64> = dense.bins.iter().copied().filter(|&w| w != 0).collect();
        assert_eq!(dense.occupied, nonzero.len());
        assert_eq!(dense.count, nonzero.iter().sum::<u64>());
        if dense.count > 0 {
            assert!(dense.min_index <= dense.max_index);
            assert!(dense.bins[dense.pos(dense.min_index)] > 0);
            assert!(dense.bins[dense.pos(dense.max_index)] > 0);
        } else {
            assert!(dense.bins.iter().all(|&w| w == 0));
        }
    }

    #[test]
    fn collapsing_lowest_and_highest_preserve_dense_invariants() {
        for kind in [
            StoreKind::CollapsingLowestDense { max_num_bins: 4 },
            StoreKind::CollapsingHighestDense { max_num_bins: 4 },
            StoreKind::UnboundedDense,
        ] {
            let mut store = Store::new(kind).unwrap();
            for idx in [10, 12, 0, -5, 100, -100, 50] {
                store.add(idx, 2);
                assert_dense_invariants(&store);
            }
            assert_eq!(store.total_weight(), 14);

            let forward: Vec<_> = store.iter_asc().collect();
            let mut backward: Vec<_> = store.iter_asc().rev().collect();
            backward.reverse();
            assert_eq!(forward, backward);
            assert_eq!(forward.len(), store.num_occupied());

            store.reset();
            assert_dense_invariants(&store);
            assert!(store.is_empty());
        }
    }

    #[test]
    fn extreme_i32_indices_do_not_overflow_offset_or_span() {
        assert_eq!(span_u64(i32::MIN, i32::MAX), 1_u64 << 32);
        let unbounded = DenseStore::new(CollapseMode::None);
        let capped_len = unbounded.desired_len(MAX_DENSE_LEN as u64);
        assert_eq!(capped_len, MAX_DENSE_LEN);
        #[cfg(target_pointer_width = "64")]
        {
            assert_eq!(
                unbounded.desired_len(4_000_000_001),
                MAX_DENSE_LEN,
                "10% growth slack must clamp to 2^32 index domain"
            );
            assert_eq!(
                centered_offset(-2_000_000_000, 2_000_000_000, MAX_DENSE_LEN),
                i32::MIN
            );
            assert_eq!(centered_offset(i32::MIN, i32::MAX, MAX_DENSE_LEN), i32::MIN);
        }
        for kind in [
            StoreKind::CollapsingLowestDense { max_num_bins: 8 },
            StoreKind::CollapsingHighestDense { max_num_bins: 8 },
        ] {
            let mut store = Store::new(kind).unwrap();
            store.add(i32::MIN, 1);
            assert_dense_invariants(&store);
            store.add(i32::MAX, 3);
            assert_dense_invariants(&store);
            assert_eq!(store.total_weight(), 4);

            // Reverse order (`i32::MAX` first, then `i32::MIN`) exercises the opposite collapse
            // branch over the full `2^32` index span.
            let mut reverse = Store::new(kind).unwrap();
            reverse.add(i32::MAX, 3);
            assert_dense_invariants(&reverse);
            reverse.add(i32::MIN, 1);
            assert_dense_invariants(&reverse);
            assert_eq!(reverse.total_weight(), 4);
        }
    }

    #[test]
    fn pre_windowed_bounds_and_cross_store_merge_match_incremental_updates() {
        let samples = [
            (-500, 3_u64),
            (-120, 1),
            (-4, 5),
            (0, 2),
            (7, 4),
            (19, 1),
            (250, 6),
        ];
        let bounds = Some((-500, 250));

        let mut wide_sparse = Store::new(StoreKind::Sparse).unwrap();
        let mut wide_unbounded = Store::new(StoreKind::UnboundedDense).unwrap();
        for &(idx, w) in &samples {
            wide_sparse.add(idx, w);
            wide_unbounded.add(idx, w);
        }

        for kind in [
            StoreKind::CollapsingLowestDense { max_num_bins: 8 },
            StoreKind::CollapsingHighestDense { max_num_bins: 8 },
            StoreKind::UnboundedDense,
            StoreKind::Sparse,
        ] {
            let mut incremental = Store::new(kind).unwrap();
            let mut pre_windowed = Store::with_wire_bounds(kind, bounds).unwrap();
            for &(idx, w) in &samples {
                incremental.add(idx, w);
                pre_windowed.add_in_bounds(idx, w);
            }
            assert_dense_invariants(&incremental);
            assert_dense_invariants(&pre_windowed);
            assert_eq!(pre_windowed.is_collapsed(), incremental.is_collapsed());
            assert_eq!(
                pre_windowed.iter_asc().collect::<Vec<_>>(),
                incremental.iter_asc().collect::<Vec<_>>()
            );

            for src in [&wide_sparse, &wide_unbounded] {
                let mut merged = Store::new(kind).unwrap();
                merged.merge_from(src);
                assert_dense_invariants(&merged);
                assert_eq!(merged.is_collapsed(), incremental.is_collapsed());
                assert_eq!(
                    merged.iter_asc().collect::<Vec<_>>(),
                    incremental.iter_asc().collect::<Vec<_>>()
                );
            }
        }
    }
}
