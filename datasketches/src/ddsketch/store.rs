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
use super::weight::DdSketchWeight;
use crate::error::Error;

const INITIAL_DENSE_OVERHEAD: usize = 64;
const MAX_I32_SPAN: u64 = (i32::MAX as i64 - i32::MIN as i64 + 1) as u64;
const MAX_VEC_WORD_LEN: u64 = ((isize::MAX as usize) / size_of::<u64>()) as u64;

const _: () = assert!(size_of::<f64>() == size_of::<u64>());

/// Maximum number of 8-byte weight slots a `DenseStore` can hold: bounded both by addressable
/// `Vec<W>` capacity (`isize::MAX / 8` on 32-bit targets) and by the 32-bit bucket index domain
/// (`2^32` on 64-bit targets).
pub const MAX_DENSE_LEN: usize = if MAX_VEC_WORD_LEN < MAX_I32_SPAN {
    MAX_VEC_WORD_LEN as usize
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
pub enum Store<W> {
    Dense(DenseStore<W>),
    Sparse(SparseStore<W>),
}

impl<W: DdSketchWeight> Store<W> {
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
            StoreKind::Sparse => Self::Sparse(SparseStore::new()),
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
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Dense(dense) => dense.occupied == 0,
            Self::Sparse(sparse) => sparse.bins.is_empty(),
        }
    }

    #[inline]
    pub const fn total_weight(&self) -> W {
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

    pub fn validate_merge_capacity(&self, other: &Self) -> Result<(), Error> {
        if other.is_empty() {
            return Ok(());
        }
        if let Self::Dense(dst) = self {
            if matches!(dst.mode, CollapseMode::None) {
                let other_min = other.min_index().expect("non-empty store has min_index");
                let other_max = other.max_index().expect("non-empty store has max_index");
                let (new_min, new_max) = if dst.occupied == 0 {
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
    pub fn add(&mut self, index: i32, weight: W) {
        if weight.is_zero() {
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
    pub fn add_in_bounds(&mut self, index: i32, weight: W) {
        if weight.is_zero() {
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
                let next_count = dst.count.add_unchecked(src.total_weight());
                for (index, weight) in src.iter_asc() {
                    let entry = dst.bins.entry(index).or_insert(W::ZERO);
                    *entry = entry.add_capped(weight, next_count);
                }
                dst.count = next_count;
            }
        }
    }

    pub fn key_at_rank_asc(&self, rank: W) -> i32 {
        match self {
            Self::Dense(dense) => dense.key_at_rank_asc(rank),
            Self::Sparse(sparse) => sparse.key_at_rank_asc(rank),
        }
    }

    pub fn key_at_rank_desc(&self, rank: W) -> i32 {
        match self {
            Self::Dense(dense) => dense.key_at_rank_desc(rank),
            Self::Sparse(sparse) => sparse.key_at_rank_desc(rank),
        }
    }

    /// Resolves non-decreasing 0-based `ranks` in ascending index order in a single scan, invoking
    /// `emit(pos, index)` for each `(rank, pos)` entry.
    pub fn scan_ranks_asc(
        &self,
        ranks: impl IntoIterator<Item = (W, usize)>,
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
                    while offset + 1 < slice.len() && target >= cumul_end {
                        offset += 1;
                        cumul_end = cumul_end.add_unchecked(slice[offset]);
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
                        let Some((&idx, &w)) = iter.next() else {
                            break;
                        };
                        cur_idx = idx;
                        cumul_end = cumul_end.add_unchecked(w);
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
        ranks: impl IntoIterator<Item = (W, usize)>,
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
                    while offset > 0 && target >= cumul_end {
                        offset -= 1;
                        cumul_end = cumul_end.add_unchecked(slice[offset]);
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
                        let Some((&idx, &w)) = iter.next() else {
                            break;
                        };
                        cur_idx = idx;
                        cumul_end = cumul_end.add_unchecked(w);
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
            Self::Dense(dense) => dense.bins.capacity().saturating_mul(size_of::<W>()),
            Self::Sparse(sparse) => sparse
                .bins
                .len()
                .saturating_mul(size_of::<i32>() + size_of::<W>() + 16),
        }
    }

    #[inline]
    pub fn iter_asc(&self) -> StoreIter<'_, W> {
        match self {
            Self::Dense(dense) => StoreIter::Dense(dense.iter_asc()),
            Self::Sparse(sparse) => StoreIter::Sparse(sparse.bins.iter()),
        }
    }

    #[inline]
    pub fn dense_contiguous_slice(&self) -> Option<(i32, &[W])> {
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
                        || (dense.occupied > 0
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
    ) -> Result<(W, Option<(i32, i32)>), Error>
    where
        F: FnMut() -> Result<W, Error>,
    {
        debug_assert!(self.is_empty());
        if num_bins == 0 {
            return Ok((W::ZERO, None));
        }
        let last_index = (i64::from(first_index) + (num_bins as i64) - 1) as i32;
        match self {
            Self::Dense(dense) => {
                let cap = dense.exact_len(num_bins as u64);
                debug_assert!(dense.occupied == 0 && num_bins <= cap);
                dense.grow_bins(cap);
                let offset = centered_offset(first_index, last_index, dense.bins.len());
                let dst_start = (i64::from(first_index) - i64::from(offset)) as usize;
                let dst = &mut dense.bins[dst_start..(dst_start + num_bins)];
                let mut sum = W::ZERO;
                let mut occupied = 0_usize;
                let mut first_nz: Option<usize> = None;
                let mut last_nz = 0_usize;
                for (i, slot) in dst.iter_mut().enumerate() {
                    let w = next_weight()?;
                    if !w.is_zero() {
                        sum = sum.checked_add(w).ok_or_else(|| {
                            Error::deserial(format!("total_weight overflows {}", W::TYPE_NAME))
                        })?;
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
                let mut sum = W::ZERO;
                let mut first_nz: Option<usize> = None;
                let mut last_nz = 0_usize;
                for i in 0..num_bins {
                    let w = next_weight()?;
                    if !w.is_zero() {
                        sum = sum.checked_add(w).ok_or_else(|| {
                            Error::deserial(format!("total_weight overflows {}", W::TYPE_NAME))
                        })?;
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
pub struct DenseStore<W> {
    bins: Vec<W>,
    count: W,
    occupied: usize,
    offset: i32,
    min_index: i32,
    max_index: i32,
    mode: CollapseMode,
    is_collapsed: bool,
}

impl<W: DdSketchWeight> DenseStore<W> {
    const fn new(mode: CollapseMode) -> Self {
        Self {
            bins: Vec::new(),
            count: W::ZERO,
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
        if self.occupied == 0 {
            None
        } else {
            Some(self.min_index)
        }
    }

    #[inline]
    const fn max_index(&self) -> Option<i32> {
        if self.occupied == 0 {
            None
        } else {
            Some(self.max_index)
        }
    }

    #[inline]
    fn active_slice(&self) -> Option<(i32, &[W])> {
        if self.occupied == 0 {
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
    fn record_at(&mut self, pos: usize, weight: W) {
        if self.bins[pos].is_zero() {
            self.occupied += 1;
        }
        self.bins[pos] = self.bins[pos].add_unchecked(weight);
        self.count = self.count.add_unchecked(weight);
    }

    #[inline]
    fn add(&mut self, index: i32, weight: W) {
        if self.occupied > 0 && index >= self.min_index && index <= self.max_index {
            let pos = self.pos(index);
            self.record_at(pos, weight);
            return;
        }
        self.add_cold(index, weight);
    }

    #[cold]
    fn add_cold(&mut self, index: i32, weight: W) {
        if self.occupied == 0 {
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
        debug_assert_eq!(self.occupied, 0);
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
    fn in_bounds_pos(&self, index: i32) -> usize {
        if index <= self.min_index {
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
        }
    }

    #[inline]
    fn add_in_bounds(&mut self, index: i32, weight: W) {
        let pos = self.in_bounds_pos(index);
        self.record_at(pos, weight);
    }

    #[inline]
    fn add_bin_in_bounds_capped(&mut self, index: i32, weight: W, cap: W) {
        let pos = self.in_bounds_pos(index);
        if self.bins[pos].is_zero() {
            self.occupied += 1;
        }
        self.bins[pos] = self.bins[pos].add_capped(weight, cap);
    }

    fn grow_bins(&mut self, target_len: usize) {
        if target_len > self.bins.len() {
            let additional = target_len - self.bins.len();
            self.bins.reserve_exact(additional);
            self.bins.resize(target_len, W::ZERO);
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
        debug_assert!(self.occupied > 0);
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
                    self.bins[old_start..=old_end].fill(W::ZERO);
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
                    let mut collapsed_weight = W::ZERO;
                    for slot in &mut self.bins[old_start..collapse_end] {
                        if !slot.is_zero() {
                            collapsed_weight = collapsed_weight.add_capped(*slot, self.count);
                            *slot = W::ZERO;
                            self.occupied -= 1;
                        }
                    }
                    if !collapsed_weight.is_zero() {
                        if self.bins[collapse_end].is_zero() {
                            self.occupied += 1;
                        }
                        self.bins[collapse_end] =
                            self.bins[collapse_end].add_capped(collapsed_weight, self.count);
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
                    self.bins[old_start..=old_end].fill(W::ZERO);
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
                    let mut collapsed_weight = W::ZERO;
                    for slot in &mut self.bins[(collapse_pos + 1)..=old_end] {
                        if !slot.is_zero() {
                            collapsed_weight = collapsed_weight.add_capped(*slot, self.count);
                            *slot = W::ZERO;
                            self.occupied -= 1;
                        }
                    }
                    if !collapsed_weight.is_zero() {
                        if self.bins[collapse_pos].is_zero() {
                            self.occupied += 1;
                        }
                        self.bins[collapse_pos] =
                            self.bins[collapse_pos].add_capped(collapsed_weight, self.count);
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
                self.bins[src_start..clear_end].fill(W::ZERO);
            } else {
                let clear_start = (dst_start + len).max(src_start);
                self.bins[clear_start..(src_start + len)].fill(W::ZERO);
            }
        }
        self.offset = new_offset;
    }

    fn merge_dense(&mut self, other: &Self) {
        debug_assert!(other.occupied > 0);
        if self.occupied == 0 {
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
                if !w.is_zero() {
                    let idx = (i64::from(other.min_index) + (i as i64)) as i32;
                    self.add_bin_in_bounds_capped(idx, w, other.count);
                }
            }
            self.count = other.count;
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

        let next_count = self.count.add_unchecked(other.count);
        // Fast contiguous overlap path when the entire source span lies within `[min_index,
        // max_index]`.
        if other_min >= self.min_index && other.max_index <= self.max_index {
            let dst_start = self.pos(other_min);
            let dst_slice = &mut self.bins[dst_start..(dst_start + src_slice.len())];
            let mut newly_occupied = 0_usize;
            for (dst, &w) in dst_slice.iter_mut().zip(src_slice) {
                if !w.is_zero() {
                    if dst.is_zero() {
                        newly_occupied += 1;
                    }
                    *dst = dst.add_capped(w, next_count);
                }
            }
            self.occupied += newly_occupied;
        } else {
            for (i, &w) in src_slice.iter().enumerate() {
                if !w.is_zero() {
                    let idx = (i64::from(other_min) + (i as i64)) as i32;
                    self.add_bin_in_bounds_capped(idx, w, next_count);
                }
            }
        }
        self.count = next_count;
        if other.mode == self.mode && other.is_collapsed {
            self.is_collapsed = true;
        }
    }

    fn merge_sparse(&mut self, src: &SparseStore<W>) {
        debug_assert!(!src.bins.is_empty());
        let src_min = src.min_index().expect("non-empty sparse store");
        let src_max = src.max_index().expect("non-empty sparse store");
        if self.occupied == 0 {
            self.init_for_wire_bounds(src_min, src_max);
            for (&idx, &w) in &src.bins {
                self.add_bin_in_bounds_capped(idx, w, src.count);
            }
            self.count = src.count;
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
        let next_count = self.count.add_unchecked(src.count);
        for (&idx, &w) in &src.bins {
            self.add_bin_in_bounds_capped(idx, w, next_count);
        }
        self.count = next_count;
    }

    fn key_at_rank_asc(&self, rank: W) -> i32 {
        debug_assert!(self.occupied > 0);
        let start = self.pos(self.min_index);
        let end = self.pos(self.max_index);
        let mut cumul = W::ZERO;
        for (i, &w) in self.bins[start..=end].iter().enumerate() {
            cumul = cumul.add_unchecked(w);
            if rank < cumul {
                return (i64::from(self.min_index) + (i as i64)) as i32;
            }
        }
        self.max_index
    }

    fn key_at_rank_desc(&self, rank: W) -> i32 {
        debug_assert!(self.occupied > 0);
        let start = self.pos(self.min_index);
        let end = self.pos(self.max_index);
        let mut cumul = W::ZERO;
        for (i, &w) in self.bins[start..=end].iter().enumerate().rev() {
            cumul = cumul.add_unchecked(w);
            if rank < cumul {
                return (i64::from(self.min_index) + (i as i64)) as i32;
            }
        }
        self.min_index
    }

    fn reset(&mut self) {
        if self.occupied > 0 {
            let start = self.pos(self.min_index);
            let end = self.pos(self.max_index);
            self.bins[start..=end].fill(W::ZERO);
        }
        self.count = W::ZERO;
        self.occupied = 0;
        self.offset = 0;
        self.min_index = 0;
        self.max_index = 0;
        self.is_collapsed = false;
    }

    #[inline]
    fn iter_asc(&self) -> DenseStoreIter<'_, W> {
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

#[derive(Debug, Clone)]
pub struct SparseStore<W> {
    bins: BTreeMap<i32, W>,
    count: W,
}

impl<W: DdSketchWeight> SparseStore<W> {
    const fn new() -> Self {
        Self {
            bins: BTreeMap::new(),
            count: W::ZERO,
        }
    }

    #[inline]
    fn min_index(&self) -> Option<i32> {
        self.bins.first_key_value().map(|(&idx, _)| idx)
    }

    #[inline]
    fn max_index(&self) -> Option<i32> {
        self.bins.last_key_value().map(|(&idx, _)| idx)
    }

    #[inline]
    fn add(&mut self, index: i32, weight: W) {
        let entry = self.bins.entry(index).or_insert(W::ZERO);
        *entry = entry.add_unchecked(weight);
        self.count = self.count.add_unchecked(weight);
    }

    fn key_at_rank_asc(&self, rank: W) -> i32 {
        debug_assert!(!self.bins.is_empty());
        let mut cumul = W::ZERO;
        for (&idx, &w) in &self.bins {
            cumul = cumul.add_unchecked(w);
            if rank < cumul {
                return idx;
            }
        }
        *self
            .bins
            .last_key_value()
            .expect("non-empty sparse store")
            .0
    }

    fn key_at_rank_desc(&self, rank: W) -> i32 {
        debug_assert!(!self.bins.is_empty());
        let mut cumul = W::ZERO;
        for (&idx, &w) in self.bins.iter().rev() {
            cumul = cumul.add_unchecked(w);
            if rank < cumul {
                return idx;
            }
        }
        *self
            .bins
            .first_key_value()
            .expect("non-empty sparse store")
            .0
    }

    fn reset(&mut self) {
        self.bins.clear();
        self.count = W::ZERO;
    }
}

#[derive(Debug, Clone)]
pub enum StoreIter<'a, W> {
    Dense(DenseStoreIter<'a, W>),
    Sparse(btree_map::Iter<'a, i32, W>),
}

impl<W: DdSketchWeight> Iterator for StoreIter<'_, W> {
    type Item = (i32, W);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dense(iter) => iter.next(),
            Self::Sparse(iter) => iter.next().map(|(&idx, &w)| (idx, w)),
        }
    }
}

impl<W: DdSketchWeight> DoubleEndedIterator for StoreIter<'_, W> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dense(iter) => iter.next_back(),
            Self::Sparse(iter) => iter.next_back().map(|(&idx, &w)| (idx, w)),
        }
    }
}

impl<W: DdSketchWeight> FusedIterator for StoreIter<'_, W> {}

#[derive(Debug, Clone)]
pub struct DenseStoreIter<'a, W> {
    slice: &'a [W],
    front_index: i32,
}

impl<W: DdSketchWeight> Iterator for DenseStoreIter<'_, W> {
    type Item = (i32, W);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while let Some((&first, rest)) = self.slice.split_first() {
            let idx = self.front_index;
            self.slice = rest;
            self.front_index = idx.wrapping_add(1);
            if !first.is_zero() {
                return Some((idx, first));
            }
        }
        None
    }
}

impl<W: DdSketchWeight> DoubleEndedIterator for DenseStoreIter<'_, W> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        while let Some((&last, rest)) = self.slice.split_last() {
            let back_index = (i64::from(self.front_index) + (rest.len() as i64)) as i32;
            self.slice = rest;
            if !last.is_zero() {
                return Some((back_index, last));
            }
        }
        None
    }
}

impl<W: DdSketchWeight> FusedIterator for DenseStoreIter<'_, W> {}

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

    #[test]
    fn extreme_i32_indices_do_not_overflow_offset_or_span() {
        assert_eq!(span_u64(i32::MIN, i32::MAX), 1_u64 << 32);
        let unbounded = DenseStore::<u64>::new(CollapseMode::None);
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
            let mut store = Store::<u64>::new(kind).unwrap();
            store.add(i32::MIN, 1);
            store.add(i32::MAX, 3);
            assert_eq!(store.total_weight(), 4);

            // Reverse order (`i32::MAX` first, then `i32::MIN`) exercises the opposite collapse
            // branch over the full `2^32` index span.
            let mut reverse = Store::<u64>::new(kind).unwrap();
            reverse.add(i32::MAX, 3);
            reverse.add(i32::MIN, 1);
            assert_eq!(reverse.total_weight(), 4);
        }
    }
}
