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

/// Bin storage strategy used by the positive and negative stores of a
/// [`DdSketch`](super::DdSketch).
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
    /// magnitudes.
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
    pub fn add(&mut self, index: i32, weight: u64) {
        if weight == 0 {
            return;
        }
        match self {
            Self::Dense(dense) => dense.add(index, weight),
            Self::Sparse(sparse) => sparse.add(index, weight),
        }
    }

    pub fn merge_from(&mut self, other: &Self) {
        if other.is_empty() {
            return;
        }
        match (self, other) {
            (Self::Dense(dst), Self::Dense(src)) => dst.merge_dense(src),
            (dst, src) => {
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
            Self::Dense(dense) => dense.bins.capacity() * size_of::<u64>(),
            Self::Sparse(sparse) => sparse.bins.len() * (size_of::<i32>() + size_of::<u64>() + 16),
        }
    }

    #[inline]
    pub fn iter_asc(&self) -> StoreIter<'_> {
        match self {
            Self::Dense(dense) => StoreIter::Dense(dense.iter_asc()),
            Self::Sparse(sparse) => StoreIter::Sparse(sparse.bins.iter()),
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
            if self.bins.len() < len {
                self.bins.resize(len, 0);
            }
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

    fn desired_len(&self, span: usize) -> usize {
        // Match the 10% growth + 64-bin slack curve used by Datadog's DenseStore.
        let padded = span.saturating_add(INITIAL_DENSE_OVERHEAD - 1);
        let grown = padded.saturating_add(padded / 10).max(span).max(1);
        match self.mode {
            CollapseMode::None => grown,
            CollapseMode::Lowest(max_bins) | CollapseMode::Highest(max_bins) => grown.min(max_bins),
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

        let span = span_len(new_min, new_max);
        let target_len = self.desired_len(span);
        if target_len > self.bins.len() {
            self.bins.resize(target_len, 0);
        }

        let cap = self.bins.len();
        if span <= cap {
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
        let len = span_len(active_min, active_max);
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
            let cap = self.desired_len(span);
            if self.bins.len() < cap {
                self.bins.resize(cap, 0);
            }

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
        }

        if self.count > 0 && (other.min_index < self.min_index || other.max_index > self.max_index)
        {
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
            return;
        }

        for (i, &w) in src_slice.iter().enumerate() {
            if w != 0 {
                let idx = (i64::from(other_min) + (i as i64)) as i32;
                self.add(idx, w);
            }
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
pub fn span_len(min_index: i32, max_index: i32) -> usize {
    debug_assert!(min_index <= max_index);
    ((i64::from(max_index) - i64::from(min_index)) + 1) as usize
}

#[inline]
fn centered_offset(min_index: i32, max_index: i32, len: usize) -> i32 {
    let span = span_len(min_index, max_index);
    debug_assert!(span <= len);
    let slack = (len - span) / 2;
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
        }
    }
}
