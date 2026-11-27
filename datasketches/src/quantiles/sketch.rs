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

use std::convert::identity;
use std::fmt;
use std::sync::OnceLock;

use crate::common::random::random_below;
use crate::common::random::random_bit;
use crate::common::sorted_view::SortedView;
use crate::error::Error;

const DEFAULT_K: u16 = 128;
const MIN_K: u16 = 2;
const MAX_K: u16 = 32_768;

fn merge_and_compact<T: Ord>(left: Vec<T>, right: Vec<T>) -> Vec<T> {
    let mut compacted = Vec::with_capacity(left.len());
    let mut left = left.into_iter().peekable();
    let mut right = right.into_iter().peekable();
    let merged = std::iter::from_fn(|| match (left.peek(), right.peek()) {
        (Some(l), Some(r)) => {
            if l <= r {
                left.next()
            } else {
                right.next()
            }
        }
        (Some(_), None) => left.next(),
        (None, Some(_)) => right.next(),
        (None, None) => None,
    });
    compacted.extend(merged.skip(usize::from(random_bit())).step_by(2));
    compacted
}

/// Classic Quantiles sketch for estimating quantiles and ranks.
///
/// A sketch built with [`update`](Self::update) starts sampling when the stream reaches `2 * k` items.
/// The stream minimum and maximum remain exact at any stream size.
/// The default is `k = 128`.
pub struct QuantilesSketch<T> {
    k: u16,
    total_weight: u64,
    // Level 0 is the unsorted buffer of unit-weight items. Each occupied level i > 0
    // holds k sorted items of weight 2^i.
    pub(super) levels: Vec<Vec<T>>,
    min_item: Option<T>,
    max_item: Option<T>,
    pub(super) sorted_view_cache: OnceLock<SortedView<T>>,
}

impl<T: Clone> Clone for QuantilesSketch<T> {
    fn clone(&self) -> Self {
        Self {
            k: self.k,
            total_weight: self.total_weight,
            levels: self.levels.clone(),
            min_item: self.min_item.clone(),
            max_item: self.max_item.clone(),
            sorted_view_cache: OnceLock::new(),
        }
    }
}

impl<T> QuantilesSketch<T> {
    fn make(k: u16) -> Self {
        Self {
            k,
            total_weight: 0,
            levels: vec![Vec::new()],
            min_item: None,
            max_item: None,
            sorted_view_cache: OnceLock::new(),
        }
    }

    /// Creates an empty sketch with accuracy parameter `k`.
    ///
    /// A larger `k` reduces rank error at the cost of more memory.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if `k` is outside `2..=32768` or is not a power of two.
    pub fn new(k: u16) -> Result<Self, Error> {
        if !(MIN_K..=MAX_K).contains(&k) || !k.is_power_of_two() {
            return Err(Error::invalid_argument(format!(
                "k must be a power of two in [{MIN_K}, {MAX_K}], got {k}"
            )));
        }
        Ok(Self::make(k))
    }

    /// Returns the current `k` parameter, which can decrease during a merge.
    pub fn k(&self) -> u16 {
        self.k
    }

    /// Returns the number of input items represented by the sketch, including merged streams.
    pub fn total_weight(&self) -> u64 {
        self.total_weight
    }

    /// Returns `true` if the sketch has observed no items.
    pub fn is_empty(&self) -> bool {
        self.total_weight == 0
    }

    /// Returns `true` if rank and quantile estimates are subject to sampling error.
    pub fn is_estimation_mode(&self) -> bool {
        self.levels.len() > 1
    }

    /// Returns the number of retained items.
    pub fn num_retained(&self) -> usize {
        self.levels.iter().map(Vec::len).sum()
    }

    /// Returns the smallest observed item, or `None` if the sketch is empty.
    pub fn min_item(&self) -> Option<&T> {
        self.min_item.as_ref()
    }

    /// Returns the largest observed item, or `None` if the sketch is empty.
    pub fn max_item(&self) -> Option<&T> {
        self.max_item.as_ref()
    }

    /// Estimates the bytes allocated for the level and cached sorted-view buffers,
    /// excluding allocations owned by individual items.
    pub fn estimated_size(&self) -> usize {
        self.levels.capacity() * size_of::<Vec<T>>()
            + self
                .levels
                .iter()
                .map(|level| level.capacity() * size_of::<T>())
                .sum::<usize>()
            + self
                .sorted_view_cache
                .get()
                .map_or(0, SortedView::estimated_size)
    }

    /// Returns an iterator over retained items and their weights in unspecified order.
    #[must_use = "iterators are lazy and do nothing unless consumed"]
    pub fn iter(&self) -> impl Iterator<Item = (&T, u64)> {
        self.levels
            .iter()
            .enumerate()
            .flat_map(|(level, items)| items.iter().map(move |item| (item, 1u64 << level)))
    }

    /// Returns the empirical normalized rank error for rank, quantile, and CDF queries.
    ///
    /// The formula estimates the 99th percentile of the maximum rank error for the current `k`.
    /// At the default `k = 128`, the error estimate is about `0.014`.
    /// A reported rank of `0.95` gives an estimated true rank between `0.936` and `0.964`.
    pub fn normalized_rank_error(&self) -> f64 {
        1.576 / f64::from(self.k).powf(0.9726)
    }

    /// Returns the empirical normalized rank error for a PMF bin at the current `k`.
    ///
    /// An interior bin's probability is the difference between the ranks of its two split points.
    /// At `k = 128`, the error estimate for the two boundaries is about 1.7 percentage points.
    pub fn normalized_pmf_error(&self) -> f64 {
        1.854 / f64::from(self.k).powf(0.9657)
    }
}

impl<T: fmt::Debug> fmt::Debug for QuantilesSketch<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuantilesSketch")
            .field("k", &self.k())
            .field("total_weight", &self.total_weight())
            .field("is_empty", &self.is_empty())
            .field("is_estimation_mode", &self.is_estimation_mode())
            .field("num_retained", &self.num_retained())
            .field("min_item", &self.min_item())
            .field("max_item", &self.max_item())
            .finish()
    }
}

impl<T> Default for QuantilesSketch<T> {
    fn default() -> Self {
        Self::make(DEFAULT_K)
    }
}

impl<T: Clone + Ord> QuantilesSketch<T> {
    fn update_extrema(&mut self, min: &T, max: &T) {
        if self.min_item.as_ref().is_none_or(|current| min < current) {
            self.min_item = Some(min.clone());
        }
        if self.max_item.as_ref().is_none_or(|current| max > current) {
            self.max_item = Some(max.clone());
        }
    }

    fn propagate_level(&mut self, mut level: usize, mut items: Vec<T>) {
        if self.levels.len() <= level {
            self.levels.resize_with(level + 1, Vec::new);
        }

        // Two runs of k items at the same level compact to k items of twice the weight.
        while let Some(current) = self.levels.get_mut(level) {
            if current.is_empty() {
                *current = items;
                return;
            }
            items = merge_and_compact(std::mem::take(current), items);
            level += 1;
        }
        self.levels.push(items);
    }

    fn insert_base(&mut self, item: T) {
        let base = &mut self.levels[0];
        base.push(item);
        if base.len() == 2 * usize::from(self.k) {
            base.sort_unstable();
            let offset = usize::from(random_bit());
            let compacted = base.drain(..).skip(offset).step_by(2).collect();
            self.propagate_level(1, compacted);
        }
    }

    fn merge_levels<Item>(
        &mut self,
        source_k: u16,
        mut levels: impl Iterator<Item = impl ExactSizeIterator<Item = Item>>,
        into_item: impl Fn(Item) -> T,
    ) {
        for item in levels.next().expect("missing base buffer") {
            self.insert_base(into_item(item));
        }

        // Only an exact source can still have a smaller k here. Its base buffer held every item.
        if source_k < self.k {
            return;
        }

        // The groups of stride items in a sorted level share one random sampling offset.
        // Moving each sample up log2(stride) levels multiplies its weight by stride,
        // preserving the source level's total weight.
        let stride = usize::from(source_k / self.k);
        let level_shift = stride.trailing_zeros() as usize;
        for (level, items) in levels.enumerate() {
            if items.len() == 0 {
                continue;
            }
            let offset = random_below(stride);
            let sampled = items.skip(offset).step_by(stride).map(&into_item).collect();
            self.propagate_level(level + 1 + level_shift, sampled);
        }
    }

    /// Updates the sketch with a new item.
    ///
    /// # Panics
    ///
    /// Panics without modifying the sketch if the stream weight would exceed [`u64::MAX`].
    pub fn update(&mut self, item: T) {
        let total_weight = self
            .total_weight
            .checked_add(1)
            .expect("total stream weight overflow");

        self.sorted_view_cache.take();
        self.update_extrema(&item, &item);
        self.insert_base(item);
        self.total_weight = total_weight;
    }

    /// Merges another sketch into this one.
    ///
    /// If `other` is exact, this sketch keeps its `k`.
    /// If `other` is in estimation mode, the merged sketch uses the smaller of the two `k` values.
    ///
    /// # Panics
    ///
    /// Panics without modifying the sketch if the combined stream weight would exceed [`u64::MAX`].
    ///
    /// # Cross-language compatibility
    ///
    /// An exact sketch keeps its `k` when merging an estimating sketch with a larger `k`.
    /// DataSketches C++ adopts the larger value.
    pub fn merge(&mut self, other: &Self) {
        let Some((min, max)) = other.min_item().zip(other.max_item()) else {
            return;
        };
        let total_weight = self
            .total_weight
            .checked_add(other.total_weight)
            .expect("total stream weight overflow");

        self.sorted_view_cache.take();
        self.update_extrema(min, max);
        if other.is_estimation_mode() && other.k < self.k {
            let source_k = std::mem::replace(&mut self.k, other.k);
            let source_levels = std::mem::replace(&mut self.levels, vec![Vec::new()]);
            self.merge_levels(
                source_k,
                source_levels.into_iter().map(Vec::into_iter),
                identity,
            );
        }
        self.merge_levels(
            other.k,
            other.levels.iter().map(|items| items.iter()),
            Clone::clone,
        );
        self.total_weight = total_weight;
    }
}
