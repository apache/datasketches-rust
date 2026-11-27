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

use crate::common::SearchCriteria;
use crate::error::Error;

#[derive(Debug, Clone)]
struct Entry<T> {
    item: T,
    // Starts as this item's weight. The finished view stores the sum through this entry.
    cumulative_weight: u64,
}

impl<T: Clone + Ord> Entry<T> {
    // A missing endpoint takes one unit of weight from its nearest retained neighbor.
    fn split_off_endpoint(&mut self, endpoint: &T) -> Option<Self> {
        if self.item == *endpoint {
            return None;
        }
        if self.cumulative_weight == 1 {
            self.item = endpoint.clone();
            return None;
        }
        self.cumulative_weight -= 1;
        Some(Self {
            item: endpoint.clone(),
            cumulative_weight: 1,
        })
    }
}

fn merge_sorted_entries<T: Ord>(left: Vec<Entry<T>>, right: Vec<Entry<T>>) -> Vec<Entry<T>> {
    let mut merged = Vec::with_capacity(left.len() + right.len());
    let mut left = left.into_iter().peekable();
    let mut right = right.into_iter().peekable();

    while let (Some(left_entry), Some(right_entry)) = (left.peek(), right.peek()) {
        if left_entry.item > right_entry.item {
            merged.push(right.next().unwrap());
        } else {
            merged.push(left.next().unwrap());
        }
    }
    merged.extend(left);
    merged.extend(right);
    merged
}

fn merge_sorted_runs<T: Ord>(runs: &mut [Vec<Entry<T>>]) -> Vec<Entry<T>> {
    match runs {
        [] => Vec::new(),
        [run] => std::mem::take(run),
        _ => {
            // A power-of-two split preserves the pairing of adjacent runs in each merge round.
            let mid = runs.len().next_power_of_two() / 2;
            let (left, right) = runs.split_at_mut(mid);
            merge_sorted_entries(merge_sorted_runs(left), merge_sorted_runs(right))
        }
    }
}

pub fn validate_rank(rank: f64) -> Result<(), Error> {
    if !(0.0..=1.0).contains(&rank) {
        return Err(Error::invalid_argument(format!(
            "rank must be in [0.0, 1.0], got {rank}"
        )));
    }
    Ok(())
}

pub fn validate_split_points<T: Ord>(split_points: &[T]) -> Result<(), Error> {
    for (index, pair) in split_points.windows(2).enumerate() {
        if pair[0] >= pair[1] {
            return Err(Error::invalid_argument(format!(
                "split points at indices {index} and {} must be strictly increasing",
                index + 1
            )));
        }
    }
    Ok(())
}

#[inline]
pub fn pmf_from_cdf(mut buckets: Vec<f64>) -> Vec<f64> {
    for index in (1..buckets.len()).rev() {
        buckets[index] -= buckets[index - 1];
    }
    buckets
}

#[derive(Clone)]
pub struct SortedEntries<T> {
    vec: Vec<Entry<T>>,
    total_weight: u64,
}

impl<T: Clone + Ord> SortedEntries<T> {
    pub fn new(levels: &[Vec<T>], is_level_zero_sorted: bool, extrema: Option<(&T, &T)>) -> Self {
        let mut runs = Vec::with_capacity(levels.len());
        for (level_index, level) in levels.iter().enumerate() {
            let weight = 1u64 << level_index;
            let mut run: Vec<_> = level
                .iter()
                .cloned()
                .map(|item| Entry {
                    item,
                    cumulative_weight: weight,
                })
                .collect();
            if level_index == 0 && !is_level_zero_sorted {
                run.sort_unstable_by(|left, right| left.item.cmp(&right.item));
            }
            if !run.is_empty() {
                runs.push(run);
            }
        }

        let mut entries = merge_sorted_runs(&mut runs);
        if let Some((min_item, max_item)) = extrema {
            let first = entries
                .first_mut()
                .expect("a sketch with extrema has retained items");
            let min = first.split_off_endpoint(min_item);
            let last = entries.last_mut().unwrap();
            let max = last.split_off_endpoint(max_item);
            if min.is_some() || max.is_some() {
                let mut adjusted = Vec::with_capacity(
                    entries.len() + usize::from(min.is_some()) + usize::from(max.is_some()),
                );
                adjusted.extend(min);
                adjusted.append(&mut entries);
                adjusted.extend(max);
                entries = adjusted;
            }
        }
        let mut total_weight = 0;
        for entry in &mut entries {
            total_weight += entry.cumulative_weight;
            entry.cumulative_weight = total_weight;
        }
        Self {
            vec: entries,
            total_weight,
        }
    }

    fn rank(&self, item: &T, criteria: SearchCriteria) -> Option<f64> {
        if self.vec.is_empty() {
            return None;
        }
        let index = if criteria == SearchCriteria::Inclusive {
            self.vec.partition_point(|entry| entry.item <= *item)
        } else {
            self.vec.partition_point(|entry| entry.item < *item)
        };

        let rank_weight = self.vec[..index]
            .last()
            .map_or(0, |entry| entry.cumulative_weight);
        Some(rank_weight as f64 / self.total_weight as f64)
    }

    pub fn interior_quantile(&self, rank: f64, criteria: SearchCriteria) -> T {
        let target = rank * self.total_weight as f64;
        let index = match criteria {
            SearchCriteria::Inclusive => {
                let weight = target.ceil() as u64;
                self.vec
                    .partition_point(|entry| entry.cumulative_weight < weight)
            }
            SearchCriteria::Exclusive => {
                let weight = target as u64;
                self.vec
                    .partition_point(|entry| entry.cumulative_weight <= weight)
            }
        };
        self.vec[index.min(self.vec.len() - 1)].item.clone()
    }

    fn quantile_at_valid_rank(
        &self,
        rank: f64,
        criteria: SearchCriteria,
        (min_item, max_item): (&T, &T),
    ) -> T {
        if rank == 0.0 {
            return min_item.clone();
        }
        if rank == 1.0 {
            return max_item.clone();
        }
        self.interior_quantile(rank, criteria)
    }

    pub fn quantiles_at_valid_ranks(
        &self,
        ranks: &[f64],
        criteria: SearchCriteria,
        extrema: (&T, &T),
    ) -> Vec<T> {
        ranks
            .iter()
            .map(|&rank| self.quantile_at_valid_rank(rank, criteria, extrema))
            .collect()
    }

    pub fn cdf_at_valid_split_points(
        &self,
        split_points: &[T],
        criteria: SearchCriteria,
    ) -> Vec<f64> {
        let mut ranks = Vec::with_capacity(split_points.len() + 1);
        for item in split_points {
            ranks.push(self.rank(item, criteria).expect("checked non-empty view"));
        }
        ranks.push(1.0);
        ranks
    }
}

/// An owned snapshot of a sketch's weighted items in sorted order.
/// Later updates to the sketch leave the snapshot unchanged.
#[derive(Clone)]
pub struct SortedView<T> {
    entries: SortedEntries<T>,
    // Ranks 0 and 1 return these original values even if a retained item compares equal.
    // For example, +0.0 and -0.0 compare equal but have different bits.
    extrema: Option<(T, T)>,
}

impl<T: fmt::Debug> fmt::Debug for SortedView<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (min_item, max_item) = self.extrema.as_ref().map(|(min, max)| (min, max)).unzip();
        f.debug_struct("SortedView")
            .field("len", &self.entries.vec.len())
            .field("total_weight", &self.entries.total_weight)
            .field("min_item", &min_item)
            .field("max_item", &max_item)
            .finish()
    }
}

#[cfg(feature = "quantiles")]
impl<T> SortedView<T> {
    pub(crate) fn estimated_size(&self) -> usize {
        self.entries.vec.capacity() * size_of::<Entry<T>>()
    }
}

impl<T: Clone + Ord> SortedView<T> {
    pub(crate) fn new(
        levels: &[Vec<T>],
        is_level_zero_sorted: bool,
        extrema: Option<(&T, &T)>,
    ) -> Self {
        Self {
            entries: SortedEntries::new(levels, is_level_zero_sorted, extrema),
            extrema: extrema.map(|(min, max)| (min.clone(), max.clone())),
        }
    }

    /// Returns whether the view contains no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.vec.is_empty()
    }

    /// Returns the number of weighted entries in the view.
    ///
    /// The view includes both stream extrema, so it can have up to two more entries than the sketch
    /// retains.
    pub fn len(&self) -> usize {
        self.entries.vec.len()
    }

    /// Returns the total stream weight represented by the view.
    pub fn total_weight(&self) -> u64 {
        self.entries.total_weight
    }

    /// Returns the approximate normalized rank of `item`.
    ///
    /// Returns `None` if the view is empty.
    pub fn rank(&self, item: &T, criteria: SearchCriteria) -> Option<f64> {
        self.entries.rank(item, criteria)
    }

    pub(crate) fn quantile_at_valid_rank(&self, rank: f64, criteria: SearchCriteria) -> Option<T> {
        let (min_item, max_item) = self.extrema.as_ref()?;
        Some(
            self.entries
                .quantile_at_valid_rank(rank, criteria, (min_item, max_item)),
        )
    }

    /// Returns the approximate quantile for `rank`.
    ///
    /// Ranks `0.0` and `1.0` return the exact minimum and maximum items of the stream.
    /// Returns `Ok(None)` if the view is empty.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if `rank` is NaN or outside `[0.0, 1.0]`, even for an empty view.
    pub fn quantile(&self, rank: f64, criteria: SearchCriteria) -> Result<Option<T>, Error> {
        validate_rank(rank)?;
        Ok(self.quantile_at_valid_rank(rank, criteria))
    }

    pub(crate) fn quantiles_at_valid_ranks(
        &self,
        ranks: &[f64],
        criteria: SearchCriteria,
    ) -> Vec<T> {
        let (min_item, max_item) = self.extrema.as_ref().expect("checked non-empty view");
        self.entries
            .quantiles_at_valid_ranks(ranks, criteria, (min_item, max_item))
    }

    /// Returns approximate quantiles for all `ranks`.
    ///
    /// Ranks `0.0` and `1.0` return the exact minimum and maximum items of the stream.
    /// Returns `Ok(None)` if the view is empty.
    /// An empty `ranks` slice on a nonempty view returns `Ok(Some(vec![]))`.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` for NaN or out-of-range ranks, even on an empty view.
    pub fn quantiles(
        &self,
        ranks: &[f64],
        criteria: SearchCriteria,
    ) -> Result<Option<Vec<T>>, Error> {
        ranks.iter().copied().try_for_each(validate_rank)?;
        if self.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.quantiles_at_valid_ranks(ranks, criteria)))
    }

    pub(crate) fn cdf_at_valid_split_points(
        &self,
        split_points: &[T],
        criteria: SearchCriteria,
    ) -> Vec<f64> {
        self.entries
            .cdf_at_valid_split_points(split_points, criteria)
    }

    /// Returns the approximate cumulative distribution over `split_points`.
    ///
    /// Returns `Ok(None)` if the view is empty.
    /// An empty `split_points` slice on a nonempty view returns `Ok(Some(vec![1.0]))`.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` for unordered or repeated split points, even on an empty view.
    pub fn cdf(
        &self,
        split_points: &[T],
        criteria: SearchCriteria,
    ) -> Result<Option<Vec<f64>>, Error> {
        validate_split_points(split_points)?;
        if self.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.cdf_at_valid_split_points(split_points, criteria)))
    }

    /// Returns the approximate probability mass over `split_points`.
    ///
    /// Returns `Ok(None)` if the view is empty.
    /// An empty `split_points` slice on a nonempty view returns `Ok(Some(vec![1.0]))`.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` for unordered or repeated split points, even on an empty view.
    pub fn pmf(
        &self,
        split_points: &[T],
        criteria: SearchCriteria,
    ) -> Result<Option<Vec<f64>>, Error> {
        Ok(self.cdf(split_points, criteria)?.map(pmf_from_cdf))
    }
}

fn matches_search_criteria<T: Ord>(retained: &T, item: &T, criteria: SearchCriteria) -> bool {
    match criteria {
        SearchCriteria::Inclusive => retained <= item,
        SearchCriteria::Exclusive => retained < item,
    }
}

fn count_rank_linear<T: Ord>(items: &[T], item: &T, criteria: SearchCriteria) -> usize {
    items
        .iter()
        .filter(|retained| matches_search_criteria(*retained, item, criteria))
        .count()
}

pub fn rank<T: Ord>(
    levels: &[Vec<T>],
    is_level_zero_sorted: bool,
    extrema: Option<(&T, &T)>,
    total_weight: u64,
    item: &T,
    criteria: SearchCriteria,
) -> Option<f64> {
    if total_weight == 0 {
        return None;
    }
    let mut rank_weight = if let [items] = levels {
        // Keep a direct scan to preserve query latency for small exact sketches.
        count_rank_linear(items, item, criteria) as u64
    } else {
        let mut weight = 0u64;
        for (level, items) in levels.iter().enumerate() {
            // Only level zero can hold an unsorted update buffer.
            let is_sorted = level > 0 || is_level_zero_sorted;
            let count = if is_sorted {
                items.partition_point(|retained| matches_search_criteria(retained, item, criteria))
            } else {
                count_rank_linear(items, item, criteria)
            };
            weight += (count as u64) << level;
        }
        weight
    };

    // Transferring weight to an extremum changes the rank weight only if exactly one of its two
    // items qualifies. Each donor is the nearest retained item, so the minimum can add one unit
    // only when no retained items qualify, and the maximum can remove one only when all qualify.
    if let Some((min_item, max_item)) = extrema {
        if rank_weight == 0 {
            if matches_search_criteria(min_item, item, criteria) {
                rank_weight = 1;
            }
        } else if rank_weight == total_weight && !matches_search_criteria(max_item, item, criteria)
        {
            rank_weight = total_weight - 1;
        }
    }

    Some(rank_weight as f64 / total_weight as f64)
}
