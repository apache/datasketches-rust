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

use std::cmp::Ordering;

use crate::common::SearchCriteria;
use crate::error::Error;

/// An owned, sorted snapshot of a KLL sketch.
///
/// Build one with [`KllSketch::sorted_view`](super::KllSketch::sorted_view) when running repeated
/// queries against the same sketch state.
#[derive(Debug, Clone)]
pub struct SortedView<T: Clone> {
    entries: Vec<Entry<T>>,
    total_weight: u64,
}

#[derive(Debug, Clone)]
struct Entry<T> {
    item: T,
    cumulative_weight: u64,
}

impl<T: Clone + Ord> SortedView<T> {
    fn from_sorted(mut entries: Vec<Entry<T>>) -> Self {
        let mut total_weight = 0u64;
        for entry in &mut entries {
            total_weight += entry.cumulative_weight;
            entry.cumulative_weight = total_weight;
        }
        Self {
            entries,
            total_weight,
        }
    }

    /// Returns whether the view contains no retained items.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the number of retained items in the view.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns the total stream weight represented by the view.
    pub fn total_weight(&self) -> u64 {
        self.total_weight
    }

    /// Returns the approximate normalized rank of `item`.
    ///
    /// Returns `None` if the view is empty.
    pub fn rank(&self, item: &T, criteria: SearchCriteria) -> Option<f64> {
        if self.is_empty() {
            return None;
        }
        let index = if criteria == SearchCriteria::Inclusive {
            upper_bound(&self.entries, item)
        } else {
            lower_bound(&self.entries, item)
        };

        if index == 0 {
            return Some(0.0);
        }
        Some(self.entries[index - 1].cumulative_weight as f64 / self.total_weight as f64)
    }

    /// Returns the approximate quantile for `rank`.
    ///
    /// Returns `Ok(None)` if the view is empty.
    ///
    /// # Errors
    ///
    /// Returns an error if `rank` is outside `[0.0, 1.0]`.
    pub fn quantile(&self, rank: f64, criteria: SearchCriteria) -> Result<Option<T>, Error> {
        if !(0.0..=1.0).contains(&rank) {
            return Err(Error::invalid_argument(format!(
                "rank must be in [0.0, 1.0], got {rank}"
            )));
        }
        if self.is_empty() {
            return Ok(None);
        }

        let weight = if criteria == SearchCriteria::Inclusive {
            (rank * self.total_weight as f64).ceil() as u64
        } else {
            (rank * self.total_weight as f64) as u64
        };
        let index = if criteria == SearchCriteria::Inclusive {
            lower_bound_by_weight(&self.entries, weight)
        } else {
            upper_bound_by_weight(&self.entries, weight)
        };

        Ok(Some(
            self.entries[index.min(self.entries.len() - 1)].item.clone(),
        ))
    }

    /// Returns approximate quantiles for all `ranks`.
    ///
    /// Returns `Ok(None)` if the view is empty.
    ///
    /// # Errors
    ///
    /// Returns an error if any rank is outside `[0.0, 1.0]`.
    pub fn quantiles(
        &self,
        ranks: &[f64],
        criteria: SearchCriteria,
    ) -> Result<Option<Vec<T>>, Error> {
        for &rank in ranks {
            if !(0.0..=1.0).contains(&rank) {
                return Err(Error::invalid_argument(format!(
                    "rank must be in [0.0, 1.0], got {rank}"
                )));
            }
        }
        if self.is_empty() {
            return Ok(None);
        }
        let quantiles = ranks
            .iter()
            .map(|&rank| {
                self.quantile(rank, criteria)
                    .map(|quantile| quantile.expect("checked non-empty view"))
            })
            .collect::<Result<_, _>>()?;
        Ok(Some(quantiles))
    }

    /// Returns the approximate cumulative distribution over `split_points`.
    ///
    /// Returns `Ok(None)` if the view is empty.
    ///
    /// # Errors
    ///
    /// Returns an error if the split points are invalid.
    pub fn cdf(
        &self,
        split_points: &[T],
        criteria: SearchCriteria,
    ) -> Result<Option<Vec<f64>>, Error> {
        check_split_points(split_points)?;
        if self.is_empty() {
            return Ok(None);
        }
        let mut ranks = Vec::with_capacity(split_points.len() + 1);
        for item in split_points {
            ranks.push(self.rank(item, criteria).expect("checked non-empty view"));
        }
        ranks.push(1.0);
        Ok(Some(ranks))
    }

    /// Returns the approximate probability mass over `split_points`.
    ///
    /// Returns `Ok(None)` if the view is empty.
    ///
    /// # Errors
    ///
    /// Returns an error if the split points are invalid.
    pub fn pmf(
        &self,
        split_points: &[T],
        criteria: SearchCriteria,
    ) -> Result<Option<Vec<f64>>, Error> {
        let Some(mut buckets) = self.cdf(split_points, criteria)? else {
            return Ok(None);
        };
        for index in (1..buckets.len()).rev() {
            buckets[index] -= buckets[index - 1];
        }
        Ok(Some(buckets))
    }
}

pub fn build_sorted_view<T: Clone + Ord>(
    levels: &[Vec<T>],
    is_level_zero_sorted: bool,
) -> SortedView<T> {
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

    while runs.len() > 1 {
        let mut merged_runs = Vec::with_capacity(runs.len().div_ceil(2));
        let mut iter = runs.into_iter();
        while let Some(left) = iter.next() {
            if let Some(right) = iter.next() {
                merged_runs.push(merge_sorted_entries(left, right));
            } else {
                merged_runs.push(left);
            }
        }
        runs = merged_runs;
    }

    SortedView::from_sorted(runs.pop().unwrap_or_default())
}

fn merge_sorted_entries<T: Ord>(left: Vec<Entry<T>>, right: Vec<Entry<T>>) -> Vec<Entry<T>> {
    let mut merged = Vec::with_capacity(left.len() + right.len());
    let mut left = left.into_iter().peekable();
    let mut right = right.into_iter().peekable();

    while let (Some(left_entry), Some(right_entry)) = (left.peek(), right.peek()) {
        if left_entry.item.cmp(&right_entry.item) == Ordering::Greater {
            merged.push(right.next().unwrap());
        } else {
            merged.push(left.next().unwrap());
        }
    }
    merged.extend(left);
    merged.extend(right);
    merged
}

fn check_split_points<T: Ord>(split_points: &[T]) -> Result<(), Error> {
    for (index, pair) in split_points.windows(2).enumerate() {
        if pair[0].cmp(&pair[1]) != Ordering::Less {
            return Err(Error::invalid_argument(format!(
                "split points at indices {index} and {} must be strictly increasing",
                index + 1
            )));
        }
    }
    Ok(())
}

fn lower_bound<T: Ord>(entries: &[Entry<T>], item: &T) -> usize {
    entries.partition_point(|entry| entry.item.cmp(item) == Ordering::Less)
}

fn upper_bound<T: Ord>(entries: &[Entry<T>], item: &T) -> usize {
    entries.partition_point(|entry| entry.item.cmp(item) != Ordering::Greater)
}

fn lower_bound_by_weight<T>(entries: &[Entry<T>], weight: u64) -> usize {
    entries.partition_point(|entry| entry.cumulative_weight < weight)
}

fn upper_bound_by_weight<T>(entries: &[Entry<T>], weight: u64) -> usize {
    entries.partition_point(|entry| entry.cumulative_weight <= weight)
}
