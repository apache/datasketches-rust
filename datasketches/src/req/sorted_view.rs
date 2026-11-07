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

//! Sorted view implementation for efficient quantile queries.

use crate::common::SearchCriteria;
use crate::error::Error;

/// An owned, sorted snapshot of a [`ReqSketch`](crate::req::ReqSketch).
///
/// Obtain one with [`ReqSketch::sorted_view`](crate::req::ReqSketch::sorted_view). The view is
/// independent of the sketch: it can be queried (and sent to other threads) while the sketch keeps
/// receiving updates, and it keeps answering from the state it was taken at. Building it costs
/// `O(retained · log retained)`; each subsequent query is `O(log retained)`, so it is the right
/// tool for repeated quantile/rank queries.
#[derive(Debug, Clone)]
pub struct SortedView<T> {
    items: Vec<T>,
    cumulative_weights: Vec<u64>,
    total_weight: u64,
    min_item: Option<T>,
    max_item: Option<T>,
}

impl<T> SortedView<T>
where
    T: Clone + Ord,
{
    /// Creates a sorted view, combining the weights of equal items.
    pub(super) fn new(
        mut weighted_items: Vec<(T, u64)>,
        min_item: Option<T>,
        max_item: Option<T>,
    ) -> Self {
        weighted_items.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        let mut items: Vec<T> = Vec::with_capacity(weighted_items.len());
        let mut cumulative_weights = Vec::with_capacity(weighted_items.len());
        let mut cumulative_weight = 0u64;

        for (item, weight) in weighted_items {
            if let Some(last) = items.last() {
                if last == &item {
                    cumulative_weight += weight;
                    let last_idx = cumulative_weights.len() - 1;
                    cumulative_weights[last_idx] = cumulative_weight;
                    continue;
                }
            }
            cumulative_weight += weight;
            items.push(item);
            cumulative_weights.push(cumulative_weight);
        }

        Self {
            items,
            cumulative_weights,
            total_weight: cumulative_weight,
            min_item,
            max_item,
        }
    }

    /// Returns true if the sorted view is empty.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns the number of distinct retained items in the view.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns the total weight (stream length captured) of all items.
    pub fn total_weight(&self) -> u64 {
        self.total_weight
    }

    /// Returns the approximate normalized rank of `item` in `[0.0, 1.0]`.
    ///
    /// Returns `None` if the view is empty.
    pub fn rank(&self, item: &T, criteria: SearchCriteria) -> Option<f64> {
        if self.is_empty() {
            return None;
        }
        let pos = match criteria {
            SearchCriteria::Inclusive => self.items.partition_point(|x| x <= item),
            SearchCriteria::Exclusive => self.items.partition_point(|x| x < item),
        };
        if pos == 0 {
            Some(0.0)
        } else {
            Some(self.cumulative_weights[pos - 1] as f64 / self.total_weight as f64)
        }
    }

    /// Returns the approximate quantile at the given normalized rank.
    ///
    /// Ranks `0.0` and `1.0` return the exact minimum and maximum items of the stream. Returns
    /// `Ok(None)` if the view is empty.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if `rank` is NaN or outside `[0.0, 1.0]`, even if the view is
    /// empty.
    pub fn quantile(&self, rank: f64, criteria: SearchCriteria) -> Result<Option<T>, Error> {
        if !(0.0..=1.0).contains(&rank) {
            return Err(Error::invalid_argument(format!(
                "rank {rank} must be in [0, 1]"
            )));
        }
        if self.is_empty() {
            return Ok(None);
        }

        // Compaction may discard extrema, and large weights can lose precision in f64.
        if rank == 0.0 {
            return Ok(Some(self.min_item.as_ref().unwrap().clone()));
        }
        if rank == 1.0 {
            return Ok(Some(self.max_item.as_ref().unwrap().clone()));
        }

        let target_weight = match criteria {
            SearchCriteria::Inclusive => (rank * self.total_weight as f64).ceil() as u64,
            SearchCriteria::Exclusive => (rank * self.total_weight as f64) as u64,
        };

        let index = match criteria {
            SearchCriteria::Inclusive => self
                .cumulative_weights
                .partition_point(|&w| w < target_weight),
            SearchCriteria::Exclusive => self
                .cumulative_weights
                .partition_point(|&w| w <= target_weight),
        };

        Ok(Some(self.items[index.min(self.items.len() - 1)].clone()))
    }

    /// Returns the probability mass function (PMF) over the given split points.
    ///
    /// The result contains one more value than `split_points`. Returns `Ok(None)` if the view is
    /// empty. Otherwise, an empty `split_points` slice returns `Ok(Some(vec![1.0]))`.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if the split points are not strictly increasing, even if the view
    /// is empty.
    pub fn pmf(
        &self,
        split_points: &[T],
        criteria: SearchCriteria,
    ) -> Result<Option<Vec<f64>>, Error> {
        let Some(mut result) = self.cdf(split_points, criteria)? else {
            return Ok(None);
        };
        for index in (1..result.len()).rev() {
            result[index] -= result[index - 1];
        }
        Ok(Some(result))
    }

    /// Returns the cumulative distribution function (CDF) over the given split points.
    ///
    /// The result contains one more value than `split_points` and ends at `1.0`. Returns
    /// `Ok(None)` if the view is empty. Otherwise, an empty `split_points` slice returns
    /// `Ok(Some(vec![1.0]))`.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if the split points are not strictly increasing, even if the view
    /// is empty.
    pub fn cdf(
        &self,
        split_points: &[T],
        criteria: SearchCriteria,
    ) -> Result<Option<Vec<f64>>, Error> {
        self.validate_split_points(split_points)?;
        if self.is_empty() {
            return Ok(None);
        }

        let mut result = Vec::with_capacity(split_points.len() + 1);
        for split_point in split_points {
            result.push(
                self.rank(split_point, criteria)
                    .expect("checked non-empty view"),
            );
        }
        result.push(1.0);
        Ok(Some(result))
    }

    fn validate_split_points(&self, split_points: &[T]) -> Result<(), Error> {
        if split_points.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(Error::invalid_argument(
                "Split points must be unique and monotonically increasing",
            ));
        }
        Ok(())
    }
}
