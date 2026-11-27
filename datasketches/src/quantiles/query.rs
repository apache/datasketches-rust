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

use super::sketch::QuantilesSketch;
use crate::common::SearchCriteria;
use crate::common::sorted_view::SortedView;
use crate::common::sorted_view::pmf_from_cdf;
use crate::common::sorted_view::rank;
use crate::common::sorted_view::validate_rank;
use crate::common::sorted_view::validate_split_points;
use crate::error::Error;

impl<T: Clone + Ord> QuantilesSketch<T> {
    fn build_sorted_view(&self) -> SortedView<T> {
        SortedView::new(&self.levels, false, self.min_item().zip(self.max_item()))
    }

    /// Returns an owned, sorted snapshot of the current sketch.
    pub fn sorted_view(&self) -> SortedView<T> {
        self.sorted_view_cache
            .get()
            .cloned()
            .unwrap_or_else(|| self.build_sorted_view())
    }

    fn get_or_init_sorted_view(&self) -> &SortedView<T> {
        self.sorted_view_cache
            .get_or_init(|| self.build_sorted_view())
    }

    /// Estimates the fraction of items below `item` (`Exclusive`) or at or below it (`Inclusive`).
    /// Returns `None` if the sketch is empty.
    pub fn rank(&self, item: &T, criteria: SearchCriteria) -> Option<f64> {
        rank(
            &self.levels,
            false,
            self.min_item().zip(self.max_item()),
            self.total_weight(),
            item,
            criteria,
        )
    }

    /// Returns an observed item as the estimated quantile at `rank`.
    /// Returns `Ok(None)` if the sketch is empty.
    ///
    /// For interior ranks, the target weight is `rank * total_weight`, computed in `f64`.
    /// Above `2^53`, rounding the total weight can change which item is selected.
    /// `Inclusive` selects the first item whose cumulative weight is at least the target.
    /// `Exclusive` selects the first item whose cumulative weight exceeds it.
    /// Both criteria return the recorded minimum at rank `0.0` and maximum at rank `1.0`.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if `rank` is NaN or outside `[0.0, 1.0]`.
    pub fn quantile(&self, rank: f64, criteria: SearchCriteria) -> Result<Option<T>, Error> {
        validate_rank(rank)?;
        if rank == 0.0 {
            return Ok(self.min_item().cloned());
        }
        if rank == 1.0 {
            return Ok(self.max_item().cloned());
        }
        Ok(self
            .get_or_init_sorted_view()
            .quantile_at_valid_rank(rank, criteria))
    }

    /// Returns quantiles in the order requested, or `Ok(None)` if the sketch is empty.
    ///
    /// Each result follows the same endpoint and search rules as [`quantile`](Self::quantile).
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if any rank is NaN or outside `[0.0, 1.0]`.
    pub fn quantiles(
        &self,
        ranks: &[f64],
        criteria: SearchCriteria,
    ) -> Result<Option<Vec<T>>, Error> {
        ranks.iter().copied().try_for_each(validate_rank)?;
        if self.is_empty() {
            return Ok(None);
        }
        if ranks.is_empty() {
            return Ok(Some(Vec::new()));
        }
        Ok(Some(
            self.get_or_init_sorted_view()
                .quantiles_at_valid_ranks(ranks, criteria),
        ))
    }

    /// Returns the normalized rank of each split point, followed by `1.0`.
    ///
    /// Returns `Ok(None)` if the sketch is empty.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if the split points are not strictly increasing.
    pub fn cdf(
        &self,
        split_points: &[T],
        criteria: SearchCriteria,
    ) -> Result<Option<Vec<f64>>, Error> {
        validate_split_points(split_points)?;
        if self.is_empty() {
            return Ok(None);
        }
        if split_points.is_empty() {
            return Ok(Some(vec![1.0]));
        }
        Ok(Some(
            self.get_or_init_sorted_view()
                .cdf_at_valid_split_points(split_points, criteria),
        ))
    }

    /// Returns the fraction of stream weight in each of the `split_points.len() + 1` bins.
    ///
    /// At a split point, `Inclusive` counts ties in the lower bin and `Exclusive` in the upper bin.
    /// A nonempty bin can have probability `0.0` if its cumulative ranks round to the same `f64`.
    /// Returns `Ok(None)` if the sketch is empty.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` if the split points are not strictly increasing.
    pub fn pmf(
        &self,
        split_points: &[T],
        criteria: SearchCriteria,
    ) -> Result<Option<Vec<f64>>, Error> {
        Ok(self.cdf(split_points, criteria)?.map(pmf_from_cdf))
    }
}
