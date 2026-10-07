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
/// Obtain one with [`ReqSketch::sorted_view`](crate::req::ReqSketch::sorted_view).
/// The view is independent of the sketch: it can be queried (and sent to other
/// threads) while the sketch keeps receiving updates, and it keeps answering
/// from the state it was taken at. Building it costs `O(retained · log retained)`;
/// each subsequent query is `O(log retained)`, so it is the right tool for
/// repeated quantile/rank queries.
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
    /// # Errors
    ///
    /// Returns an error if the view is empty.
    pub fn rank(&self, item: &T, criteria: SearchCriteria) -> Result<f64, Error> {
        if self.is_empty() {
            return Err(Error::invalid_argument("sketch is empty"));
        }
        let pos = match criteria {
            SearchCriteria::Inclusive => self.items.partition_point(|x| x <= item),
            SearchCriteria::Exclusive => self.items.partition_point(|x| x < item),
        };
        if pos == 0 {
            Ok(0.0)
        } else {
            Ok(self.cumulative_weights[pos - 1] as f64 / self.total_weight as f64)
        }
    }

    /// Returns the approximate quantile at the given normalized rank.
    ///
    /// Ranks `0.0` and `1.0` return the exact minimum and maximum items of the stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the view is empty or `rank` is outside `[0.0, 1.0]`.
    pub fn quantile(&self, rank: f64, criteria: SearchCriteria) -> Result<T, Error> {
        if self.is_empty() {
            return Err(Error::invalid_argument("sketch is empty"));
        }

        if !(0.0..=1.0).contains(&rank) {
            return Err(Error::invalid_argument(format!(
                "rank {rank} must be in [0, 1]"
            )));
        }

        // Compaction may discard extrema, and large weights can lose precision in f64.
        if rank == 0.0 {
            return Ok(self.min_item.as_ref().unwrap().clone());
        }
        if rank == 1.0 {
            return Ok(self.max_item.as_ref().unwrap().clone());
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

        Ok(self.items[index.min(self.items.len() - 1)].clone())
    }

    /// Returns the probability mass function (PMF) over the given split points.
    ///
    /// The result contains one more value than `split_points`.
    ///
    /// # Errors
    ///
    /// Returns an error if the view is empty or the split points are not strictly increasing.
    pub fn pmf(&self, split_points: &[T], criteria: SearchCriteria) -> Result<Vec<f64>, Error> {
        let mut result = self.cdf(split_points, criteria)?;
        for index in (1..result.len()).rev() {
            result[index] -= result[index - 1];
        }
        Ok(result)
    }

    /// Returns the cumulative distribution function (CDF) over the given split points.
    ///
    /// The result contains one more value than `split_points` and ends at `1.0`.
    ///
    /// # Errors
    ///
    /// Returns an error if the view is empty or the split points are not strictly increasing.
    pub fn cdf(&self, split_points: &[T], criteria: SearchCriteria) -> Result<Vec<f64>, Error> {
        if self.is_empty() {
            return Err(Error::invalid_argument("sketch is empty"));
        }

        self.validate_split_points(split_points)?;

        let mut result = Vec::with_capacity(split_points.len() + 1);
        for split_point in split_points {
            result.push(self.rank(split_point, criteria)?);
        }
        result.push(1.0);
        Ok(result)
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

#[cfg(test)]
mod tests {
    use googletest::assert_that;
    use googletest::prelude::all;
    use googletest::prelude::anything;
    use googletest::prelude::err;
    use googletest::prelude::ge;
    use googletest::prelude::le;
    use googletest::prelude::near;

    use super::*;

    fn create_test_view() -> SortedView<i32> {
        let weighted_items = vec![(1, 1), (3, 1), (5, 1), (7, 1), (9, 1)];
        SortedView::new(weighted_items, Some(1), Some(9))
    }

    #[test]
    fn test_sorted_view_creation() {
        let view = create_test_view();
        assert_eq!(view.len(), 5);
        assert_eq!(view.total_weight(), 5);
        assert!(!view.is_empty());
    }

    #[test]
    fn test_rank_queries() -> Result<(), Error> {
        let view = create_test_view();

        // Test exact matches
        assert_that!(view.rank(&1, SearchCriteria::Inclusive)?, near(0.2, 1e-10));
        assert_that!(view.rank(&1, SearchCriteria::Exclusive)?, near(0.0, 1e-10));

        // Test values between items
        assert_that!(view.rank(&2, SearchCriteria::Inclusive)?, near(0.2, 1e-10));
        assert_that!(view.rank(&6, SearchCriteria::Inclusive)?, near(0.6, 1e-10));

        // Test edge cases
        assert_that!(view.rank(&0, SearchCriteria::Inclusive)?, near(0.0, 1e-10));
        assert_that!(view.rank(&10, SearchCriteria::Inclusive)?, near(1.0, 1e-10));
        Ok(())
    }

    #[test]
    fn test_quantile_queries() -> Result<(), Error> {
        let view = create_test_view();

        // Test edge cases
        assert_eq!(view.quantile(0.0, SearchCriteria::Inclusive)?, 1);
        assert_eq!(view.quantile(1.0, SearchCriteria::Inclusive)?, 9);

        // Test middle values
        let median = view.quantile(0.5, SearchCriteria::Inclusive)?;
        assert_that!(median, all!(ge(3), le(7))); // Should be around the middle (values are 1,3,5,7,9)

        // Test various ranks
        let q25 = view.quantile(0.25, SearchCriteria::Inclusive)?;
        let q75 = view.quantile(0.75, SearchCriteria::Inclusive)?;
        assert_that!(q25, le(median));
        assert_that!(median, le(q75));
        Ok(())
    }

    #[test]
    fn test_pmf() -> Result<(), Error> {
        let view = create_test_view();
        let split_points = vec![3, 7];

        let pmf = view.pmf(&split_points, SearchCriteria::Inclusive)?;
        assert_eq!(pmf.len(), 3); // 2 split points create 3 intervals

        // Sum should be approximately 1.0
        let sum: f64 = pmf.iter().sum();
        assert_that!(sum, near(1.0, 1e-10));
        Ok(())
    }

    #[test]
    fn test_cdf() -> Result<(), Error> {
        let view = create_test_view();
        let split_points = vec![3, 7];

        let cdf = view.cdf(&split_points, SearchCriteria::Inclusive)?;
        assert_eq!(cdf.len(), 3);

        // CDF should be monotonically increasing
        for i in 1..cdf.len() {
            assert_that!(cdf[i], ge(cdf[i - 1]));
        }

        // Last value should be 1.0
        assert_that!(cdf[cdf.len() - 1], near(1.0, 1e-10));
        Ok(())
    }

    #[test]
    fn test_empty_view() {
        let view: SortedView<i32> = SortedView::new(vec![], None, None);
        assert!(view.is_empty());
        assert_eq!(view.len(), 0);
        assert_eq!(view.total_weight(), 0);

        // Operations on empty view should return errors
        assert_that!(view.rank(&5, SearchCriteria::Inclusive), err(anything()));
        assert_that!(
            view.quantile(0.5, SearchCriteria::Inclusive),
            err(anything())
        );
    }
}
