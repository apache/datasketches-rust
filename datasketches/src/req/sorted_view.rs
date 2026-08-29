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

use crate::error::Error;
use crate::req::SearchCriteria;

/// An owned, sorted snapshot of a [`ReqSketch`](crate::req::ReqSketch)'s items with
/// their cumulative weights.
///
/// Obtain one with [`ReqSketch::sorted_view`](crate::req::ReqSketch::sorted_view).
/// The view is independent of the sketch: it can be queried (and sent to other
/// threads) while the sketch keeps receiving updates, and it keeps answering
/// from the state it was taken at. Building it costs `O(retained · log retained)`;
/// each subsequent query is `O(log retained)`, so it is the right tool for
/// repeated quantile/rank queries.
#[derive(Debug, Clone)]
pub struct SortedView<T> {
    /// Items in sorted order
    items: Vec<T>,
    /// Cumulative weights for each item
    cumulative_weights: Vec<u64>,
    /// Total weight of all items
    total_weight: u64,
}

impl<T> SortedView<T>
where
    T: Clone + Ord,
{
    /// Creates a new sorted view from weighted items.
    ///
    /// # Arguments
    /// * `weighted_items` - Vector of (item, weight) pairs
    ///
    /// The items will be sorted and cumulative weights computed.
    pub(super) fn new(mut weighted_items: Vec<(T, u64)>) -> Self {
        if weighted_items.is_empty() {
            return Self {
                items: vec![],
                cumulative_weights: vec![],
                total_weight: 0,
            };
        }

        // Sort by item value - use unstable sort for better performance
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
        }
    }

    /// Returns true if the sorted view is empty.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns the number of distinct items in the sorted view.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns the total weight (stream length captured) of all items.
    pub fn total_weight(&self) -> u64 {
        self.total_weight
    }

    /// Returns the approximate normalized rank of the given item in `[0.0, 1.0]`.
    ///
    /// # Arguments
    /// * `item` - The item to find the rank for
    /// * `criteria` - Whether to include the item's weight in the rank
    ///
    /// # Errors
    /// Returns an error if the view is empty.
    pub fn rank(&self, item: &T, criteria: SearchCriteria) -> Result<f64, Error> {
        if self.is_empty() {
            return Err(Error::invalid_argument("sketch is empty"));
        }
        match criteria {
            SearchCriteria::Inclusive => {
                // Find the last position where items[i] <= item
                // partition_point finds first index where predicate is false
                let pos = self.items.partition_point(|x| x <= item);
                if pos == 0 {
                    Ok(0.0)
                } else {
                    Ok(self.cumulative_weights[pos - 1] as f64 / self.total_weight as f64)
                }
            }
            SearchCriteria::Exclusive => {
                // Find the last position where items[i] < item
                let pos = self.items.partition_point(|x| x < item);
                if pos == 0 {
                    Ok(0.0)
                } else {
                    Ok(self.cumulative_weights[pos - 1] as f64 / self.total_weight as f64)
                }
            }
        }
    }

    /// Returns the approximate quantile for the given normalized rank.
    ///
    /// # Arguments
    /// * `rank` - A normalized rank in [0.0, 1.0]
    /// * `criteria` - Search criteria for quantile selection
    ///
    /// # Returns
    /// The item at approximately the given rank
    pub fn quantile(&self, rank: f64, criteria: SearchCriteria) -> Result<T, Error> {
        if self.is_empty() {
            return Err(Error::invalid_argument("sketch is empty"));
        }

        if !(0.0..=1.0).contains(&rank) {
            return Err(Error::invalid_argument(format!(
                "rank {rank} must be in [0, 1]"
            )));
        }

        // Handle edge cases
        if rank == 0.0 {
            match criteria {
                SearchCriteria::Inclusive => return Ok(self.items[0].clone()),
                SearchCriteria::Exclusive => return Ok(self.items[0].clone()),
            }
        }
        if rank == 1.0 {
            return Ok(self.items[self.items.len() - 1].clone());
        }

        // Convert rank to target cumulative weight
        // uint64_t weight = static_cast<uint64_t>(inclusive ? std::ceil(rank * total_weight_) :
        // rank * total_weight_);
        let target_weight = match criteria {
            SearchCriteria::Inclusive => (rank * self.total_weight as f64).ceil() as u64,
            SearchCriteria::Exclusive => (rank * self.total_weight as f64) as u64,
        };

        let index = match criteria {
            SearchCriteria::Inclusive => {
                // Equivalent to C++ lower_bound: first index where cumulative_weight >= target
                self.cumulative_weights
                    .partition_point(|&w| w < target_weight)
            }
            SearchCriteria::Exclusive => {
                // Equivalent to C++ upper_bound: first index where cumulative_weight > target
                self.cumulative_weights
                    .partition_point(|&w| w <= target_weight)
            }
        };

        if index >= self.items.len() {
            return Ok(self.items[self.items.len() - 1].clone());
        }

        Ok(self.items[index].clone())
    }

    /// Returns the Probability Mass Function (PMF) for the given split points.
    ///
    /// # Arguments
    /// * `split_points` - Array of split points that divide the domain
    /// * `criteria` - Search criteria for boundary handling
    ///
    /// # Returns
    /// Array of probabilities for each interval defined by the split points
    pub fn pmf(&self, split_points: &[T], criteria: SearchCriteria) -> Result<Vec<f64>, Error> {
        if self.is_empty() {
            return Err(Error::invalid_argument("sketch is empty"));
        }

        self.validate_split_points(split_points)?;

        let mut result = Vec::with_capacity(split_points.len() + 1);
        let mut prev_rank = 0.0;

        for split_point in split_points {
            let rank = self.rank(split_point, criteria)?;
            result.push(rank - prev_rank);
            prev_rank = rank;
        }

        // Add the final interval
        result.push(1.0 - prev_rank);

        Ok(result)
    }

    /// Returns the Cumulative Distribution Function (CDF) for the given split points.
    ///
    /// # Arguments
    /// * `split_points` - Array of split points that divide the domain
    /// * `criteria` - Search criteria for boundary handling
    ///
    /// # Returns
    /// Array of cumulative probabilities at each split point
    pub fn cdf(&self, split_points: &[T], criteria: SearchCriteria) -> Result<Vec<f64>, Error> {
        if self.is_empty() {
            return Err(Error::invalid_argument("sketch is empty"));
        }

        self.validate_split_points(split_points)?;

        let mut result = Vec::with_capacity(split_points.len() + 1);
        let mut cumulative = 0.0;

        let pmf = self.pmf(split_points, criteria)?;
        for mass in pmf {
            cumulative += mass;
            result.push(cumulative);
        }

        Ok(result)
    }

    // Private helper methods

    fn validate_split_points(&self, split_points: &[T]) -> Result<(), Error> {
        if split_points.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(Error::invalid_argument(
                "Split points must be unique and monotonically increasing".to_string(),
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
        SortedView::new(weighted_items)
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
        let view: SortedView<i32> = SortedView::new(vec![]);
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
