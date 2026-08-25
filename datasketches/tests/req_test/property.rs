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

#![cfg(feature = "req")]

//! Property-based ReqSketch tests.

use datasketches::req::ReqSketch;
use datasketches::req::SearchCriteria;
use proptest::prelude::*;

proptest! {
    #[test]
    fn prop_quantile_rank_consistency(
        values in prop::collection::vec(0.0f64..1000.0, 500..1500),
    ) {
        let mut sketch = ReqSketch::new();
        for value in values {
            sketch.update(value);
        }

        // These sizes push the sketch past the compaction threshold, so the
        // round-trip exercises the estimation path rather than exact storage.
        prop_assume!(sketch.is_estimation_mode());

        for rank in [0.1, 0.25, 0.5, 0.75, 0.9] {
            let quantile = sketch
                .quantile(rank, SearchCriteria::Inclusive)
                .expect("quantile should succeed");
            let recovered = sketch
                .rank(&quantile, SearchCriteria::Inclusive)
                .expect("rank should succeed");

            // The recovered rank must land within the sketch's own 3-sigma rank
            // interval for the target rank (plus a small cushion for snapping to a
            // stored item). This scales with k and n, unlike a fixed slack, so it
            // actually constrains the result instead of always passing.
            let lower = sketch.rank_lower_bound(rank, 3) - 0.02;
            let upper = sketch.rank_upper_bound(rank, 3) + 0.02;
            prop_assert!(
                (lower..=upper).contains(&recovered),
                "rank {rank} -> quantile {quantile} -> recovered {recovered}, expected within [{lower:.4}, {upper:.4}]"
            );
        }
    }

    #[test]
    fn prop_sketch_bounds(values in prop::collection::vec(-1000.0f64..1000.0, 1..1000)) {
        let mut sketch = ReqSketch::new();
        for value in &values {
            sketch.update(*value);
        }

        if sketch.is_empty() {
            return Ok(());
        }

        let true_min = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let true_max = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));

        prop_assert_eq!(sketch.min_item(), Some(&true_min));
        prop_assert_eq!(sketch.max_item(), Some(&true_max));

        for rank in [0.0, 0.25, 0.5, 0.75, 1.0] {
            let quantile = sketch
                .quantile(rank, SearchCriteria::Inclusive)
                .expect("quantile should succeed");
            prop_assert!(
                quantile >= true_min && quantile <= true_max,
                "quantile {} out of bounds [{}, {}]",
                quantile,
                true_min,
                true_max
            );
        }
    }

    #[test]
    fn prop_rank_monotonicity(values in prop::collection::vec(0.0f64..1000.0, 10..100)) {
        let mut sketch = ReqSketch::new();
        for value in values {
            sketch.update(value);
        }

        if sketch.is_empty() {
            return Ok(());
        }

        let mut last_rank = -1.0;
        for value in [0.0, 100.0, 200.0, 500.0, 800.0, 1000.0] {
            let rank = sketch
                .rank(&value, SearchCriteria::Inclusive)
                .expect("rank should succeed");
            prop_assert!(rank >= last_rank, "rank {} after {}", rank, last_rank);
            prop_assert!((0.0..=1.0).contains(&rank), "rank {} out of bounds", rank);
            last_rank = rank;
        }
    }
}
