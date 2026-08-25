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

//! Property-based ReqSketch tests.

use datasketches::req::ReqSketch;
use datasketches::req::SearchCriteria;
use quickcheck::Gen;
use quickcheck::QuickCheck;
use quickcheck::TestResult;

#[test]
fn prop_quantile_rank_consistency() {
    fn property(values: Vec<u64>) -> TestResult {
        if !(500..1500).contains(&values.len()) {
            return TestResult::discard();
        }

        let mut sketch = ReqSketch::new();
        for value in values {
            sketch.update(value);
        }

        // These sizes push the sketch past the compaction threshold, so the
        // round-trip exercises the estimation path rather than exact storage.
        if !sketch.is_estimation_mode() {
            return TestResult::discard();
        }

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
            assert!(
                (lower..=upper).contains(&recovered),
                "rank {rank} -> quantile {quantile} -> recovered {recovered}, expected within [{lower:.4}, {upper:.4}]"
            );
        }

        TestResult::passed()
    }

    QuickCheck::new()
        .tests(256)
        .min_tests_passed(256)
        .rng(Gen::new(1500))
        .quickcheck(property as fn(Vec<u64>) -> TestResult);
}

#[test]
fn prop_sketch_bounds() {
    fn property(values: Vec<i64>) -> TestResult {
        if !(1..1000).contains(&values.len()) {
            return TestResult::discard();
        }

        let mut sketch = ReqSketch::new();
        for value in &values {
            sketch.update(*value);
        }

        let true_min = values.iter().copied().min().expect("values are non-empty");
        let true_max = values.iter().copied().max().expect("values are non-empty");

        assert_eq!(sketch.min_item(), Some(&true_min));
        assert_eq!(sketch.max_item(), Some(&true_max));

        for rank in [0.0, 0.25, 0.5, 0.75, 1.0] {
            let quantile = sketch
                .quantile(rank, SearchCriteria::Inclusive)
                .expect("quantile should succeed");
            assert!(
                quantile >= true_min && quantile <= true_max,
                "quantile {} out of bounds [{}, {}]",
                quantile,
                true_min,
                true_max
            );
        }

        TestResult::passed()
    }

    QuickCheck::new()
        .tests(256)
        .min_tests_passed(256)
        .rng(Gen::new(1000))
        .quickcheck(property as fn(Vec<i64>) -> TestResult);
}

#[test]
fn prop_rank_monotonicity() {
    fn property(values: Vec<u32>) -> TestResult {
        if !(10..100).contains(&values.len()) {
            return TestResult::discard();
        }

        let mut sketch = ReqSketch::new();
        for value in values {
            sketch.update(value);
        }

        let mut last_rank = -1.0;
        for value in [
            0,
            u32::MAX / 10,
            u32::MAX / 5,
            u32::MAX / 2,
            (u32::MAX / 5) * 4,
            u32::MAX,
        ] {
            let rank = sketch
                .rank(&value, SearchCriteria::Inclusive)
                .expect("rank should succeed");
            assert!(rank >= last_rank, "rank {} after {}", rank, last_rank);
            assert!((0.0..=1.0).contains(&rank), "rank {} out of bounds", rank);
            last_rank = rank;
        }

        TestResult::passed()
    }

    QuickCheck::new()
        .tests(256)
        .min_tests_passed(256)
        .rng(Gen::new(100))
        .quickcheck(property as fn(Vec<u32>) -> TestResult);
}
