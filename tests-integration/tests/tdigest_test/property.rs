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

//! Property-based t-digest tests.

use datasketches::tdigest::TDigestMut;
use quickcheck::Gen;
use quickcheck::QuickCheck;
use quickcheck::TestResult;

const RANK_STEPS: usize = 500;

fn digest_of(values: &[u32]) -> TDigestMut {
    let mut tdigest = TDigestMut::new(100).unwrap();
    for value in values {
        tdigest.update(f64::from(*value) / 4096.0);
    }
    tdigest
}

#[test]
fn prop_quantile_is_non_decreasing_and_within_the_observed_range() {
    fn property(values: Vec<u32>) -> TestResult {
        if !(500..1500).contains(&values.len()) {
            return TestResult::discard();
        }

        let mut tdigest = digest_of(&values);
        let min = tdigest.min_value().unwrap();
        let max = tdigest.max_value().unwrap();

        let mut previous = f64::NEG_INFINITY;
        for step in 0..=RANK_STEPS {
            let rank = step as f64 / RANK_STEPS as f64;
            let quantile = tdigest.quantile(rank).unwrap();
            assert!(
                (min..=max).contains(&quantile),
                "quantile {quantile} at rank {rank} escapes [{min}, {max}]"
            );
            assert!(
                quantile >= previous,
                "quantile {quantile} at rank {rank} is below {previous} at the preceding rank"
            );
            previous = quantile;
        }

        TestResult::passed()
    }

    QuickCheck::new()
        .tests(128)
        .min_tests_passed(128)
        .rng(Gen::new(1200))
        .quickcheck(property as fn(Vec<u32>) -> TestResult);
}

#[test]
fn prop_merged_quantile_is_non_decreasing() {
    fn property(left: Vec<u32>, right: Vec<u32>) -> TestResult {
        if left.len() < 300 || right.len() < 300 {
            return TestResult::discard();
        }

        let mut merged = digest_of(&left);
        merged.merge(&digest_of(&right));
        let max = merged.max_value().unwrap();

        let mut previous = merged.min_value().unwrap();
        for step in 0..=RANK_STEPS {
            let rank = step as f64 / RANK_STEPS as f64;
            let quantile = merged.quantile(rank).unwrap();
            assert!(
                (previous..=max).contains(&quantile),
                "merged quantile {quantile} at rank {rank} escapes [{previous}, {max}]"
            );
            previous = quantile;
        }

        TestResult::passed()
    }

    QuickCheck::new()
        .tests(64)
        .min_tests_passed(64)
        .rng(Gen::new(900))
        .quickcheck(property as fn(Vec<u32>, Vec<u32>) -> TestResult);
}

#[test]
fn prop_cdf_is_a_distribution() {
    fn property(values: Vec<u32>) -> TestResult {
        if !(500..1500).contains(&values.len()) {
            return TestResult::discard();
        }

        let mut tdigest = digest_of(&values);
        let min = tdigest.min_value().unwrap();
        let max = tdigest.max_value().unwrap();
        if min == max {
            return TestResult::discard();
        }

        let mut split_points = Vec::with_capacity(RANK_STEPS);
        for step in 1..RANK_STEPS {
            let point = min + (max - min) * (step as f64 / RANK_STEPS as f64);
            if split_points.last().is_none_or(|last| point > *last) {
                split_points.push(point);
            }
        }

        let mut previous = 0.0;
        for point in &split_points {
            let rank = tdigest.rank(*point).unwrap();
            assert!(
                (0.0..=1.0).contains(&rank),
                "rank {rank} at value {point} escapes [0, 1]"
            );
            assert!(
                rank >= previous,
                "rank {rank} at value {point} is below {previous} at the preceding value"
            );
            previous = rank;
        }

        let pmf = tdigest.pmf(&split_points).unwrap();
        for mass in &pmf {
            assert!(*mass >= 0.0, "negative mass {mass} in {pmf:?}");
        }
        let total: f64 = pmf.iter().sum();
        assert!((total - 1.0).abs() < 1e-9, "masses sum to {total}");

        TestResult::passed()
    }

    QuickCheck::new()
        .tests(128)
        .min_tests_passed(128)
        .rng(Gen::new(1200))
        .quickcheck(property as fn(Vec<u32>) -> TestResult);
}
