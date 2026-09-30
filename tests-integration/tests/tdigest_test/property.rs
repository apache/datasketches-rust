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

fn assert_quantiles_are_monotonic(tdigest: &mut TDigestMut) {
    let min = tdigest.min_value().unwrap();
    let max = tdigest.max_value().unwrap();
    let mut previous = min;

    for step in 0..=RANK_STEPS {
        let rank = step as f64 / RANK_STEPS as f64;
        let quantile = tdigest.quantile(rank).unwrap().unwrap();
        assert!(
            (previous..=max).contains(&quantile),
            "quantile {quantile} at rank {rank} is outside [{previous}, {max}]"
        );
        previous = quantile;
    }
}

#[test]
fn prop_quantile_is_non_decreasing_and_within_the_observed_range() {
    fn property(values: Vec<u32>) -> TestResult {
        if !(500..1500).contains(&values.len()) {
            return TestResult::discard();
        }

        assert_quantiles_are_monotonic(&mut digest_of(&values));

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

        let mut tdigest = digest_of(&left);
        tdigest.merge(&digest_of(&right));
        assert_quantiles_are_monotonic(&mut tdigest);

        TestResult::passed()
    }

    QuickCheck::new()
        .tests(64)
        .min_tests_passed(64)
        .rng(Gen::new(900))
        .quickcheck(property as fn(Vec<u32>, Vec<u32>) -> TestResult);
}
