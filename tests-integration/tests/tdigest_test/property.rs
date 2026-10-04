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
    let ranks = (0..=RANK_STEPS)
        .map(|step| step as f64 / RANK_STEPS as f64)
        .collect::<Vec<_>>();
    let quantiles = tdigest.quantiles(&ranks).unwrap();

    for (&rank, quantile) in ranks.iter().zip(quantiles) {
        assert_eq!(tdigest.quantile(rank), Some(quantile));
        assert!(
            (previous..=max).contains(&quantile),
            "quantile {quantile} at rank {rank} is outside [{previous}, {max}]"
        );
        previous = quantile;
    }
}

#[test]
fn prop_finite_values_survive_partial_aggregation() {
    fn property(bits: Vec<u64>) -> TestResult {
        let mut values: Vec<_> = bits
            .into_iter()
            .map(f64::from_bits)
            .filter(|value| value.is_finite())
            .collect();
        if values.is_empty() {
            return TestResult::discard();
        }
        let mut digest = TDigestMut::default();
        for chunk in values.chunks(64) {
            let mut partial = TDigestMut::default();
            for &value in chunk {
                partial.update(value);
            }
            digest.merge(&TDigestMut::deserialize(&partial.serialize()).unwrap());
        }
        let mut digest = TDigestMut::deserialize(&digest.serialize()).unwrap();
        assert_eq!(digest.total_weight(), values.len() as u64);
        assert_quantiles_are_monotonic(&mut digest);

        // Use the input samples as split points, independently of the computed quantiles.
        values.sort_unstable_by(f64::total_cmp);
        values.dedup();
        assert_eq!(digest.quantile(0.), values.first().copied());
        assert_eq!(digest.quantile(1.), values.last().copied());
        let cdf = digest.cdf(&values).unwrap();
        assert!(cdf.is_sorted());
        assert!(cdf.iter().all(|rank| (0.0..=1.0).contains(rank)));
        let pmf = digest.pmf(&values).unwrap();
        assert!(pmf.iter().all(|p| (0.0..=1.0).contains(p)));
        assert!((pmf.iter().sum::<f64>() - 1.).abs() < 1e-12);
        TestResult::passed()
    }

    QuickCheck::new()
        .tests(128)
        .min_tests_passed(128)
        .rng(Gen::new(512))
        .quickcheck(property as fn(Vec<u64>) -> TestResult);
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

#[test]
fn prop_batch_quantiles_preserve_input_order_and_duplicates() {
    fn property(values: Vec<u32>, ranks: Vec<u8>) {
        let tdigest = digest_of(&values);
        let frozen = tdigest.clone().freeze();
        let mut ranks = ranks
            .into_iter()
            .map(|rank| f64::from(rank) / f64::from(u8::MAX))
            .collect::<Vec<_>>();
        if let Some(&rank) = ranks.first() {
            ranks.push(rank);
        }
        ranks.extend_from_slice(&[1.0, 0.0, -0.0]);

        for sorted in [false, true] {
            if sorted {
                ranks.sort_unstable_by(f64::total_cmp);
            }
            let expected = if frozen.is_empty() {
                None
            } else {
                Some(
                    ranks
                        .iter()
                        .map(|&rank| frozen.quantile(rank).unwrap())
                        .collect::<Vec<_>>(),
                )
            };
            assert_eq!(tdigest.clone().quantiles(&ranks), expected);
            assert_eq!(frozen.quantiles(&ranks), expected);
        }
    }

    QuickCheck::new()
        .tests(256)
        .rng(Gen::new(1200))
        .quickcheck(property as fn(Vec<u32>, Vec<u8>));
}
