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

use datasketches::uddsketch::UddSketch;
use quickcheck::QuickCheck;

use super::build;
use super::shuffle;

fn assert_equivalent(values: &[f64], limit: u32, alpha: f64, seed: u64) {
    let expected = build(values, limit, alpha);
    if !values.is_empty() {
        let mut sorted = values.to_vec();
        sorted.sort_by(f64::total_cmp);
        for q in [0.25, 0.5, 0.75] {
            let exact = sorted[(q * (sorted.len() - 1) as f64) as usize];
            let estimate = expected.quantile(q).unwrap().unwrap();
            if exact == 0.0 {
                assert_eq!(estimate, 0.0);
            } else {
                assert!(((estimate - exact) / exact).abs() <= expected.relative_error());
            }
        }
    }
    let mut reordered = values.to_vec();
    shuffle(&mut reordered, seed);
    assert_eq!(
        build(&reordered, limit, alpha).serialize(),
        expected.serialize()
    );
    for shards in [1, 2, 7, 31] {
        let mut partials = (0..shards)
            .map(|_| UddSketch::new(limit, alpha).unwrap())
            .collect::<Vec<_>>();
        for (i, &value) in reordered.iter().enumerate() {
            partials[i % shards].update(value).unwrap();
        }
        let mut linear = UddSketch::new(limit, alpha).unwrap();
        let mut serialized = UddSketch::new(limit, alpha).unwrap();
        for partial in partials.iter().rev() {
            linear.merge(partial).unwrap();
            serialized.merge_serialized(&partial.serialize()).unwrap();
            assert_eq!(serialized.serialize(), linear.serialize());
        }
        assert_eq!(linear.serialize(), expected.serialize());
        while partials.len() > 1 {
            let tree_seed = seed.wrapping_add(partials.len() as u64);
            shuffle(&mut partials, tree_seed);
            let right = partials.pop().unwrap();
            let left = partials.last_mut().unwrap();
            left.merge_serialized(&right.serialize()).unwrap();
            *left = UddSketch::deserialize(&left.serialize()).unwrap();
        }
        assert_eq!(partials[0].serialize(), expected.serialize());
        assert_eq!(
            partials[0]
                .quantiles(&[0.0, 0.5, 0.9, 0.99, 0.999, 1.0])
                .unwrap(),
            expected
                .quantiles(&[0.0, 0.5, 0.9, 0.99, 0.999, 1.0])
                .unwrap()
        );
    }
}

#[test]
fn floating_boundary_regression() {
    // Re-evaluating logarithms after collapse can disagree with the fixed integer hierarchy.
    let controls = [
        1.0100505037878156,
        1.0304555644703977,
        1.0512728486011127,
        1.0725106839263876,
    ];
    let target = 1.127_501_361_846_421;
    let mut values = controls.to_vec();
    values.push(target);
    assert_equivalent(&values, 3, 0.01, 42);
    let mut first = build(&[target], 3, 0.01);
    let control = build(&controls, 3, 0.01);
    first.merge(&control).unwrap();
    assert_eq!(first.serialize(), build(&values, 3, 0.01).serialize());
}

#[test]
fn hierarchy_boundaries_and_extreme_values() {
    for alpha in [1e-9, 0.001, 0.01, 0.5, 0.999] {
        let step = f64::ln_1p(alpha) - f64::ln_1p(-alpha);
        let mut values = vec![
            0.0,
            -0.0,
            f64::MAX,
            -f64::MAX,
            f64::MIN_POSITIVE,
            -f64::MIN_POSITIVE,
        ];
        for exponent in [-1000, -100, -4, -1, 0, 1, 2, 3, 6, 100, 1000] {
            let boundary = (step * f64::from(exponent)).exp();
            if boundary.is_normal() {
                for bits in [
                    boundary.to_bits() - 1,
                    boundary.to_bits(),
                    boundary.to_bits() + 1,
                ] {
                    let value = f64::from_bits(bits);
                    if value.is_finite() {
                        values.extend([value, -value]);
                    }
                }
            }
        }
        for bits in 1..128 {
            values.extend([f64::from_bits(bits), -f64::from_bits(bits)]);
        }
        for limit in [3, 7, 32, 512] {
            assert_equivalent(&values, limit, alpha, 103);
        }
    }
}

#[test]
fn arbitrary_multisets_and_merge_trees() {
    fn property(bits: Vec<u64>, seed: u64) -> bool {
        let values = bits
            .into_iter()
            .map(f64::from_bits)
            .filter(|x| x.is_finite())
            .collect::<Vec<_>>();
        assert_equivalent(&values, 3 + (seed % 40) as u32, 0.01, seed.max(1));
        true
    }
    QuickCheck::new()
        .tests(200)
        .quickcheck(property as fn(Vec<u64>, u64) -> bool);
}
