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

use datasketches::tdigest::TDigestMut;
use googletest::assert_that;
use googletest::prelude::{eq, near};

#[test]
fn test_empty() {
    let mut tdigest = TDigestMut::new(10);
    assert!(tdigest.is_empty());
    assert_eq!(tdigest.k(), 10);
    assert_eq!(tdigest.total_weight(), 0);
    assert_eq!(tdigest.min_value(), None);
    assert_eq!(tdigest.max_value(), None);
    assert_eq!(tdigest.rank(0.0), None);
    assert_eq!(tdigest.quantile(0.5), None);

    // TODO: Support PMF and CDF
    // const double split_points[1] {0};
    // REQUIRE_THROWS_AS(td.get_PMF(split_points, 1), std::runtime_error);
    // REQUIRE_THROWS_AS(td.get_CDF(split_points, 1), std::runtime_error);
}

#[test]
fn test_one_value() {
    let mut tdigest = TDigestMut::new(100);
    tdigest.update(1.0);
    assert_eq!(tdigest.k(), 100);
    assert_eq!(tdigest.total_weight(), 1);
    assert_eq!(tdigest.min_value(), Some(1.0));
    assert_eq!(tdigest.max_value(), Some(1.0));
    assert_eq!(tdigest.rank(0.99), Some(0.0));
    assert_eq!(tdigest.rank(1.0), Some(0.5));
    assert_eq!(tdigest.rank(1.01), Some(1.0));
    assert_eq!(tdigest.quantile(0.0), Some(1.0));
    assert_eq!(tdigest.quantile(0.5), Some(1.0));
    assert_eq!(tdigest.quantile(1.0), Some(1.0));
}

#[test]
fn test_many_values() {
    let n = 10000;

    let mut tdigest = TDigestMut::default();
    for i in 0..n {
        tdigest.update(i as f64);
    }

    assert!(!tdigest.is_empty());
    assert_eq!(tdigest.total_weight(), n);
    assert_eq!(tdigest.min_value(), Some(0.0));
    assert_eq!(tdigest.max_value(), Some((n - 1) as f64));

    assert_that!(tdigest.rank(0.0).unwrap(), near(0.0, 0.0001));
    assert_that!(tdigest.rank((n / 4) as f64).unwrap(), near(0.25, 0.0001));
    assert_that!(tdigest.rank((n / 2) as f64).unwrap(), near(0.5, 0.0001));
    assert_that!(
        tdigest.rank((n * 3 / 4) as f64).unwrap(),
        near(0.75, 0.0001)
    );
    assert_that!(tdigest.rank(n as f64).unwrap(), eq(1.0));
    assert_that!(tdigest.quantile(0.0).unwrap(), eq(0.0));
    assert_that!(
        tdigest.quantile(0.5).unwrap(),
        near((n / 2) as f64, 0.03 * (n / 2) as f64)
    );
    assert_that!(
        tdigest.quantile(0.9).unwrap(),
        near((n as f64) * 0.9, 0.01 * (n as f64) * 0.9)
    );
    assert_that!(
        tdigest.quantile(0.95).unwrap(),
        near((n as f64) * 0.95, 0.01 * (n as f64) * 0.95)
    );
    assert_that!(tdigest.quantile(1.0).unwrap(), eq((n - 1) as f64));

    // TODO: Later until PMF and CDF are supported
    // const double split_points[1] {n / 2};
    // const auto pmf = td.get_PMF(split_points, 1);
    // REQUIRE(pmf.size() == 2);
    // REQUIRE(pmf[0] == Approx(0.5).margin(0.0001));
    // REQUIRE(pmf[1] == Approx(0.5).margin(0.0001));
    // const auto cdf = td.get_CDF(split_points, 1);
    // REQUIRE(cdf.size() == 2);
    // REQUIRE(cdf[0] == Approx(0.5).margin(0.0001));
    // REQUIRE(cdf[1] == 1);
}

#[test]
fn test_rank_two_values() {
    let mut tdigest = TDigestMut::new(100);
    tdigest.update(1.0);
    tdigest.update(2.0);
    assert_eq!(tdigest.rank(0.99), Some(0.0));
    assert_eq!(tdigest.rank(1.0), Some(0.25));
    assert_eq!(tdigest.rank(1.25), Some(0.375));
    assert_eq!(tdigest.rank(1.5), Some(0.5));
    assert_eq!(tdigest.rank(1.75), Some(0.625));
    assert_eq!(tdigest.rank(2.0), Some(0.75));
    assert_eq!(tdigest.rank(2.01), Some(1.0));
}

#[test]
fn test_rank_repeated_values() {
    let mut tdigest = TDigestMut::new(100);
    tdigest.update(1.0);
    tdigest.update(1.0);
    tdigest.update(1.0);
    tdigest.update(1.0);
    assert_eq!(tdigest.rank(0.99), Some(0.0));
    assert_eq!(tdigest.rank(1.0), Some(0.5));
    assert_eq!(tdigest.rank(1.01), Some(1.0));
}

#[test]
fn test_repeated_blocks() {
    let mut tdigest = TDigestMut::new(100);
    tdigest.update(1.0);
    tdigest.update(2.0);
    tdigest.update(2.0);
    tdigest.update(3.0);
    assert_eq!(tdigest.rank(0.99), Some(0.0));
    assert_eq!(tdigest.rank(1.0), Some(0.125));
    assert_eq!(tdigest.rank(2.0), Some(0.5));
    assert_eq!(tdigest.rank(3.0), Some(0.875));
    assert_eq!(tdigest.rank(3.01), Some(1.0));
}

#[test]
fn test_merge_small() {
    let mut td1 = TDigestMut::new(10);
    td1.update(1.0);
    td1.update(2.0);
    let mut td2 = TDigestMut::new(10);
    td2.update(2.0);
    td2.update(3.0);
    td1.merge(&td2);
    assert_eq!(td1.min_value(), Some(1.0));
    assert_eq!(td1.max_value(), Some(3.0));
    assert_eq!(td1.total_weight(), 4);
    assert_eq!(td1.rank(0.99), Some(0.0));
    assert_eq!(td1.rank(1.0), Some(0.125));
    assert_eq!(td1.rank(2.0), Some(0.5));
    assert_eq!(td1.rank(3.0), Some(0.875));
    assert_eq!(td1.rank(3.01), Some(1.0));
}

#[test]
fn test_merge_large() {
    let n = 10000;

    let mut td1 = TDigestMut::new(10);
    let mut td2 = TDigestMut::new(10);
    let sup = n / 2;
    for i in 0..sup {
        td1.update(i as f64);
        td2.update((sup + i) as f64);
    }
    td1.merge(&td2);

    assert_eq!(td1.total_weight(), n);
    assert_eq!(td1.min_value(), Some(0.0));
    assert_eq!(td1.max_value(), Some((n - 1) as f64));

    assert_that!(td1.rank(0.0).unwrap(), near(0.0, 0.0001));
    assert_that!(td1.rank((n / 4) as f64).unwrap(), near(0.25, 0.0001));
    assert_that!(td1.rank((n / 2) as f64).unwrap(), near(0.5, 0.0001));
    assert_that!(td1.rank((n * 3 / 4) as f64).unwrap(), near(0.75, 0.0001));
    assert_that!(td1.rank(n as f64).unwrap(), eq(1.0));
}
