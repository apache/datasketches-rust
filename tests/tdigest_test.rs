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

use datasketches::tdigest::TDigest;

#[test]
fn test_empty() {
    let mut tdigest = TDigest::new(10);
    assert!(tdigest.is_empty());
    assert_eq!(tdigest.k(), 10);
    assert_eq!(tdigest.total_weight(), 0);
    assert_eq!(tdigest.min_value(), None);
    assert_eq!(tdigest.max_value(), None);
    assert_eq!(tdigest.get_rank(0.0), None);
    assert_eq!(tdigest.get_quantile(0.5), None);

    // TODO: Support PMF and CDF
    // const double split_points[1] {0};
    // REQUIRE_THROWS_AS(td.get_PMF(split_points, 1), std::runtime_error);
    // REQUIRE_THROWS_AS(td.get_CDF(split_points, 1), std::runtime_error);
}

#[test]
fn test_one_value() {
    let mut tdigest = TDigest::new(100);
    tdigest.update(1.0);
    assert_eq!(tdigest.k(), 100);
    assert_eq!(tdigest.total_weight(), 1);
    assert_eq!(tdigest.min_value(), Some(1.0));
    assert_eq!(tdigest.max_value(), Some(1.0));
    assert_eq!(tdigest.get_rank(0.99), Some(0.0));
    assert_eq!(tdigest.get_rank(1.0), Some(0.5));
    assert_eq!(tdigest.get_rank(1.01), Some(1.0));
    assert_eq!(tdigest.get_quantile(0.0), Some(1.0));
    assert_eq!(tdigest.get_quantile(0.5), Some(1.0));
    assert_eq!(tdigest.get_quantile(1.0), Some(1.0));
}

#[test]
fn test_many_values() {
    // TODO: Later until PMF and CDF are supported
    // const size_t n = 10000;
    // tdigest_double td;
    // for (size_t i = 0; i < n; ++i) td.update(i);
    // REQUIRE_FALSE(td.is_empty());
    // REQUIRE(td.get_total_weight() == n);
    // REQUIRE(td.get_min_value() == 0);
    // REQUIRE(td.get_max_value() == n - 1);
    // REQUIRE(td.get_rank(0) == Approx(0).margin(0.0001));
    // REQUIRE(td.get_rank(n / 4) == Approx(0.25).margin(0.0001));
    // REQUIRE(td.get_rank(n / 2) == Approx(0.5).margin(0.0001));
    // REQUIRE(td.get_rank(n * 3 / 4) == Approx(0.75).margin(0.0001));
    // REQUIRE(td.get_rank(n) == 1);
    // REQUIRE(td.get_quantile(0) == 0);
    // REQUIRE(td.get_quantile(0.5) == Approx(n / 2).epsilon(0.03));
    // REQUIRE(td.get_quantile(0.9) == Approx(n * 0.9).epsilon(0.01));
    // REQUIRE(td.get_quantile(0.95) == Approx(n * 0.95).epsilon(0.01));
    // REQUIRE(td.get_quantile(1) == n - 1);
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
    let mut tdigest = TDigest::new(100);
    tdigest.update(1.0);
    tdigest.update(2.0);
    assert_eq!(tdigest.get_rank(0.99), Some(0.0));
    assert_eq!(tdigest.get_rank(1.0), Some(0.25));
    assert_eq!(tdigest.get_rank(1.25), Some(0.375));
    assert_eq!(tdigest.get_rank(1.5), Some(0.5));
    assert_eq!(tdigest.get_rank(1.75), Some(0.625));
    assert_eq!(tdigest.get_rank(2.0), Some(0.75));
    assert_eq!(tdigest.get_rank(2.01), Some(1.0));
}

#[test]
fn test_rank_repeated_values() {
    let mut tdigest = TDigest::new(100);
    tdigest.update(1.0);
    tdigest.update(1.0);
    tdigest.update(1.0);
    tdigest.update(1.0);
    assert_eq!(tdigest.get_rank(0.99), Some(0.0));
    assert_eq!(tdigest.get_rank(1.0), Some(0.5));
    assert_eq!(tdigest.get_rank(1.01), Some(1.0));
}

#[test]
fn test_repeated_blocks() {
    let mut tdigest = TDigest::new(100);
    tdigest.update(1.0);
    tdigest.update(2.0);
    tdigest.update(2.0);
    tdigest.update(3.0);
    assert_eq!(tdigest.get_rank(0.99), Some(0.0));
    assert_eq!(tdigest.get_rank(1.0), Some(0.125));
    assert_eq!(tdigest.get_rank(2.0), Some(0.5));
    assert_eq!(tdigest.get_rank(3.0), Some(0.875));
    assert_eq!(tdigest.get_rank(3.01), Some(1.0));
}
