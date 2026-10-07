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

use datasketches::req::RankAccuracy;
use datasketches::req::ReqFloat;
use datasketches::req::ReqSketch;
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;

pub(super) const DEFAULT_K: u16 = 12;

pub(super) fn prepared_sketch() -> ReqSketch<ReqFloat<f64>> {
    build_sketch(&values(100_000))
}

pub(super) fn build_sketch(values: &[ReqFloat<f64>]) -> ReqSketch<ReqFloat<f64>> {
    let mut sketch = ReqSketch::new(DEFAULT_K, RankAccuracy::HighRank).unwrap();
    for value in values {
        sketch.update(*value);
    }
    sketch
}

/// Uniform values in `[0, 1_000_000)`, generated with a fixed seed for
/// reproducible benchmarks.
pub(super) fn values(len: usize) -> Vec<ReqFloat<f64>> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..len)
        .map(|_| ReqFloat::<f64>::new(rng.random_range(0.0..1_000_000.0)).unwrap())
        .collect()
}
