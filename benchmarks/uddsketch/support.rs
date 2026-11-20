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
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;

pub(super) const DEFAULT_ALPHA: f64 = 0.01;

/// Generates a signed heavy-tailed lognormal-like stream spanning multiple orders of magnitude so
/// bucket indexing and uniform collapse are both exercised.
pub(super) fn values(len: usize) -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..len)
        .map(|i| {
            let normal_approx: f64 = (0..4).map(|_| rng.random_range(-3.0..3.0)).sum();
            let mag = (normal_approx * 1.2).exp();
            if i % 5 == 0 { -mag } else { mag }
        })
        .collect()
}

pub(super) fn build_sketch(values: &[f64], max_buckets: u32) -> UddSketch {
    let mut sketch = UddSketch::new(max_buckets, DEFAULT_ALPHA).unwrap();
    for &v in values {
        sketch.update(v);
    }
    sketch
}

pub(super) fn prepared_sketch(len: usize, max_buckets: u32) -> UddSketch {
    build_sketch(&values(len), max_buckets)
}
