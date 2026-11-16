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

use datasketches::density::DensitySketch;
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;

pub(super) const DEFAULT_K: u16 = 64;

// Coordinates are scaled by 1/sqrt(dim) so pairwise squared distances stay O(1) across dimensions,
// exercising the non-trivial Gaussian kernel range instead of underflowing to zero.
pub(super) fn points(len: usize, dim: u32) -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(42);
    let scale = 1.0 / (dim as f64).sqrt();
    let count = len * dim as usize;
    (0..count)
        .map(|_| rng.random_range(-scale..scale))
        .collect()
}

pub(super) fn query_point(dim: u32) -> Vec<f64> {
    vec![0.0; dim as usize]
}

pub(super) fn build_sketch(points: &[f64], dim: u32) -> DensitySketch<f64> {
    let mut sketch = DensitySketch::<f64>::new(DEFAULT_K, dim).unwrap();
    for point in points.chunks_exact(dim as usize) {
        sketch.update(point).unwrap();
    }
    sketch
}

pub(super) fn prepared_sketch(len: usize, dim: u32) -> DensitySketch<f64> {
    build_sketch(&points(len, dim), dim)
}
