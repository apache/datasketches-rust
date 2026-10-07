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

use datasketches::kll::KllFloat;
use datasketches::kll::KllSketch;
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;

pub(super) const DEFAULT_K: u16 = 200;

pub(super) fn values(len: usize) -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..len)
        .map(|_| rng.random_range(0.0..1_000_000.0))
        .collect()
}

pub(super) fn build_sketch(values: &[f64]) -> KllSketch<KllFloat<f64>> {
    let mut sketch = KllSketch::new(DEFAULT_K).unwrap();
    for &value in values {
        sketch.update(KllFloat::<f64>::new(value).unwrap());
    }
    sketch
}

pub(super) fn prepared_sketch() -> KllSketch<KllFloat<f64>> {
    build_sketch(&values(100_000))
}
