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

use datasketches::ddsketch::DdSketch;
use datasketches::ddsketch::DdSketchBuilder;
use datasketches::ddsketch::MappingKind;
use datasketches::ddsketch::StoreKind;

pub(super) const DEFAULT_ACCURACY: f64 = 0.01;
pub(super) const PARTIAL_GROUPS: usize = 64;
pub(super) const ROWS_PER_PARTIAL: usize = 256;

/// Deterministic heavy-tailed positive latency workload spanning roughly `[1e-3, 1e5]`.
pub(super) fn latency_values(len: usize) -> Vec<f64> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let u = ((state >> 11) as f64 + 1.0) * (1.0 / ((1_u64 << 53) as f64 + 1.0));
            // Log-uniform across 8 orders of magnitude to exercise multi-bucket distributions.
            1e-3 * 1e8_f64.powf(u)
        })
        .collect()
}

pub(super) fn build_sketch(values: &[f64], mapping: MappingKind, store: StoreKind) -> DdSketch {
    let mut sketch = DdSketchBuilder::new(DEFAULT_ACCURACY)
        .mapping(mapping)
        .store(store)
        .build()
        .unwrap();
    for &value in values {
        sketch.update(value);
    }
    sketch
}

pub(super) fn prepared_sketch() -> DdSketch {
    build_sketch(
        &latency_values(100_000),
        MappingKind::Cubic,
        StoreKind::default(),
    )
}

pub(super) fn partial_sketches() -> Vec<DdSketch> {
    let values = latency_values(PARTIAL_GROUPS * ROWS_PER_PARTIAL);
    values
        .chunks_exact(ROWS_PER_PARTIAL)
        .map(|chunk| build_sketch(chunk, MappingKind::Cubic, StoreKind::default()))
        .collect()
}
