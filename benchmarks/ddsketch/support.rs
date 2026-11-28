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

/// Deterministic positive non-dyadic fractional weights so `varfloat64` exercises full mantissa
/// encoding rather than short integer counts.
pub(super) fn fractional_weights(len: usize) -> Vec<f64> {
    let mut state = 0xd1b5_4a32_d192_ed03_u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state % 9) + 1) as f64 * 0.1 + 1.0 / ((state % 13) + 3) as f64
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

pub(super) fn build_weighted_u64_sketch(
    values: &[f64],
    weights: &[u64],
    mapping: MappingKind,
    store: StoreKind,
) -> DdSketch<u64> {
    let mut sketch = DdSketchBuilder::new(DEFAULT_ACCURACY)
        .mapping(mapping)
        .store(store)
        .build::<u64>()
        .unwrap();
    for (&value, &weight) in values.iter().zip(weights) {
        sketch.update_with_weight(value, weight);
    }
    sketch
}

pub(super) fn build_weighted_f64_sketch(
    values: &[f64],
    weights: &[f64],
    mapping: MappingKind,
    store: StoreKind,
) -> DdSketch<f64> {
    let mut sketch = DdSketchBuilder::new(DEFAULT_ACCURACY)
        .mapping(mapping)
        .store(store)
        .build::<f64>()
        .unwrap();
    for (&value, &weight) in values.iter().zip(weights) {
        sketch.update_with_weight(value, weight);
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

pub(super) fn prepared_f64_sketch() -> DdSketch<f64> {
    let values = latency_values(100_000);
    let weights = fractional_weights(100_000);
    build_weighted_f64_sketch(&values, &weights, MappingKind::Cubic, StoreKind::default())
}

pub(super) fn partial_sketches() -> Vec<DdSketch> {
    let values = latency_values(PARTIAL_GROUPS * ROWS_PER_PARTIAL);
    values
        .chunks_exact(ROWS_PER_PARTIAL)
        .map(|chunk| build_sketch(chunk, MappingKind::Cubic, StoreKind::default()))
        .collect()
}

pub(super) fn partial_f64_sketches() -> Vec<DdSketch<f64>> {
    let total = PARTIAL_GROUPS * ROWS_PER_PARTIAL;
    let values = latency_values(total);
    let weights = fractional_weights(total);
    values
        .chunks_exact(ROWS_PER_PARTIAL)
        .zip(weights.chunks_exact(ROWS_PER_PARTIAL))
        .map(|(v_chunk, w_chunk)| {
            build_weighted_f64_sketch(v_chunk, w_chunk, MappingKind::Cubic, StoreKind::default())
        })
        .collect()
}
