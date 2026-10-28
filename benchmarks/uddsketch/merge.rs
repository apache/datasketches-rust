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
use divan::Bencher;
use divan::black_box;

use super::support::build;
use super::support::values;

#[divan::bench(args = [1, 8, 64, 256])]
fn partials(bencher: Bencher, shards: usize) {
    let values = values(131_072);
    let partials = values
        .chunks(values.len() / shards)
        .map(build)
        .collect::<Vec<_>>();
    bencher.bench_local(|| {
        let mut merged = UddSketch::default();
        for partial in black_box(&partials) {
            merged.merge(partial).unwrap();
        }
        black_box(merged)
    });
}

#[divan::bench(consts = [false, true], args = [8, 64, 16_384])]
fn serialized_partials<const DIRECT: bool>(bencher: Bencher, rows: usize) {
    let partials = (0..64)
        .map(|group| {
            let mut sketch = UddSketch::default();
            for row in 0..rows {
                sketch.update(row as f64 + group as f64 / 64.0).unwrap();
            }
            sketch.serialize()
        })
        .collect::<Vec<_>>();
    bencher.bench_local(|| {
        let mut merged = UddSketch::default();
        for bytes in black_box(&partials) {
            if DIRECT {
                merged.merge_serialized(bytes).unwrap();
            } else {
                merged
                    .merge(&UddSketch::deserialize(bytes).unwrap())
                    .unwrap();
            }
        }
        black_box(merged)
    });
}
