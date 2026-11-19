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

use std::hint::black_box;

use benchmarks::Bencher;
use benchmarks::Runner;
use datasketches::cpc::CpcSketch;
use datasketches::cpc::CpcUnion;

fn partial(lg_k: u8, first: u64, items: u64) -> CpcSketch {
    let mut sketch = CpcSketch::new(lg_k).unwrap();
    for value in first..first + items {
        sketch.update(value);
    }
    sketch
}

fn empty_from_sliding(bencher: Bencher, lg_k: u8) {
    // Reuse one Sliding input across rounds so each fresh union measures its first merge.
    let source = partial(lg_k, 0, 32_u64 << lg_k);

    bencher.bench_local(|| {
        let mut union = CpcUnion::new(lg_k).unwrap();
        union.update(black_box(&source)).unwrap();
        union
    });
}

fn merge_partials(bencher: Bencher, src_lg_k: u8, dst_lg_k: u8) {
    let k = 1_u64 << src_lg_k;
    // Uneven partitions produce sparse, hybrid, pinned, and sliding inputs in the same union.
    let mut first = 0;
    let partials = [k / 64, k / 4, 2 * k, 32 * k].map(|items| {
        let sketch = partial(src_lg_k, first, items);
        first += items;
        sketch
    });
    bencher.bench_local(|| {
        let mut union = CpcUnion::new(dst_lg_k).unwrap();
        for sketch in black_box(&partials) {
            union.update(sketch).unwrap();
        }
        union
    });
}

fn merge_serialized_partials(bencher: Bencher, items_per_partition: u64) {
    const LG_K: u8 = 21;
    // A distributed group receives encoded partial states and emits an encoded result.
    let partials: Vec<_> = (0..4)
        .map(|i| partial(LG_K, i * items_per_partition, items_per_partition).serialize())
        .collect();
    bencher.bench_local(|| {
        let mut union = CpcUnion::new(LG_K).unwrap();
        for bytes in black_box(&partials) {
            union
                .update(&CpcSketch::deserialize(bytes).unwrap())
                .unwrap();
        }
        union.to_sketch().serialize()
    });
}

pub fn register(runner: &mut Runner) {
    for (lg_k, rounds) in [(11, 230_000), (14, 26_000)] {
        runner.case(
            format!("cpc::union::empty_from_sliding/{lg_k}"),
            rounds,
            |b| empty_from_sliding(b, lg_k),
        );
    }
    for (src_lg_k, dst_lg_k, rounds) in [(11, 11, 100_000), (14, 11, 11_000)] {
        runner.case(
            format!("cpc::union::merge_partials/{src_lg_k}_into_{dst_lg_k}"),
            rounds,
            |b| merge_partials(b, src_lg_k, dst_lg_k),
        );
    }
    for (items, rounds) in [(4_096, 550), (262_144, 13)] {
        runner.case(
            format!("cpc::union::merge_serialized_partials/lg21/{items}"),
            rounds,
            |b| merge_serialized_partials(b, items),
        );
    }
}
