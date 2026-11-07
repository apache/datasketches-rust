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
use datasketches::hll::HllSketch;
use datasketches::hll::HllType;
use datasketches::hll::HllUnion;

fn partials(lg_k: u8, count: usize, rows: usize) -> Vec<HllSketch> {
    (0..count)
        .map(|partial| {
            let mut sketch = HllSketch::new(lg_k, HllType::Hll8).unwrap();
            for value in partial * rows..(partial + 1) * rows {
                sketch.update(value as u64);
            }
            sketch
        })
        .collect()
}

fn empty_from_dense(bencher: Bencher, lg_k: u8) {
    let source = partials(lg_k, 1, 1 << lg_k).pop().unwrap();
    bencher.bench_local(|| {
        let mut union = HllUnion::new(lg_k).unwrap();
        union.update(black_box(&source));
        black_box(union)
    });
}

fn recreated_small_partials(bencher: Bencher, lg_k: u8) {
    let partials = partials(lg_k, 400, 200);
    bencher.bench_local(|| {
        let mut sketch = HllSketch::new(lg_k, HllType::Hll8).unwrap();
        for partial in black_box(&partials) {
            let mut union = HllUnion::new(lg_k).unwrap();
            union.update(&sketch);
            union.update(partial);
            sketch = union.to_sketch(HllType::Hll8);
        }
        black_box(sketch.estimate());
        black_box(sketch)
    });
}

fn persistent_small_partials(bencher: Bencher, lg_k: u8) {
    let partials = partials(lg_k, 400, 200);
    bencher.bench_local(|| {
        let mut union = HllUnion::new(lg_k).unwrap();
        for partial in black_box(&partials) {
            union.update(partial);
        }
        black_box(union.estimate());
        black_box(union.to_sketch(HllType::Hll8))
    });
}

fn dense_partials(bencher: Bencher, lg_k: u8) {
    let partials = partials(lg_k, 16, (1 << lg_k) / 4);
    bencher.bench_local(|| {
        let mut union = HllUnion::new(lg_k).unwrap();
        for partial in black_box(&partials) {
            union.update(partial);
        }
        black_box(union.estimate());
        black_box(union.to_sketch(HllType::Hll8))
    });
}

fn dense_partials_with_queries(bencher: Bencher, lg_k: u8) {
    let partials = partials(lg_k, 16, (1 << lg_k) / 4);
    bencher.bench_local(|| {
        let mut union = HllUnion::new(lg_k).unwrap();
        for partial in black_box(&partials) {
            union.update(partial);
            black_box(union.estimate());
        }
        black_box(union.to_sketch(HllType::Hll8))
    });
}

fn downsampled_dense_partials(bencher: Bencher, lg_k: u8) {
    let partials = partials(lg_k + 1, 16, (1 << lg_k) / 2);
    bencher.bench_local(|| {
        let mut union = HllUnion::new(lg_k).unwrap();
        for partial in black_box(&partials) {
            union.update(partial);
        }
        black_box(union.estimate());
        black_box(union.to_sketch(HllType::Hll8))
    });
}

pub fn register(runner: &mut Runner) {
    for &(lg_k, rounds) in &[(12, 2_000_000), (18, 70_000)] {
        runner.case(
            format!("hll::union::empty_from_dense/{lg_k:?}"),
            rounds,
            |b| empty_from_dense(b, lg_k),
        );
    }
    for &(lg_k, rounds) in &[(12, 500), (18, 90)] {
        runner.case(
            format!("hll::union::recreated_small_partials/{lg_k:?}"),
            rounds,
            |b| recreated_small_partials(b, lg_k),
        );
    }
    for &(lg_k, rounds) in &[(12, 500), (18, 300)] {
        runner.case(
            format!("hll::union::persistent_small_partials/{lg_k:?}"),
            rounds,
            |b| persistent_small_partials(b, lg_k),
        );
    }
    for &(lg_k, rounds) in &[(12, 80_000), (18, 700)] {
        runner.case(
            format!("hll::union::dense_partials/{lg_k:?}"),
            rounds,
            |b| dense_partials(b, lg_k),
        );
    }
    for &(lg_k, rounds) in &[(12, 7_000), (18, 60)] {
        runner.case(
            format!("hll::union::dense_partials_with_queries/{lg_k:?}"),
            rounds,
            |b| dense_partials_with_queries(b, lg_k),
        );
    }
    for &(lg_k, rounds) in &[(12, 6_000), (18, 80)] {
        runner.case(
            format!("hll::union::downsampled_dense_partials/{lg_k:?}"),
            rounds,
            |b| downsampled_dense_partials(b, lg_k),
        );
    }
}
