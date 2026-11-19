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

fn empty_from_sliding(bencher: Bencher, lg_k: u8) {
    // Reuse one Sliding input across rounds so each fresh union measures its first merge.
    let mut source = CpcSketch::new(lg_k).unwrap();
    for value in 0..(32_u64 << lg_k) {
        source.update(value);
    }

    bencher.bench_local(|| {
        let mut union = CpcUnion::new(lg_k).unwrap();
        union.update(black_box(&source)).unwrap();
        union
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
}
