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

use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

use super::support::build_mut_digest;
use super::support::values;

#[divan::bench]
fn initial_buffer(bencher: Bencher) {
    // The default k=200 digest buffers 1,640 values before automatic compression.
    let values = values(1_640);
    let digest = build_mut_digest(&values);

    bencher
        .counter(ItemsCount::new(values.len()))
        .with_inputs(|| digest.clone())
        .bench_local_values(|mut digest| black_box(digest.rank(0.5)));
}

#[divan::bench]
fn unmerged_tail(bencher: Bencher) {
    let values = values(3_280);
    let mut digest = build_mut_digest(&values[..1_640]);
    black_box(digest.rank(0.5).unwrap().unwrap());
    for &value in &values[1_640..] {
        digest.update(value);
    }

    bencher
        .counter(ItemsCount::new(1_640_usize))
        .with_inputs(|| digest.clone())
        .bench_local_values(|mut digest| black_box(digest.rank(0.5)));
}

#[divan::bench]
fn freeze_initial_buffer(bencher: Bencher) {
    let values = values(1_640);
    let digest = build_mut_digest(&values);

    bencher
        .counter(ItemsCount::new(values.len()))
        .with_inputs(|| digest.clone())
        .bench_local_values(|digest| black_box(digest.freeze()));
}
