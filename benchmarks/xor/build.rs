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
use datasketches::xor::XorFilter;
use datasketches::xor::XorFilterBuilder;
use datasketches::xor::XorFilterType;

use crate::xor::hashes;

// Repeated values model a column batch with four rows per distinct hash.
fn repeated_hashes(rows: usize) -> Vec<u64> {
    hashes(rows / 4).into_iter().cycle().take(rows).collect()
}

fn distinct_8(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality);
    bencher
        .bench_local(|| black_box(XorFilter::from_hashes(&keys, XorFilterType::Xor8, 0).unwrap()));
}

fn xorf_distinct_8(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality);
    bencher.bench_local(|| black_box(xorf::Xor8::from(keys.as_slice())));
}

fn deduplicate_8(bencher: Bencher, rows: usize) {
    let keys = repeated_hashes(rows);
    bencher.bench_local(|| {
        let mut builder = XorFilterBuilder::new(XorFilterType::Xor8, 0);
        builder.update_hashes(keys.iter().copied());
        black_box(builder.build().unwrap())
    });
}

fn xorf_deduplicate_8(bencher: Bencher, rows: usize) {
    let keys = repeated_hashes(rows);
    bencher.bench_local(|| {
        // Include the caller's deduplication, as for DataSketches.
        let mut distinct = keys.clone();
        distinct.sort_unstable();
        distinct.dedup();
        black_box(xorf::Xor8::from(distinct.as_slice()))
    });
}

fn distinct_16(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality);
    bencher
        .bench_local(|| black_box(XorFilter::from_hashes(&keys, XorFilterType::Xor16, 0).unwrap()));
}

fn xorf_distinct_16(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality);
    bencher.bench_local(|| black_box(xorf::Xor16::from(keys.as_slice())));
}

fn deduplicate_16(bencher: Bencher, rows: usize) {
    let keys = repeated_hashes(rows);
    bencher.bench_local(|| {
        let mut builder = XorFilterBuilder::new(XorFilterType::Xor16, 0);
        builder.update_hashes(keys.iter().copied());
        black_box(builder.build().unwrap())
    });
}

fn xorf_deduplicate_16(bencher: Bencher, rows: usize) {
    let keys = repeated_hashes(rows);
    bencher.bench_local(|| {
        // Include the caller's deduplication, as for DataSketches.
        let mut distinct = keys.clone();
        distinct.sort_unstable();
        distinct.dedup();
        black_box(xorf::Xor16::from(distinct.as_slice()))
    });
}

fn distinct_32(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality);
    bencher
        .bench_local(|| black_box(XorFilter::from_hashes(&keys, XorFilterType::Xor32, 0).unwrap()));
}

fn xorf_distinct_32(bencher: Bencher, cardinality: usize) {
    let keys = hashes(cardinality);
    bencher.bench_local(|| black_box(xorf::Xor32::from(keys.as_slice())));
}

fn deduplicate_32(bencher: Bencher, rows: usize) {
    let keys = repeated_hashes(rows);
    bencher.bench_local(|| {
        let mut builder = XorFilterBuilder::new(XorFilterType::Xor32, 0);
        builder.update_hashes(keys.iter().copied());
        black_box(builder.build().unwrap())
    });
}

fn xorf_deduplicate_32(bencher: Bencher, rows: usize) {
    let keys = repeated_hashes(rows);
    bencher.bench_local(|| {
        // Include the caller's deduplication, as for DataSketches.
        let mut distinct = keys.clone();
        distinct.sort_unstable();
        distinct.dedup();
        black_box(xorf::Xor32::from(distinct.as_slice()))
    });
}

pub fn register(runner: &mut Runner) {
    for (size, rounds) in [(1_000, 60_000), (16_384, 2_000), (1_000_000, 12)] {
        runner.compare(
            &format!("xor::build::8_distinct/{size}"),
            rounds,
            &[
                ("DataSketches", &|b| distinct_8(b, size)),
                ("xorf", &|b| xorf_distinct_8(b, size)),
            ],
        );
        runner.compare(
            &format!("xor::build::8_deduplicate/{size}"),
            rounds,
            &[
                ("DataSketches", &|b| deduplicate_8(b, size)),
                ("xorf", &|b| xorf_deduplicate_8(b, size)),
            ],
        );
        runner.compare(
            &format!("xor::build::16_distinct/{size}"),
            rounds,
            &[
                ("DataSketches", &|b| distinct_16(b, size)),
                ("xorf", &|b| xorf_distinct_16(b, size)),
            ],
        );
        runner.compare(
            &format!("xor::build::16_deduplicate/{size}"),
            rounds,
            &[
                ("DataSketches", &|b| deduplicate_16(b, size)),
                ("xorf", &|b| xorf_deduplicate_16(b, size)),
            ],
        );
        runner.compare(
            &format!("xor::build::32_distinct/{size}"),
            rounds,
            &[
                ("DataSketches", &|b| distinct_32(b, size)),
                ("xorf", &|b| xorf_distinct_32(b, size)),
            ],
        );
        runner.compare(
            &format!("xor::build::32_deduplicate/{size}"),
            rounds,
            &[
                ("DataSketches", &|b| deduplicate_32(b, size)),
                ("xorf", &|b| xorf_deduplicate_32(b, size)),
            ],
        );
    }
}
