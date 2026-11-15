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
use datasketches::tdigest::TDigestMut;

use super::support::build_mut_digest;
use super::support::values;

// ((partial states, values per state), rounds). Keep the work equal across merge strategies.
const SERIALIZED_INPUTS: &[((usize, usize), usize)] = &[
    ((1, 8), 2_000_000),
    ((64, 8), 9_000),
    ((512, 8), 800),
    ((64, 64), 5_000),
    ((512, 64), 300),
    ((64, 4_096), 2_000),
];

fn serialized_borrowed(bencher: Bencher, shape: (usize, usize)) {
    let (num_partials, rows_per_partial) = shape;
    let partials = serialize_partials(&values(num_partials * rows_per_partial), rows_per_partial);

    bencher.bench_local(|| {
        let mut merged = TDigestMut::default();
        for bytes in black_box(&partials) {
            merged.merge(&TDigestMut::deserialize(bytes).unwrap());
        }
        final_quantile(merged)
    });
}

fn serialized_owned(bencher: Bencher, shape: (usize, usize)) {
    let (num_partials, rows_per_partial) = shape;
    let partials = serialize_partials(&values(num_partials * rows_per_partial), rows_per_partial);

    bencher.bench_local(|| {
        let merged = black_box(&partials)
            .iter()
            .map(|bytes| TDigestMut::deserialize(bytes))
            .collect::<Result<TDigestMut, _>>()
            .unwrap();
        final_quantile(merged)
    });
}

fn serialized_in_batches(bencher: Bencher, shape: (usize, usize)) {
    let (num_partials, rows_per_partial) = shape;
    let partials = serialize_partials(&values(num_partials * rows_per_partial), rows_per_partial);

    bencher.bench_local(|| {
        let mut merged = TDigestMut::default();
        for batch in black_box(&partials).chunks(16) {
            let batch = batch
                .iter()
                .map(|bytes| TDigestMut::deserialize(bytes))
                .collect::<Result<TDigestMut, _>>()
                .unwrap();
            merged.merge(&batch);
        }
        final_quantile(merged)
    });
}

fn end_to_end_borrowed(bencher: Bencher, rows_per_partial: usize) {
    let inputs = values(64 * rows_per_partial);

    bencher.bench_local(|| {
        let partials = serialize_partials(black_box(&inputs), rows_per_partial);
        let mut merged = TDigestMut::default();
        for bytes in &partials {
            merged.merge(&TDigestMut::deserialize(bytes).unwrap());
        }
        final_quantile(merged)
    });
}

fn end_to_end_owned(bencher: Bencher, rows_per_partial: usize) {
    let inputs = values(64 * rows_per_partial);

    bencher.bench_local(|| {
        let partials = serialize_partials(black_box(&inputs), rows_per_partial);
        let merged = partials
            .iter()
            .map(|bytes| TDigestMut::deserialize(bytes))
            .collect::<Result<TDigestMut, _>>()
            .unwrap();
        final_quantile(merged)
    });
}

fn serialize_partials(inputs: &[f64], rows_per_partial: usize) -> Vec<Vec<u8>> {
    inputs
        .chunks(rows_per_partial)
        .map(|values| build_mut_digest(values).serialize())
        .collect()
}

fn final_quantile(mut merged: TDigestMut) -> Option<f64> {
    // A later aggregation stage decodes the serialized merge result.
    let bytes = merged.serialize();
    TDigestMut::deserialize(black_box(&bytes))
        .unwrap()
        .quantile(black_box(0.9))
        .unwrap()
}

pub fn register(runner: &mut Runner) {
    let end_to_end_cases = &[(8, 8_000), (64, 3_000)];
    for &(shape, rounds) in SERIALIZED_INPUTS {
        runner.case(
            format!("tdigest::aggregation::serialized_borrowed/{shape:?}"),
            rounds,
            |b| serialized_borrowed(b, shape),
        );
    }
    for &(shape, rounds) in SERIALIZED_INPUTS {
        runner.case(
            format!("tdigest::aggregation::serialized_owned/{shape:?}"),
            rounds,
            |b| serialized_owned(b, shape),
        );
    }
    for &(shape, rounds) in SERIALIZED_INPUTS {
        runner.case(
            format!("tdigest::aggregation::serialized_in_batches/{shape:?}"),
            rounds,
            |b| serialized_in_batches(b, shape),
        );
    }
    for &(rows_per_partial, rounds) in end_to_end_cases {
        runner.case(
            format!("tdigest::aggregation::end_to_end_borrowed/{rows_per_partial:?}"),
            rounds,
            |b| end_to_end_borrowed(b, rows_per_partial),
        );
    }
    for &(rows_per_partial, rounds) in end_to_end_cases {
        runner.case(
            format!("tdigest::aggregation::end_to_end_owned/{rows_per_partial:?}"),
            rounds,
            |b| end_to_end_owned(b, rows_per_partial),
        );
    }
}
