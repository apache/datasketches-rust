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

use datasketches::tdigest::TDigestMut;
use divan::Bencher;
use divan::black_box;
use divan::counter::ItemsCount;

use super::support::build_mut_digest;
use super::support::values;

// (number of partial states, values per state). Each partial samples the same distribution.
// Small shapes keep the combined weight at most k / 2, where no two centroids can merge.
const SERIALIZED_INPUTS: &[(usize, usize)] = &[
    (1, 8),
    (16, 2),
    (64, 1),
    (64, 8),
    (512, 8),
    (64, 64),
    (512, 64),
    (64, 4_096),
];

#[divan::bench(args = SERIALIZED_INPUTS, sample_size = 1)]
fn serialized_borrowed(bencher: Bencher, shape: (usize, usize)) {
    let (num_partials, rows_per_partial) = shape;
    let partials = serialize_partials(&values(num_partials * rows_per_partial), rows_per_partial);

    bencher
        .counter(ItemsCount::new(num_partials * rows_per_partial))
        .bench_local(|| {
            let mut merged = TDigestMut::default();
            for bytes in black_box(&partials) {
                merged.merge(&TDigestMut::deserialize(bytes).unwrap());
            }
            final_quantile(merged)
        });
}

#[divan::bench(args = SERIALIZED_INPUTS, sample_size = 1)]
fn serialized_owned(bencher: Bencher, shape: (usize, usize)) {
    let (num_partials, rows_per_partial) = shape;
    let partials = serialize_partials(&values(num_partials * rows_per_partial), rows_per_partial);

    bencher
        .counter(ItemsCount::new(num_partials * rows_per_partial))
        .bench_local(|| {
            let merged = black_box(&partials)
                .iter()
                .map(|bytes| TDigestMut::deserialize(bytes))
                .collect::<Result<TDigestMut, _>>()
                .unwrap();
            final_quantile(merged)
        });
}

#[divan::bench(args = SERIALIZED_INPUTS, sample_size = 1)]
fn serialized_in_batches(bencher: Bencher, shape: (usize, usize)) {
    let (num_partials, rows_per_partial) = shape;
    let partials = serialize_partials(&values(num_partials * rows_per_partial), rows_per_partial);

    bencher
        .counter(ItemsCount::new(num_partials * rows_per_partial))
        .bench_local(|| {
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

#[divan::bench(args = [8, 64], sample_size = 1)]
fn end_to_end_borrowed(bencher: Bencher, rows_per_partial: usize) {
    let inputs = values(64 * rows_per_partial);

    bencher
        .counter(ItemsCount::new(inputs.len()))
        .bench_local(|| {
            let partials = serialize_partials(black_box(&inputs), rows_per_partial);
            let mut merged = TDigestMut::default();
            for bytes in &partials {
                merged.merge(&TDigestMut::deserialize(bytes).unwrap());
            }
            final_quantile(merged)
        });
}

#[divan::bench(args = [8, 64], sample_size = 1)]
fn end_to_end_owned(bencher: Bencher, rows_per_partial: usize) {
    let inputs = values(64 * rows_per_partial);

    bencher
        .counter(ItemsCount::new(inputs.len()))
        .bench_local(|| {
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
}
