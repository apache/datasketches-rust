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

use datasketches::codec::SketchBytes;
use datasketches::codec::SketchSlice;
use datasketches::error::Error;
use datasketches::tuple::DefaultUnionPolicy;
use datasketches::tuple::DefaultUpdatePolicy;
use datasketches::tuple::SummaryCombinePolicy;
use datasketches::tuple::SummaryPolicy;
use datasketches::tuple::SummaryUpdatePolicy;
use datasketches::tuple::TupleSketchBuilder;
use datasketches::tuple::TupleSummaryValue;
use datasketches::tuple::TupleUnion;
use datasketches::tuple::TupleUnionBuilder;
use divan::Bencher;
use divan::black_box;

const ROWS: [u64; 4] = [0, 4, 100, 80_000];

fn u64_union(rows: u64) -> TupleUnion<DefaultUnionPolicy<u64>> {
    let mut sketch = TupleSketchBuilder::new(DefaultUpdatePolicy::<u64>::default())
        .build()
        .unwrap();
    for value in 0..rows {
        sketch.update(value, 1);
    }
    let mut union = TupleUnionBuilder::new(DefaultUnionPolicy::<u64>::default())
        .build()
        .unwrap();
    union.update(&sketch).unwrap();
    union
}

#[divan::bench(args = ROWS, consts = [true, false])]
fn serialize_union<const ORDERED: bool>(bencher: Bencher, rows: u64) {
    let union = u64_union(rows);
    bencher.bench_local(|| black_box(&union).serialize(ORDERED));
}

#[divan::bench(args = ROWS, consts = [true, false])]
fn serialize_union_via_sketch<const ORDERED: bool>(bencher: Bencher, rows: u64) {
    let union = u64_union(rows);
    bencher.bench_local(|| black_box(&union).to_sketch(ORDERED).serialize());
}

/// Heap-backed summary holding a few short labels per key.
#[derive(Clone, Default)]
struct Labels(Vec<String>);

impl TupleSummaryValue for Labels {
    fn serialize_size(&self) -> usize {
        4 + self.0.iter().map(|label| 4 + label.len()).sum::<usize>()
    }

    fn serialize_value(&self, bytes: &mut SketchBytes) {
        bytes.write_u32_le(self.0.len() as u32);
        for label in &self.0 {
            bytes.write_u32_le(label.len() as u32);
            bytes.write(label.as_bytes());
        }
    }

    fn deserialize_value(_cursor: &mut SketchSlice<'_>) -> Result<Self, Error> {
        unimplemented!("only serialization is benchmarked")
    }
}

#[derive(Clone, Copy)]
struct LabelsPolicy;

impl SummaryPolicy for LabelsPolicy {
    type Summary = Labels;

    fn create(&self) -> Self::Summary {
        Labels::default()
    }
}

impl SummaryUpdatePolicy<u64> for LabelsPolicy {
    fn update(&self, summary: &mut Self::Summary, value: u64) {
        for i in 0..4 {
            summary.0.push(format!("label-{value:08}-{i}"));
        }
    }
}

impl SummaryCombinePolicy for LabelsPolicy {
    fn combine(&self, summary: &mut Self::Summary, other: &Self::Summary) {
        summary.0.extend(other.0.iter().cloned());
    }
}

fn labels_union(rows: u64) -> TupleUnion<LabelsPolicy> {
    let mut sketch = TupleSketchBuilder::new(LabelsPolicy).build().unwrap();
    for value in 0..rows {
        sketch.update(value, value);
    }
    let mut union = TupleUnionBuilder::new(LabelsPolicy).build().unwrap();
    union.update(&sketch).unwrap();
    union
}

#[divan::bench(args = ROWS, consts = [true, false])]
fn serialize_labels_union<const ORDERED: bool>(bencher: Bencher, rows: u64) {
    let union = labels_union(rows);
    bencher.bench_local(|| black_box(&union).serialize(ORDERED));
}

#[divan::bench(args = ROWS, consts = [true, false])]
fn serialize_labels_union_via_sketch<const ORDERED: bool>(bencher: Bencher, rows: u64) {
    let union = labels_union(rows);
    bencher.bench_local(|| black_box(&union).to_sketch(ORDERED).serialize());
}
