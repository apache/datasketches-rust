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

mod a_not_b;
mod intersection;
mod jaccard_similarity;
mod sketch;
mod union;

use datasketches::aod::ArrayOfDoublesCombiner;
use datasketches::aod::ArrayOfDoublesSketch;
use datasketches::aod::ArrayOfDoublesSketchBuilder;

#[derive(Debug, Clone, Copy)]
struct SumCombiner;

impl ArrayOfDoublesCombiner for SumCombiner {
    fn combine(&self, existing: &mut [f64], incoming: &[f64]) {
        for (existing, incoming) in existing.iter_mut().zip(incoming) {
            *existing += incoming;
        }
    }
}

fn sketch_with_range(start: u64, count: u64, num_values: u8) -> ArrayOfDoublesSketch {
    let mut sketch = ArrayOfDoublesSketchBuilder::default()
        .num_values(num_values)
        .build()
        .unwrap();
    for key in start..start + count {
        sketch
            .update(key, vec![key as f64; usize::from(num_values)])
            .unwrap();
    }
    sketch
}
