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

use datasketches::countmin::CountMinSketch;
use divan::Bencher;
use divan::black_box;

#[divan::bench(args = [0.5, 0.99, 1.0])]
fn u64(bencher: Bencher, factor: f64) {
    let mut sketch = CountMinSketch::<u64>::new(4, 16_384).unwrap();
    for item in 0..128_u64 {
        sketch.update_with_weight(item, (1 << 53) + item);
    }

    bencher
        .with_inputs(|| sketch.clone())
        .bench_local_refs(|sketch| black_box(sketch).decay(black_box(factor)));
}
