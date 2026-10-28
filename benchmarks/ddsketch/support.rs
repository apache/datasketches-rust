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

use datasketches::ddsketch::DdSketch;

pub fn values(len: usize) -> Vec<f64> {
    let mut state = 42u64;
    (0..len)
        .map(|_| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let u = ((state >> 11) as f64 + 1.0) / ((1u64 << 53) as f64 + 1.0);
            0.001 / u.sqrt()
        })
        .collect()
}

pub fn build(values: &[f64], limit: u32) -> DdSketch {
    let mut sketch = DdSketch::new(limit, 0.01).unwrap();
    for &value in values {
        sketch.update(value).unwrap();
    }
    sketch
}
