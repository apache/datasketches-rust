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

mod accuracy;
mod merge;
mod update;

use datasketches::uddsketch::UddSketch;

fn build(values: &[f64], limit: u32, alpha: f64) -> UddSketch {
    let mut sketch = UddSketch::new(limit, alpha).unwrap();
    for &value in values {
        sketch.update(value).unwrap();
    }
    sketch
}

fn shuffle<T>(values: &mut [T], mut seed: u64) {
    for i in (1..values.len()).rev() {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        values.swap(i, seed as usize % (i + 1));
    }
}
