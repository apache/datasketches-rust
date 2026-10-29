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

use datasketches::uddsketch::UddSketch;

pub fn values(n: usize) -> Vec<f64> {
    (0..n)
        .map(|i| (((i * 7919) % n) as f64 / n as f64 * 12.0).exp())
        .collect()
}

pub fn build(values: &[f64]) -> UddSketch {
    let mut sketch = UddSketch::default();
    for &value in values {
        sketch.update(value).unwrap();
    }
    sketch
}
