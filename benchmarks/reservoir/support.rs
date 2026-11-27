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

use datasketches::reservoir::ReservoirSketch;

pub(super) fn values(len: usize) -> Vec<i64> {
    (0..len as i64).collect()
}

pub(super) fn build_sketch(items: &[i64], k: usize) -> ReservoirSketch<i64> {
    let mut sketch = ReservoirSketch::<i64>::new(k).unwrap();
    for &item in items {
        sketch.update(item);
    }
    sketch
}

pub(super) fn prepared_sketch(len: usize, k: usize) -> ReservoirSketch<i64> {
    build_sketch(&values(len), k)
}
