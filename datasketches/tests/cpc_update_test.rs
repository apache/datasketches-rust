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

use datasketches::common::NumStdDev;
use datasketches::cpc::CpcSketch;
use googletest::assert_that;
use googletest::prelude::ge;
use googletest::prelude::le;
use googletest::prelude::near;

const RELATIVE_ERROR_FOR_LG_K_11: f64 = 0.02;

#[test]
fn test_empty() {
    let sketch = CpcSketch::new(11);
    assert!(sketch.is_empty());
    assert_eq!(sketch.estimate(), 0.0);
    assert_eq!(sketch.lower_bound(NumStdDev::One), 0.0);
    assert_eq!(sketch.upper_bound(NumStdDev::One), 0.0);
}

#[test]
fn test_one_value() {
    let mut sketch = CpcSketch::new(11);
    sketch.update(1);
    assert!(!sketch.is_empty());
    assert_eq!(sketch.estimate(), 1.0);
    assert_that!(sketch.estimate(), ge(sketch.lower_bound(NumStdDev::One)));
    assert_that!(sketch.estimate(), le(sketch.upper_bound(NumStdDev::One)));
}

#[test]
fn test_many_values() {
    const N: usize = 10000;
    const N_F64: f64 = N as f64;

    let mut sketch = CpcSketch::new(11);
    for i in 0..N {
        sketch.update(i);
    }
    assert!(!sketch.is_empty());
    assert_that!(
        sketch.estimate(),
        near(N_F64, RELATIVE_ERROR_FOR_LG_K_11 * N_F64)
    );
    assert_that!(sketch.estimate(), ge(sketch.lower_bound(NumStdDev::One)));
    assert_that!(sketch.estimate(), le(sketch.upper_bound(NumStdDev::One)));
}
