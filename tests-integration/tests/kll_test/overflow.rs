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

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use datasketches::error::ErrorKind;
use datasketches::kll::KllSketch;

#[test]
fn weight_overflow_preserves_state() {
    let mut one = KllSketch::<i64>::new(8).unwrap();
    one.update(0);
    let mut sketch = one.clone();
    // Doubling and adding one reaches the exact limit through valid public operations.
    for _ in 0..63 {
        sketch.merge(&sketch.clone()).unwrap();
        sketch.update(0);
    }
    assert_eq!(sketch.n(), u64::MAX);
    let before = sketch.serialize();

    assert!(catch_unwind(AssertUnwindSafe(|| sketch.update(1))).is_err());
    assert!(sketch.serialize() == before, "overflow changed the sketch");
    assert_eq!(
        sketch.merge(&one).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert!(sketch.serialize() == before, "overflow changed the sketch");
}
