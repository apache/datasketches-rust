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

use datasketches::tdigest::TDigestMut;

#[test]
fn weight_overflow_preserves_buffer_and_extrema() {
    let mut one = TDigestMut::new(20).unwrap();
    one.update(0.0);
    let mut sketch = one.clone();
    for _ in 0..63 {
        sketch.merge(&sketch.clone());
        sketch.update(0.0);
    }
    assert_eq!(sketch.total_weight(), u64::MAX);
    let before = sketch.clone().serialize();

    sketch.update(f64::NAN);
    sketch.merge(&TDigestMut::default());
    assert!(catch_unwind(AssertUnwindSafe(|| sketch.update(1.0))).is_err());
    assert_eq!(sketch.clone().serialize(), before);
    assert!(catch_unwind(AssertUnwindSafe(|| sketch.merge(&one))).is_err());
    assert_eq!(sketch.clone().serialize(), before);

    let mut target = TDigestMut::default();
    target.update(1.0);
    let before = target.clone().serialize();
    assert!(catch_unwind(AssertUnwindSafe(|| target.merge(&sketch))).is_err());
    assert_eq!(target.serialize(), before);
}
