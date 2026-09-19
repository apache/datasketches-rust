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

use datasketches::hll::HllSketch;
use datasketches::hll::HllType;
use datasketches::hll::HllUnion;
use insta::assert_snapshot;

#[test]
fn display_empty_sketch() {
    let sketch = HllSketch::new(12, HllType::Hll8).unwrap();

    assert_snapshot!(sketch, @r"
    HLL Sketch Summary:
      lg config k       : 12
      target type       : Hll8
      current mode      : List
      lower bound       : 0
      estimate          : 0
      upper bound       : 0
    ");
}

#[test]
fn display_populated_sketch() {
    let mut sketch = HllSketch::new(10, HllType::Hll4).unwrap();
    for value in 0..1_000 {
        sketch.update(value);
    }

    let summary = sketch.to_string();
    assert!(summary.contains("target type       : Hll4\n"));
    assert!(summary.contains("current mode      : Hll\n"));
    assert!(!summary.contains("estimate          : 0\n"));
}

#[test]
fn display_union() {
    let mut union = HllUnion::new(12).unwrap();
    union.update_value("apple");

    let summary = union.to_string();
    assert!(summary.starts_with("HLL Union Summary:\n"));
    assert!(summary.contains("lg max k          : 12\n"));
    assert!(summary.contains("lg config k       : 12\n"));
    assert!(!summary.contains("estimate          : 0\n"));
}
