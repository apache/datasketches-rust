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

use datasketches::cpc::CpcSketch;
use datasketches::cpc::CpcUnion;
use insta::assert_snapshot;

#[test]
fn display_empty_sketch() {
    let sketch = CpcSketch::new(11).unwrap();

    assert_snapshot!(sketch, @r"
    CPC Sketch Summary:
      flavor            : Empty
      lg k              : 11
      merged            : false
      estimate          : 0
      num coupons       : 0
    ");
}

#[test]
fn display_populated_sketch() {
    let mut sketch = CpcSketch::new(11).unwrap();
    sketch.update("apple");

    let summary = sketch.to_string();
    assert!(summary.contains("flavor            : Sparse\n"));
    assert!(summary.contains("num coupons       : 1\n"));
    assert!(!summary.contains("estimate          : 0\n"));
}

#[test]
fn display_union() {
    let mut sketch = CpcSketch::new(11).unwrap();
    sketch.update("apple");
    let mut union = CpcUnion::new(11).unwrap();
    union.update(&sketch).unwrap();

    assert_snapshot!(union, @r"
    CPC Union Summary:
      lg k              : 11
      state             : Accumulator
      num coupons       : 1
    ");
}
