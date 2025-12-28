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

//! Example demonstrating theta sketch usage

use datasketches::theta::ThetaSketch;

fn main() {
    println!("=== Theta Sketch Example ===\n");

    // Example 1: Basic usage
    println!("1. Basic Theta Sketch Usage:");
    let mut sketch = ThetaSketch::builder().set_lg_k(10).build();

    for i in 0..100 {
        sketch.update(format!("item_{}", i));
    }
    sketch.update("duplicatee_item");
    sketch.update("duplicatee_item");

    println!("   Estimate: {:.2}", sketch.get_estimate());
    println!("   Theta: {:.6}", sketch.get_theta());
    println!("   Num retained: {}", sketch.get_num_retained());
    println!();

    // Example 2: Add more data to enter estimation mode
    println!("2. Add more data to enter estimation mode:");
    for i in 0..5000 {
        sketch.update(format!("item_{}", i));
    }
    println!("   Estimate: {:.2}", sketch.get_estimate());
    println!("   Theta: {:.6}", sketch.get_theta());
    println!("   Num retained: {}", sketch.get_num_retained());
    println!();
}
