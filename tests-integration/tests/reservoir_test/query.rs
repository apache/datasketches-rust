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

#[test]
fn estimate_subset_sum_handles_empty_exact_and_sampling_modes() {
    let mut sketch = ReservoirSketch::<i64>::new(32).unwrap();

    let empty_summary = sketch.estimate_subset_sum(|_| true);
    assert_eq!(empty_summary.lower_bound(), 0.0);
    assert_eq!(empty_summary.estimate(), 0.0);
    assert_eq!(empty_summary.upper_bound(), 0.0);
    assert_eq!(empty_summary.total_weight(), 0.0);

    for i in 0..20_i64 {
        sketch.update(i);
    }
    let exact_even = sketch.estimate_subset_sum(|&x| x % 2 == 0);
    assert_eq!(exact_even.lower_bound(), 10.0);
    assert_eq!(exact_even.estimate(), 10.0);
    assert_eq!(exact_even.upper_bound(), 10.0);
    assert_eq!(exact_even.total_weight(), 20.0);

    for i in 20..1_000_i64 {
        sketch.update(i);
    }

    let all_true = sketch.estimate_subset_sum(|_| true);
    assert_eq!(all_true.estimate(), 1_000.0);
    assert_eq!(all_true.upper_bound(), 1_000.0);
    assert!(all_true.lower_bound() < 1_000.0);
    assert_eq!(all_true.total_weight(), 1_000.0);

    let all_false = sketch.estimate_subset_sum(|_| false);
    assert_eq!(all_false.lower_bound(), 0.0);
    assert_eq!(all_false.estimate(), 0.0);
    assert!(all_false.upper_bound() > 0.0);
    assert_eq!(all_false.total_weight(), 1_000.0);

    let half = sketch.estimate_subset_sum(|&x| x < 500);
    assert!(half.lower_bound() <= half.estimate());
    assert!(half.estimate() <= half.upper_bound());
    assert_eq!(half.total_weight(), 1_000.0);
}
