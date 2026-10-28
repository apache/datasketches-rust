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

fn assert_relative_close(estimate: f64, exact: f64, accuracy: f64) {
    assert!(estimate.is_finite());
    if exact == 0.0 {
        assert_eq!(estimate, 0.0);
    } else {
        let error = (estimate - exact).abs() / exact.abs();
        assert!(
            error <= accuracy + 2e-12,
            "{estimate} vs {exact}: error {error}, accuracy {accuracy}"
        );
    }
}

fn next_random(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state
}
