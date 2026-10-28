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

#[derive(Clone, Debug)]
pub struct Mapping {
    log_gamma: f64,
    log_scale: f64,
}

impl Mapping {
    pub fn new(accuracy: f64) -> Self {
        Self {
            // ln(1 + 2 alpha / (1 - alpha)) avoids cancellation for small alpha.
            log_gamma: (2.0 * accuracy / (1.0 - accuracy)).ln_1p(),
            log_scale: (-accuracy).ln_1p(),
        }
    }

    pub fn index(&self, magnitude: f64) -> i64 {
        (magnitude.ln() / self.log_gamma).ceil() as i64
    }

    pub fn index_bounds(&self, magnitude: f64) -> (i64, i64) {
        let coordinate = magnitude.ln() / self.log_gamma;
        // Deserialization must accept a boundary value assigned to the adjacent
        // bin by another platform's logarithm. Leave room for rounding in ln,
        // the logarithmic base, and division without admitting arbitrary bins.
        let tolerance = 8.0 * f64::EPSILON * coordinate.abs().max(1.0);
        (
            (coordinate - tolerance).ceil() as i64,
            (coordinate + tolerance).ceil() as i64,
        )
    }

    pub fn value(&self, index: i64) -> f64 {
        // Evaluate (1 - alpha) * gamma^index in log space: gamma^index itself
        // may overflow even when the representative is finite.
        (index as f64 * self.log_gamma + self.log_scale)
            .exp()
            .min(f64::MAX)
    }
}
