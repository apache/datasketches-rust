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

use crate::error::Error;

/// Combines the value arrays of keys shared by two ArrayOfDoubles sketches during intersection.
pub trait ArrayOfDoublesCombiner {
    /// Combines `incoming` into `existing` in place.
    ///
    /// Both slices have length equal to the intersection operator's configured `num_values`.
    fn combine(&self, existing: &mut [f64], incoming: &[f64]);
}

pub fn validate_num_values(num_values: u8) -> Result<(), Error> {
    if num_values == 0 {
        return Err(Error::invalid_argument(
            "num_values must be greater than 0, got 0",
        ));
    }
    Ok(())
}

pub fn check_num_values(expected: u8, actual: usize, operation: &str) -> Result<(), Error> {
    if actual != usize::from(expected) {
        return Err(Error::invalid_argument(format!(
            "num_values mismatch for {operation}: expected {expected}, got {actual}"
        )));
    }
    Ok(())
}
